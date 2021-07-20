from config import QUANDL_KEY, DATABASE_URI

import psycopg2 as pg
from psycopg2.extras import execute_values
import pandas as pd
import numpy as np
import quandl


def update_tickers():
    conn = pg.connect(DATABASE_URI)

    with conn:
        with conn.cursor() as cur:
            cur.execute("SELECT MAX(lastupdated) FROM tickers;")
            date = cur.fetchone()[0]

    tickers = quandl.get_table(
        "SHARADAR/TICKERS", paginate=True, lastupdated={"gt": date}
    )
    # Drop rows with NaN in primary key columns
    tickers = tickers.dropna(subset=["permaticker", "ticker"])
    # Drop duplicate entries
    tickers = tickers.drop_duplicates(["permaticker", "ticker"])
    # Replace isdelisted column with approriate values for Postgres boolean data type
    tickers["isdelisted"] = tickers["isdelisted"].replace({"N": "false", "Y": "true"})
    # Replace NaN with None for psycopg2 type conversion
    tickers = tickers.replace({np.nan: None})
    # Drop table column because we don't need it
    tickers = tickers.drop(columns=["table"])

    print("Tickers read into memory")

    sql = f"INSERT INTO tickers ({', '.join(tickers.columns)}) VALUES %s ON CONFLICT (permaticker) DO UPDATE SET ({', '.join(tickers.columns)}) = ({', '.join('EXCLUDED.' + tickers.columns)})"

    with conn:
        with conn.cursor() as cur:
            execute_values(cur, sql, list(tickers.itertuples(index=False, name=None)))
            print("Tickers table updated.")


def update_prices():
    conn = pg.connect(DATABASE_URI)

    with conn:
        with conn.cursor() as cur:
            cur.execute("SELECT MAX(lastupdated) FROM prices;")
            date = cur.fetchone()[0]

        with conn.cursor() as cur:
            cur.execute("SELECT permaticker, ticker FROM tickers")
            results = cur.fetchall()
            # Get all tickers we have defined in the tickers table
            valid_tickers = pd.DataFrame(results, columns=["permaticker", "ticker"])

    sep = quandl.get_table("SHARADAR/SEP", paginate=True, lastupdated={"gt": date})
    # Get permaticker via merge
    sep = sep.merge(valid_tickers, on="ticker", how="left")
    # Set data frequency
    sep["frequency"] = "DAILY"
    sep = sep.replace({np.nan: None})
    sep = sep.dropna(subset=["ticker"])

    sep_sql = f"INSERT INTO prices ({', '.join(sep.columns)}) VALUES %s ON CONFLICT (ticker, date, frequency) DO UPDATE SET ({', '.join(sep.columns)}) = ({', '.join('EXCLUDED.' + sep.columns)})"

    print("SEP read into memory")

    sfp = quandl.get_table("SHARADAR/SFP", paginate=True, lastupdated={"gt": date})
    sfp = sfp.merge(valid_tickers, on="ticker", how="left")
    sfp["frequency"] = "DAILY"
    sfp = sfp.replace({np.nan: None})
    sfp = sfp.dropna(subset=["ticker"])

    sfp_sql = f"INSERT INTO prices ({', '.join(sfp.columns)}) VALUES %s ON CONFLICT (ticker, date, frequency) DO UPDATE SET ({', '.join(sfp.columns)}) = ({', '.join('EXCLUDED.' + sfp.columns)})"

    print("SFP read into memory")

    with conn:
        with conn.cursor() as cur:
            execute_values(cur, sep_sql, list(sep.itertuples(index=False, name=None)))
            print("SEP written into database.")
            execute_values(cur, sfp_sql, list(sfp.itertuples(index=False, name=None)))
            print("SFP written into database.")


def update_fundamentals():
    quandl.read_key()
    conn = pg.connect(DATABASE_URI)

    with conn:
        with conn.cursor() as cur:
            cur.execute("SELECT MAX(lastupdated) FROM fundamentals;")
            date = cur.fetchone()[0]

        with conn.cursor() as cur:
            cur.execute("SELECT permaticker, ticker FROM tickers")
            results = cur.fetchall()
            # Get all tickers we have defined in the tickers table
            valid_tickers = pd.DataFrame(results, columns=["permaticker", "ticker"])

    sf1 = quandl.get_table("SHARADAR/SF1", paginate=True, lastupdated={"gt": date})
    sf1 = sf1.merge(valid_tickers, on="ticker", how="left")
    sf1 = sf1.replace({np.nan: None})
    sf1 = sf1.dropna(subset=["ticker"])

    sql = f"INSERT INTO fundamentals ({', '.join(sf1.columns)}) VALUES %s ON CONFLICT (ticker, dimension, datekey, reportperiod) DO UPDATE SET ({', '.join(sf1.columns)}) = ({', '.join('EXCLUDED.' + sf1.columns)})"

    print("SF1 read into memory")

    with conn:
        with conn.cursor() as cur:
            execute_values(cur, sql, list(sf1.itertuples(index=False, name=None)))
            print("Fundamentals table updated.")


if __name__ == "__main__":
    update_tickers()
    update_prices()
    update_fundamentals()
