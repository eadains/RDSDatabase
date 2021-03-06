import psycopg2 as pg
from psycopg2.extras import execute_values
import numpy as np
import pandas as pd
import quandl
import zipfile

from config import QUANDL_KEY, DATABASE_URI

quandl.ApiConfig.api_key = QUANDL_KEY


def get_csv_zip(filepath):
    """
    Gets CSV file from inside zipfile specified by filepath string
    """
    file = zipfile.ZipFile(filepath)
    csv_name = file.namelist()[0]
    frame = pd.read_csv(file.open(csv_name))
    return frame


def get_tickers(conn):
    with conn:
        with conn.cursor() as cur:
            cur.execute("SELECT MAX(lastupdated) FROM tickers;")
            date = cur.fetchone()[0]

    tickers = quandl.get_table(
        "SHARADAR/TICKERS", paginate=True, lastupdated={"gte": date}
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

    return tickers


def get_prices(conn):
    with conn:
        with conn.cursor() as cur:
            cur.execute("SELECT MAX(lastupdated) FROM prices WHERE frequency='DAILY'")
            date = cur.fetchone()[0]

    quandl.export_table("SHARADAR/SEP", lastupdated={"gte": date}, filename="sep.zip")
    sep = get_csv_zip("sep.zip")
    # Set data frequency
    sep["frequency"] = "DAILY"
    sep = sep.replace({np.nan: None})
    sep = sep.dropna(subset=["ticker"])

    quandl.export_table("SHARADAR/SFP", lastupdated={"gte": date}, filename="sfp.zip")
    sfp = get_csv_zip("sfp.zip")
    sfp["frequency"] = "DAILY"
    sfp = sfp.replace({np.nan: None})
    sfp = sfp.dropna(subset=["ticker"])

    return sep, sfp


def get_fundamentals(conn):
    with conn:
        with conn.cursor() as cur:
            cur.execute("SELECT MAX(lastupdated) FROM fundamentals;")
            date = cur.fetchone()[0]

    quandl.export_table("SHARADAR/SF1", lastupdated={"gte": date}, filename="sf1.zip")
    sf1 = get_csv_zip("sf1.zip")
    sf1 = sf1.replace({np.nan: None})
    sf1 = sf1.dropna(subset=["ticker"])

    return sf1


def update_database():
    conn = pg.connect(DATABASE_URI)
    tickers = get_tickers(conn)
    print("Tickers read into memory.")
    sep, sfp = get_prices(conn)
    print("Prices read into memory.")
    sf1 = get_fundamentals(conn)
    print("Fundamentals read into memory.")

    tickers_sql = f"INSERT INTO tickers ({', '.join(tickers.columns)}) VALUES %s ON CONFLICT (permaticker) DO UPDATE SET ({', '.join(tickers.columns)}) = ({', '.join('EXCLUDED.' + tickers.columns)})"
    sep_sql = f"INSERT INTO prices ({', '.join(sep.columns)}) VALUES %s ON CONFLICT (ticker, date, frequency) DO UPDATE SET ({', '.join(sep.columns)}) = ({', '.join('EXCLUDED.' + sep.columns)})"
    sfp_sql = f"INSERT INTO prices ({', '.join(sfp.columns)}) VALUES %s ON CONFLICT (ticker, date, frequency) DO UPDATE SET ({', '.join(sfp.columns)}) = ({', '.join('EXCLUDED.' + sfp.columns)})"
    sf1_sql = f"INSERT INTO fundamentals ({', '.join(sf1.columns)}) VALUES %s ON CONFLICT (ticker, dimension, datekey, reportperiod) DO UPDATE SET ({', '.join(sf1.columns)}) = ({', '.join('EXCLUDED.' + sf1.columns)})"

    with conn:
        with conn.cursor() as cur:
            execute_values(
                cur, tickers_sql, list(tickers.itertuples(index=False, name=None))
            )
            print("Tickers table updated.")
            execute_values(cur, sep_sql, list(sep.itertuples(index=False, name=None)))
            print("SEP written into database.")
            execute_values(cur, sfp_sql, list(sfp.itertuples(index=False, name=None)))
            print("SFP written into database.")
            execute_values(cur, sf1_sql, list(sf1.itertuples(index=False, name=None)))
            print("Fundamentals table updated.")

    print("Transaction successfully committed.")
    conn.close()


if __name__ == "__main__":
    update_database()
