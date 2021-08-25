import psycopg2 as pg
from psycopg2.extras import execute_values
import pandas as pd
import requests
import zipfile
import io

from config import DATABASE_URI, FIRSTRATE_URL


def update_firstrate():
    conn = pg.connect(DATABASE_URI)
    with conn:
        with conn.cursor() as cur:
            cur.execute(
                "SELECT MAX(date) FROM prices WHERE (frequency='MINUTE' AND ticker='^GSPC');"
            )
            date = cur.fetchall()[0][0]

    r = requests.get(FIRSTRATE_URL, stream=True)
    file = io.BytesIO(r.raw.read())
    file = zipfile.ZipFile(file)

    spx = pd.read_csv(
        file.open("SPX_1min.txt"),
        names=["date", "open", "high", "low", "close"],
        parse_dates=["date"],
    )
    # Drop potential date duplicates. Data quality is questionable
    spx = spx.drop_duplicates(subset=["date"])
    # Get only new records for insertion
    spx = spx[spx["date"] > date]
    # Firstrate includes a column thats always zero for some reason
    spx = spx.drop(columns=["?"])
    # Set database columns
    spx["frequency"] = "MINUTE"
    spx["ticker"] = "^GSPC"

    sql = f"""INSERT INTO prices ({", ".join(spx.columns)}) VALUES %s ON CONFLICT (ticker, date, frequency) DO UPDATE SET ({", ".join(spx.columns)}) = ({", ".join("EXCLUDED." + spx.columns)})"""

    with pg.connect(DATABASE_URI) as conn:
        with conn.cursor() as cur:
            execute_values(cur, sql, list(spx.itertuples(index=False, name=None)))


if __name__ == "__main__":
    update_firstrate()
