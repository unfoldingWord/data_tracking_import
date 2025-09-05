from sqlalchemy import create_engine
from sqlalchemy.exc import SQLAlchemyError
import os
import datetime as dt
from datetime import timedelta
import pandas as pd
from dotenv import load_dotenv
from eventregistry import (
    EventRegistry,
    QueryArticlesIter,
    ReturnInfo,
    ArticleInfoFlags,
    SourceInfoFlags,
)
from functions import get_logger

load_dotenv()

# -----------------------------
# Config
# -----------------------------
DAYS_BACK = 31             # <-- last N days
MAX_ITEMS_30D = None        # safety cap (None = no cap; or set an int like 2000)

DATA_TYPES = ["news", "pr", "blog"]
KEYWORDS_EXACT = os.getenv('PR_KEYWORDS')

mylogger = get_logger()

def last_n_days_bounds(n: int) -> tuple[str, str]:
    today = dt.date.today()
    start = (today - timedelta(days=n)).isoformat()
    end = today.isoformat()
    return start, end

def fetch_last_n_days(er: EventRegistry, n: int, max_items: int | None) -> list[dict]:
    dateStart, dateEnd = last_n_days_bounds(n)

    q = QueryArticlesIter(
        keywords=KEYWORDS_EXACT,
        keywordsLoc="body,title",
        keywordSearchMode="exact",
        dateStart=dateStart,
        dateEnd=dateEnd,
        dataType=DATA_TYPES,
    )

    ret = ReturnInfo(
        articleInfo=ArticleInfoFlags(
            body=True,
            authors=True,
            links=True,
            extractedDates=True
        ),
        sourceInfo=SourceInfoFlags(
            title=True,
            description=True
        )
    )

    cap = max_items if (isinstance(max_items, int) and max_items > 0) else 10**9
    rows = []
    for art in q.execQuery(er, sortBy="date", maxItems=cap, returnInfo=ret):
        rows.append(art)
    return rows

def normalize_articles(rows: list[dict]) -> pd.DataFrame:
    if not rows:
        return pd.DataFrame()

    df = pd.json_normalize(rows, sep=".")
    if "authors" in df.columns:
        df["authors_names"] = df["authors"].apply(
            lambda xs: ", ".join(a.get("name","") for a in (xs or []) if isinstance(xs, list) and isinstance(a, dict)) or None
            if isinstance(xs, list) else None
        )
    # convenient column order for review
    keep = [
        "uri","url","title","body","eventUri","dataType","lang",
        "date","time","dateTime","dateTimePub",
        "source.uri","source.title","source.description",
        "links","extractedDates","authors_names"
    ]
    keep = [c for c in keep if c in df.columns] + [c for c in df.columns if c not in keep]
    return df[keep]

def main() -> pd.DataFrame:
    API_KEY = (os.getenv("NEWSAPI_KEY") or "").strip()
    if not API_KEY:
        raise SystemExit("Set NEWSAPI_KEY in your .env")

    er = EventRegistry(apiKey=API_KEY, host="https://eventregistry.org", allowUseOfArchive=True)

    mylogger.debug(f"Fetching last {DAYS_BACK} days …")
    rows = fetch_last_n_days(er, DAYS_BACK, MAX_ITEMS_30D)

    # de-dup by uri
    seen, all_rows = set(), []
    added = 0
    for r in rows:
        u = r.get("uri")
        if u and u not in seen:
            seen.add(u)
            all_rows.append(r)
            added += 1
    mylogger.debug(f"  got {len(rows)} (added {added} unique)")

    df = normalize_articles(all_rows)

    mylogger.info(f"\nTotal unique articles in last {DAYS_BACK} days: {len(df)}")

    # Quick “which sources hit” summary
    if not df.empty:
        src_cols = [c for c in ["source.uri", "source.title", "dataType"] if c in df.columns]
        if src_cols:
            hits = (
                df.groupby(src_cols, dropna=False)
                  .size()
                  .reset_index(name="article_count")
                  .sort_values("article_count", ascending=False)
            )
            #print("\nTop sources (last 30 days):")
            with pd.option_context("display.max_colwidth", 100):
                mylogger.debug(hits.head(25).to_string(index=False))
        # small preview
        with pd.option_context("display.max_colwidth", 120):
            mylogger.debug("\nSample rows:")
            mylogger.debug(df.head(10).to_string(index=False))

    return df

if __name__ == "__main__":
    df = main()

    # Safely drop optional columns only if present
    drop_maybe = [
        'eventUri','time','dateTime','dateTimePub','links','extractedDates',
        'sim','image','sentiment','wgt','relevance'
    ]
    drop_cols = [c for c in drop_maybe if c in df.columns]
    db_ready = df.drop(columns=drop_cols) if drop_cols else df.copy()

    mylogger.debug("Loading internal data tables into pipeline.")

    # Initialize engine object to None
    engine = None

    try:
        # Get database credentials from environment variables
        db_user = os.getenv("TDB_USER")
        db_password = os.getenv("TDB_PASSWORD")
        db_name = os.getenv("TDB_DB")
        db_host = os.getenv("TDB_HOST")

        # Construct the database URI for SQLAlchemy using the MariaDB dialect
#        db_uri = f"mariadb+mariadbconnector://{db_user}:{db_password}@{db_host}/{db_name}"
        db_uri = f"mysql+pymysql://{db_user}:{db_password}@{db_host}/{db_name}?charset=utf8mb4"

        # Create a SQLAlchemy engine
        engine = create_engine(db_uri, pool_pre_ping=True, pool_recycle=3600)

        # Test the connection by trying to connect
        with engine.connect() as connection:
            mylogger.debug("Successfully connected to the MariaDB database using SQLAlchemy with mysql+pymysql dialect")

        # Ingest
        if not db_ready.empty:
            with engine.begin() as conn:
                db_ready.to_sql(
                    name="positive_pr",
                    con=conn,
                    if_exists="append",  # 'append' keeps adding the latest 30-day finds
                    index=False,
                    chunksize=10_000,
                    method="multi"
                )

            mylogger.info(f"Successfully imported {len(db_ready):,} rows to positive_pr table.")
        else:
            mylogger.info("No rows to import (db_ready is empty)")

    except SQLAlchemyError as err:
        mylogger.error(f"Database Error: {err}")
    except Exception as e:
        mylogger.exception(f"An unexpected error occurred: {e}")
    finally:
        if engine:
            engine.dispose()
            mylogger.debug("SQLAlchemy engine connections disposed.")
