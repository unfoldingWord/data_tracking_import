import os
import numpy as np
import pandas as pd
from dotenv import load_dotenv
from sqlalchemy import create_engine, text
from sqlalchemy.exc import SQLAlchemyError

load_dotenv()

def collect_metrics() -> dict:
    """
    Computes and returns FRED summary metrics as a dict for the orchestrator.
    Keys:
      - total_pr
      - open_resources_aquifer
      - distinct_completed_OBS_count
      - total_product_count
      - bible_count_rolled
      - nt_count_rolled
      - ot_count_rolled
      - unique_language_engagement_ids
    """

    # Build DB URI (matches your script’s connector settings)
    db_user = os.getenv("TDB_USER")
    db_password = os.getenv("TDB_PASSWORD")
    db_name = os.getenv("TDB_DB")
    db_host = os.getenv("TDB_HOST")
#    db_uri = f"mariadb+mariadbconnector://{db_user}:{db_password}@{db_host}/{db_name}"
    db_uri = f"mysql+pymysql://{db_user}:{db_password}@{db_host}/{db_name}?charset=utf8mb4"

    engine = None
    try:
        engine = create_engine(db_uri, pool_pre_ping=True, pool_recycle=3600)

        # --- Load the tables you use in your script ---
        master = pd.read_sql("SELECT * FROM master_uw_translation_projects;", engine)
        kr1 = pd.read_sql("SELECT * FROM kr1_progress_data;", engine)

        # COUNT(*) as scalar
        total_pr = int(pd.read_sql("SELECT COUNT(*) AS c FROM positive_pr;", engine)["c"].iloc[0])

        # --- Completed OBS (distinct products) ---
        OBS_only = master[master['resource_package'] == "OBS"]
        completed_OBS = OBS_only[OBS_only['project_status'] == "Completed"]
        distinct_completed_OBS_count = completed_OBS[[
            'language_engagement_id',
            'english_short_name',
            'bible_book_ref',
            'project_status',
            'resource_format'
        ]].drop_duplicates().shape[0]

        # --- Scripture Text “book” vs rolled-up Bible/OT/NT counts ---
        BT_growth = master[master['resource_package'] == "Scripture Text"]
        books_view = BT_growth[
            (~BT_growth['scriptural_association'].isin(['Bible', 'OT', 'NT'])) &
            (BT_growth['project_status'].isin(['Active', 'Inactive', 'Completed']))
        ]
        book_count = int(len(books_view))

        bible_view = BT_growth[
            (BT_growth['scriptural_association'].isin(['Bible', 'OT', 'NT'])) &
            (BT_growth['project_status'].isin(['Active', 'Inactive', 'Completed']))
        ].sort_values(by='primary_anglicized_name').copy()

        conditions = [
            bible_view['scriptural_association'] == 'Bible',
            bible_view['scriptural_association'] == 'OT',
            bible_view['scriptural_association'] == 'NT'
        ]
        choices = [66, 39, 27]
        bible_view['num_books'] = np.select(conditions, choices, default=1)

        bible_count = int(bible_view['num_books'].sum())
        total_product_count = int(bible_count + book_count)

        # Rolled-up counts + unique language engagements
        rolled_up = master[
            (master['resource_package'] == "Scripture Text") &
            (master['project_status'].isin(["Active", "Inactive", "Completed"]))
        ][[
            'language_engagement_id',
            'primary_anglicized_name',
            'subtag_new',
            'scripture_text_name',
            'resource_format',
            'translation_type',
            'project_status',
            'bible_book_ref'
        ]].drop_duplicates()

        rolled_up_counts = rolled_up.groupby('bible_book_ref').size().reset_index(name='Counts')
        book_counts_dict = rolled_up_counts.set_index('bible_book_ref')['Counts'].to_dict()

        bible_count_rolled = int(book_counts_dict.get('BIBLE', 0))
        ot_count_rolled = int(book_counts_dict.get('OT', 0))
        nt_count_rolled = int(book_counts_dict.get('NT', 0))
        unique_language_engagement_ids = int(rolled_up['language_engagement_id'].nunique())

        # Openly licensed resources from Aquifer
        no_na_kr1 = kr1[kr1['resource_name'].notna()]
        open_resources_aquifer = int(len(no_na_kr1))

        return {
            "total_pr_count": total_pr,
            "open_resources_aquifer": open_resources_aquifer,
            "distinct_completed_OBS_count": distinct_completed_OBS_count,
            "total_translated_product": total_product_count,
            "bible_count_rolled": bible_count_rolled,
            "nt_count_rolled": nt_count_rolled,
            "ot_count_rolled": ot_count_rolled,
            "unique_les_w_products": unique_language_engagement_ids,
        }

    except SQLAlchemyError as e:
        return {"status": "error", "error_message": str(e)}
    finally:
        if engine:
            engine.dispose()
