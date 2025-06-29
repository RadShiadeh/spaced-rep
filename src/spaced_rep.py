from datetime import datetime, timedelta
import polars as pl
from typing import Optional
import threading
from concurrent.futures import ThreadPoolExecutor, wait

SEEN_PATH = "./data/seen.csv"
REVISIONS_PATH = "./data/revisions.csv"

def read_data(path: str) -> pl.DataFrame:
    data = pl.read_csv(path)
    
    return data


def write_data(data: pl.DataFrame, path: str) -> None:
    data.write_csv(path)


def load_seen_and_rev():
    with ThreadPoolExecutor() as exc:
        seen_future = exc.submit(read_data, SEEN_PATH)
        rev_future = exc.submit(read_data, REVISIONS_PATH)
        wait([seen_future, rev_future])
        return seen_future.result(), rev_future.result()

def grab_revision_list(date: str = None):
    """
    Prints the list of topics scheduled for revision, defults to today,
    along with their metadata from seen.json
    """
    
    try:
        print_statement = f"\nTopics to revise for date: ({date})"
        if not date:
            date = datetime.now().strftime("%Y-%m-%d")
            print_statement = f"\nTopics to revise for today:"
            
        df_seen, df_rev = load_seen_and_rev()

        print(print_statement)

        if date not in df_rev["date"]:
            print("No revisions scheduled for date parsed.")
            return
        
        topics_list = df_rev.filter(pl.col("date") == date)["topic"]
        for topic in topics_list:
            filtered_seen_df = df_seen.filter(pl.col("topic") == topic)
            if filtered_seen_df.is_empty():
                print(f"⚠️ Warning: {topic} exists in revisions but not in seen.")
            
            row = filtered_seen_df.row(0)
            reviewed_on = row[1]
            url = row[3]

            print(f"🔹 {topic}")
            print(f"    first reviewed: {reviewed_on}")
            print(f"    link/notes:           {url}")
                    
    except Exception as e:
        raise e
    

def update_entry(topic: str, date_to_remove_from: str = None, reset_rate: int = 0):
    """
    update an already existing topic

    Args:
        topic (str): the topic to be updated
        date_to_remove_from (str): must be in yyyy-MM-dd format
        reset_rate (int): reset_rate (int): Level to reset the revision schedule (valid values: 0 to 8 inclusive). 0 means start over.
    """
    
    try:
        if reset_rate not in (0, 1, 2, 3, 4, 5, 6, 7, 8):
            raise ValueError("reset_rate must be between 0 and 8")
        
        if not date_to_remove_from:
            date_to_remove_from = datetime.now().strftime("%Y-%m-%d")

        df_seen, df_rev = load_seen_and_rev()
        
        topic = topic.strip().lower()
        if topic not in df_seen["topic"]:
            raise KeyError(f"topic {topic} does not exist, add a new entry")
        
        seen_write = threading.Thread(target=update_seen_concur, args=(df_seen, topic, reset_rate, date_to_remove_from), name="seen_write_thread")
        seen_write.start()
        
        df_rev = remove_topic_from_revs(df_rev, topic, date_to_remove_from)
        print(f"\nremoved topic: {topic} from revisions list from {date_to_remove_from} onwards")
        
        update_revision(df_rev, topic, date_to_remove_from, reset_rate)
        print(f"\nrevision schedule for topic: {topic} updated starting from {datetime.strptime(date_to_remove_from, '%Y-%m-%d') + timedelta(days=2 ** reset_rate)} with reset rate: {reset_rate}")
        seen_write.join()
    except Exception as e:
        raise e


def update_seen_concur(df_seen: pl.DataFrame, topic: str, reset_rate: int, date_to_remove_from: str):
    topic = topic.strip().lower()
    df_seen = df_seen.with_columns([
        pl.when(pl.col("topic") == topic)
        .then(pl.lit(reset_rate))
        .otherwise(pl.col("reset_idx"))
        .alias("reset_idx"),

        pl.when(pl.col("topic") == topic)
        .then(pl.lit(date_to_remove_from))
        .otherwise(pl.col("date"))
        .alias("date"),
    ])
    
    write_data(df_seen, SEEN_PATH)
    

def remove_topic_from_revs(df: pl.DataFrame, topic: str, date: str):
    """
    remove topic from date onwards in revisions

    Args:
        data (dict): revision data
        topic (str): string topic to be removed from date
        date (str): date key to start looking from
    returns:
        data (dict): revision data
    """
    topic = topic.strip().lower()
    start_date = datetime.strptime(date, "%Y-%m-%d")
    
    df = df.filter(
        ~(
            (pl.col("topic") == topic) &
            (pl.col("date").str.strptime(pl.Date) >= pl.lit(start_date))
        )
    )

    return df

def add_new_topic(topic: str, date: str = None, url: Optional[str] = "not_provided") -> None:
    """
    add new topic to the seen.json, update the revisions log

    Args:
        topic (str): the page/name/topic whatever to review
        date (str): date reviewed, in yyyy-MM-dd format
        url (str): url link to the page/topic to be reviewed
    """
    try:
        topic = topic.strip().lower()
        df_seen, df_rev = load_seen_and_rev()
        
        if topic in df_seen["topic"]:
            print(f"Warning, unexpecxted func calls: topic: {topic} already present in seen df, update the entry instead")
            return

        if not date:
            date = datetime.now()
            date = date.strftime("%Y-%m-%d")
        
        with ThreadPoolExecutor() as exc:
            seen_future_write = exc.submit(add_new_topic_seen_update, df_seen, topic, date, url)
            rev_future_write = exc.submit(update_revision, df_rev, topic, date)
            
            futures = [seen_future_write, rev_future_write]
            wait(futures)
            for f in futures:
                f.result()
        
        print(f"added new topic: {topic}")
    except Exception as e:
        raise e


def add_new_topic_seen_update(df_seen: pl.DataFrame, topic: str, date: str, url: str = "not provided"):
    try:
        topic = topic.strip().lower()
        
        new_row = {"topic": topic, "date": date, "url": url or "", "reset_idx": 0}
        df_seen_new_row = pl.DataFrame([new_row])
        df_seen_new_row = df_seen_new_row.cast(df_seen.schema)
        
        df_seen = pl.concat([df_seen_new_row, df_seen], how="vertical")
        
        write_data(df_seen, SEEN_PATH)
    except Exception as e:
        raise e

def update_revision(df: pl.DataFrame, topic: str, date: str, reset_idx: int = 0):
    """
    add new revision entry to the revisions.json

    Args:
        data (dict): json data
        topic (str): topic being added
        date (str): date it would start at to calculate the revision days
        reset_idx (int): how much to reset
    """
    try:
        if reset_idx not in (0, 1, 2, 3, 4, 5, 6, 7, 8):
            raise ValueError("reset_rate must be between 0 and 8")
        
        topic = topic.strip().lower()
        df = build_space_rep(df, topic, date, reset_idx)
        
        write_data(df, REVISIONS_PATH)
    except Exception as e:
        raise e
    

def build_space_rep(df: pl.DataFrame, topic: str, date: str, reset_rate: int = 0):
    """
    Generate spaced repetition schedule for a topic, starting at a date and rate.

    Args:
        df (pl.DataFrame): The current revision schedule with columns ["date", "topic"].
        topic (str): The topic to build a schedule for.
        date (str): Start date in 'YYYY-MM-DD' format.
        reset_rate (int): Integer from 0 to 8.
                          0 = schedule starts at 2^0 (1 day later),
                          8 = starts 256 days later.

    Returns:
        pl.DataFrame: Updated revision schedule DataFrame with new entries added.
    """
    reset_rate = max(0, min(reset_rate, 8))
    date_start = datetime.strptime(date, "%Y-%m-%d")
    topic = topic.strip().lower()

    new_rows = []
    for i in range(reset_rate, 9):
        curr_day = date_start + timedelta(days=2 ** i)
        key = curr_day.strftime("%Y-%m-%d")
        new_rows.append({"date": key, "topic": topic})
    
    df_new = pl.DataFrame(new_rows)
    df = pl.concat([df, df_new], how="vertical")
    df = df.unique(subset=["date", "topic"])
        
    return df