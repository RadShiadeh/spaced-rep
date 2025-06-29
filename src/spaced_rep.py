from datetime import datetime, timedelta
import json
from typing import Optional
import threading
from concurrent.futures import ThreadPoolExecutor, wait

SEEN_PATH = "./data/seen.json"
REVISIONS_PATH = "./data/revisions.json"

def read_data(path: str):
    data = None
    with open(path, 'r') as jd:
        data = json.load(jd)
        jd.close()
    
    return data


def write_data(data, path: str):
    with open(path, 'w') as f:
        json.dump(data, f, indent=4)


def grab_todays_revision_list():
    """
    Prints the list of topics scheduled for revision today,
    along with their metadata from seen.json
    """
    date = datetime.now().strftime("%Y-%m-%d")
    data_rev = read_data(REVISIONS_PATH)
    data_seen = read_data(SEEN_PATH)

    print(f"\nTopics to revise for today ({date}):")

    if date not in data_rev or not data_rev[date]:
        print("No revisions scheduled for today.")
        return

    for topic in data_rev[date]:
        if topic in data_seen:
            reviewed_on, _, url = data_seen[topic]
            print(f"🔹 {topic}")
            print(f"    Last reviewed: {reviewed_on}")
            if url:
                print(f"    URL:           {url}")
        else:
            print(f" Warning: {topic} exists in revisions but not in seen.json.")        


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

        rev_data = None
        data_seen = None
        with ThreadPoolExecutor() as exc:
            future_rev = exc.submit(read_data, REVISIONS_PATH)
            future_seen = exc.submit(read_data, SEEN_PATH)
            
            futures = [future_seen, future_rev]
            wait(futures)
            
            data_seen = future_seen.result()
            rev_data = future_rev.result()
        
        topic = topic.strip().lower()
        if topic not in data_seen:
            raise KeyError(f"topic {topic} does not exist, add a new entry")
        
        data_seen[topic][1] = reset_rate
        data_seen[topic][0] = date_to_remove_from
        
        seen_write = threading.Thread(target=write_data, args=(data_seen, SEEN_PATH), name="seen_write_thread")
        seen_write.start()
        
        rev_data = remove_topic_from_revs(rev_data, topic, date_to_remove_from)
        print(f"\nremoved topic: {topic} from revisions list from {date_to_remove_from} onwards")
        
        update_revision(rev_data, topic, date_to_remove_from, reset_rate)
        print(f"\nrevision schedule for topic: {topic} updated starting from {datetime.strptime(date_to_remove_from, '%Y-%m-%d') + timedelta(days=2 ** reset_rate)} with reset rate: {reset_rate}")
        seen_write.join()
    except Exception:
        raise
    

def remove_topic_from_revs(data: dict, topic: str, date: str):
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
    for i in range(512):
        key_date = start_date + timedelta(days=i)
        key_date = key_date.strftime("%Y-%m-%d")
        if key_date in data.keys() and topic in data[key_date]:
            data[key_date].remove(topic)
    
    return data

def add_new_topic(topic: str, date: str = None, url: Optional[str] = None) -> None:
    """
    add new topic to the seen.json, update the revisions log

    Args:
        topic (str): the page/name/topic whatever to review
        date (str): date reviewed, in yyyy-MM-dd format
        url (str): url link to the page/topic to be reviewed
    """
    try:
        topic = topic.strip().lower()
        data = read_data(SEEN_PATH)
        
        if topic in data.keys():
            raise KeyError("topic already present, update the entry instead")

        if not date:
            date = datetime.now()
            date = date.strftime("%Y-%m-%d")
            
        data[topic] = [date, 0, url]
        
        seen_write = threading.Thread(target=write_data, args=(data, SEEN_PATH), name="seen_write_thread")
        update_thread = threading.Thread(target=update_revision, args=(topic, date), name="update_thread")        
        
        seen_write.start()
        update_thread.start()

        seen_write.join()
        update_thread.join()
        
        print(f"added new topic: {topic}")
    except Exception as e:
        raise e


def update_revision(data: dict, topic: str, date: str, reset_idx: int = 0):
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
        data = build_space_rep(data, topic, date, reset_idx)
        write_data(data, REVISIONS_PATH)
    except Exception:
        raise
    

def build_space_rep(data: dict, topic: str, date: str, reset_rate: int = 0):
    """
    Generate spaced repetition schedule for a topic, starting at a date and rate.

    Args:
        data (dict): the revision data
        topic (str): The topic to build a schedule for.
        date (str): Start date in 'YYYY-MM-DD' format.
        reset_rate (int): Integer from 0 to 8. 
                          0 = schedule starts at 2^0 (1 day later), 
                          8 = starts 256 days later.

    Returns:
        dict: Updated JSON-like dictionary of revision dates to topics.
    """
    reset_rate = max(0, min(reset_rate, 8))
    date_start = datetime.strptime(date, "%Y-%m-%d")
    topic = topic.strip().lower()

    for i in range(reset_rate, 9):
        curr_day = date_start + timedelta(days=2 ** i)
        key = curr_day.strftime("%Y-%m-%d")

        if key not in data:
            data[key] = []

        if topic not in data[key]:
            data[key].append(topic)

    return data