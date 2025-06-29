from datetime import datetime
import os
import polars as pl
from src.spaced_rep import add_new_topic, update_entry, grab_revision_list
from datetime import timedelta
import sys

# Setup test files
SEEN_PATH = "./data/seen.csv"
REVISIONS_PATH = "./data/revisions.csv"

def test_add_new_topic():
    print("\n==== Test: add_new_topic ====")
    topic = "python basics"
    url = "https://example.com/git"
    date = "2024-06-28"
    
    add_new_topic(topic=topic, date=date, url=url)
    
    seen_df = pl.read_csv(SEEN_PATH)
    assert topic in seen_df["topic"].to_list(), "Topic not added to seen.csv"

    rev_df = pl.read_csv(REVISIONS_PATH)
    future_dates = [(datetime.strptime(date, "%Y-%m-%d") + timedelta(days=2 ** i)).strftime("%Y-%m-%d") for i in range(9)]
    rev_dates_for_topic = rev_df.filter(pl.col("topic") == topic)["date"].to_list()
    assert set(future_dates).issubset(set(rev_dates_for_topic)), "Revision schedule not created correctly"

def test_update_existing_topic():
    print("\n==== Test: update_entry ====")
    topic = "python basics"
    date_to_reset = "2024-06-24"
    reset_rate = 1

    update_entry(topic=topic, date_to_remove_from=date_to_reset, reset_rate=reset_rate)

    seen_df = pl.read_csv(SEEN_PATH)
    updated_row = seen_df.filter(pl.col("topic") == topic).row(0)
    assert updated_row[1] == date_to_reset, "Seen date not updated"
    assert updated_row[3] == reset_rate, "Reset index not updated"

    rev_df = pl.read_csv(REVISIONS_PATH)
    expected_new_date = (datetime.strptime(date_to_reset, "%Y-%m-%d") + timedelta(days=2 ** reset_rate)).strftime("%Y-%m-%d")
    assert expected_new_date in rev_df["date"].to_list(), "New revision entry missing"

def test_grab_revision_list():
    print("\n==== Test: grab_revision_list ====")
    today = datetime.now().strftime("%Y-%m-%d")
    add_new_topic("test topic", date=today, url="http://example.com/test")
    print("Expect to see 'test topic' printed with today's date:")
    grab_revision_list()


def run_tests():
    print("🧪 Running spaced repetition tests...")

    # Run tests
    test_add_new_topic()
    test_update_existing_topic()
    test_grab_revision_list()
    
    print("\n✅ All manual tests completed.\n")


if __name__ == "__main__":
    #run_tests()
    add_new_topic("concurrency_multi_threading_python", "2025-06-29")
    add_new_topic("leetcode_two_sum", "2025-06-29", "https://leetcode.com/problems/two-sum/description/")
    
    add_new_topic("concurrency_multi_threading_python", "2025-06-29") # should throw warning
    
    grab_revision_list()
    print("\n----------------------------------------------")
    grab_revision_list("2025-06-30")