from src.spaced_rep import add_new_topic, update_entry, grab_todays_revision_list

def main():
    # add new
    add_new_topic("spaced_rep")
    
    # grab todays topics
    grab_todays_revision_list()
    
    # update an entry
    update_entry("spaced_rep", "2025-06-30", 2)

if __name__ == "__main__":
    main()