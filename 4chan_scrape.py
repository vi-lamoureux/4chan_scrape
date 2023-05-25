from sqlalchemy import create_engine
from sqlalchemy import text
import requests
import json
import time
import re
from datetime import datetime
from concurrent.futures import ThreadPoolExecutor
import psycopg2


start_time = time.time()

# PostgreSQL connection details (need to be filled in)
host="host"
port=5432
user="user"
password="password"
database="database name"

# Use try-except block for database connection
try:
    db_uri = f"postgresql://{user}:{password}@{host}:{port}/{database}"
    engine = create_engine(db_uri)
    conn = engine.connect()
except Exception as e:
    print("Error connecting to database: ", e)


# Prepare INSERT queries for threads and replies
thread_insert_query = text("INSERT INTO threads (no, now, name, sub, com, country_name, scrape_time) VALUES (:no, :now, :name, :sub, :com, :country_name, :scrape_time)")
reply_insert_query = text("INSERT INTO replies (thread_no, reply_no, now, name, thread_sub, com, country_name, scrape_time) VALUES (:thread_no, :reply_no, :now, :name, :thread_sub, :com, :country_name, :scrape_time)")

# Prepare SELECT queries for threads and replies
thread_select_query = text("SELECT id FROM threads WHERE no = :no")
reply_select_query = text("SELECT id FROM replies WHERE thread_no = :thread_no AND reply_no = :reply_no")

# URL for 4chan JSON API (specify board)
url = "https://a.4cdn.org/[board]/catalog.json"

# Use a session object for HTTP requests
session = requests.Session()

# Use try-except block for API request
try:
    # Send request to 4chan JSON API and parse response
    response = session.get(url)
    json_data = response.json()
except Exception as e:
    print("Error connecting to API: ", e)


# Define a function to remove HTML tags and other unwanted characters from a string
def clean_text(text):
    text = re.sub(r'<[^>]*>', '', text) # remove HTML tags
    text = re.sub(r'&gt;&gt;\d{9}', '', text) # remove &gt;&gt; followed by 9 digits
    text = re.sub(r'&gt;', '>', text)   # replace &gt; with >
    text = re.sub(r'&#039;', '', text) # remove &#xxx;
    return text

# Set to store thread numbers and replies
thread_nos = set()
reply_nos = set()

# Get current timestamp for database entry
now = datetime.now().strftime('%Y-%m-%d %H:%M:%S')

def process_thread(thread):
    # Extract relevant variables
    thread_no = thread['no']
    thread_now = thread['now']
    thread_name = thread.get('name', None)
    thread_sub = thread.get('sub', None)
    thread_com = clean_text(thread.get('com', ''))
    thread_country_name = thread.get('country', None)

    # Check if thread already exists in database
    result = conn.execute(thread_select_query, {"no": thread_no}).first()

    if not result:
        # Insert new thread into database
        conn.execute(thread_insert_query, {"no": thread_no, "now": thread_now, "name": thread_name, "sub": thread_sub, "com": thread_com, "country_name": thread_country_name, "scrape_time": now})

    # Add thread_no to set
    thread_nos.add(thread_no)

    # Extract the last 5 replies for the thread
    last_replies = thread.get('last_replies', [])
    for reply in last_replies:
        reply_no = reply['no']
        reply_now = reply['now']
        reply_name = reply.get('name', None)
        reply_sub = thread_sub
        reply_com = clean_text(reply.get('com', ''))
        reply_country_name = reply.get('country', None)

        # Check if reply already exists in database
        result = conn.execute(reply_select_query, {"thread_no": thread_no, "reply_no": reply_no}).first()

        if not result:
            # Insert new reply into database
            conn.execute(reply_insert_query, {"thread_no": thread_no, "reply_no": reply_no, "now": reply_now, "name": reply_name, "thread_sub": reply_sub, "com": reply_com, "country_name": reply_country_name, "scrape_time": now})

        # Add reply_no to set
        reply_nos.add(reply_no)


# Loop through all threads on /pol/ board
with ThreadPoolExecutor(max_workers=8) as executor:
    for page in json_data:
        for thread in page['threads']:
            executor.submit(process_thread, thread)

# Count the number of unique replies and threads added in the current run
unique_thread_count = conn.execute(text("SELECT COUNT(DISTINCT no) FROM threads WHERE scrape_time = :now"), {"now": now}).scalar()
unique_reply_count = conn.execute(text("SELECT COUNT(DISTINCT reply_no) FROM replies WHERE scrape_time = :now"), {"now": now}).scalar()
conn.commit()

# Execute the DELETE statement
delete_query = "DELETE FROM replies WHERE com IS NULL OR com ~ '^[[:space:]]*$'"
conn.execute(text(delete_query))
conn.commit()

# Count the running total number of threads and replies
threads_total = conn.execute(text("SELECT COUNT(*) FROM threads")).scalar()
replies_total = conn.execute(text("SELECT COUNT(*) FROM replies")).scalar()

# Close the database connection
conn.close()

# Determine length of time it took for script to run
end_time = time.time()

elapsed_time_seconds = end_time - start_time
minutes, seconds = divmod(elapsed_time_seconds, 60)
elapsed_time = ("{} minutes and {:.2f} seconds".format(int(minutes), seconds))

# Define the message you want to print
output_message1 = f"Script completed at {now}."
output_message2 = f"Entered {unique_thread_count} unique thread numbers. Total = {threads_total}."
output_message3 = f"Entered {unique_reply_count} unique reply numbers. Total = {replies_total}."
output_message4 = f"Script completed in {elapsed_time}."

# Open the text file in append mode
with open("/Users/virginialamoureux/Documents/python_scripts/4chan_testscrape_output/4chan_testscrape_output.txt", "a") as file:
    # Write the output message to the file
    file.write("\n" + output_message1 + "\n" + output_message2 + "\n" + output_message3 + "\n" + output_message4 + "\n" + "-----------------------------------------------------" + "\n")

# Print output message
print(output_message1 + "\n" + output_message2 + "\n" + output_message3 + "\n" + output_message4 + "\n")
