from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker, scoped_session
from sqlalchemy import text
import requests
import json
import time
import re
from datetime import datetime
from concurrent.futures import ThreadPoolExecutor

start_time = time.time()

# PostgreSQL connection details (need to be filled in)
host="[host]"
port=5432 # typical PostgreSQL port
user="[user]"
password="[password]"
database="[database name]"

# Use try-except block for database connection
try:
    db_uri = f"postgresql://{user}:{password}@{host}:{port}/{database}"
    engine = create_engine(db_uri)
    Session = scoped_session(sessionmaker(bind=engine))
except Exception as e:
    print("Error connecting to database: ", e)

# URL for 4chan JSON API
url = "https://a.4cdn.org/[board]/catalog.json"

# Define a function to remove HTML tags and other unwanted characters from a string
def clean_text(text):
    text = re.sub(r'<[^>]*>', '', text) # remove HTML tags
    text = re.sub(r'&gt;&gt;\d{9}', '', text) # remove &gt;&gt; followed by 9 digits
    text = re.sub(r'&gt;', '>', text)   # replace &gt; with >
    text = re.sub(r'&#039;', '', text) # remove &#xxx;
    return text

# Get current timestamp for database entry at the start of the script
now = datetime.now().strftime('%Y-%m-%d %H:%M:%S')

def process_thread(thread):
    session = Session()
    # Extract relevant variables
    thread_no = thread['no']
    thread_now = thread['now']
    thread_name = thread.get('name', None)
    thread_sub = thread.get('sub', None)
    thread_com = clean_text(thread.get('com', ''))
    thread_country_name = thread.get('country', None)

    # Variables to store data to be inserted
    thread_data = []
    reply_data = []

    # Check if thread already exists in database
    result = session.execute(text("SELECT id FROM threads WHERE no = :no"), {"no": thread_no}).first()

    if not result:
        # Store new thread data
        thread_data.append({"no": thread_no, "now": thread_now, "name": thread_name, "sub": thread_sub, "com": thread_com, "country_name": thread_country_name, "scrape_time": now})

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
        result = session.execute(text("SELECT id FROM replies WHERE thread_no = :thread_no AND reply_no = :reply_no"), {"thread_no": thread_no, "reply_no": reply_no}).first()

        if not result:
            # Store new reply data
            reply_data.append({"thread_no": thread_no, "reply_no": reply_no, "now": reply_now, "name": reply_name, "thread_sub": reply_sub, "com": reply_com, "country_name": reply_country_name, "scrape_time": now})

    if thread_data:
        session.execute(text("INSERT INTO threads (no, now, name, sub, com, country_name, scrape_time) VALUES (:no, :now, :name, :sub, :com, :country_name, :scrape_time)"), thread_data)
    if reply_data:
        session.execute(text("INSERT INTO replies (thread_no, reply_no, now, name, thread_sub, com, country_name, scrape_time) VALUES (:thread_no, :reply_no, :now, :name, :thread_sub, :com, :country_name, :scrape_time)"), reply_data)

    session.commit()
    Session.remove()

# Send request to 4chan JSON API and parse response
response = requests.get(url)
json_data = response.json()

# Loop through all threads on /pol/ board
with ThreadPoolExecutor(max_workers=8) as executor:
    for page in json_data:
        for thread in page['threads']:
            executor.submit(process_thread, thread)

session = Session()
# Count the number of unique replies and threads added in the current run
unique_thread_count = session.execute(text("SELECT COUNT(DISTINCT no) FROM threads WHERE scrape_time = :now"), {"now": now}).scalar()
unique_reply_count = session.execute(text("SELECT COUNT(DISTINCT reply_no) FROM replies WHERE scrape_time = :now"), {"now": now}).scalar()
session.commit()

# Execute the DELETE statement
delete_query = "DELETE FROM replies WHERE com IS NULL OR com ~ '^[[:space:]]*$'"
session.execute(text(delete_query))
session.commit()

# Count the running total number of threads and replies
threads_total = session.execute(text("SELECT COUNT(*) FROM threads")).scalar()
replies_total = session.execute(text("SELECT COUNT(*) FROM replies")).scalar()
Session.remove()

# Determine length of time it took for script to run
end_time = time.time()
elapsed_time_seconds = end_time - start_time
minutes, seconds = divmod(elapsed_time_seconds, 60)
elapsted_time = ("{} minutes and {:.2f} seconds".format(int(minutes), seconds))

# Define the message you want to print
output_message1 = f"Script completed at {now}."
output_message2 = f"Entered {unique_thread_count} unique thread numbers. Total = {threads_total}."
output_message3 = f"Entered {unique_reply_count} unique reply numbers. Total = {replies_total}."
output_message4 = f"Script completed in {elapsted_time}."

# Open the text file in append mode
with open("/Users/virginialamoureux/Documents/python_scripts/4chan_testscrape_output/4chan_testscrape_output.txt", "a") as file:
    # Write the output message to the file
    file.write("\n" + output_message1 + "\n" + output_message2 + "\n" + output_message3 + "\n" + output_message4 + "\n" + "-----------------------------------------------------" + "\n")

# Print output message
print(output_message1 + "\n" + output_message2 + "\n" + output_message3 + "\n" + output_message4 + "\n")
