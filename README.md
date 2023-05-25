# 4chan_scrape

Python script(s) used for scraping a 4chan board.

Notably, the script(s) were written for use on an M1 mac/ARM, which can be very finicky when it comes to supporting a number of python packages. It utilizes SQLAlchemy has psycopg2 as a dependency, which may need to be installed using the binary (psycopg2-binary).

4chan_scrape.py scrapes data from the 4chan JSON API and inserts it into a PostgreSQL database, while also cleaning the text and removing empty replies.
    
It performs web scraping of the 4chan /pol/ board using the 4chan JSON API, extracts relevant information from the threads and replies, cleans the text, and inserts the data into a PostgreSQL database. It also prints and writes to a text file the running total number of unique threads and replies in the database.

Additionally, it uses ThreadPoolExecutor to more efficiently make use of CPU cores in the execution of these tasks.


Packages:
- SQLAlchemy
- requests
- json
- time
- re
- ThreadPoolExecutor
    
I used this to scrape data from the /pol/ board for research focused on extremism in unmoderated social media, but it can be used to scrape any board.
