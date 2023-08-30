# Import required modules
import re
from os import environ
from sqlalchemy import create_engine, Column, Integer, String, Float, ForeignKey
from sqlalchemy.orm import sessionmaker, scoped_session, relationship, declarative_base
from nltk.sentiment.vader import SentimentIntensityAnalyzer
from tqdm import tqdm

# Define regular expression to identify URLs for text cleaning
url_pattern = re.compile(r'http[s]?://(?:[a-zA-Z]|[0-9]|[$-_@.&+]|[!*\\(\\),]|(?:%[0-9a-fA-F][0-9a-fA-F]))+')


# Function to clean text, removing URLs and other unwanted characters
def clean_text(text):
    # Remove URLs
    cleaned_text = re.sub(url_pattern, '', text)
    # Remove HTML tags
    cleaned_text = re.sub(r'<[^>]*>', '', cleaned_text)
    # Remove special characters often used for quoted replies or threads
    cleaned_text = re.sub(r'&gt;&gt;\d{9}', '', cleaned_text)
    # Replace HTML encoded characters
    cleaned_text = re.sub(r'&gt;', '>', cleaned_text)
    cleaned_text = re.sub(r'&amp;', '&', cleaned_text)
    cleaned_text = re.sub(r'&#039;', '', cleaned_text)
    cleaned_text = re.sub(r'&quot;', '', cleaned_text)
    return cleaned_text


# Initialize database connection
def init_db(user, password, host, port, database):
    try:
        # Create PostgreSQL connection string
        db_url = f"postgresql://{user}:{password}@{host}:{port}/{database}"
        # Initialize SQLAlchemy engine
        engine = create_engine(db_url)
        # Return session object
        return scoped_session(sessionmaker(bind=engine))
    except Exception as e:
        print("Error connecting to database: ", e)
        raise


# Define ORM models for database tables
def init_table_models(Base):
    class Replies(Base):
        __tablename__ = 'replies'
        id = Column('id', Integer, primary_key=True, autoincrement=True)
        com = Column('com', String, nullable=False)

    class VaderAnalysis(Base):
        __tablename__ = 'vader_analysis'
        id = Column('id', Integer, ForeignKey('replies.id'), primary_key=True, unique=True)
        sentiment_score = Column('sentiment_score', Float)
        com = Column('com', String)
        reply = relationship("Replies")

    return Replies, VaderAnalysis


# Function to batch-insert sentiment analysis results into database
def batch_insert_sentiment(session, Replies, VaderAnalysis, sia, batch_size=1000):
    offset = 0
    # Count the total number of rows in the 'Replies' table
    total_rows = session.query(Replies).count()

    # Loop through the batches of records
    for _ in tqdm(range(0, total_rows, batch_size), desc="Processing"):
        batch = session.query(Replies).offset(offset).limit(batch_size).all()
        if not batch:
            break

        insert_data = []
        # Retrieve existing IDs to avoid duplicates
        existing_ids = {x[0] for x in
                        session.query(VaderAnalysis.id).filter(VaderAnalysis.id.in_([row.id for row in batch])).all()}

        # Perform sentiment analysis and prepare data for insert
        for row in batch:
            if row.id not in existing_ids:
                cleaned_text = clean_text(row.com)
                polarity_score = sia.polarity_scores(cleaned_text)
                compound_score = polarity_score['compound']
                insert_data.append({
                    'id': row.id,
                    'sentiment_score': compound_score,
                    'com': cleaned_text})

        # Perform the actual database insert
        if insert_data:
            session.bulk_insert_mappings(VaderAnalysis, insert_data)
            session.commit()

        offset += batch_size


# Main execution block
if __name__ == "__main__":
    # Initialize VADER sentiment analyzer
    sia = SentimentIntensityAnalyzer()

    # Get environment variables for PostgreSQL connection
    host = environ.get("DB_HOST", "localhost")
    port = environ.get("DB_PORT", 5432)
    user = environ.get("DB_USER", "user_placeholder")
    password = environ.get("DB_PASSWORD", "password_placeholder")
    database = environ.get("DB_NAME", "db_placeholder")

    # Initialize the database session
    Session = init_db(user, password, host, port, database)

    # Initialize SQLAlchemy base class and table models
    Base = declarative_base()
    Replies, VaderAnalysis = init_table_models(Base)

    # Perform batch sentiment analysis and insert results into database
    with Session() as session:
        batch_insert_sentiment(session, Replies, VaderAnalysis, sia)
