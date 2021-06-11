import sqlalchemy
import pandas as pd
from sqlalchemy.orm import sessionmaker
import requests
import json
from datetime import datetime
import datetime
import sqlite3

# 1484811043508
DATABASE_LOCATION = "sqlite:///my_played_tracks.sqlite"
USER_ID = "seo2ihmfhz0y8mxjiqd77ttl2"
TOKEN = "BQBcu15I-aWaJN2cDcJKT_FP1reCWy0NfQMxEVx755zokQkaAEziiOn3TV5X9tqjz8koam9gmsaE9lyS_IYZO_c50564jxYQyV0QCpzEfWRElj3isXQtJ9bZfecideL63LlKEsp_kwaDV63ROcz3NDbd1rVKpCqWwedG"

def check_if_valid_data(df: pd.DataFrame) -> bool:

    if df.empty:
        print("No songs.")
        return False

    # If we got duplicate, the data pipeline should get failed

    if pd.Series(df['played_at']).is_unique:
        pass
    else:
        raise Exception("Primary Key Check is violated.")

    # any null in rows & columns

    if df.isnull().values.any():
        raise Exception("Null Values detected.")


    yesterday = datetime.datetime.now() - datetime.timedelta(days=1)
    yesterday = yesterday.replace(hour=0, minute=0, second=0, microsecond=0)

    timestamps = df["timestamp"].tolist()
    print(timestamps)
    for timestamp in timestamps:
        if datetime.datetime.strptime(timestamp, '%Y-%m-%d') != yesterday:
            pass

    return True

if __name__ == "__main__":

    # Extract part of the ETL process

    headers = {
        "Accept": "application/json",
        "Content-Type": "application/json",
        "Authorization": "Bearer {token}".format(token=TOKEN)
    }

    # Convert time to Unix timestamp in miliseconds
    today = datetime.datetime.now()
    yesterday = today - datetime.timedelta(days=1)
    yesterday_unix_timestamp = int(yesterday.timestamp()) * 1000

    req = requests.get("https://api.spotify.com/v1/me/player/recently-played?after={time}".format(time=yesterday_unix_timestamp), headers = headers)

    data = req.json()

    song_names = []
    artist_names = []
    played_at_list = []
    timestamps = []

    # Extracting only the relevant bits of data from json
    for song in data["items"]:
        song_names.append(song["track"]["name"])
        artist_names.append(song["track"]["album"]["artists"][0]["name"])
        played_at_list.append(song["played_at"])
        timestamps.append(song["played_at"][0:10])

    song_dict = {
        "song_name": song_names,
        "artist_name": artist_names,
        "played_at": played_at_list,
        "timestamp": timestamps
    }

    song_df = pd.DataFrame(song_dict, columns=["song_name", "artist_name", "played_at", "timestamp"])
    # print(song_df)

    if check_if_valid_data(song_df):
        print("Valid Data, proceed to Load Stage.")

    #  load

    engine = sqlalchemy.create_engine(DATABASE_LOCATION)
    conn = sqlite3.connect("my_played_tracks.sqlite")
    cursor = conn.cursor()

    sql_query = """
        CREATE TABLE IF NOT EXISTS my_played_tracks(
            song_name VARCHAR(200),
            artist_name VARCHAR(200),
            played_at VARCHAR(200),
            timestamp VARCHAR(200),
            CONSTRAINT primary_key_constraint PRIMARY KEY (played_at)
        )
    """

    cursor.execute(sql_query)
    print("Opened Database succesfully.")

    try:
        song_df.to_sql("my_played_tracks", engine, index=False, if_exists='append')

    except:
        print("Database already exists.")

    cursor.close()
    print("Closed Database.")