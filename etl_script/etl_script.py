import subprocess
import time
import numpy as np
import pandas as pd
import re
import psycopg2

def wait_for_postgres(host, max_retries=5, delay_seconds=5):
    """Wait for PostgreSQL to become available."""
    retries = 0
    while retries < max_retries:
        try:
            result = subprocess.run(
                ["pg_isready", "-h", host], check=True, capture_output=True, text=True)
            if "accepting connections" in result.stdout:
                print("Successfully connected to PostgreSQL!")
                return True
        except subprocess.CalledProcessError as e:
            print(f"Error connecting to PostgreSQL: {e}")
            retries += 1
            print(
                f"Retrying in {delay_seconds} seconds... (Attempt {retries}/{max_retries})")
            time.sleep(delay_seconds)
    print("Max retries reached. Exiting.")
    return False

# Use the function before running the ELT process
if not wait_for_postgres(host="postgres"):
    exit(1)

print("Starting ETL script...")

def remove_special_chars(text):
    if pd.notna(text):
        text = str(text)
        text = re.sub(r'[^A-Za-z0-9 ]+', '', text)
        return text.strip()

def remove_newline_char(text):
    return re.sub(r'\n', ' ', text)

def remove_leading_trailing_whitespace(text):
    if pd.notna(text):
        return str.strip(text)
    
def remove_tick(text):
    if pd.notna(text):
        return re.sub(r"'", '', text)

def get_year(text):
    if pd.notna(text):
        text = str(text)
        text_list = re.findall(r'\d{4}', text)
        if len(text_list) != 0:
            text = int(text_list[0])
        else:
            text = np.nan
        return text

def clean_genre(text):
    if pd.notna(text):
        text = remove_newline_char(text)
        return text.strip()

def clean_oneline(text):
    return remove_tick(remove_newline_char(text).strip())

def get_directors(text):
    text = remove_newline_char(text)
    match = re.search('Director', text)
    if match:
        _, end = match.span()
        text = text[end:]
        names = re.findall(r'([A-Za-z.,\- ]+)(?=\|)', text)
        # names = str([name.strip() for name in names])
        names = str(names)
        names = re.sub(r"[\[\]']", '', names) 
        return names.strip()

def get_actors(text):
    text = remove_newline_char(text)
    match = re.search('Star', text)
    if match:
        _, end = match.span()
        text = text[end:]
        start = text.find(':')
        text = text[start+1:]
        return text.strip()

def clean_votes(text):
    if pd.notna(text):
        text = remove_special_chars(text)
        return int(text)

def clean_gross(text):
    if pd.notna(text):
        number = float(re.findall(r'\d+.\d+', text)[0])
        is_million = False
        if re.search(r'M', text):
            is_million = True
        if is_million:
            number *= 1e6
        return int(number)

# EXTRACT

df = pd.read_csv('movies.csv')

# TRANSFORM

# general cleaning
df = df.drop_duplicates()

# feature engineering
df['MOVIES_CLEAN'] = df['MOVIES'].apply(remove_tick).apply(remove_leading_trailing_whitespace)
df['YEAR_CLEAN'] = df['YEAR'].apply(get_year)
df['GENRE_CLEAN'] = df['GENRE'].apply(clean_genre)
df['RATING_CLEAN'] = df['RATING'].apply(float)
df['ONE-LINE_CLEAN'] = df['ONE-LINE'].apply(clean_oneline)
df['director_CLEAN'] = df['STARS'].apply(get_directors)
df['actors_CLEAN'] = df['STARS'].apply(get_actors)
df['VOTES_CLEAN'] = df['VOTES'].apply(clean_votes)
df['RunTime_CLEAN'] = df['RunTime'].apply(lambda x: x if x == np.nan else float(x))
df['Gross_CLEAN'] = df['Gross'].apply(clean_gross)

# Table creation
movie = df[['MOVIES_CLEAN', 'YEAR_CLEAN', 'RATING_CLEAN', 'ONE-LINE_CLEAN', 'VOTES_CLEAN', 'RunTime_CLEAN', 'Gross_CLEAN']]
movie.fillna({'Gross_CLEAN': 0}, inplace=True)
movie.fillna('NULL', inplace=True)

movie_genre = df[['MOVIES_CLEAN', 'GENRE_CLEAN']]
movie_genre.loc[:,'GENRE_CLEAN'] = movie_genre.loc[:,'GENRE_CLEAN'].str.split(', ')
movie_genre = movie_genre.explode('GENRE_CLEAN', ignore_index=True)
movie_genre.drop_duplicates(inplace=True)

genre = pd.DataFrame({'name': movie_genre['GENRE_CLEAN'].unique()})

movie_director = df[['MOVIES_CLEAN', 'director_CLEAN']]
movie_director.loc[:,'director_CLEAN'] = movie_director.loc[:,'director_CLEAN'].str.split(',')
movie_director = movie_director.explode('director_CLEAN', ignore_index=True)
movie_director['director_CLEAN'] = movie_director['director_CLEAN'].apply(remove_leading_trailing_whitespace)
movie_director['director_CLEAN'] = movie_director['director_CLEAN'].apply(remove_tick)
movie_director.drop_duplicates(inplace=True)

director = pd.DataFrame({'name': movie_director['director_CLEAN'].unique()}) 

movie_actor = df[['MOVIES_CLEAN', 'actors_CLEAN']]
movie_actor.loc[:,'actors_CLEAN'] = movie_actor.loc[:,'actors_CLEAN'].str.split(', ')
movie_actor = movie_actor.explode('actors_CLEAN', ignore_index=True)
movie_actor['actors_CLEAN'] = movie_actor['actors_CLEAN'].apply(remove_leading_trailing_whitespace)
movie_actor['actors_CLEAN'] = movie_actor['actors_CLEAN'].apply(remove_tick)
movie_actor.drop_duplicates(inplace=True)

actor = pd.DataFrame({'name': movie_actor['actors_CLEAN'].unique()})

# Lookup Table
movie_lookup = pd.DataFrame({'MOVIES_CLEAN': movie['MOVIES_CLEAN'].unique()})
movie_lookup['movie_id'] = [i+1 for i in range(len(movie_lookup))]

genre_lookup = genre.copy()
genre_lookup['genre_id'] = [i+1 for i in range(len(genre_lookup))]

director_lookup = director.copy()
director_lookup['director_id'] = [i+1 for i in range(len(director_lookup))]

actor_lookup = actor.copy()
actor_lookup['actor_id'] = [i+1 for i in range(len(actor_lookup))]

# Junction Table
movie_genre_id = pd.merge(movie_genre, movie_lookup, on='MOVIES_CLEAN', how='left')
movie_genre_id = pd.merge(movie_genre_id, genre_lookup, left_on='GENRE_CLEAN', right_on='name', how='left')
movie_genre_id = movie_genre_id[['movie_id', 'genre_id']]

movie_director_id = pd.merge(movie_director, movie_lookup, on='MOVIES_CLEAN', how='left')
movie_director_id = pd.merge(movie_director_id, director_lookup, left_on='director_CLEAN', right_on='name', how='left')
movie_director_id = movie_director_id[['movie_id', 'director_id']]

movie_actor_id = pd.merge(movie_actor, movie_lookup, on='MOVIES_CLEAN', how='left')
movie_actor_id = pd.merge(movie_actor_id, actor_lookup, left_on='actors_CLEAN', right_on='name', how='left')
movie_actor_id = movie_actor_id[['movie_id', 'actor_id']]

# LOAD

conn = psycopg2.connect('host=postgres dbname=cad-it-db user=cad-it password=c4d-1t!23')
cur = conn.cursor()

for _, row in movie.iterrows():
    insert_query = "INSERT INTO movie (title, movie_year, rating, description, votes, runtime, gross) VALUES ('%s', %s, %s, '%s', %s, %s, %s)"%(row['MOVIES_CLEAN'], row['YEAR_CLEAN'], row['RATING_CLEAN'], row['ONE-LINE_CLEAN'], row['VOTES_CLEAN'], row['RunTime_CLEAN'], row['Gross_CLEAN'])
    cur.execute(insert_query)    

for _, row in genre.iterrows():
    insert_query = "INSERT INTO genre (name) VALUES ('%s')"%(row['name'])
    cur.execute(insert_query)

for _, row in director.iterrows():
    insert_query = "INSERT INTO director (name) VALUES ('%s')"%(row['name'])
    cur.execute(insert_query)

for _, row in actor.iterrows():
    insert_query = "INSERT INTO actor (name) VALUES ('%s')"%(row['name'])
    cur.execute(insert_query)

for _, row in movie_genre_id.iterrows():
    insert_query = "INSERT INTO movie_genre (movie_id, genre_id) VALUES (%s, %s)"%(row['movie_id'], row['genre_id'])
    cur.execute(insert_query)

for _, row in movie_director_id.iterrows():
    insert_query = "INSERT INTO movie_director (movie_id, director_id) VALUES (%s, %s)"%(row['movie_id'], row['director_id'])
    cur.execute(insert_query)

for _, row in movie_actor_id.iterrows():
    insert_query = "INSERT INTO movie_actor (movie_id, actor_id) VALUES (%s, %s)"%(row['movie_id'], row['actor_id'])
    cur.execute(insert_query)

conn.commit()
cur.close()
conn.close()
print("Ending ELT script...")