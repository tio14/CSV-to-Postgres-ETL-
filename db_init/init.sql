CREATE TABLE movie(
    movie_id SERIAL PRIMARY KEY,
    title VARCHAR(200) NOT NULL,
    movie_year REAL,
    rating DECIMAL(3,1),
    description VARCHAR(1000),
    votes REAL,
    runtime REAL,
    gross REAL
);

CREATE TABLE genre(
    genre_id SERIAL PRIMARY KEY,
    name VARCHAR(50) NOT NULL
);

CREATE TABLE movie_genre(
    movie_id INTEGER REFERENCES movie(movie_id),
    genre_id INTEGER REFERENCES genre(genre_id),
    PRIMARY KEY (movie_id, genre_id)
);

CREATE TABLE director(
    director_id SERIAL PRIMARY KEY,
    name VARCHAR(50) NOT NULL
);

CREATE TABLE movie_director(
    movie_id INTEGER REFERENCES movie(movie_id),
    director_id INTEGER REFERENCES director(director_id),
    PRIMARY KEY (movie_id, director_id)
);

CREATE TABLE actor(
    actor_id SERIAL PRIMARY KEY,
    name VARCHAR(50) NOT NULL
);

CREATE TABLE movie_actor(
    movie_id INTEGER REFERENCES movie(movie_id),
    actor_id INTEGER REFERENCES actor(actor_id),
    PRIMARY KEY (movie_id, actor_id)
);

-- INSERT INTO films (title, release_date, price, rating, user_rating) VALUES
-- ('Inception', '2010-07-16', 12.99, 'PG-13', 4.8),
-- ('The Shawshank Redemption', '1994-09-23', 9.99, 'R', 4.9),
-- ('The Godfather', '1972-03-24', 14.99, 'R', 4.7),
-- ('The Dark Knight', '2008-07-18', 11.99, 'PG-13', 4.8),
-- ('Pulp Fiction', '1994-10-14', 10.99, 'R', 4.6),
-- ('The Matrix', '1999-03-31', 9.99, 'R', 4.7),
-- ('Forrest Gump', '1994-07-06', 8.99, 'PG-13', 4.5),
-- ('Toy Story', '1995-11-22', 7.99, 'G', 4.4),
-- ('Jurassic Park', '1993-06-11', 9.99, 'PG-13', 4.3),
-- ('Avatar', '2009-12-18', 12.99, 'PG-13', 4.2),
-- ('Blade Runner 2049', '2017-10-06', 13.99, 'R', 4.3),
-- ('Mad Max: Fury Road', '2015-05-15', 11.99, 'R', 4.6),
-- ('Coco', '2017-11-22', 9.99, 'PG', 4.8),
-- ('Dunkirk', '2017-07-21', 12.99, 'PG-13', 4.5),
-- ('Spider-Man: Into the Spider-Verse', '2018-12-14', 10.99, 'PG', 4.9),
-- ('Parasite', '2019-10-11', 14.99, 'R', 4.6),
-- ('Whiplash', '2014-10-10', 9.99, 'R', 4.7),
-- ('Inside Out', '2015-06-19', 9.99, 'PG', 4.8),
-- ('The Grand Budapest Hotel', '2014-03-28', 10.99, 'R', 4.4),
-- ('La La Land', '2016-12-09', 11.99, 'PG-13', 4.5);