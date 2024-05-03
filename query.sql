-- 1. View number of unique film titles	
CREATE VIEW view_unique_film_titles AS
SELECT 
	DISTINCT movies
FROM movies
ORDER BY movies;

-- 2. View movie starring Lena Headey
CREATE VIEW view_lena_headey AS
SELECT
    m.movies,
    m.year_of_release,
    me.rating
FROM
    movies m
    JOIN movies_episode me ON m.movie_id = me.movie_id
    JOIN movies_stars ms ON m.movie_id = ms.movie_id
WHERE
    lower(ms.stars) LIKE '%lena headey%'
ORDER BY
    m.year_of_release;

-- 3. View name and total gross per directors
CREATE VIEW gross_per_directors AS
SELECT
    directors,
    COALESCE(SUM(gross), 0) AS total_gross
FROM
    movies m
    JOIN movies_episode me ON m.movie_id = me.movie_id
    JOIN movies_directors md ON m.movie_id = md.movie_id
WHERE
    gross IS NOT NULL
GROUP BY
    1
HAVING
    COALESCE(SUM(gross), 0) > 0
ORDER BY
    total_gross desc;
	
-- 4. Top 5 comedy gender movies with highest gross
CREATE VIEW top_5_comedy_movie AS
SELECT
	m.movies,
	m.year_of_release,
	AVG(me.rating),
	SUM(gross)
FROM
    movies m
    JOIN movies_episode me ON m.movie_id = me.movie_id
    JOIN movies_genre mg ON m.movie_id = mg.movie_id
WHERE lower(genre) = 'comedy'
GROUP BY 
	1, 2
LIMIT 5;

-- 5. Movies directed by Martin Scorsese and Starring Robert de Niro
CREATE VIEW movies_martin_and_robert AS
SELECT
    m.movies,
	m.year_of_release,
	me.rating
FROM
    movies m
    JOIN movies_episode me ON m.movie_id = me.movie_id
    JOIN movies_directors md ON m.movie_id = md.movie_id
	JOIN movies_stars ms ON m.movie_id = ms.movie_id
WHERE
	lower(ms.stars) = 'robert de niro'
	AND lower(md.directors) = 'martin scorsese'