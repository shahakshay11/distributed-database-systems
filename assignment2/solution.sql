-- Q1
create table query1 as
select g.name as name, count(hg.movieid) as moviecount from hasagenre hg 
join genres g using (genreid)
group by name;

--select * from query1;

--Q2
CREATE TABLE query2 AS
SELECT g.name AS name, avg(rating) AS rating
FROM movies m
         JOIN hasagenre h on h.movieid = m.movieid
         JOIN ratings r on r.movieid = m.movieid
         JOIN genres g on h.genreid = g.genreid
GROUP BY g.name;

--Q3
create table query3 as
select m.title as title, count(r.movieid) as CountOfRatings from ratings r 
join movies m on m.movieid = r.movieid
group by title
having count(r.movieid)>=10;

--select * from query3;

--Q4
CREATE TABLE query4 AS
SELECT m.movieid AS movieid, m.title AS title
FROM movies m
         JOIN hasagenre h USING (movieid)
         JOIN genres g USING (genreid)
WHERE g.name = 'Comedy';

--Q5
create table query5 as
select m.title as title, avg(r.rating) as average from ratings r 
join movies m using (movieid)
group by title
order by title desc;

--Q6
CREATE TABLE query6 AS
SELECT avg(r.rating) AS rating
FROM movies m
         JOIN ratings r USING (movieid)
         JOIN hasagenre h USING (movieid)
         JOIN genres g USING (genreid)
WHERE g.name = 'Comedy';

--Q7
create table query7 as
SELECT avg(rating) FROM
(SELECT movieid
FROM movies m
JOIN ratings r USING (movieid)
    JOIN hasagenre h USING (movieid)
    JOIN genres g USING (genreid)
WHERE g.name = 'Romance'
INTERSECT
SELECT movieid
FROM movies m
JOIN ratings r USING (movieid)
    JOIN hasagenre h USING (movieid)
    JOIN genres g USING (genreid)
WHERE g.name = 'Comedy') X
join ratings USING (movieid)
;


--Q8
create table query8 as
SELECT avg(rating) FROM
(SELECT movieid
FROM movies m
JOIN ratings r USING (movieid)
    JOIN hasagenre h USING (movieid)
    JOIN genres g USING (genreid)
WHERE g.name = 'Romance'
EXCEPT
SELECT movieid
FROM movies m
JOIN ratings r USING (movieid)
    JOIN hasagenre h USING (movieid)
    JOIN genres g USING (genreid)
WHERE g.name = 'Comedy') X
join ratings USING (movieid)
;

--select * from query8


--Q9
CREATE TABLE query9 AS
SELECT movieid,rating FROM ratings
WHERE ratings.userid = :v1;



--Q10
-- Creating intermediate table similarities
CREATE TABLE similarities AS
SELECT r1.movieid AS movieid1, r2.movieid AS movieid2, 1 - abs(r1.rating - r2.rating) / 5 AS sim
FROM (SELECT movieid, avg(rating) AS rating FROM ratings GROUP BY movieid) r1,
     (SELECT movieid, avg(rating) AS rating FROM ratings GROUP BY movieid) r2
WHERE r1.movieid <> r2.movieid;

--To optimize the search query
CREATE INDEX movieid1 on  similarities(movieid1);
CREATE INDEX movieid2 on similarities(movieid2);

Create table recommendation AS
SELECT m.title
FROM (SELECT movieid2 AS i, sim, r.rating
      FROM similarities AS s
               JOIN ratings r ON s.movieid1 = r.movieid AND userid = :v1
      WHERE movieid2 NOT IN (SELECT movieid FROM ratings WHERE userid = :v1)
     ) T1
         JOIN movies m ON i = movieid
GROUP BY m.movieid, m.title
HAVING (sum(T1.sim * T1.rating) / sum(T1.sim)) > 3.9
ORDER BY m.title;
