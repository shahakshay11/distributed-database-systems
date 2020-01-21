
select count(*) from movies where movieid in (select movieid from ratings)

drop table similarities CASCADE
CREATE TABLE similarities AS
SELECT r1.movieid AS movieid1, r2.movieid AS movieid2, 1 - abs(r1.rating - r2.rating) / 5 AS sim
FROM (SELECT movieid, avg(rating) as rating FROM ratings group by movieid) r1,
     (SELECT movieid, avg(rating) as rating FROM ratings group by movieid) r2
WHERE r1.movieid <> r2.movieid;


CREATE INDEX movieid1 on  similarities_new(movieid1);

create index movieid2 on similarities_new(movieid2);

drop index movieid1 on similarities

-- drop view allratings
-- Create view allratings as
-- select X.movieid as movieid,0 as rating from (Select movieid from movies where movieid not in (Select movieid from ratings) order by movieid asc) X UNION 
-- select rmid as movieid,average as rating from movieratingview
-- order by movieid asc



--select * from similarities where movieid1 = 3565 and movieid2 = 2026



create table recommendation_scores as
select Y.i as i, sum(Y.sim * Y.rating) / sum(Y.sim) AS P
from 
(Select movieid1,movieid2 as i,sim, r.rating as rating
from similarities join allratings r on similarities.movieid1 = r.movieid
where movieid1 in (Select movieid from ratings where userid=10) 
and movieid2 not in (select movieid from ratings where userid=10)
)Y
group by y.i
having (sum(Y.sim * Y.rating) / sum(Y.sim)) > 3.9



Create table recommendation
SELECT Y.i AS i, sum(Y.sim * Y.rating) / sum(Y.sim) AS P
FROM (SELECT movieid1, movieid2 AS i, sim, r.rating AS rating
      FROM similarities AS s
               JOIN ratings r ON s.movieid1 = r.movieid
      WHERE userid = 10
        AND movieid2 NOT IN (SELECT movieid FROM ratings WHERE userid = 10)
     ) Y
GROUP BY y.i
HAVING (sum(Y.sim * Y.rating) / sum(Y.sim)) > 3.9;



CREATE TABLE recommendation AS
SELECT m.title
FROM (SELECT movieid2 AS i, sim, r.rating
      FROM similarities AS s
               JOIN ratings r ON s.movieid1 = r.movieid AND userid = 1
      WHERE movieid2 NOT IN (SELECT movieid FROM ratings WHERE userid = 1)
     ) Y
         JOIN movies m ON (i = movieid)
GROUP BY m.movieid,m.title
HAVING (sum(Y.sim * Y.rating) / sum(Y.sim)) > 3.9;

select * from recommendation


select count(*) from movies
select count(*) from ratings where userid = 1

SELECT title,count(1) from movies GROUP BY title HAVING count(1)>1;



Create table recommendation AS
SELECT m.title
FROM (SELECT movieid2 AS i, sim, r.rating
      FROM similarities_new AS s
               JOIN ratings r ON s.movieid1 = r.movieid AND userid = 10
      WHERE movieid2 NOT IN (SELECT movieid FROM ratings WHERE userid = 10)
     ) Y
         JOIN movies m ON (i = movieid)
GROUP BY m.movieid, m.title
HAVING (sum(Y.sim * Y.rating) / sum(Y.sim)) > 3.9
ORDER BY m.title;

SELECT m.title
FROM (SELECT movieid2 AS i, sim, r.rating
      FROM similarities AS s
               JOIN ratings r ON s.movieid1 = r.movieid AND userid = 5
      WHERE movieid2 NOT IN (SELECT movieid FROM ratings WHERE userid = 5)
     ) T1
         JOIN movies m ON i = movieid
GROUP BY m.movieid, m.title
HAVING (sum(T1.sim * T1.rating) / sum(T1.sim)) > 3.9
ORDER BY m.title;





drop table query1 CASCADE; 
-- drop table query2 CASCADE;
drop table query3 CASCADE ;
drop table query4 CASCADE ;
drop table query5 CASCADE ;
drop table query6 CASCADE ;
drop table query7 CASCADE ;
drop table query8 CASCADE ;
drop table query9 CASCADE ;

drop table recommendation CASCADE ;
drop table similarities CASCADE ;