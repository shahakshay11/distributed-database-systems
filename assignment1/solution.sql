--drop table users;
Create table users(
	userid integer not null,
	name text,
	PRIMARY KEY(userid)
);

--drop table movies;
Create table movies(
	movieid integer not null,
	title text,
	PRIMARY KEY(movieid)
);

--drop table genres;
Create table genres(
	genreid integer not null,
	name text,
	PRIMARY KEY(genreid)
);

--drop table hasagenre;
Create table hasagenre(
	genreid integer REFERENCES genres (genreid),
	movieid integer REFERENCES movies (movieid),
	PRIMARY KEY(movieid,genreid)
);

--drop table ratings;
Create table ratings(
	userid integer REFERENCES users (userid) not null,
	movieid integer REFERENCES movies (movieid) not null,
	rating numeric,
	check(rating >=0 and rating <=5),
	timestamp bigint not null default (extract(epoch from now()) * 1000),
	PRIMARY KEY(userid,movieid)
);

--drop table taginfo;
Create table taginfo(
	tagid integer not null,
	content text,
	PRIMARY KEY(tagid)
);

--drop table tags;
Create table tags(
	userid integer REFERENCES users (userid),
	movieid integer REFERENCES movies (movieid),
	tagid integer REFERENCES taginfo (tagid),
	timestamp bigint not null default (extract(epoch from now()) * 1000),
	PRIMARY KEY(userid,movieid,tagid)
);
