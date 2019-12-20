create table prejoin_movie_date
(
    movie_id integer primary key,
    movie_title varchar(500),
    year integer,
    weekday integer,
    date integer,
    month integer,
    year_quarter integer,
    year_half integer
);
insert into prejoin_movie_date
select movie_id,movie_title,year,weekday,date,month,year_quarter,year_half
from movie_info_fact natural join movie_release_date;
show index from movie_info;

create table prejoin_movie_actor
(
    movie_id integer,
    movie_title varchar(500),
    actor varchar(500),
    is_lead boolean,
    primary key (movie_id, actor)
);
insert into prejoin_movie_actor
select movie_id, movie_title, actor, is_lead
from movie_info_fact natural join movie_actor;

create table prejoin_movie_director
(
    movie_id integer,
    movie_title varchar(500),
    director_name varchar(500),
    primary key (movie_id, director_name)
);
insert into prejoin_movie_director
select movie_id, movie_title, director_name
from movie_info_fact natural join movie_director;

create table prejoin_movie_type
(
    movie_id integer,
    movie_title varchar(500),
    type varchar(500),
    primary key (movie_id, type)
);
insert into prejoin_movie_type
select movie_id, movie_title, type
from movie_info_fact natural join movie_type;

create table prejoin_movie_version
(
    movie_id integer,
    movie_title varchar(500),
    version varchar(500),
    primary key (movie_id, version)
);
insert into prejoin_movie_version
select movie_id, movie_title, version
from movie_info_fact natural join movie_version;

create table prejoin_movie_review
(
    productID integer,
    movie_title varchar(500),
    userID varchar(500),
    profileName varchar(500),
    num_reader integer,
    num_helpfulness integer,
    score decimal(2,1),
    time date,
    summary varchar(10000),
    text text,
    primary key (productID, userID, score, time, summary)
);
insert into prejoin_movie_review
select movie_id, movie_title, userID, profileName, num_reader, num_helpfulness, score, time, summary, text
from movie_info_fact join movie_review
where movie_info_fact.movie_id = movie_review.productID;

select count(*) from prejoin_movie_review where score = 0;