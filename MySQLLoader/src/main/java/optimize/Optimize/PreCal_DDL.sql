create table temp
(
    movie_id integer primary key,
    avg_score decimal(1000,6),
    num_comment bigint
);
insert into temp
select productID, avg(score), count(*)
from movie_review
group by productID;


create table precal_movie_avg_score
(
    movie_id integer primary key ,
    movie_title varchar(500),
    avg_score decimal(1000,6),
    num_comment bigint
);
insert into precal_movie_avg_score
select movie_id, movie_title, avg_score, num_comment
from movie_info_fact natural join temp;

create table precal_collaboration_actor
(
    actor1 varchar(500),
    actor2 varchar(500),
    collaboration_time integer,
    primary key (actor1, actor2)
);

create table precal_collaboration_director
(
    director_name1 varchar(500),
    director_name2 varchar(500),
    collaboration_time integer,
    primary key (director_name1, director_name2)
);

create table precal_collaboration_director_actor
(
    director_name varchar(500),
    actor varchar(500),
    collaboration_time integer,
    primary key (director_name, actor)
);


select * from precal_collaboration_actor;
select * from precal_collaboration_director;
select * from precal_collaboration_director_actor;

select * from movie_director where director_name = "Richard Ian Cox";
select * from movie_actor where actor = "Richard Ian Cox";
select count(distinct actor) from movie_actor;
select count(distinct director_name) from movie_director;
