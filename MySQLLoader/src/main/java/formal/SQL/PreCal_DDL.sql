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
    num_comment bigint,
    constraint avg_score_fk foreign key (movie_id) references movie_info_fact(movie_id)
);
insert into precal_movie_avg_score
select movie_id, movie_title, avg_score, num_comment
from movie_info_fact natural join temp;

create table precal_collaboration_actor
(
    actor1 varchar(500),
    actor2 varchar(500),
    collaboration_time integer,
    primary key (actor1, actor2),
    constraint precal_collaboration_actor_fk1 foreign key (actor1) references prejoin_movie_actor(actor),
    constraint precal_collaboration_actor_fk2 foreign key (actor2) references prejoin_movie_actor(actor)
);

create table precal_collaboration_director
(
    director_name1 varchar(500),
    director_name2 varchar(500),
    collaboration_time integer,
    primary key (director_name1, director_name2),
    constraint precal_collaboration_director_fk1 foreign key (director_name1) references prejoin_movie_director(director_name),
    constraint precal_collaboration_director_fk2 foreign key (director_name2) references prejoin_movie_director(director_name)
);

create table precal_collaboration_director_actor
(
    director_name varchar(500),
    actor varchar(500),
    collaboration_time integer,
    primary key (director_name, actor),
    constraint precal_collaboration_director_actor_fk1 foreign key (director_name) references prejoin_movie_director(director_name),
    constraint precal_collaboration_director_actor_fk2 foreign key (actor) references prejoin_movie_actor(actor)
);

