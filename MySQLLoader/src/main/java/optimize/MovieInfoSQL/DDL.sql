create table movie_release_date
(
    date_key integer primary key,
    date integer,
    month integer,
    year_quarter integer,
    year_half integer
);
create table movie_info_fact
(
    movie_id integer primary key,
    movie_title varchar(500),
    year integer,
    weekday integer,
    date_key integer,
    constraint fact_date_fk foreign key (date_key) references movie_release_date(date_key)
);
create table movie_amazon_id
(
    movie_id integer,
    amazon_id varchar(500),
    primary key (movie_id, amazon_id),
    constraint movie_amazon_id_fk foreign key (movie_id) references movie_info_fact(movie_id)
);
create table movie_director
(
    movie_id integer,
    director_name varchar(500),
    primary key (movie_id, director_name),
    constraint director_fk foreign key(movie_id) references movie_info_fact(movie_id)
);
create table movie_type
(
    movie_id integer,
    type varchar(500),
    primary key (movie_id, type),
    constraint movie_type_fk foreign key(movie_id) references movie_info_fact(movie_id)
);
create table movie_version
(
    movie_id integer,
    version varchar(500),
    primary key (movie_id, version),
    constraint movie_version_fk foreign key(movie_id) references movie_info_fact(movie_id)
);
create table movie_actor
(
    movie_id integer,
    actor varchar(500),
    is_lead boolean,
    primary key (movie_id, actor),
    constraint actor_fk foreign key(movie_id) references movie_info_fact(movie_id)
)