create table Query1
(
    year integer,
    month integer,
    date integer,
    weekday integer,
    year_quarter integer,
    year_half integer,
    count integer,
    primary key (year, month, date, weekday, year_quarter, year_half)
);

select * from movie_info_fact;
delete from Query1;
select * from Query1;
select year, count(*) from movie_info_fact group by year;
drop table Query1;
select year, month, count(*) from movie_info_fact natural join movie_release_date group by (year, month);
select weekday, count(*) from movie_info_fact group by weekday;
insert into Query1 values (0,0,0,7,0,0,4872);
select * from Query1 where (year, month, weekday) = (0,0,7);
