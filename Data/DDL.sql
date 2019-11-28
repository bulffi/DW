这些 SQL 都是 SQL Server 的定义，不可直接用于其他的数据库
有两张表 一张是评论（从 txt 中导出） 另一张是电影信息（爬取）
create table movie_review 
(
	product_id varchar(max) not null,
	user_id varchar(max) not null,
	id int identity
		constraint movie_review_pk
			primary key nonclustered,
	profile_name varchar(max),
	helpfulness varchar(max),
	score decimal(2,1),
	review_time date,
	summary varchar(max),
	review_text text
)
go

create unique index movie_review_id_uindex
	on movie_review (id)
go

create table movie_info
(
	movie_id varchar(500) not null   取并集
		constraint movie_info_pk
			primary key nonclustered,
	movie_title varchar(500),
	directors varchar(500),  如果导演不完全相同，我们重新分离！
	release_date date default datefromparts(0,1,1), 取最早
	movie_type varchar(500),  取并集
	movie_version varchar(500),  取并集
	actors varchar(2000) 		取并集
)
go

create unique index movie_info_movie_id_uindex
	on movie_info (movie_id)
go

