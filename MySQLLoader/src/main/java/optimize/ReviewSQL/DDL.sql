create table movie_review
(
    productID varchar(500),
    userID varchar(500),
    profileName varchar(500),
    num_reader integer,
    num_helpfulness integer,
    score decimal(2,1),
    time date,
    summary varchar(10000),
    text text
);