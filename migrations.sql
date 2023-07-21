create table users
(
    id int constraint user_pk primary key,
    name    varchar(255) not null
);


create table account
(
    name    varchar(255) not null constraint account_pk primary key,
    balance integer not null,
    user_id int not null constraint fk_user references users(id)
);


