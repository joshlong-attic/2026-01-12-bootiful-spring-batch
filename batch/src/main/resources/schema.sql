create table if not exists dogs
(
    id          bigint not null,
    name        text   not null,
    description text   not null,
    gender      char   not null,
    image       text   not null
);