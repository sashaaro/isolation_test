create table public.sale
(
    id         integer not null
        constraint sale_pk
            primary key,
    product_id integer not null,
    quantity   integer not null,
    price      integer
);

