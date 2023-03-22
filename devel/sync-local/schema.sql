create table erc721_transfers
(
    id          text not null constraint erc721_transfers_pk primary key,
    version     integer,
    at          text,
    number      text,
    "from"      text,
    "to"        text,
    token_id    text,
    tx_hash     text,
    ordinal     text,
    contract    text,
    timestamp   text
);

create table cursors
(
    id         text not null constraint cursor_pk primary key,
    cursor     text,
    block_num  bigint,
    block_id   text
);
