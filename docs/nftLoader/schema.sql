create table transfers
(
    id                  text not null constraint transfers_pk primary key,
    version             integer,
    at                  text,
    schema              text,
    block_number        text,
    from_address        text,
    to_address          text,
    operator            text,
    quantity            text,
    token_id            text,
    contract_address    text,
    tx_hash             text,
    ordinal             text,
    timestamp           text
);

create table nft
(
    id          text not null constraint nft_transfer_pk primary key,
    contract    text,
    token_id    text,
    owner       text,
    tokenUri    text,
    metadata    text
);

create table contract
(
    id          text not null constraint contract_pk primary key,
    contract    text,
    base_uri    text,
    deployer    text
);

create table cursors
(
    id         text not null constraint cursor_pk primary key,
    cursor     text,
    block_num  bigint,
    block_id   text
);