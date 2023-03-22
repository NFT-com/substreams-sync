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
    deployer    text
);

create table cursors
(
    id         text not null constraint cursor_pk primary key,
    cursor     text,
    block_num  bigint,
    block_id   text
);