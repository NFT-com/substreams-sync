create table nft_transfer
(
    id          text not null constraint nft_transfer_pk primary key,
    from        text,
    to          text,
    contract    text,
    tokenId     text,
    hash        text,
    timestamp   text
);

create table nft
(
    id          text not null constraint nft_transfer_pk primary key,
    contract    text,
    tokenId     text,
    owner       text,
    tokenUri    text,
    metadata    text,
);

create table contract
(
    id          text not null constraint contract_pk primary key,
    contract    text,
    deployer    text,
);

create table cursors
(
    id         text not null constraint cursor_pk primary key,
    cursor     text,
    block_num  bigint,
    block_id   text
);