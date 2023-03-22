mod abi;
mod block_timestamp;
mod pb;

use self::block_timestamp::BlockTimestamp;
use pb::block_meta::BlockMeta;
use substreams::store::{
    self, DeltaProto, StoreNew, StoreSetIfNotExists, StoreSetIfNotExistsProto,
};
use substreams::Hex;
use substreams_database_change::pb::database::{table_change::Operation, DatabaseChanges};
use substreams_ethereum::pb as ethpb;

use hex_literal::hex;
use pb::erc721;
use substreams::prelude::*;
use substreams::{log, store::StoreAddInt64, Hex};
use substreams_ethereum::{pb::eth::v2 as eth, NULL_ADDRESS};

// Bored Ape Club Contract
const TRACKED_CONTRACT: [u8; 20] = hex!("bc4ca0eda7647a8ab7c2061c2e118a18a936f13d");

substreams_ethereum::init!();

// fn transform_block_to_block_meta(blk: ethpb::eth::v2::Block) -> (BlockTimestamp, BlockMeta) {
//     let timestamp = BlockTimestamp::from_block(&blk);
//     let header = blk.header.unwrap();

//     (
//         timestamp,
//         BlockMeta {
//             number: blk.number,
//             hash: blk.hash,
//             parent_hash: header.parent_hash,
//             timestamp: Some(header.timestamp.unwrap()),
//         },
//     )
// }

// #[substreams::handlers::store]
// fn store_block_meta_start(blk: ethpb::eth::v2::Block, s: StoreSetIfNotExistsProto<BlockMeta>) {
//     let (timestamp, meta) = transform_block_to_block_meta(blk);

//     s.set_if_not_exists(meta.number, timestamp.start_of_day_key(), &meta);
//     s.set_if_not_exists(meta.number, timestamp.start_of_month_key(), &meta);
// }

fn transform_block_to_erc721_transfers(blk: ethpb::eth::v2::Block) -> (BlockTimestamp, Vec<erc721::Transfer>) {
    let timestamp = BlockTimestamp::from_block(&blk);
    let header = blk.header.unwrap();

    (
        timestamp,
        blk
        .events::<abi::erc721::events::Transfer>(&[&TRACKED_CONTRACT])
        .map(|(transfer, log)| {
            substreams::log::info!("NFT Transfer seen");

            erc721::Transfer {
                number: blk.number,
                from: transfer.from,
                to: transfer.to,
                token_id: transfer.token_id.to_u64(),
                tx_hash: log.receipt.transaction.hash.clone(),
                ordinal: log.block_index() as u64,
                timestamp: Some(header.timestamp.unwrap()),
            }
        })
    )
}

/// Parses block and saves to store
#[substreams::handlers::store]
fn store_transfers(blk: ethpb::eth::v2::Block, s: StoreSetIfNotExistsProto<erc721::Transfer>) {
    let (timestamp, erc721Transfers) = transform_block_to_erc721_transfers(blk);

    // for loop over erc721Transfers
    for transfer in erc721Transfers {
        if transfer.from != NULL_ADDRESS {
            log::info!("Found a transfer out {}", Hex(&transfer.tx_hash));
            s.set_if_not_exists(transfer.number, timestamp.start_of_day_key(), &transfer);
        }

        if transfer.to != NULL_ADDRESS {
            log::info!("Found a transfer in {}", Hex(&transfer.tx_hash));
            s.set_if_not_exists(transfer.number, timestamp.start_of_day_key(), &transfer);
        }
    }
}

// /// Extracts transfers events from the contract
// #[substreams::handlers::map]
// fn map_transfers(blk: eth::Block) -> Result<erc721::Transfers, substreams::errors::Error> {
//     Ok(erc721::Transfers {
//         transfers: blk
//             .events::<abi::erc721::events::Transfer>(&[&TRACKED_CONTRACT])
//             .map(|(transfer, log)| {
//                 substreams::log::info!("NFT Transfer seen");

//                 erc721::Transfer {
//                     tx_hash: log.receipt.transaction.hash.clone(),
//                     from: transfer.from,
//                     to: transfer.to,
//                     token_id: transfer.token_id.to_u64(),
//                     ordinal: log.block_index() as u64,
//                 }
//             })
//             .collect(),
//     })
// }

// #[substreams::handlers::map]
// fn db_out(
//     block_meta_start: store::Deltas<DeltaProto<BlockMeta>>,
// ) -> Result<DatabaseChanges, substreams::errors::Error> {
//     let mut database_changes: DatabaseChanges = Default::default();
//     transform_block_meta_to_database_changes(&mut database_changes, block_meta_start);
//     Ok(database_changes)
// }

#[substreams::handlers::map]
fn db_out(
    ercTransferStart: store::Deltas<DeltaProto<erc721::Transfer>>,
) -> Result<DatabaseChanges, substreams::errors::Error> {
    let mut database_changes: DatabaseChanges = Default::default();
    transform_erc721_transfers_to_database_changes(&mut database_changes, ercTransferStart);
    Ok(database_changes)
}

fn transform_erc721_transfers_to_database_changes(
    changes: &mut DatabaseChanges,
    deltas: store::Deltas<DeltaProto<erc::Transfer>>,
) {
    use substreams::pb::substreams::store_delta::Operation;

    for delta in deltas.deltas {
        match delta.operation {
            Operation::Create => push_create(
                changes,
                &delta.key,
                BlockTimestamp::from_key(&delta.key),
                delta.ordinal,
                delta.new_value,
            ),
            Operation::Update => push_update(
                changes,
                &delta.key,
                delta.ordinal,
                delta.old_value,
                delta.new_value,
            ),
            Operation::Delete => todo!(),
            x => panic!("unsupported opeation {:?}", x),
        }
    }
}

fn push_create(
    changes: &mut DatabaseChanges,
    key: &str,
    timestamp: BlockTimestamp,
    ordinal: u64,
    value: erc721::Transfer,
) {
    changes
        .push_change("erc721", key, ordinal, Operation::Create)
        .change("at", (None, timestamp))
        .change("number", (None, value.number))
        .change("from", (None, value.from))
        .change("to", (None, value.to))
        .change("token_id", (None, value.token_id))
        .change("tx_hash", (None, Hex(value.tx_hash)))
        .change("ordinal", (None, value.ordinal))
        .change("timestamp", (None, value.timestamp.unwrap()));
}

fn push_update(
    changes: &mut DatabaseChanges,
    key: &str,
    ordinal: u64,
    old_value: erc721::Transfer,
    new_value: erc721::Transfer,
) {
    changes
        .push_change("erc721", key, ordinal, Operation::Update)
        .change("number", (old_value.number, new_value.number))
        .change("from", (old_value.from, new_value.from))
        .change("to", (Hex(old_value.to), Hex(new_value.to)))
        .change(
            "token_id",
            (old_value.token_id, new_value.token_id),
        )
        .change(
            "tx_hash",
            (Hex(old_value.tx_hash), Hex(new_value.tx_hash)),
        )
        .change(
            "ordinal",
            (old_value.ordinal, new_value.ordinal),
        )
        .change(
            "timestamp",
            (&old_value.timestamp.unwrap(), &new_value.timestamp.unwrap()),
        );
}

// fn transform_block_meta_to_database_changes(
//     changes: &mut DatabaseChanges,
//     deltas: store::Deltas<DeltaProto<BlockMeta>>,
// ) {
//     use substreams::pb::substreams::store_delta::Operation;

//     for delta in deltas.deltas {
//         match delta.operation {
//             Operation::Create => push_create(
//                 changes,
//                 &delta.key,
//                 BlockTimestamp::from_key(&delta.key),
//                 delta.ordinal,
//                 delta.new_value,
//             ),
//             Operation::Update => push_update(
//                 changes,
//                 &delta.key,
//                 delta.ordinal,
//                 delta.old_value,
//                 delta.new_value,
//             ),
//             Operation::Delete => todo!(),
//             x => panic!("unsupported opeation {:?}", x),
//         }
//     }
// }

// consider moving back into a standalone file
//#[path = "db_out.rs"]
//mod db;
// fn push_create(
//     changes: &mut DatabaseChanges,
//     key: &str,
//     timestamp: BlockTimestamp,
//     ordinal: u64,
//     value: BlockMeta,
// ) {
//     changes
//         .push_change("block_meta", key, ordinal, Operation::Create)
//         .change("at", (None, timestamp))
//         .change("number", (None, value.number))
//         .change("hash", (None, Hex(value.hash)))
//         .change("parent_hash", (None, Hex(value.parent_hash)))
//         .change("timestamp", (None, value.timestamp.unwrap()));
// }

// fn push_update(
//     changes: &mut DatabaseChanges,
//     key: &str,
//     ordinal: u64,
//     old_value: BlockMeta,
//     new_value: BlockMeta,
// ) {
//     changes
//         .push_change("block_meta", key, ordinal, Operation::Update)
//         .change("number", (old_value.number, new_value.number))
//         .change("hash", (Hex(old_value.hash), Hex(new_value.hash)))
//         .change(
//             "parent_hash",
//             (Hex(old_value.parent_hash), Hex(new_value.parent_hash)),
//         )
//         .change(
//             "timestamp",
//             (&old_value.timestamp.unwrap(), &new_value.timestamp.unwrap()),
//         );
// }
