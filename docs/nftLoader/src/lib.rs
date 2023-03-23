mod abi;
mod block_timestamp;
mod pb;

use self::block_timestamp::BlockTimestamp;
use substreams::store::{
    self, DeltaProto, StoreNew, StoreSetIfNotExists, StoreSetIfNotExistsProto,
};
use substreams_database_change::pb::database::{table_change::Operation, DatabaseChanges};
use substreams_ethereum::pb as ethpb;

use hex_literal::hex;
use pb::erc721;
use substreams::{log, Hex};
use substreams_ethereum::{NULL_ADDRESS};

// Bored Ape Club Contract
const TRACKED_CONTRACT: [u8; 20] = hex!("bc4ca0eda7647a8ab7c2061c2e118a18a936f13d");

substreams_ethereum::init!();

fn transform_block_to_erc721_transfers(blk: ethpb::eth::v2::Block) -> (BlockTimestamp, Vec<erc721::Transfer>) {
    let timestamp = BlockTimestamp::from_block(&blk);
    let header = blk.header.as_ref().unwrap();

    (
        timestamp,
        blk
        .events::<abi::erc721::events::Transfer>(&[&TRACKED_CONTRACT])
        .map(|(transfer, log)| {
            substreams::log::info!("NFT Transfer seen");

            erc721::Transfer {
                block_number: blk.number,
                from_address: transfer.from,
                to_address: transfer.to,
                contract_address: log.address().to_vec(), // TODO: verify this is the correct method
                token_id: transfer.token_id.to_bytes_be().1,
                tx_hash: log.receipt.transaction.hash.clone(),
                ordinal: log.block_index() as u64,
                timestamp: Some(header.timestamp.as_ref().unwrap().clone()),
            }
        })
        .collect::<Vec<erc721::Transfer>>(), // Collect the results into a Vec
    )
}

/// Parses block and saves to store
#[substreams::handlers::store]
fn store_transfers(blk: ethpb::eth::v2::Block, s: StoreSetIfNotExistsProto<erc721::Transfer>) {
    let (_timestamp, erc721_transfers) = transform_block_to_erc721_transfers(blk);

    let unique_key = |transfer: &erc721::Transfer| {
        format!(
            "{}-{}-{}-{}",
            Hex(&transfer.contract_address),
            Hex(&transfer.token_id),
            Hex(&transfer.from_address),
            Hex(&transfer.to_address),
        )
    };

    // for loop over erc721_transfers
    for transfer in erc721_transfers {
        if transfer.from_address != NULL_ADDRESS {
            log::info!("Found a transfer out {}", Hex(&transfer.tx_hash));
            s.set_if_not_exists(transfer.ordinal, unique_key(&transfer), &transfer);
        }

        if transfer.to_address != NULL_ADDRESS {
            log::info!("Found a transfer in {}", Hex(&transfer.tx_hash));
            s.set_if_not_exists(transfer.ordinal, unique_key(&transfer), &transfer);
        }
    }
}

#[substreams::handlers::map]
fn db_out(
    erc_transfer_start: store::Deltas<DeltaProto<erc721::Transfer>>,
) -> Result<DatabaseChanges, substreams::errors::Error> {
    let mut database_changes: DatabaseChanges = Default::default();
    transform_erc721_transfers_to_database_changes(&mut database_changes, erc_transfer_start);
    Ok(database_changes)
}

fn transform_erc721_transfers_to_database_changes(
    changes: &mut DatabaseChanges,
    deltas: store::Deltas<DeltaProto<erc721::Transfer>>,
) {
    use substreams::pb::substreams::store_delta::Operation;

    for delta in deltas.deltas {
        match delta.operation {
            Operation::Create => push_create(
                changes,
                &delta.key,
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
    ordinal: u64,
    value: erc721::Transfer,
) {
    changes
        .push_change("erc721_transfers", key, ordinal, Operation::Create)
        .change("block_number", (None, value.block_number))
        .change("from_address", (None, Hex(value.from_address)))
        .change("to_address", (None, Hex(value.to_address)))
        .change("contract_address", (None, Hex(value.contract_address)))
        .change("token_id", (None, Hex(value.token_id)))
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
        .push_change("erc721_transfers", key, ordinal, Operation::Update)
        .change("block_number", (old_value.block_number, new_value.block_number))
        .change("from_address", (Hex(old_value.from_address), Hex(new_value.from_address)))
        .change("to_address", (Hex(old_value.to_address), Hex(new_value.to_address)))
        .change("contract_address", (Hex(old_value.contract_address), Hex(new_value.contract_address)))
        .change(
            "token_id",
            (Hex(old_value.token_id), Hex(new_value.token_id)),
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
