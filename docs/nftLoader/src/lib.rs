// Import the required modules
mod abi;
mod pb;
mod block_timestamp;

use pb::transfers;
use pb::transfers::transfer::Schema;
use substreams_database_change::pb::database::{table_change::Operation, DatabaseChanges};
use substreams_ethereum::pb as ethpb;

use prost_types::Timestamp;
use self::block_timestamp::BlockTimestamp;
use substreams::{log, Hex};

use substreams::scalar::BigInt;
use substreams_ethereum::Event;

use abi::erc1155::events::TransferBatch as ERC1155TransferBatchEvent;
use abi::erc1155::events::TransferSingle as ERC1155TransferSingleEvent;
use abi::erc20::events::Transfer as ERC20TransferEvent;
use abi::erc721::events::Transfer as ERC721TransferEvent;

substreams_ethereum::init!();

#[substreams::handlers::map]
fn db_out(
    blk: ethpb::eth::v2::Block
) -> Result<DatabaseChanges, substreams::errors::Error> {
    let (_timestamp, transfers) = transform_block_to_transfers(blk);

    let mut database_changes: DatabaseChanges;

    // for loop over transfers
    for transfer in transfers {
        database_changes: DatabaseChanges = Default::default();
        transform_erc721_transfers_to_database_changes(&mut database_changes, transfer);
    }

    Ok(database_changes)
}

fn transform_block_to_transfers(blk: ethpb::eth::v2::Block) -> (BlockTimestamp, Vec<transfers::Transfer>) {
    let header = blk.header.as_ref().unwrap();
    let timestamp = BlockTimestamp::from_block(&blk);

    let transfers: Vec<transfers::Transfer> = blk.receipts().flat_map(|receipt| {
        let hash = &receipt.transaction.hash;
        let timestamp = Some(header.timestamp.as_ref().unwrap().clone());

        receipt.receipt.logs.iter().flat_map(move |log| {
            let erc20_transfers = Vec::new();

            // TODO: commented out as we don't want to get all erc20s right now
            // let erc20_transfers = ERC20TransferEvent::match_and_decode(log).map(|event| new_erc20_transfer(
            //     hash,
            //     log.block_index,
            //     log.address.to_vec(),
            //     blk.number,
            //     timestamp.clone(),
            //     event
            // ));

            let erc721_transfers = ERC721TransferEvent::match_and_decode(log).map(|event| new_erc721_transfer(
                hash,
                log.block_index,
                log.address.to_vec(),
                blk.number,
                timestamp.clone(),
                event
            ));

            let erc1155_single_transfers = ERC1155TransferSingleEvent::match_and_decode(log).map(|event| new_erc1155_single_transfer(
                hash,
                log.block_index,
                log.address.to_vec(),
                blk.number,
                timestamp.clone(),
                event
            ));

            let erc1155_batch_transfers = ERC1155TransferBatchEvent::match_and_decode(log).map(|event| new_erc1155_batch_transfer(
                hash,
                log.block_index,
                log.address.to_vec(),
                blk.number,
                timestamp.clone(),
                event
            )).into_iter().flatten();

            erc20_transfers
                .into_iter()
                .chain(erc721_transfers.into_iter())
                .chain(erc1155_single_transfers.into_iter())
                .chain(erc1155_batch_transfers)
        })
    }).collect();

    (timestamp, transfers)
}

#[allow(dead_code)]
fn new_erc20_transfer(
    hash: &[u8],
    ordinal: u32,
    contract_address: Vec<u8>,
    block_number: u64,
    timestamp: Option<Timestamp>,
    event: ERC20TransferEvent
) -> transfers::Transfer {
    transfers::Transfer {
        schema: schema_to_string(Schema::Erc20),
        from_address: event.from,
        to_address: event.to,
        quantity: event.value.to_string(),
        tx_hash: hash.to_vec(),
        ordinal: ordinal as u64,
        contract_address: contract_address,
        block_number: block_number,
        timestamp: timestamp,

        operator: Vec::new(),
        token_id: Vec::new(),
    }
}

fn new_erc721_transfer(
    hash: &[u8],
    ordinal: u32,
    contract_address: Vec<u8>,
    block_number: u64,
    timestamp: Option<Timestamp>,
    event: ERC721TransferEvent
) -> transfers::Transfer {
    transfers::Transfer {
        schema: schema_to_string(Schema::Erc721),
        from_address: event.from,
        to_address: event.to,
        quantity: "1".to_string(),
        tx_hash: hash.to_vec(),
        ordinal: ordinal as u64,
        token_id: event.token_id.to_bytes_be().1,
        contract_address: contract_address,
        block_number: block_number,
        timestamp: timestamp,

        operator: Vec::new(),
    }
}

fn new_erc1155_single_transfer(
    hash: &[u8],
    ordinal: u32,
    contract_address: Vec<u8>, 
    block_number: u64,
    timestamp: Option<Timestamp>,
    event: ERC1155TransferSingleEvent,
) -> transfers::Transfer {
    new_erc1155_transfer(
        hash,
        ordinal,
        &event.from,
        &event.to,
        &event.id,
        &event.value,
        &event.operator,
        contract_address,
        block_number,
        timestamp,
    )
}

fn new_erc1155_batch_transfer(
    hash: &[u8],
    ordinal: u32,
    contract_address: Vec<u8>,
    block_number: u64,
    timestamp: Option<Timestamp>,
    event: ERC1155TransferBatchEvent,
) -> Vec<transfers::Transfer> {
    if event.ids.len() != event.values.len() {
        log::info!("There is a different count for ids ({}) and values ({}) in transaction {} for log at block index {}, ERC1155 spec says lenght should match, ignoring the log completely for now",
            event.ids.len(),
            event.values.len(),
            Hex(&hash).to_string(),
            ordinal,
        );

        return vec![];
    }

    event
        .ids
        .iter()
        .enumerate()
        .map(|(i, id)| {
            let value = event.values.get(i).unwrap();

            new_erc1155_transfer(
                hash,
                ordinal,
                &event.from,
                &event.to,
                id,
                value,
                &event.operator,
                contract_address.clone(),
                block_number,
                timestamp.clone(),
            )
        })
        .collect()
}

fn new_erc1155_transfer(
    hash: &[u8],
    ordinal: u32,
    from: &[u8],
    to: &[u8],
    token_id: &BigInt,
    quantity: &BigInt,
    operator: &[u8],
    contract_address: Vec<u8>,
    block_number: u64,
    timestamp: Option<Timestamp>,
) -> transfers::Transfer {
    transfers::Transfer {
        schema: schema_to_string(Schema::Erc1155),
        from_address: from.to_vec(),
        to_address: to.to_vec(),
        quantity: quantity.to_string(),
        tx_hash: hash.to_vec(),
        ordinal: ordinal as u64,
        operator: operator.to_vec(),
        token_id: token_id.to_bytes_be().1,
        contract_address: contract_address,
        block_number: block_number,
        timestamp: timestamp,
    }
}

fn schema_to_string(schema: Schema) -> String {
    match schema {
        Schema::Erc20 => "erc20",
        Schema::Erc721 => "erc721",
        Schema::Erc1155 => "erc1155",
    }
    .to_string()
}

fn transform_erc721_transfers_to_database_changes(
    changes: &mut DatabaseChanges,
    transfer: transfers::Transfer,
) {
    let unique_key = |transfer: &transfers::Transfer| {
        format!(
            "{}-{}-{}-{}-{}-{}",
            Hex(&transfer.contract_address),
            Hex(&transfer.token_id),
            Hex(&transfer.from_address),
            Hex(&transfer.to_address),
            Hex(&transfer.tx_hash),
            &transfer.ordinal
        )
    };

    log::info!("Found a transfer {}", unique_key(&transfer));

    push_create(
        changes,
        &unique_key(&transfer),
        transfer.ordinal,
        transfer,
    )
}

fn push_create(
    changes: &mut DatabaseChanges,
    key: &str,
    ordinal: u64,
    value: transfers::Transfer,
) {
    changes
        .push_change("transfers", key, ordinal, Operation::Create)
        .change("schema", (None, value.schema))
        .change("block_number", (None, value.block_number))
        .change("from_address", (None, Hex(value.from_address)))
        .change("to_address", (None, Hex(value.to_address)))
        .change("operator", (None, Hex(value.operator)))
        .change("quantity", (None, value.quantity))
        .change("token_id", (None, Hex(value.token_id)))
        .change("contract_address", (None, Hex(value.contract_address)))
        .change("tx_hash", (None, Hex(value.tx_hash)))
        .change("ordinal", (None, value.ordinal))
        .change("timestamp", (None, value.timestamp.unwrap()));
}