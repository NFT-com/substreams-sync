// Import the required modules
mod abi;
mod pb;
mod block_timestamp;

use pb::transfers;
use pb::transfers::transfer::Schema;
use substreams::store::{self, DeltaProto, StoreNew, StoreSetIfNotExists, StoreSetIfNotExistsProto};
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

 // let timestamp = BlockTimestamp::from_block(&blk);
    // let header = blk.header.as_ref().unwrap();

    // let transfers: Vec<transfers::Transfer> = (&*TRACKED_CONTRACTS)
    //     .iter() // non parallel execution primarily due to trying parallel and getting this error
    //     // substreams encountered an error
    //     // {"error": "receive stream next message: rpc error: code =
    //     // Internal desc = error building pipeline: failed setup request: parallel processing run:
    //     // scheduler run: process job result for target \"store_transfers\": worker ended in error: receiving stream resp:
    //     // rpc error: code = Internal desc = panic at block #14894146 (980beeda46767b72194cd169cbcd0cb4abbe308d2048571f4068f4c498f2e0e8):
    //     // cannot Set or Del a value on a state.Builder with an ordinal lower than the previous"}
    //     // @Gavin: seems like the ordinals in parallel don't work very well as they can be lower than the previous ordinal in a parallel setup
    //     .flat_map(|&contract_address| {
    //         blk.events::<abi::erc721::events::Transfer>(&[contract_address]) // Pass a slice of contract addresses
    //             .map(|(transfer, log)| {
    //                 substreams::log::info!("NFT Transfer seen");

    //                 transfers::Transfer {
    //                     block_number: blk.number,
    //                     from_address: transfer.from,
    //                     to_address: transfer.to,
    //                     contract_address: log.address().to_vec(),
    //                     token_id: transfer.token_id.to_bytes_be().1,
    //                     tx_hash: log.receipt.transaction.hash.clone(),
    //                     ordinal: log.block_index() as u64,
    //                     timestamp: Some(header.timestamp.as_ref().unwrap().clone()),
    //                 }
    //             })
    //             .collect::<Vec<transfers::Transfer>>()
    //     })
    //     .collect();
fn transform_block_to_transfers(blk: ethpb::eth::v2::Block) -> (BlockTimestamp, Vec<transfers::Transfer>) {
    let header = blk.header.as_ref().unwrap();
    let timestamp = BlockTimestamp::from_block(&blk);

    let transfers: Vec<transfers::Transfer> = blk.receipts().flat_map(|receipt| {
        let hash = &receipt.transaction.hash;
        let timestamp = Some(header.timestamp.as_ref().unwrap().clone());

        receipt.receipt.logs.iter().flat_map(move |log| {
            let erc20_transfers = ERC20TransferEvent::match_and_decode(log).map(|event| new_erc20_transfer(
                hash,
                log.block_index,
                log.address.to_vec(),
                blk.number,
                timestamp.clone(),
                event
            ));

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

/// Parses block and saves to store
#[substreams::handlers::store]
fn store_transfers(blk: ethpb::eth::v2::Block, s: StoreSetIfNotExistsProto<transfers::Transfer>) {
    let (_timestamp, transfers) = transform_block_to_transfers(blk);

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

    // for loop over transfers
    for transfer in transfers {
        log::info!("Found a transfer {}", unique_key(&transfer));
        s.set_if_not_exists(transfer.ordinal, unique_key(&transfer), &transfer);
    }
}

#[substreams::handlers::map]
fn db_out(
    erc_transfer_start: store::Deltas<DeltaProto<transfers::Transfer>>,
) -> Result<DatabaseChanges, substreams::errors::Error> {
    let mut database_changes: DatabaseChanges = Default::default();
    transform_erc721_transfers_to_database_changes(&mut database_changes, erc_transfer_start);
    Ok(database_changes)
}

fn transform_erc721_transfers_to_database_changes(
    changes: &mut DatabaseChanges,
    deltas: store::Deltas<DeltaProto<transfers::Transfer>>,
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

fn push_update(
    changes: &mut DatabaseChanges,
    key: &str,
    ordinal: u64,
    old_value: transfers::Transfer,
    new_value: transfers::Transfer,
) {
    changes
        .push_change("transfers", key, ordinal, Operation::Update)
        .change("schema", (old_value.schema, new_value.schema))
        .change("block_number", (old_value.block_number, new_value.block_number))
        .change("from_address", (Hex(old_value.from_address), Hex(new_value.from_address)))
        .change("to_address", (Hex(old_value.to_address), Hex(new_value.to_address)))
        .change("operator", (Hex(old_value.operator), Hex(new_value.operator)))
        .change("quantity", (old_value.quantity, new_value.quantity))
        .change(
            "token_id",
            (Hex(old_value.token_id), Hex(new_value.token_id)),
        )
        .change("contract_address", (Hex(old_value.contract_address), Hex(new_value.contract_address)))
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
