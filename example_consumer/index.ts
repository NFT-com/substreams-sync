import { Client } from 'pg';

const connectionString = 'postgres://dev-node:insecure-change-me-in-prod@localhost:5432/dev-node?sslmode=disable';

const client = new Client({
  connectionString,
});

client.connect();

client.query('LISTEN transfers');

console.log('Waiting for new inserts...');

client.on('notification', (msg: any) => {
  const [schema, blockNumber, tokenId, contractAddress, quantity, fromAddress, toAddress, txHash, timestamp] = msg.payload.split("|");

  const returnString = `${schema} found at block ${blockNumber}, contract=${contractAddress}, tokenId=${tokenId}, from=${fromAddress}, to=${toAddress}, ${quantity > 1 ? `quantity=${quantity}, ` : ''}txHash=${txHash}, timestamp=${timestamp}`;
  if (fromAddress == '0000000000000000000000000000000000000000') {
    console.log(`[MINT]: ${returnString}`);
  } else if (toAddress == '0000000000000000000000000000000000000000') {
    console.log(`[BURN]: ${returnString}`);
  } else {
    console.log(`[TRANSFER]: ${returnString}`);
  }
});
