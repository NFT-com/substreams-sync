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

  if (fromAddress == '0000000000000000000000000000000000000000') {
    console.log(`[MINTED]: ${schema}/${contractAddress}/${tokenId} to ${toAddress}, ${quantity > 1 ? `quantity=${quantity}, ` : ''}`);
  } else if (toAddress == '0000000000000000000000000000000000000000') {
    console.log(`[BURNED]: ${schema}/${contractAddress}/${tokenId} from ${fromAddress}, ${quantity > 1 ? `quantity=${quantity}, ` : ''}`);
  } else {
    console.log(`[TRANSFERRED]: ${schema}/${contractAddress}/${tokenId} from ${fromAddress} to ${toAddress}, ${quantity > 1 ? `quantity=${quantity}, ` : ''}`);
  }
});
