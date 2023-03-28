import { Client } from 'pg';

const connectionString = 'postgres://dev-node:insecure-change-me-in-prod@localhost:5432/dev-node?sslmode=disable';

const client = new Client({
  connectionString,
});

client.connect();

client.query('LISTEN transfers');

console.log('Waiting for new inserts...');

client.on('notification', (msg: any) => {
  console.log(`Received notification: ${msg.channel} ${msg.payload}`);
  console.log(`New row data: ${msg.payload}`);
});
