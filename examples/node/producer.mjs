import fs from 'node:fs';
import { Kafka } from 'kafkajs';

const bootstrap = process.env.KAFKA_BOOTSTRAP; // host:9092
const topic = process.env.KAFKA_TOPIC ?? 'app1.events';
const username = process.env.KAFKA_USERNAME;
const password = process.env.KAFKA_PASSWORD;
const caPath = process.env.KAFKA_CA_LOCATION ?? './ca.crt';

if (!bootstrap || !username || !password) {
    throw new Error('Missing env: KAFKA_BOOTSTRAP, KAFKA_USERNAME, KAFKA_PASSWORD');
}

const kafka = new Kafka({
    clientId: 'node-producer',
    brokers: [bootstrap],
    ssl: { ca: [fs.readFileSync(caPath, 'utf8')] },
    sasl: { mechanism: 'scram-sha-512', username, password },
});

const producer = kafka.producer();
await producer.connect();
await producer.send({
    topic,
    messages: [{ value: JSON.stringify({ hello: 'world' }) }],
});
await producer.disconnect();
console.log('sent');
