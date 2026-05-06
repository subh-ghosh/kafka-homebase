import fs from 'node:fs';
import { Kafka } from 'kafkajs';

const bootstrap = process.env.KAFKA_BOOTSTRAP;
const topic = process.env.KAFKA_TOPIC ?? 'app1.events';
const username = process.env.KAFKA_USERNAME;
const password = process.env.KAFKA_PASSWORD;
const caPath = process.env.KAFKA_CA_LOCATION ?? './ca.crt';
const groupId = process.env.KAFKA_GROUP ?? 'app1';

if (!bootstrap || !username || !password) {
    throw new Error('Missing env: KAFKA_BOOTSTRAP, KAFKA_USERNAME, KAFKA_PASSWORD');
}

const kafka = new Kafka({
    clientId: 'node-consumer',
    brokers: [bootstrap],
    ssl: { ca: [fs.readFileSync(caPath, 'utf8')] },
    sasl: { mechanism: 'scram-sha-512', username, password },
});

const consumer = kafka.consumer({ groupId });
await consumer.connect();
await consumer.subscribe({ topic, fromBeginning: true });

await consumer.run({
    eachMessage: async ({ topic, partition, message }) => {
        console.log(`${topic}[${partition}]@${message.offset}: ${message.value?.toString()}`);
    },
});
