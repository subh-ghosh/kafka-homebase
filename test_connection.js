const { Kafka } = require('kafkajs')

async function run() {
  const kafka = new Kafka({
    clientId: 'test-app',
    brokers: ['broker.subartaghosh.co.in:9092'],
    ssl: { rejectUnauthorized: false },
    sasl: {
      mechanism: 'scram-sha-512',
      username: 'subartaghosh',
      password: '2SYObZBcgwNKZa86!'
    }
  })

  console.log("Connecting Producer...");
  const producer = kafka.producer()
  await producer.connect()
  
  console.log("Sending Message...");
  await producer.send({
    topic: 'subartaghosh.events',
    messages: [{ value: 'Hello from the internet!!' }],
  })
  console.log("Message sent successfully!");
  await producer.disconnect();

  console.log("Connecting Consumer...");
  const consumer = kafka.consumer({ groupId: 'subartaghosh-test-group' })
  await consumer.connect()
  await consumer.subscribe({ topic: 'subartaghosh.events', fromBeginning: true })

  console.log("Waiting for message...");
  await consumer.run({
    eachMessage: async ({ topic, partition, message }) => {
      console.log(`Received message: ${message.value.toString()}`)
      process.exit(0);
    },
  })
}

run().catch(console.error);
