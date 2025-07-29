// order-service/src/kafka.js

import { Kafka, Partitioners, logLevel } from 'kafkajs';

console.log(`Using Kafka broker: ${process.env.KAFKA_BROKER || 'kafka:9092'}`);

const kafka = new Kafka({
    brokers: [process.env.KAFKA_BROKER || 'kafka:9092'],
    logLevel: logLevel.ERROR,
    retry: {
        initialRetryTime: 5000,
        retries: 10
    }
});

const producer = kafka.producer({
    createPartitioner: Partitioners.DefaultPartitioner,
    allowAutoTopicCreation: true
});

let isConnected = false;

async function ensureConnection() {
    if (!isConnected) {
        console.log('Connected to Kafka...');
        await producer.connect();
        isConnected = true;
        console.log('Connected to Kafka successfully.');
    }
}

export async function produce(topic, message) {
    try {
        await ensureConnection();
        await producer.send({
            topic: topic || process.env.KAFKA_TOPIC_ORDERS,
            messages: [{ value: JSON.stringify(message) }]
        });
        console.log(`Produced message to topic ${topic}:`, message);
    } catch (err) {
        console.error('Error producing message:', err);
        throw err;
    }
}