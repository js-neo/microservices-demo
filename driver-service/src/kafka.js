// driver-service/src/kafka.js

import { Kafka } from 'kafkajs';

const kafka = new Kafka({
    brokers: [process.env.KAFKA_BROKER || 'kafka:9092'],
    retry: {
        initialRetryTime: 100,
        retries: 10
    }
});

const consumer = kafka.consumer({
    groupId: 'driver-group',
    sessionTimeout: parseInt(process.env.KAFKA_CONSUMER_SESSION_TIMEOUT, 10) || 30000,
});

let isConnected = false;

async function ensureConnection() {
    if (!isConnected) {
        await consumer.connect();
        isConnected = true;
    }
}

export async function consume(topic, callback) {
    try {
        await ensureConnection();
        await consumer.subscribe({
            topic: topic || process.env.KAFKA_TOPIC_ORDERS,
            fromBeginning: true
        });

        await consumer.run({
            eachMessage: async ({ message }) => {
                try {
                    const value = JSON.parse(message.value.toString());
                    console.log(`Received message from topic ${topic}:`, value);
                    callback(value);
                } catch (err) {
                    console.error('Error processing message:', err);
                }
            }
        });
    } catch (err) {
        console.error('Error in consumer:', err);
        throw err;
    }
}