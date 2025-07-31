// driver-service/src/kafka.js

import { Kafka, logLevel } from 'kafkajs';

const kafka = new Kafka({
    brokers: [process.env.KAFKA_BROKER || 'kafka:9092'],
    logLevel: logLevel.ERROR,
    retry: {
        initialRetryTime: 5000,
        retries: 15,
        maxRetryTime: 60000,
        factor: 0.2,
        multiplier: 1.5
    }
});

const consumer = kafka.consumer({
    groupId: 'driver-group',
    sessionTimeout: parseInt(process.env.KAFKA_CONSUMER_SESSION_TIMEOUT, 10) || 30000,
    heartbeatInterval: parseInt(process.env.KAFKA_CONSUMER_HEARTBEAT_INTERVAL, 10) || 3000,
    maxBytesPerPartition: 1048576,
    readUncommitted: false,
    retry: {
        maxRetryTime: 30000,
        initialRetryTime: 1000
    }
});

let isRunning = false;

export async function consume(topic, callback) {
    try {
        if (isRunning) {
            console.warn('Consumer is already running');
            return;
        }

        console.log('Connecting consumer...');
        await consumer.connect();
        isRunning = true;

        console.log(`Subscribing to topic ${topic}`);
        await consumer.subscribe({
            topic: topic || process.env.KAFKA_TOPIC_ORDERS,
            fromBeginning: false
        });

        console.log('Starting consumer...');
        await consumer.run({
            autoCommit: true,
            autoCommitInterval: 5000,
            autoCommitThreshold: 100,
            partitionsConsumedConcurrently: 3,
            eachMessage: async ({ topic, partition, message }) => {
                try {
                    const value = JSON.parse(message.value.toString());
                    console.log(`Processing message`, {
                        topic,
                        partition,
                        offset: message.offset,
                        timestamp: message.timestamp,
                        key: message.key?.toString(),
                        size: message.size
                    });

                    await callback(value);
                } catch (err) {
                    console.error('Error processing message:', {
                        error: err.message,
                        stack: err.stack,
                        topic,
                        partition,
                        offset: message.offset
                    });
                }
            }
        });
    } catch (err) {
        console.error('Consumer error:', {
            error: err.message,
            stack: err.stack
        });

        isRunning = false;
        try {
            await consumer.disconnect();
        } catch (disconnectErr) {
            console.error('Error disconnecting consumer:', disconnectErr);
        }

        setTimeout(() => consume(topic, callback), 10000);
    }
}

process.on('SIGTERM', async () => {
    try {
        if (isRunning) {
            await consumer.disconnect();
            console.log('Kafka consumer disconnected gracefully');
        }
    } catch (err) {
        console.error('Error disconnecting consumer:', err);
    } finally {
        process.exit(0);
    }
});