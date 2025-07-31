// order-service/src/kafka.js

import { Kafka, Partitioners, logLevel } from 'kafkajs';

const kafka = new Kafka({
    brokers: [process.env.KAFKA_BROKER || 'kafka:9092'],
    logLevel: logLevel.ERROR,
    retry: {
        initialRetryTime: 5000,
        retries: 10,
        maxRetryTime: 60000,
        factor: 0.2,
        multiplier: 1.5
    }
});

const producer = kafka.producer({
    createPartitioner: Partitioners.DefaultPartitioner,
    allowAutoTopicCreation: true,
    idempotent: process.env.KAFKA_PRODUCER_IDEMPOTENCE === 'true',
    transactionTimeout: 30000
});

let isConnected = false;
let connectionPromise = null;

async function ensureConnection() {
    if (isConnected) return;

    if (!connectionPromise) {
        connectionPromise = (async () => {
            try {
                console.log('Connecting to Kafka producer...');
                await producer.connect();
                isConnected = true;
                console.log('Kafka producer connected successfully');
            } catch (err) {
                console.error('Failed to connect to Kafka:', err);
                isConnected = false;
                connectionPromise = null;
                throw err;
            }
        })();
    }

    return connectionPromise;
}

export async function produce(topic, message) {
    try {
        await ensureConnection();

        const result = await producer.send({
            topic: topic || process.env.KAFKA_TOPIC_ORDERS,
            messages: [{
                value: JSON.stringify(message),
                timestamp: Date.now()
            }],
            acks: -1
        });

        console.log(`Produced message to topic ${topic}`, {
            message,
            result: {
                topicName: result[0].topicName,
                partition: result[0].partition,
                offset: result[0].offset
            }
        });

        return result;
    } catch (err) {
        console.error('Error producing message:', {
            error: err.message,
            stack: err.stack,
            topic,
            message
        });

        if (err.name === 'KafkaJSConnectionError' ||
            err.name === 'KafkaJSNonRetriableError') {
            isConnected = false;
            connectionPromise = null;
        }

        throw err;
    }
}

process.on('SIGTERM', async () => {
    try {
        if (isConnected) {
            await producer.disconnect();
            console.log('Kafka producer disconnected gracefully');
        }
    } catch (err) {
        console.error('Error disconnecting producer:', err);
    } finally {
        process.exit(0);
    }
});