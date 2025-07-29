// notification-service/src/kafka.js

import { Kafka } from 'kafkajs';

const kafka = new Kafka({
    brokers: [process.env.KAFKA_BROKER || 'kafka:9092'],
    retry: {
        initialRetryTime: 100,
        retries: 10
    }
});

const consumer = kafka.consumer({
    groupId: process.env.KAFKA_GROUP_ID || 'notification-group',
    sessionTimeout: parseInt(process.env.KAFKA_CONSUMER_SESSION_TIMEOUT, 10) || 30000
});

async function start() {
    try {
        await consumer.connect();
        console.log('Consumer connected to Kafka');

        await consumer.subscribe({
            topic: process.env.KAFKA_TOPIC_ORDERS || 'order-created',
            fromBeginning: true
        });

        console.log(`Subscribed to topic: ${process.env.KAFKA_TOPIC_ORDERS}`);

        await consumer.run({
            eachMessage: async ({ message }) => {
                try {
                    const order = JSON.parse(message.value.toString());
                    console.log(`Notification: Order ${order.id} created`, order);
                } catch (err) {
                    console.error('Error processing notification:', err);
                }
            },
        });
    } catch (err) {
        console.error('Error in notification service:', err);
        process.exit(1);
    }
}

start();