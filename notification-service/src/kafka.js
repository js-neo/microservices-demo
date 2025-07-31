// notification-service/src/kafka.js

import { Kafka, logLevel } from 'kafkajs';

const kafka = new Kafka({
    brokers: [process.env.KAFKA_BROKER || 'kafka:9092'],
    logLevel: logLevel.INFO,
    retry: {
        initialRetryTime: 3000,
        retries: 15,
        maxRetryTime: 30000,
        factor: 0.2,
        multiplier: 2,
        restartOnFailure: async (error) => {
            return ![
                'KafkaJSNumberOfRetriesExceeded',
                'KafkaJSNonRetriableError'
            ].includes(error.name);
        }
    }
});

const consumer = kafka.consumer({
    groupId: process.env.KAFKA_GROUP_ID || 'notification-group',
    sessionTimeout: parseInt(process.env.KAFKA_CONSUMER_SESSION_TIMEOUT, 10) || 45000,
    heartbeatInterval: parseInt(process.env.KAFKA_CONSUMER_HEARTBEAT_INTERVAL, 10) || 15000,
    maxBytesPerPartition: 1048576,
    readUncommitted: false,
    retry: {
        maxRetryTime: 30000,
        initialRetryTime: 1000,
        restartOnFailure: true
    }
});

const consumerStats = {
    startTime: new Date().toISOString(),
    messagesProcessed: 0,
    lastMessageTime: null,
    errors: 0
};

async function checkTopicExists(admin, topic) {
    try {
        const topics = await admin.listTopics();
        return topics.includes(topic);
    } catch (err) {
        console.error('Error checking topic existence:', err);
        return false;
    }
}

async function startConsumer() {
    let isRunning = false;
    let retryCount = 0;
    const MAX_RETRIES = 5;
    const admin = kafka.admin();

    const runConsumer = async () => {
        try {
            if (isRunning) return;

            console.log('Initializing notification consumer...');

            await admin.connect();
            const topic = process.env.KAFKA_TOPIC_ORDERS || 'order-created';

            let topicExists = await checkTopicExists(admin, topic);
            while (!topicExists && retryCount < MAX_RETRIES) {
                console.log(`Topic ${topic} not found, retrying... (${retryCount + 1}/${MAX_RETRIES})`);
                await new Promise(resolve => setTimeout(resolve, 5000));
                topicExists = await checkTopicExists(admin, topic);
                retryCount++;
            }

            if (!topicExists) {
                throw new Error(`Topic ${topic} does not exist after ${MAX_RETRIES} retries`);
            }

            console.log('Connecting consumer...');
            await consumer.connect();
            isRunning = true;
            retryCount = 0;

            console.log(`Subscribing to topic ${topic}`);
            await consumer.subscribe({ topic, fromBeginning: false });

            console.log('Starting consumer processing...');
            await consumer.run({
                autoCommit: true,
                autoCommitInterval: 10000,
                autoCommitThreshold: 50,
                eachMessage: async ({ topic, partition, message }) => {
                    const startTime = Date.now();
                    try {
                        const order = JSON.parse(message.value.toString());
                        consumerStats.messagesProcessed++;
                        consumerStats.lastMessageTime = new Date().toISOString();

                        console.log('Processing notification:', {
                            orderId: order.id,
                            topic,
                            partition,
                            offset: message.offset,
                            timestamp: message.timestamp,
                            processingTime: `${Date.now() - startTime}ms`
                        });

                        console.log(`Sending notification for order ${order.id}`);

                    } catch (err) {
                        consumerStats.errors++;
                        console.error('Error processing message:', {
                            error: err.message,
                            topic,
                            partition,
                            offset: message.offset,
                            processingTime: `${Date.now() - startTime}ms`
                        });
                    }
                }
            });

        } catch (err) {
            console.error('Consumer error:', {
                error: err.message,
                stack: err.stack,
                retryCount: `${retryCount + 1}/${MAX_RETRIES}`
            });

            isRunning = false;
            try {
                await consumer.disconnect();
                await admin.disconnect();
            } catch (disconnectErr) {
                console.error('Error during disconnection:', disconnectErr);
            }

            if (retryCount >= MAX_RETRIES) {
                console.error('Max retries reached, exiting...');
                process.exit(1);
            }

            const delay = Math.min(1000 * Math.pow(2, retryCount), 30000);
            console.log(`Retrying in ${delay}ms...`);
            setTimeout(runConsumer, delay);
            retryCount++;
        }
    };

    const shutdown = async () => {
        try {
            console.log('Shutting down consumer gracefully...');
            if (isRunning) {
                await consumer.disconnect();
                await admin.disconnect();
                console.log('Consumer disconnected gracefully');
            }
            process.exit(0);
        } catch (err) {
            console.error('Error during shutdown:', err);
            process.exit(1);
        }
    };

    process.on('SIGTERM', shutdown);
    process.on('SIGINT', shutdown);

    await runConsumer();
}

startConsumer().catch(err => {
    console.error('Fatal error during consumer startup:', err);
    process.exit(1);
});