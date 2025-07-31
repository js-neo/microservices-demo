// order-service/src/app.js

import express from 'express';
import axios from 'axios';
import { produce } from './kafka.js';

const app = express();
app.use(express.json());

app.get('/health', (req, res) => {
    res.status(200).json({
        status: 'OK',
        timestamp: new Date().toISOString(),
        service: 'order-service',
        version: '2025.07'
    });
});

app.get('/metrics', async (req, res) => {
    try {
        res.json({
            kafkaStatus: 'connected',
            uptime: process.uptime(),
            memoryUsage: process.memoryUsage()
        });
    } catch (err) {
        res.status(500).json({ error: err.message });
    }
});

app.post('/orders', async (req, res) => {
    const startTime = Date.now();
    try {
        console.log('Received order creation request');
        const { clientId, pickupLocation } = req.body;

        if (!clientId || !pickupLocation) {
            return res.status(400).json({
                success: false,
                error: 'Missing required fields: clientId and pickupLocation',
                timestamp: new Date().toISOString()
            });
        }

        const order = {
            id: Date.now(),
            clientId,
            pickupLocation,
            status: 'pending',
            createdAt: new Date().toISOString(),
            serviceVersion: '2025.07'
        };

        console.log('Creating order:', order);

        const kafkaStart = Date.now();
        await produce('order-created', order);
        const kafkaTime = Date.now() - kafkaStart;

        const driverStart = Date.now();
        const driverResponse = await axios.post('http://driver-service:3002/drivers/assign', {
            orderId: order.id
        }, {
            timeout: 5000
        });
        const driverTime = Date.now() - driverStart;

        res.json({
            success: true,
            metadata: {
                kafkaLatencyMs: kafkaTime,
                driverServiceLatencyMs: driverTime,
                totalProcessingTimeMs: Date.now() - startTime,
                processedAt: new Date().toISOString()
            },
            order,
            driver: driverResponse.data
        });

    } catch (err) {
        console.error('Error creating order:', err);
        const errorResponse = {
            success: false,
            error: err.message,
            timestamp: new Date().toISOString(),
            processingTimeMs: Date.now() - startTime
        };

        if (err.response) {
            errorResponse.details = err.response.data;
        }

        res.status(500).json(errorResponse);
    }
});

const PORT = process.env.PORT || 3001;
app.listen(PORT, () => {
    console.log(`Order service started on port ${PORT}`);
    console.log(`Kafka broker: ${process.env.KAFKA_BROKER}`);
    console.log(`Kafka topic: ${process.env.KAFKA_TOPIC_ORDERS}`);
    console.log(`Service version: 2025.07`);
});