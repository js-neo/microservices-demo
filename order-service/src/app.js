import express from 'express';
import axios from 'axios';
import { produce } from './kafka.js';

const app = express();
app.use(express.json());

app.post('/orders', async (req, res) => {
    try {
        console.log('Received order creation request');
        const { clientId, pickupLocation } = req.body;
        const order = {
            id: Date.now(),
            clientId,
            pickupLocation,
            status: 'pending',
            createdAt: new Date().toISOString()
        };

        console.log('Creating order:', order);

        console.log('Sending to Kafka...');
        await produce('order-created', order);
        console.log('Sent to Kafka successfully');

        console.log('Assigning driver...');
        const response = await axios.post('http://driver-service:3002/drivers/assign', {
            orderId: order.id
        }, {
            timeout: 5000
        });
        console.log('Driver assigned:', response.data);

        res.json({
            success: true,
            order,
            driver: response.data
        });
    } catch (err) {
        console.error('Error creating order:', err);
        res.status(500).json({
            success: false,
            error: err.message
        });
    }
});

app.listen(3001, () => {
    console.log('Order service started on port 3001');
    console.log(`Kafka broker: ${process.env.KAFKA_BROKER}`);
    console.log(`Kafka topic: ${process.env.KAFKA_TOPIC_ORDERS}`);
});