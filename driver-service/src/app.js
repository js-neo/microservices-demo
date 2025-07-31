// driver-service/src/app.js

import express from 'express';
import { consume } from './kafka.js';

const app = express();
app.use(express.json());

app.post('/drivers/assign', (req, res) => {
    const { orderId } = req.body;
    const response = {
        orderId,
        driver: {
            id: 123,
            name: 'John Doe'
        }
    };
    console.log('Assigning driver:', response);
    res.json(response);
});

consume(process.env.KAFKA_TOPIC_ORDERS, (message) => {
    console.log(`DriverService: Processing order ${message.id}`);
});

app.listen(3002, () => {
    console.log('Driver service started on port 3002');
    console.log(`Kafka broker: ${process.env.KAFKA_BROKER}`);
    console.log(`Kafka topic: ${process.env.KAFKA_TOPIC_ORDERS}`);
});