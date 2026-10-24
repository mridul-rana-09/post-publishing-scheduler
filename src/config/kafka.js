const { Kafka } = require('kafkajs');

const brokers = (process.env.KAFKA_BROKER || 'localhost:9092')
    .split(',')
    .map((broker) => broker.trim())
    .filter(Boolean);

if (brokers.length === 0) {
    throw new Error('KAFKA_BROKER must contain at least one broker address.');
}

const kafka = new Kafka({
    clientId: process.env.KAFKA_CLIENT_ID || 'post-publishing-scheduler',
    brokers
});

const TOPICS = {
    PUBLISH_REQUESTS: 'publish-requests',
    PUBLISH_DLQ: 'publish-dlq'
};

module.exports = { kafka, TOPICS };
