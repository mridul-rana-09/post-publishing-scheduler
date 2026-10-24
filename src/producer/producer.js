const { kafka, TOPICS } = require('../config/kafka');

const producer = kafka.producer();
let connected = false;

async function connectProducer() {
    if (!connected) {
        await producer.connect();
        connected = true;
        console.log('Kafka producer connected.');
    }
}

async function publishRequest(event) {
    await connectProducer();

    await producer.send({
        topic: TOPICS.PUBLISH_REQUESTS,
        messages: [{
            key: event.eventId,
            value: JSON.stringify(event)
        }]
    });
}

async function sendToDeadLetterQueue(message) {
    await connectProducer();

    await producer.send({
        topic: TOPICS.PUBLISH_DLQ,
        messages: [{
            key: message.eventId || null,
            value: JSON.stringify(message)
        }]
    });
}

async function disconnectProducer() {
    if (connected) {
        await producer.disconnect();
        connected = false;
        console.log('Kafka producer disconnected.');
    }
}

module.exports = {
    connectProducer,
    publishRequest,
    sendToDeadLetterQueue,
    disconnectProducer
};