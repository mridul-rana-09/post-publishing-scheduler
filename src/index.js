require('dotenv').config();

const { kafka, TOPICS } = require('./config/kafka');
const {
    connectProducer,
    disconnectProducer
} = require('./producer/producer');
const {
    startConsumer,
    disconnectConsumer
} = require('./consumer/consumer');
const { startScheduler, stopScheduler } = require('./scheduler/scheduler');

let isShuttingDown = false;

async function ensureTopicsExist() {
    const admin = kafka.admin();
    await admin.connect();
    try {
        await admin.createTopics({
            waitForLeaders: true,
            topics: [
                { topic: TOPICS.PUBLISH_REQUESTS, numPartitions: 1, replicationFactor: 1 },
                { topic: TOPICS.PUBLISH_DLQ, numPartitions: 1, replicationFactor: 1 }
            ]
        });
    } finally {
        await admin.disconnect();
    }
    console.log(`Kafka topics ready: ${TOPICS.PUBLISH_REQUESTS}, ${TOPICS.PUBLISH_DLQ}.`);
}

async function shutdown(exitCode = 0) {
    if (isShuttingDown) {
        return;
    }
    isShuttingDown = true;
    process.exitCode = exitCode;
    stopScheduler();

    const cleanupResults = await Promise.allSettled([
        disconnectConsumer(),
        disconnectProducer()
    ]);

    cleanupResults.forEach((result) => {
        if (result.status === 'rejected') {
            console.error('Error during Kafka shutdown:', result.reason);
            process.exitCode = 1;
        }
    });
}

async function main() {
    await ensureTopicsExist();
    await connectProducer();
    await startConsumer(() => {
        void shutdown(1);
    });
    startScheduler();

    process.once('SIGINT', () => void shutdown());
    process.once('SIGTERM', () => void shutdown());
}

main().catch(async (error) => {
    console.error('Application startup failed:', error);
    await shutdown(1);
});