const { kafka, TOPICS } = require('../config/kafka');
const { sendToDeadLetterQueue } = require('../producer/producer');
const {
    getEvent,
    markProcessed,
    markFailed
} = require('../utils/eventTracker');
const { retryWithExponentialBackoff } = require('../utils/retry');

const consumer = kafka.consumer({
    groupId: process.env.KAFKA_GROUP_ID || 'post-publishing-scheduler'
});
let connected = false;
const maxAttempts = Number(process.env.MAX_ATTEMPTS || 5);
const baseDelayMs = Number(process.env.RETRY_BASE_DELAY_MS || 1000);

if (!Number.isInteger(maxAttempts) || maxAttempts < 1 || maxAttempts > 5) {
    throw new Error('MAX_ATTEMPTS must be an integer between 1 and 5.');
}
if (!Number.isInteger(baseDelayMs) || baseDelayMs < 0) {
    throw new Error('RETRY_BASE_DELAY_MS must be a non-negative integer.');
}

function parseMessage(value) {
    const rawValue = value.toString();
    try {
        return { event: JSON.parse(rawValue), rawValue };
    } catch (error) {
        return { error, rawValue };
    }
}

function isValidEvent(event) {
    return event
        && typeof event.eventId === 'string'
        && event.eventId.length > 0
        && event.post
        && typeof event.post.id === 'string'
        && typeof event.post.content === 'string';
}

async function publishPost(post, attempt) {
    if (Number.isInteger(post.demoFailures) && attempt <= post.demoFailures) {
        throw new Error(`Simulated demo failure ${attempt} for post "${post.id}".`);
    }

    // Replace this demo action with the publishing platform's API call.
    console.log(`Publishing post "${post.id}": ${post.content}`);
}

async function deadLetter(
    { eventId, rawValue, error, attempts },
    {
        sendToDlq = sendToDeadLetterQueue,
        recordFailed = markFailed
    } = {}
) {
    await sendToDlq({
        eventId: eventId || null,
        originalMessage: rawValue,
        error: error.message,
        attempts,
        failedAt: new Date().toISOString()
    });

    if (eventId) {
        await recordFailed(eventId, error, attempts);
    }
    console.error(
        `Sent event ${eventId || '(unknown event)'} to ${TOPICS.PUBLISH_DLQ} after ${attempts} attempt(s): ${error.message}`
    );
}

async function handleMessage(value, dependencies = {}) {
    const {
        publish = publishPost,
        sendToDlq = sendToDeadLetterQueue,
        findEvent = getEvent,
        recordProcessed = markProcessed,
        recordFailed = markFailed
    } = dependencies;
    const deadLetterDependencies = { sendToDlq, recordFailed };
    const { event, error: parseError, rawValue } = parseMessage(value);
    if (parseError) {
        await deadLetter({
            eventId: null,
            rawValue,
            error: new Error(`Message is not valid JSON: ${parseError.message}`),
            attempts: 1
        }, deadLetterDependencies);
        return;
    }

    if (!isValidEvent(event)) {
        await deadLetter({
            eventId: event && typeof event.eventId === 'string'
                ? event.eventId
                : null,
            rawValue,
            error: new Error('Message is missing eventId, post.id, or post.content.'),
            attempts: 1
        }, deadLetterDependencies);
        return;
    }

    const previousEvent = await findEvent(event.eventId);
    if (previousEvent && ['processed', 'failed'].includes(previousEvent.status)) {
        console.log(`Skipping already completed event ${event.eventId} (${previousEvent.status}).`);
        return;
    }

    let attempts;
    try {
        await retryWithExponentialBackoff(
            (attempt) => publish(event.post, attempt),
            {
                maxAttempts,
                baseDelayMs,
                onRetry: ({ attempt, delayMs, error }) => {
                    console.warn(
                        `Publishing event ${event.eventId} failed on attempt ${attempt}; retrying in ${delayMs}ms: ${error.message}`
                    );
                }
            }
        );
    } catch (error) {
        attempts = error.attempts || maxAttempts;
        await deadLetter({
            eventId: event.eventId,
            rawValue,
            error,
            attempts
        }, deadLetterDependencies);
        return;
    }

    await (dependencies.recordProcessed || markProcessed)(event.eventId);
    console.log(`Successfully processed event ${event.eventId}.`);
}

async function startConsumer(onFatalError) {
    await consumer.connect();
    connected = true;
    await consumer.subscribe({
        topic: TOPICS.PUBLISH_REQUESTS,
        fromBeginning: true
    });
    console.log(`Kafka consumer connected; listening to ${TOPICS.PUBLISH_REQUESTS}.`);

    const runPromise = consumer.run({
        eachMessage: async ({ topic, message }) => {
            if (topic !== TOPICS.PUBLISH_REQUESTS) {
                return;
            }
            await handleMessage(message.value || Buffer.alloc(0));
        }
    });

    runPromise.catch((error) => {
        console.error('Kafka consumer stopped unexpectedly:', error);
        if (onFatalError) {
            onFatalError(error);
        }
    });
}

async function disconnectConsumer() {
    if (connected) {
        await consumer.disconnect();
        connected = false;
        console.log('Kafka consumer disconnected.');
    }
}

module.exports = { startConsumer, disconnectConsumer, handleMessage };