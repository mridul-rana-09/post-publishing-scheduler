const assert = require('node:assert/strict');
const { spawnSync } = require('node:child_process');
const fs = require('node:fs/promises');
const os = require('node:os');
const path = require('node:path');
const { after, test } = require('node:test');

const temporaryDirectory = require('node:fs').mkdtempSync(
    path.join(os.tmpdir(), 'post-publishing-scheduler-')
);
process.env.EVENT_STORE_FILE = path.join(temporaryDirectory, 'events.json');
process.env.MAX_ATTEMPTS = '5';
process.env.RETRY_BASE_DELAY_MS = '0';

const sentMessages = [];
const consumerCalls = { connected: false, subscription: null, runOptions: null };
const mockConsumer = {
    async connect() {
        consumerCalls.connected = true;
    },
    async subscribe(options) {
        consumerCalls.subscription = options;
    },
    run(options) {
        consumerCalls.runOptions = options;
        return Promise.resolve();
    },
    async disconnect() {}
};
const mockKafka = {
    producer() {
        return {
            async connect() {},
            async send(request) {
                sentMessages.push(request);
            },
            async disconnect() {}
        };
    },
    consumer() {
        return mockConsumer;
    }
};
const kafkaJsPath = require.resolve('kafkajs');
require.cache[kafkaJsPath] = {
    id: kafkaJsPath,
    filename: kafkaJsPath,
    loaded: true,
    exports: {
        Kafka: class Kafka {
            constructor() {
                return mockKafka;
            }
        }
    }
};

const { TOPICS } = require('../src/config/kafka');
const producer = require('../src/producer/producer');
const scheduler = require('../src/scheduler/scheduler');
const consumer = require('../src/consumer/consumer');
const eventTracker = require('../src/utils/eventTracker');
const { retryWithExponentialBackoff } = require('../src/utils/retry');

after(async () => {
    await fs.rm(temporaryDirectory, { recursive: true, force: true });
});

test('producer sends requests and dead letters to their configured Kafka topics', async () => {
    await producer.publishRequest({ eventId: 'topic-check', post: { id: 'topic-check' } });
    await producer.sendToDeadLetterQueue({ eventId: 'dlq-check', error: 'test' });

    assert.deepEqual(
        sentMessages.slice(-2).map((message) => message.topic),
        [TOPICS.PUBLISH_REQUESTS, TOPICS.PUBLISH_DLQ]
    );
    assert.equal(sentMessages.at(-2).messages[0].key, 'topic-check');
});

test('consumer subscribes to publish-requests before consuming', async () => {
    await consumer.startConsumer();
    assert.equal(consumerCalls.connected, true);
    assert.deepEqual(consumerCalls.subscription, {
        topic: TOPICS.PUBLISH_REQUESTS,
        fromBeginning: true
    });
    assert.equal(typeof consumerCalls.runOptions.eachMessage, 'function');
    await consumer.disconnectConsumer();
});

test('scheduler queues only due posts and persists the enqueue marker', async () => {
    const postsFile = path.join(temporaryDirectory, 'posts.json');
    const now = Date.parse('2026-10-23T00:00:00.000Z');
    const duePost = {
        id: 'scheduler-due',
        content: 'This post is due.',
        scheduledAt: '2026-10-22T00:00:00.000Z'
    };
    await fs.writeFile(postsFile, JSON.stringify([
        duePost,
        {
            id: 'scheduler-future',
            content: 'This post is not due.',
            scheduledAt: '2035-01-01T00:00:00.000Z'
        }
    ]));

    const initialMessageCount = sentMessages.length;
    await scheduler.scanForDuePosts({ postsFile, now });
    const requests = sentMessages
        .slice(initialMessageCount)
        .filter((message) => message.topic === TOPICS.PUBLISH_REQUESTS);

    assert.equal(requests.length, 1);
    const queuedEvent = JSON.parse(requests[0].messages[0].value);
    assert.equal(queuedEvent.post.id, duePost.id);
    assert.equal((await eventTracker.getEvent(queuedEvent.eventId)).status, 'enqueued');

    await scheduler.scanForDuePosts({ postsFile, now });
    assert.equal(
        sentMessages.filter((message) => (
            message.topic === TOPICS.PUBLISH_REQUESTS
            && message.messages[0].key === queuedEvent.eventId
        )).length,
        1
    );
});

test('retry helper uses exponential delays and never exceeds five attempts', async () => {
    const delays = [];
    let attempts = 0;
    const result = await retryWithExponentialBackoff(
        async (attempt) => {
            attempts += 1;
            if (attempt < 5) {
                throw new Error('retry this attempt');
            }
            return 'published';
        },
        {
            maxAttempts: 5,
            baseDelayMs: 1,
            onRetry: ({ delayMs }) => delays.push(delayMs)
        }
    );

    assert.equal(result, 'published');
    assert.equal(attempts, 5);
    assert.deepEqual(delays, [1, 2, 4, 8]);

    let failedAttempts = 0;
    await assert.rejects(
        retryWithExponentialBackoff(
            async () => {
                failedAttempts += 1;
                throw 'permanent failure';
            },
            { maxAttempts: 5, baseDelayMs: 0 }
        ),
        (error) => error.attempts === 5 && error.message === 'permanent failure'
    );
    assert.equal(failedAttempts, 5);
    await assert.rejects(
        retryWithExponentialBackoff(async () => {}, { maxAttempts: 6 }),
        RangeError
    );
});

test('consumer retries, sends permanent failures to the DLQ, and persists the failed state', async () => {
    const event = {
        eventId: 'permanent-failure',
        post: { id: 'demo-dlq', content: 'Always fails.' }
    };
    const initialMessageCount = sentMessages.length;
    let attempts = 0;

    await consumer.handleMessage(Buffer.from(JSON.stringify(event)), {
        publish: async () => {
            attempts += 1;
            throw new Error('simulated publishing failure');
        }
    });

    assert.equal(attempts, 5);
    const deadLetter = sentMessages
        .slice(initialMessageCount)
        .find((message) => message.topic === TOPICS.PUBLISH_DLQ);
    assert.ok(deadLetter, 'expected the failed event to be sent to publish-dlq');
    assert.equal(JSON.parse(deadLetter.messages[0].value).eventId, event.eventId);
    assert.equal((await eventTracker.getEvent(event.eventId)).status, 'failed');
    assert.equal((await eventTracker.getEvent(event.eventId)).attempts, 5);
});

test('consumer publishes once and skips a duplicate after the event store is reloaded', async () => {
    const event = {
        eventId: 'duplicate-after-reload',
        post: { id: 'demo-duplicate', content: 'Publish this once.' }
    };
    let publishCount = 0;
    const dependencies = {
        publish: async () => {
            publishCount += 1;
        }
    };

    const message = Buffer.from(JSON.stringify(event));
    await consumer.handleMessage(message, dependencies);
    assert.equal((await eventTracker.getEvent(event.eventId)).status, 'processed');
    assert.equal(publishCount, 1);

    const trackerPath = require.resolve('../src/utils/eventTracker');
    delete require.cache[trackerPath];
    const reloadedTracker = require('../src/utils/eventTracker');
    await consumer.handleMessage(message, {
        ...dependencies,
        findEvent: reloadedTracker.getEvent,
        recordProcessed: reloadedTracker.markProcessed
    });

    assert.equal(publishCount, 1);
    assert.equal((await reloadedTracker.getEvent(event.eventId)).status, 'processed');
});

test('persistent idempotency survives an actual application-process restart', async () => {
    const event = {
        eventId: 'duplicate-after-process-restart',
        post: { id: 'demo-process-restart', content: 'Publish this once.' }
    };
    const markerPath = path.join(temporaryDirectory, 'restart-publishes.txt');
    const consumerPath = path.resolve(__dirname, '../src/consumer/consumer.js');
    const script = `
        const fs = require('node:fs/promises');
        const { handleMessage } = require(${JSON.stringify(consumerPath)});
        const event = ${JSON.stringify(event)};
        handleMessage(Buffer.from(JSON.stringify(event)), {
            publish: () => fs.appendFile(process.env.PUBLISH_MARKER, 'published\\\\n')
        }).catch((error) => {
            console.error(error);
            process.exitCode = 1;
        });
    `;
    const childEnvironment = {
        ...process.env,
        EVENT_STORE_FILE: path.join(temporaryDirectory, 'restart-events.json'),
        PUBLISH_MARKER: markerPath,
        MAX_ATTEMPTS: '5',
        RETRY_BASE_DELAY_MS: '0'
    };

    for (let processNumber = 0; processNumber < 2; processNumber += 1) {
        const result = spawnSync(process.execPath, ['-e', script], {
            cwd: path.resolve(__dirname, '..'),
            env: childEnvironment,
            encoding: 'utf8'
        });
        assert.equal(
            result.status,
            0,
            `child process ${processNumber + 1} failed:\n${result.stderr}`
        );
    }

    const publishCount = (await fs.readFile(markerPath, 'utf8'))
        .trim()
        .split(/\r?\n/)
        .length;
    assert.equal(publishCount, 1);
});

test('invalid Kafka payloads are routed to the dead-letter topic', async () => {
    const initialMessageCount = sentMessages.length;
    await consumer.handleMessage(Buffer.from('not-json'));
    const deadLetter = sentMessages
        .slice(initialMessageCount)
        .find((message) => message.topic === TOPICS.PUBLISH_DLQ);

    assert.ok(deadLetter);
    assert.match(JSON.parse(deadLetter.messages[0].value).error, /not valid JSON/);
});
