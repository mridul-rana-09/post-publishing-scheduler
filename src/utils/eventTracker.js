const fs = require('fs/promises');
const path = require('path');
const { randomUUID } = require('crypto');

const projectRoot = path.resolve(__dirname, '../..');
const storePath = path.resolve(
    projectRoot,
    process.env.EVENT_STORE_FILE || 'data/events.json'
);

let pendingWrite = Promise.resolve();

async function readEvents() {
    try {
        const contents = await fs.readFile(storePath, 'utf8');
        const events = JSON.parse(contents);

        if (!events || typeof events !== 'object' || Array.isArray(events)) {
            throw new Error(`Event store at ${storePath} must contain a JSON object.`);
        }

        return events;
    } catch (error) {
        if (error.code === 'ENOENT') {
            return {};
        }
        throw error;
    }
}

async function getEvent(eventId) {
    if (typeof eventId !== 'string' || eventId.length === 0) {
        throw new TypeError('eventId must be a non-empty string.');
    }

    await pendingWrite;
    const events = await readEvents();
    return events[eventId] || null;
}

function updateEvent(eventId, changes) {
    if (typeof eventId !== 'string' || eventId.length === 0) {
        return Promise.reject(new TypeError('eventId must be a non-empty string.'));
    }

    const update = async () => {
        const events = await readEvents();
        const existingEvent = events[eventId] || {};
        const safeChanges = changes.status === 'enqueued'
            && ['processed', 'failed'].includes(existingEvent.status)
            ? { enqueuedAt: existingEvent.enqueuedAt || changes.enqueuedAt }
            : changes;

        events[eventId] = {
            ...existingEvent,
            ...safeChanges,
            eventId,
            updatedAt: new Date().toISOString()
        };

        await fs.mkdir(path.dirname(storePath), { recursive: true });
        const temporaryPath = `${storePath}.${process.pid}.${randomUUID()}.tmp`;
        await fs.writeFile(temporaryPath, `${JSON.stringify(events, null, 2)}\n`, 'utf8');
        await fs.rename(temporaryPath, storePath);
        return events[eventId];
    };

    const result = pendingWrite.then(update);
    pendingWrite = result.catch(() => {});
    return result;
}

async function markEnqueued(eventId) {
    return updateEvent(eventId, {
        status: 'enqueued',
        enqueuedAt: new Date().toISOString()
    });
}

async function markProcessed(eventId) {
    return updateEvent(eventId, {
        status: 'processed',
        processedAt: new Date().toISOString(),
        error: null
    });
}

async function markFailed(eventId, error, attempts) {
    return updateEvent(eventId, {
        status: 'failed',
        failedAt: new Date().toISOString(),
        attempts,
        error: error.message
    });
}

module.exports = {
    getEvent,
    markEnqueued,
    markProcessed,
    markFailed
};