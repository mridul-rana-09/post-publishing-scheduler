const fs = require('fs/promises');
const path = require('path');
const cron = require('node-cron');
const { createHash } = require('crypto');
const { publishRequest } = require('../producer/producer');
const { getEvent, markEnqueued } = require('../utils/eventTracker');

const projectRoot = path.resolve(__dirname, '../..');
const postsPath = path.resolve(projectRoot, process.env.POSTS_FILE || 'data/posts.json');
const cronSchedule = process.env.CRON_SCHEDULE || '* * * * *';
let scheduledTask;
let scanInProgress = false;

function createEventId(post) {
    const scheduledAt = new Date(post.scheduledAt).toISOString();
    const identity = `${post.id}:${scheduledAt}`;
    return createHash('sha256').update(identity).digest('hex');
}

function validatePost(post, index) {
    if (!post || typeof post !== 'object' || Array.isArray(post)) {
        throw new Error(`Post at index ${index} must be a JSON object.`);
    }
    if (typeof post.id !== 'string' || post.id.trim() === '') {
        throw new Error(`Post at index ${index} must have a non-empty string id.`);
    }
    if (typeof post.content !== 'string' || post.content.trim() === '') {
        throw new Error(`Post "${post.id}" must have non-empty content.`);
    }
    if (!Number.isFinite(Date.parse(post.scheduledAt))) {
        throw new Error(`Post "${post.id}" must have a valid scheduledAt date.`);
    }
}

async function scanForDuePosts({
    postsFile = postsPath,
    publish = publishRequest,
    findEvent = getEvent,
    recordEnqueued = markEnqueued,
    now = Date.now()
} = {}) {
    if (scanInProgress) {
        return;
    }

    scanInProgress = true;
    try {
        const postsContents = await fs.readFile(postsFile, 'utf8');
        const posts = JSON.parse(postsContents);
        if (!Array.isArray(posts)) {
            throw new Error(`Posts file at ${postsFile} must contain a JSON array.`);
        }
        posts.forEach(validatePost);

        for (const post of posts) {
            if (Date.parse(post.scheduledAt) > now) {
                continue;
            }

            const eventId = createEventId(post);
            const existingEvent = await findEvent(eventId);
            if (existingEvent && existingEvent.enqueuedAt) {
                continue;
            }

            const event = {
                eventId,
                post,
                requestedAt: new Date().toISOString()
            };

            try {
                await publish(event);
                await recordEnqueued(eventId);
                console.log(`Queued due post "${post.id}" (event ${eventId}).`);
            } catch (error) {
                console.error(`Could not queue post "${post.id}":`, error);
            }
        }
    } catch (error) {
        console.error('The scheduler could not scan the posts file:', error);
    } finally {
        scanInProgress = false;
    }
}

function startScheduler() {
    if (!cron.validate(cronSchedule)) {
        throw new Error(`Invalid CRON_SCHEDULE value: "${cronSchedule}".`);
    }

    scheduledTask = cron.schedule(cronSchedule, () => {
        void scanForDuePosts();
    });
    void scanForDuePosts();
    console.log(`Post scheduler started with cron schedule "${cronSchedule}".`);

    return scheduledTask;
}

function stopScheduler() {
    if (scheduledTask) {
        scheduledTask.stop();
        scheduledTask = undefined;
    }
}

module.exports = { startScheduler, stopScheduler, scanForDuePosts };