function sleep(milliseconds) {
    return new Promise((resolve) => setTimeout(resolve, milliseconds));
}

async function retryWithExponentialBackoff(
    operation,
    {
        maxAttempts = 5,
        baseDelayMs = 1000,
        onRetry = () => {}
    } = {}
) {
    if (!Number.isInteger(maxAttempts) || maxAttempts < 1 || maxAttempts > 5) {
        throw new RangeError('maxAttempts must be an integer between 1 and 5.');
    }
    if (!Number.isFinite(baseDelayMs) || baseDelayMs < 0) {
        throw new RangeError('baseDelayMs must be a non-negative number.');
    }

    for (let attempt = 1; attempt <= maxAttempts; attempt += 1) {
        try {
            return await operation(attempt);
        } catch (error) {
            const failure = error instanceof Error ? error : new Error(String(error));
            if (attempt === maxAttempts) {
                failure.attempts = attempt;
                throw failure;
            }

            const delayMs = baseDelayMs * (2 ** (attempt - 1));
            onRetry({ attempt, delayMs, error: failure });
            await sleep(delayMs);
        }
    }

    throw new Error('Retry operation ended unexpectedly.');
}

module.exports = { retryWithExponentialBackoff };