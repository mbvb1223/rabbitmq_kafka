<?php

// Shared by both sides. Job 3 is the troublemaker: slow (8 s) or, in poison mode, it throws.
const BAD_JOB = 3;

function job(int $id): string
{
    return json_encode(['id' => $id, 'at' => microtime(true)]);
}

function work(string $body, bool $poison = false): string
{
    ['id' => $id, 'at' => $at] = json_decode($body, true);

    if ($id === BAD_JOB && $poison) {
        throw new RuntimeException("job $id is poison");
    }
    usleep($id === BAD_JOB ? 8_000_000 : 200_000);

    return sprintf('job %2d  done %4.1f s after publish', $id, microtime(true) - $at);
}
