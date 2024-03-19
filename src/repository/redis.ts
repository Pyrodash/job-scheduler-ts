import { Redis } from 'ioredis'
import { JobRepository, ScheduledJob } from '.'

interface RedisScheduledJob<Params = unknown> {
    id: string
    job: string
    rate?: number
    params: Params
}

function parseJob<Params>(
    job: RedisScheduledJob<Params>,
    partition: number,
): ScheduledJob<Params> {
    return {
        id: job.id,
        job: job.job,
        schedule: {
            rate: job.rate,
            nextRun: null,
        },
        partition: partition,
        params: job.params,
    }
}

function serializeJob<Params>(job: ScheduledJob<Params>): string {
    const redisJob: RedisScheduledJob<Params> = {
        id: job.id,
        job: job.job,
        rate: job.schedule.rate,
        params: job.params,
    }

    return JSON.stringify(redisJob)
}

function keyForPartition(partition: number) {
    return `jobs.${partition}`
}

export class RedisJobRepository implements JobRepository {
    private redis: Redis

    constructor(redis: Redis) {
        this.redis = redis
    }

    async getAll(partition: number): Promise<ScheduledJob[]> {
        const now = Date.now()
        const rawJobs = await this.redis.zrangebyscore(
            keyForPartition(partition),
            '-inf',
            now,
        )

        const jobs: ScheduledJob[] = []

        for (const rawJob of rawJobs) {
            const job = parseJob(JSON.parse(rawJob), partition)

            jobs.push(job)
        }

        return jobs
    }

    async add(...jobs: ScheduledJob[]): Promise<void> {
        const partitionJobs = new Map<number, (string | number)[]>()
        const jobIndex = new Map<string, string>()

        const pipeline = this.redis.pipeline()

        for (const job of jobs) {
            if (!partitionJobs.has(job.partition)) {
                partitionJobs.set(job.partition, [])
            }

            const serializedJob = serializeJob(job)

            partitionJobs
                .get(job.partition)
                .push(job.schedule.nextRun, serializedJob)

            jobIndex.set(job.id, serializedJob)
        }

        pipeline.hset('jobs', jobIndex)

        for (const [partition, scoreMembers] of partitionJobs.entries()) {
            pipeline.zadd(keyForPartition(partition), ...scoreMembers)
        }

        await pipeline.exec()
    }

    async delete(...jobs: ScheduledJob[]): Promise<void> {
        const partitionJobs = new Map<number, string[]>()
        const jobIds = []

        const pipeline = this.redis.pipeline()

        for (const job of jobs) {
            if (!partitionJobs.has(job.partition)) {
                partitionJobs.set(job.partition, [])
            }

            partitionJobs.get(job.partition).push(serializeJob(job))
            jobIds.push(job.id)
        }

        pipeline.hdel('jobs', ...jobIds)

        for (const [partition, members] of partitionJobs.entries()) {
            pipeline.zrem(keyForPartition(partition), ...members)
        }

        await pipeline.exec()
    }

    async deleteById(id: string): Promise<void> {
        const rawJob = await this.redis.hget('jobs', id)

        if (rawJob) {
            const job: ScheduledJob = JSON.parse(rawJob)
            const pipeline = this.redis.pipeline()

            pipeline.hdel('jobs', id)
            pipeline.zrem(keyForPartition(job.partition), rawJob)

            await pipeline.exec()
        }
    }
}
