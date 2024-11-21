import { JobContext, JobHandler } from '../queue/queue.queue'
import { getJobName, Job, JobBuilder, JobDetails, JobPayload } from '../types'
import { EventEmitter } from '../utils/events'
import {
    INVALID_PARAMS_ERROR,
    MISSING_PARAMS_ERROR,
    CancelableJobScheduler,
} from './scheduler.scheduler'
import Bull from 'bull'
import Redis, { RedisOptions } from 'ioredis'

export const DEFAULT_CONFIG: RedisConfig = {
    host: '127.0.0.1',
    port: 6379,
    maxRetriesPerRequest: null,
    enableReadyCheck: false,
}

export class RedisJob<Params> extends JobContext<Params> {
    constructor(private job: Bull.Job<JobPayload<Params>>) {
        const payload = job.data

        super(String(job.id), job.queue.name, payload.rate, payload.params)
    }

    public async stop(): Promise<void> {
        await this.job.queue.removeJobs(this.id)
    }
}

export interface RedisConfig extends RedisOptions {}

export class RedisScheduler
    extends EventEmitter
    implements CancelableJobScheduler
{
    private config: RedisConfig

    private queues = new Map<String, Bull.Queue>()
    private consumers = new Set<String>()
    private handlers = new Map<String, Set<JobHandler>>()

    private client: Redis
    private subscriber: Redis

    constructor(config: RedisConfig = {}) {
        super()

        this.config = {
            ...DEFAULT_CONFIG,
            ...config,
        }

        this.client = this.createRedisClient('bclient')
        this.subscriber = this.createRedisClient('bclient')
    }

    private getQueue(topic: string): Bull.Queue {
        if (this.queues.has(topic)) return this.queues.get(topic)

        const queue = new Bull(topic, {
            createClient: this.createRedisClient.bind(this),
        })

        this.queues.set(topic, queue)

        return queue
    }

    private createRedisClient(
        type: 'client' | 'subscriber' | 'bclient',
    ): Redis {
        switch (type) {
            case 'client':
                return this.client
            case 'subscriber':
                return this.subscriber
            default:
                return new Redis(this.config)
        }
    }

    private createProcessor(topic: string) {
        return async (job: Bull.Job) => {
            const context = new RedisJob(job)
            const handlers = this.handlers.get(topic)

            const promises = []

            for (const handler of handlers) {
                promises.push(handler(context))
            }

            await Promise.all(promises)
            await job.queue.removeJobs(context.id)

            if (context.rate) {
                await this.add(topic, {
                    id: context.id,
                    params: context.params,
                    schedule: {
                        deliverAt: Date.now() + context.rate,
                        rate: context.rate,
                    },
                })
            }
        }
    }

    private async ensureConsumer(topic: string): Promise<void> {
        if (this.consumers.has(topic)) return

        const queue = this.getQueue(topic)

        queue.process(this.createProcessor(topic))

        this.consumers.add(topic)
        this.handlers.set(topic, new Set())
    }

    async register<Params>(job: Job<Params>): Promise<void> {
        const jobName = getJobName(job)
        const handler = job.handle.bind(job)

        await this.ensureConsumer(jobName)

        this.handlers.get(jobName).add(handler)
    }

    async handle<Params>(
        jobName: string,
        handler: JobHandler<Params>,
    ): Promise<void> {
        await this.ensureConsumer(jobName)

        this.handlers.get(jobName).add(handler)
    }

    protected async add<Params>(
        jobName: string,
        details: JobDetails<Params>,
    ): Promise<void> {
        const queue = this.getQueue(jobName)
        const payload: JobPayload<Params> = {
            id: details.id,
            params: details.params,
            rate: details.schedule.rate,
        }

        await queue.add(payload, {
            delay: details.schedule.deliverAt - Date.now(),
            jobId: details.id,
            removeOnComplete: true,
        })
    }

    schedule<Params>(job: JobBuilder<Params>): Promise<void>
    schedule<Params>(
        jobName: string,
        details: JobDetails<Params>,
    ): Promise<void>
    async schedule<Params>(
        job: JobBuilder<Params> | string,
        details?: JobDetails<Params>,
    ): Promise<void> {
        let jobName: string

        if (typeof job === 'string' && details) {
            jobName = job
        } else if (job instanceof JobBuilder) {
            jobName = job.jobName
            details = job.details
        } else {
            throw INVALID_PARAMS_ERROR
        }

        if (!details.id || !details.schedule) throw MISSING_PARAMS_ERROR
        if (!details.schedule.deliverAt) {
            details.schedule.deliverAt = Date.now()

            if (details.schedule.rate) {
                details.schedule.deliverAt += details.schedule.rate
            }
        }

        await this.add(jobName, details)
    }

    async cancel(jobName: string, id: string): Promise<void> {
        const queue = this.getQueue(jobName)

        await queue.removeJobs(id)
    }

    async destroy(): Promise<void> {
        for (const [_, queue] of this.queues) {
            await queue.close()
        }
    }
}
