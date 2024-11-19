import { PulsarQueue } from '../queue/pulsar.queue'
import { JobScheduler } from './scheduler.scheduler'
import { Queue, QueueConfig, JobHandler } from '../queue/queue.queue'
import { Config, getJobName, Job, JobBuilder, JobDetails } from '../types'
import { EventEmitter } from '../utils/events'

export const DEFAULT_CONFIG: Config = {
    queue: {
        url: 'pulsar://localhost:6650',
        topic: 'persistent://public/default',
        producerTimeoutMs: 0,
        cleanupIntervalMs: 0,
    },
}

export class DefaultJobScheduler extends EventEmitter implements JobScheduler {
    private config: Config
    private queue: Queue

    constructor(config: Config = {}) {
        super()

        this.config = {
            ...DEFAULT_CONFIG,
            ...config,
        }

        this.handleError = this.handleError.bind(this)

        this.queue = new PulsarQueue(
            this.config.queue as QueueConfig,
            this.config.repository,
        )

        this.queue.on('error', this.handleError) // redirect errors
    }

    private handleError(err: Error) {
        this.emit('error', err)
    }

    register<Params>(job: Job<Params>): Promise<void> {
        const jobName = getJobName(job)
        const handler = job.handle.bind(job)

        return this.queue.addHandler(jobName, handler)
    }

    handle<Params>(
        jobName: string,
        handler: JobHandler<Params>,
    ): Promise<void> {
        return this.queue.addHandler(jobName, handler)
    }

    schedule<Params>(job: JobBuilder<Params>): Promise<string>
    schedule<Params>(
        jobName: string,
        details: JobDetails<Params>,
    ): Promise<string>
    schedule<Params>(
        job: JobBuilder<Params> | string,
        details?: JobDetails<Params>,
    ): Promise<string> {
        if (typeof job === 'string' && details) {
            return this.queue.add(job, details)
        } else if (job instanceof JobBuilder) {
            return this.queue.add(job.jobName, job.details)
        } else {
            throw new Error('Invalid parameters')
        }
    }

    public cancel(id: string): Promise<void> {
        return this.queue.remove(id)
    }

    public async destroy(): Promise<void> {
        await this.queue.destroy()

        this.queue.off('error', this.handleError)
    }
}
