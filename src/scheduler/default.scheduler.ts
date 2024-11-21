import { PulsarQueue } from '../queue/pulsar.queue'
import { JobScheduler } from './scheduler.scheduler'
import { Queue, QueueConfig, JobHandler } from '../queue/queue.queue'
import { Config, getJobName, Job, JobBuilder, JobDetails } from '../types'
import { EventEmitter } from '../utils/events'

export const DEFAULT_CONFIG: Config = {
    url: 'pulsar://localhost:6650',
    topic: 'persistent://public/default',
    producerTimeoutMs: 0,
    cleanupIntervalMs: 0,
}

export const MISSING_PARAMS_ERROR = new Error('Missing job parameters')
export const INVALID_PARAMS_ERROR = new Error('Invalid parameters')

export class DefaultJobScheduler extends EventEmitter implements JobScheduler {
    private config: Config
    private queue: Queue

    constructor(config: Partial<Config> = {}) {
        super()

        this.config = {
            ...DEFAULT_CONFIG,
            ...config,
        }

        this.handleError = this.handleError.bind(this)

        if (!config.queue) {
            this.queue = new PulsarQueue(this.config as QueueConfig)
        } else {
            this.queue = config.queue
        }

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

    schedule<Params>(job: JobBuilder<Params>): Promise<void>
    schedule<Params>(
        jobName: string,
        details: JobDetails<Params>,
    ): Promise<void>
    schedule<Params>(
        job: JobBuilder<Params> | string,
        details?: JobDetails<Params>,
    ): Promise<void> {
        if (typeof job === 'string' && details) {
            return this.queue.add(job, details)
        } else if (job instanceof JobBuilder) {
            const { details } = job

            if (!details.id || !details.schedule) throw MISSING_PARAMS_ERROR
            if (!details.schedule.deliverAt) {
                if (details.schedule.rate) {
                    details.schedule.deliverAt =
                        Date.now() + details.schedule.rate
                } else {
                    details.schedule.deliverAt = Date.now()
                }
            }

            return this.queue.add(job.jobName, details)
        } else {
            throw INVALID_PARAMS_ERROR
        }
    }

    public async destroy(): Promise<void> {
        await this.queue.destroy()

        this.queue.off('error', this.handleError)
    }
}
