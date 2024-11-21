import { PulsarQueue, PulsarQueueConfig } from '../queue/pulsar.queue'
import {
    JobScheduler,
    INVALID_PARAMS_ERROR,
    MISSING_PARAMS_ERROR,
} from './scheduler.scheduler'
import { JobHandler } from '../queue/queue.queue'
import { getJobName, Job, JobBuilder, JobDetails } from '../types'
import { EventEmitter } from '../utils/events'

export interface PulsarConfig extends PulsarQueueConfig {
    queue?: PulsarQueue
}

export const DEFAULT_CONFIG: PulsarConfig = {
    url: 'pulsar://localhost:6650',
    topic: 'persistent://public/default',
    producerTimeoutMs: 0,
    cleanupIntervalMs: 0,
}

export class PulsarScheduler extends EventEmitter implements JobScheduler {
    protected config: PulsarConfig
    protected queue: PulsarQueue

    constructor(config: Partial<PulsarConfig> = {}) {
        super()

        this.config = {
            ...DEFAULT_CONFIG,
            ...config,
        }

        this.handleError = this.handleError.bind(this)

        if (!config.queue) {
            this.queue = this.createQueue()
        } else {
            this.queue = config.queue
        }

        this.queue.on('error', this.handleError) // redirect errors
    }

    protected createQueue(): PulsarQueue {
        return new PulsarQueue(this.config)
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
                details.schedule.deliverAt = Date.now()

                if (details.schedule.rate) {
                    details.schedule.deliverAt += details.schedule.rate
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
