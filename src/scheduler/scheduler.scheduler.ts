import { JobHandler } from 'queue/queue.queue'
import { Job, JobBuilder, JobDetails } from '../types'
import { EventHandler, EventType } from '../utils/events'

export const MISSING_PARAMS_ERROR = new Error('Missing job parameters')
export const INVALID_PARAMS_ERROR = new Error('Invalid parameters')

export interface JobScheduler {
    on(event: EventType, handler: EventHandler): void
    register<Params>(job: Job<Params>): Promise<void>
    handle<Params>(jobName: string, handler: JobHandler<Params>): Promise<void>
    schedule<Params>(job: JobBuilder<Params>): Promise<void>
    schedule<Params>(
        jobName: string,
        details: JobDetails<Params>,
    ): Promise<void>
    destroy(): Promise<void>
}

export interface CancelableJobScheduler extends JobScheduler {
    cancel(jobName: string, id: string): Promise<void>
}
