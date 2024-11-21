import { JobHandler } from 'queue/queue.queue'
import { Job, JobBuilder, JobDetails } from '../types'
import { EventHandler, EventType } from '../utils/events'

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
