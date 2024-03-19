import { JobScheduler } from './scheduler'
import { getName } from './utils'

export interface ScheduleInfo {
    rate?: number
    nextRun: number
}

export interface JobDetails<Params = unknown> {
    id: string
    job: string
    params: Params
    schedule: ScheduleInfo
}

export class JobBuilder {
    private scheduler?: JobScheduler
    private details: JobDetails = {
        id: null,
        job: null,
        params: null,
        schedule: null,
    }

    constructor(scheduler?: JobScheduler) {
        this.scheduler = scheduler
    }

    public withJob(job: Job): this {
        this.details.job = getName(job)

        return this
    }

    public withSchedule(schedule: ScheduleInfo): this {
        this.details.schedule = schedule

        return this
    }

    public withId(id: string): this {
        this.details.id = id

        return this
    }

    public withParams<Params>(params: Params): this {
        this.details.params = params

        return this
    }

    public at(timestamp: number): this {
        this.details.schedule = { nextRun: timestamp }

        return this
    }

    public recurring(rateInSeconds: number): this {
        this.details.schedule = {
            rate: rateInSeconds,
            nextRun: Date.now() + rateInSeconds * 1000,
        }

        return this
    }

    public build(): JobDetails {
        return this.details
    }

    public schedule(): Promise<void> {
        if (!this.scheduler) {
            throw new Error(
                'JobBuilder was instantiated without a scheduler. Use `JobScheduler.add(JobBuilder.build())` instead.',
            )
        }

        return this.scheduler.add(this.build())
    }
}

export interface JobContext<Params> {
    id: string
    params: Params
}

export abstract class Job<Params = null> {
    private scheduler: JobScheduler

    constructor(scheduler: JobScheduler) {
        scheduler.register(this)
    }

    protected buildSchedule?(): ScheduleInfo

    abstract run(context: JobContext<Params>): Promise<void> | void

    public schedule(id: string, params?: Params): Promise<void> {
        const builder = new JobBuilder()
            .withId(id)
            .withParams(params)
            .withJob(this)

        if (!this.buildSchedule) {
            throw new Error(
                'Invalid use of `Job.schedule()`. This method can only be used if `Job.buildSchedule()` is implemented.',
            )
        }

        builder.withSchedule(this.buildSchedule())

        return builder.schedule()
    }

    public at(timestamp: number): JobBuilder {
        return new JobBuilder().withJob(this).at(timestamp)
    }

    public recurring(rateInSeconds: number): JobBuilder {
        return new JobBuilder().withJob(this).recurring(rateInSeconds)
    }

    public remove(id: string): Promise<void> {
        return this.scheduler.remove(this, id)
    }
}

export function Recurring(rateInSeconds: number) {
    return function (ctr: Function) {
        ctr.prototype.buildSchedule = (): ScheduleInfo => ({
            rate: rateInSeconds,
            nextRun: Date.now() + rateInSeconds,
        })
    }
}
