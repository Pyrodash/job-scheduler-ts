import { getName, kebabCase } from './utils'
import { JobContext } from './queue/queue.queue'

export interface ScheduleInfo {
    rate?: number
    deliverAt: number
}

export interface JobDetails<Params> {
    id: string
    params: Params
    schedule: ScheduleInfo
}

export interface JobPayload<Params> {
    id: string
    params: Params
    rate: number
}

export class JobBuilder<Params> {
    private _details: JobDetails<Params> = {
        id: null,
        params: null,
        schedule: {
            deliverAt: 0,
            rate: null,
        },
    }

    public get details() {
        return this._details
    }

    constructor(public readonly jobName: string) {}

    public withId(id: string): this {
        this._details.id = id

        return this
    }

    public withParams(params: Params): this {
        this._details.params = params

        return this
    }

    public at(time: number): this {
        this._details.schedule.deliverAt = time

        return this
    }

    public recurring(rateMs: number): this {
        this._details.schedule.rate = rateMs

        return this
    }
}

export abstract class Job<Params> {
    static withId<Params>(id: string): JobBuilder<Params> {
        return new JobBuilder<Params>(getJobName(this)).withId(id)
    }

    static withParams<Params>(params: Params): JobBuilder<Params> {
        return new JobBuilder<Params>(getJobName(this)).withParams(params)
    }

    static at<Params>(time: number): JobBuilder<Params> {
        return new JobBuilder<Params>(getJobName(this)).at(time)
    }

    static recurring<Params>(rateMs: number): JobBuilder<Params> {
        return new JobBuilder<Params>(getJobName(this)).recurring(rateMs)
    }

    abstract handle(job: JobContext<Params>): Promise<void> | void
}

export function getJobName(job: Job<unknown>): string
export function getJobName(ctr: typeof Job): string
export function getJobName(jobOrCtr: Job<unknown> | typeof Job): string {
    const className =
        jobOrCtr instanceof Job ? getName(jobOrCtr) : jobOrCtr.name

    return kebabCase(className)
}
