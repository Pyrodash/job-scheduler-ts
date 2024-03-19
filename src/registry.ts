import { Job } from './job'
import { getName } from './utils'

export class JobRegistry {
    private jobs = new Map<string, Job>()

    public get<T>(name: string): Job<T> | null {
        return this.jobs.get(name)
    }

    public register<T>(job: Job<T>) {
        this.jobs.set(getName(job), job)
    }
}
