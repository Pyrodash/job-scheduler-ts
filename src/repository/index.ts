import { JobDetails } from '../job'

export interface ScheduledJob<Params = unknown> extends JobDetails<Params> {
    partition: number
}

export interface JobRepository {
    getAll(partition: number): Promise<ScheduledJob[]>
    add(...jobs: ScheduledJob[]): Promise<void> // insert or update
    delete(...jobs: ScheduledJob[]): Promise<void>
    deleteById(id: string): Promise<void>
}
