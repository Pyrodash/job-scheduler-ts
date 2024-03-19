import { EventEmitter } from 'events'

export const ERROR = 'error'
export const STARTED_LEADING = 'started-leading'
export const STOPPED_LEADING = 'stopped-leading'

export interface LeaderElectionConfig {
    identity: string
    leaseDuration: number // in seconds
    retryRate: number // in seconds
    renewRate: number // in seconds
}

export abstract class LeaderElectionService extends EventEmitter {
    protected config: LeaderElectionConfig

    constructor(config: LeaderElectionConfig) {
        super()

        this.config = config
    }

    public abstract start(): Promise<void>
    public abstract stop(): Promise<void>
}

export declare interface LeaderElectionService {
    on(event: 'error', listener: (err: Error) => void): this
    on(event: 'started-leading', listener: () => void): this
    on(event: 'stopped-leading', listener: () => void): this
}
