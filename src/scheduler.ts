import EventEmitter from 'events'
import { LeaderElectionService } from './leader-election'
import { MessageBus } from './message-bus'
import { NodeManager, HEARTBEAT, PING, ASSIGN_PARTITIONS } from './node_manager'
import { getName, rand } from './utils'
import { JobRepository, ScheduledJob } from './repository'
import { JobRegistry } from './registry'
import { Job, JobBuilder, JobContext, JobDetails } from './job'

// type Optional<T, K extends keyof T> = Pick<Partial<T>, K> & Omit<T, K>

const DEFAULT_CONFIG: Omit<Config, 'identity'> = {
    partitionCount: 10,
    heartbeatRate: 25,
    heartbeatDeadline: 50,
    tickRate: 60,
}

function formatJobId(details: { job: string; id: string }) {
    return `${details.job}.${details.id}`
}

export interface Config {
    partitionCount: number
    heartbeatRate: number // in seconds
    heartbeatDeadline: number // in seconds
    tickRate: number // in seconds
    identity: string
}

export class JobScheduler extends EventEmitter {
    private config: Config
    private bus: MessageBus

    private nodeManager: NodeManager
    private leaderElection: LeaderElectionService
    private repository: JobRepository
    private registry: JobRegistry

    private heartbeatTimer: NodeJS.Timeout
    private tickTimer: NodeJS.Timeout

    private partitions: number[] = []

    constructor() {
        super()

        this.sendHeartbeat = this.sendHeartbeat.bind(this)
        this.assignPartitions = this.assignPartitions.bind(this)

        this.init()
    }

    private sendHeartbeat() {
        this.bus.broadcast('heartbeat', this.config.identity)
    }

    private startHeartbeat() {
        this.heartbeatTimer = setInterval(
            this.sendHeartbeat,
            this.config.heartbeatRate,
        )
    }

    private stopHeartbeat() {
        clearInterval(this.heartbeatTimer)
    }

    private startTick() {
        this.tickTimer = setTimeout(async () => {
            await this.tick()

            this.startTick()
        }, this.config.tickRate)
    }

    private stopTick() {
        clearTimeout(this.tickTimer)
    }

    private assignPartitions(partitions: number[]) {
        this.partitions = partitions
        this.emit('assign')
    }

    protected init() {
        this.sendHeartbeat() // initial heartbeat registers the node
        this.startHeartbeat()
        this.startTick()

        this.leaderElection.on('started-leading', () =>
            this.nodeManager.start(),
        )

        this.leaderElection.on('stopped-leading', () => this.nodeManager.stop())
        this.leaderElection.start()

        this.bus.onMessage('assign-partitions', this.assignPartitions)
    }

    async destroy() {
        await this.leaderElection.stop()

        this.stopHeartbeat()
        this.stopTick()
    }

    private async runJob<T>(
        context: JobContext<T>,
        job: Job<T>,
    ): Promise<void> {
        await job.run(context)
    }

    private async tick() {
        const fetchPromises = []

        for (const partition of this.partitions) {
            fetchPromises.push(this.repository.getAll(partition))
        }

        const jobs = (await Promise.all(fetchPromises)).flat()
        const promises = []

        const deletedJobs: ScheduledJob[] = []
        const renewedJobs: ScheduledJob[] = []

        for (const scheduledJob of jobs) {
            const job = this.registry.get(scheduledJob.job)

            if (job) {
                promises.push(
                    this.runJob(
                        {
                            id: scheduledJob.id,
                            params: scheduledJob.params,
                        },
                        job,
                    ).then(() => {
                        const rate = scheduledJob.schedule.rate

                        if (rate) {
                            scheduledJob.schedule.nextRun =
                                Date.now() + rate * 1000

                            renewedJobs.push(scheduledJob)
                        } else {
                            deletedJobs.push(scheduledJob)
                        }
                    }),
                )
            }
        }

        await Promise.all(promises)

        const updatePromises = [
            this.repository.add(...renewedJobs),
            this.repository.delete(...deletedJobs),
        ]

        await Promise.all(updatePromises)
    }

    public register(job: Job): void {
        this.registry.register(job)
    }

    public add(details: JobDetails): Promise<void> {
        if (!details.id) {
            throw new Error('Cannot add a job without an identity')
        }

        if (!details.schedule) {
            throw new Error('Cannot add a job without a schedule')
        }

        details.id = formatJobId(details)

        const scheduledJob: ScheduledJob = {
            ...details,
            partition: pickPartition(),
        }

        return this.repository.add(scheduledJob)
    }

    public remove(job: Job, id: string): Promise<void> {
        const jobName = getName(job)

        return this.repository.deleteById(
            formatJobId({
                job: jobName,
                id,
            }),
        )
    }
}
