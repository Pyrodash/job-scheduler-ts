import Pulsar from 'pulsar-client'
import { v4 as uuidv4 } from 'uuid'
import { pack, unpack } from 'msgpackr'
import {
    Queue,
    QueueConfig,
    Producer,
    Consumer,
    JobHandler,
    JobContext,
} from './queue.queue'
import { JobDetails } from '../types'
import { Repository } from 'repository/repository.repository'

interface JobPayload<Params> {
    id: string
    params: Params
    rate: number
}

class PulsarJob<Params> extends JobContext<Params> {
    public isStopped = false

    public stop(): Promise<void> {
        this.isStopped = true

        return Promise.resolve()
    }
}

export class PulsarProducer implements Producer {
    constructor(private producer: Pulsar.Producer) {}

    private generateId(): string {
        return uuidv4()
    }

    async schedule<Params>(
        details: JobDetails<Params>,
        id?: string,
    ): Promise<string> {
        const { deliverAt, rate } = details.schedule
        const payload: JobPayload<Params> = {
            id: id || this.generateId(),
            params: details.params,
            rate,
        }

        await this.producer.send({
            data: pack(payload),
            eventTimestamp: deliverAt,
            deliverAt,
        })

        return payload.id
    }

    async close(): Promise<void> {
        await this.producer.close()
    }
}

export class PulsarConsumer implements Consumer {
    constructor(private consumer: Pulsar.Consumer) {}

    async cancel(internalId: string): Promise<void> {
        await this.consumer.acknowledgeId(
            Pulsar.MessageId.deserialize(Buffer.from(internalId)),
        )
    }

    async close(): Promise<void> {
        await this.consumer.close()
    }
}

export class PulsarQueue extends Queue {
    private client: Pulsar.Client

    constructor(config: QueueConfig, repo?: Repository) {
        super(config, repo)

        this.client = new Pulsar.Client({
            serviceUrl: config.url,
            operationTimeoutSeconds: 5,
        })
    }

    private buildTopic(jobName: string): string {
        return `${this.config.topic}/${jobName}`
    }

    public async createConsumer<Params>(
        jobName: string,
        callback: JobHandler<Params>,
    ): Promise<PulsarConsumer> {
        const consumer = await this.client.subscribe({
            topic: this.buildTopic(jobName),
            subscription: 'job-handler-group',
            subscriptionType: 'Shared',
            listener: async (msg) => {
                const payload: JobPayload<Params> = unpack(msg.getData())
                const job = new PulsarJob(
                    payload.id,
                    jobName,
                    payload.rate,
                    payload.params,
                )

                try {
                    const canceled = await this.repo?.isCanceled(payload.id)

                    if (!canceled) await callback(job)

                    await consumer.acknowledge(msg)

                    if (!canceled && job.rate && !job.isStopped) {
                        const producer = (await this.getProducer(
                            jobName,
                        )) as PulsarProducer

                        await producer.schedule(
                            {
                                params: job.params,
                                schedule: {
                                    rate: job.rate,
                                    deliverAt: Date.now() + job.rate,
                                },
                            },
                            job.id,
                        )
                    }
                } catch (err) {
                    this.emit('error', err)
                }
            },
        })

        return new PulsarConsumer(consumer)
    }

    public async createProducer(jobName: string): Promise<PulsarProducer> {
        const producer = await this.client.createProducer({
            topic: this.buildTopic(jobName),
            messageRoutingMode: 'RoundRobinDistribution',
            sendTimeoutMs: 1000,
        })

        return new PulsarProducer(producer)
    }
}
