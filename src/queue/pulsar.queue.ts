import Pulsar from 'pulsar-client'
import { pack, unpack } from 'msgpackr'
import {
    Queue,
    QueueConfig,
    Producer,
    Consumer,
    JobHandler,
    JobContext,
    CancelableConsumer,
} from './queue.queue'
import { JobDetails } from '../types'

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

    async schedule<Params>(details: JobDetails<Params>): Promise<string> {
        const { deliverAt, rate } = details.schedule
        const payload: JobPayload<Params> = {
            id: details.id,
            params: details.params,
            rate,
        }

        const messageId = await this.producer.send({
            data: pack(payload),
            eventTimestamp: deliverAt,
            deliverAt,
        })

        return messageId.serialize().toString('utf-8')
    }

    async close(): Promise<void> {
        await this.producer.close()
    }
}

export class PulsarConsumer implements CancelableConsumer {
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

    constructor(config: QueueConfig) {
        super(config)

        this.client = new Pulsar.Client({
            serviceUrl: config.url,
            operationTimeoutSeconds: 5,
        })
    }

    private buildTopic(jobName: string): string {
        return `${this.config.topic}/${jobName}`
    }

    protected constructConsumer(consumer: Pulsar.Consumer): Consumer {
        return new PulsarConsumer(consumer)
    }

    protected constructProducer(producer: Pulsar.Producer): Producer {
        return new PulsarProducer(producer)
    }

    public async createConsumer<Params>(
        jobName: string,
        callback: JobHandler<Params>,
    ): Promise<Consumer> {
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

                const next = async () => {
                    if (job.rate && !job.isStopped) {
                        const producer = (await this.getProducer(
                            job.jobName,
                        )) as PulsarProducer

                        await producer.schedule({
                            id: job.id,
                            params: job.params,
                            schedule: {
                                rate: job.rate,
                                deliverAt: Date.now() + job.rate,
                            },
                        })
                    }
                }

                try {
                    await this.handleJob(job, callback, next)
                    await consumer.acknowledge(msg)
                } catch (err) {
                    this.emit('error', err)
                }
            },
        })

        return this.constructConsumer(consumer)
    }

    public async createProducer(jobName: string): Promise<Producer> {
        const producer = await this.client.createProducer({
            topic: this.buildTopic(jobName),
            messageRoutingMode: 'RoundRobinDistribution',
            sendTimeoutMs: 1000,
        })

        return this.constructProducer(producer)
    }

    protected async handleJob<Params>(
        job: PulsarJob<Params>,
        handler: JobHandler<Params>,
        next: () => Promise<void>,
    ): Promise<void> {
        await handler(job)
        await next()
    }
}
