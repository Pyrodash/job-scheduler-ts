import { JobDetails } from '../types'
import { EventEmitter } from '../utils/events'

export abstract class JobContext<Params> {
    constructor(
        public id: string,
        public readonly jobName: string,
        public readonly rate: number,
        public readonly params: Params,
    ) {}

    public abstract stop(): Promise<void>
}

export type JobHandler<Params = unknown> = (
    job: JobContext<Params>,
) => Promise<void> | void

export interface Producer {
    schedule<Params>(details: JobDetails<Params>): Promise<string>
    close(): Promise<void>
}

export interface Consumer {
    close(): Promise<void>
}

export interface CancelableConsumer extends Consumer {
    cancel(internalId: string): Promise<void>
}

interface ProducerRecord {
    producer: Producer
    lastUsed: number
}

interface ConsumerRecord {
    consumer: Consumer
    handlers: Set<JobHandler>
}

const isCancelableConsumer = (
    consumer: Consumer | CancelableConsumer,
): consumer is CancelableConsumer => {
    return 'cancel' in consumer
}

export interface QueueConfig {
    producerTimeoutMs: number // How long to keep unused producers
    cleanupIntervalMs: number // How often to run the cleanup job
}

export abstract class Queue extends EventEmitter {
    private producers: Map<string, ProducerRecord> = new Map()
    private consumers: Map<string, ConsumerRecord> = new Map()

    private producerTimeoutMs: number
    private cleanupIntervalMs: number

    private cleanupTimer: NodeJS.Timeout

    constructor({ cleanupIntervalMs, producerTimeoutMs }: QueueConfig) {
        super()

        this.cleanupIntervalMs = cleanupIntervalMs
        this.producerTimeoutMs = producerTimeoutMs

        this.startCleanupJob()
    }

    protected abstract createProducer(topic: string): Promise<Producer>
    protected abstract createConsumer<Params>(
        topic: string,
        callback: JobHandler<Params>,
    ): Promise<Consumer>

    private startCleanupJob(): void {
        if (this.cleanupIntervalMs > 0) {
            this.cleanupTimer = setTimeout(
                () => this.cleanup(),
                this.cleanupIntervalMs,
            )
        }
    }

    private async cleanup(): Promise<void> {
        try {
            await this.cleanupUnusedProducers()
        } catch (err) {
            console.error('Failed to clean up producers', err)
        } finally {
            this.startCleanupJob()
        }
    }

    // Producer API

    private async cleanupUnusedProducers(): Promise<void> {
        const now = Date.now()
        const producersToRemove: string[] = []

        // Identify producers to remove
        for (const [topic, { lastUsed }] of this.producers.entries()) {
            if (now - lastUsed > this.producerTimeoutMs) {
                producersToRemove.push(topic)
            }
        }

        // Remove identified producers
        await Promise.all(
            producersToRemove.map((topic) => this.removeProducer(topic)),
        )
    }

    protected async getProducer(topic: string): Promise<Producer> {
        const existing = this.producers.get(topic)

        if (existing) {
            existing.lastUsed = Date.now()

            return existing.producer
        }

        const producer = await this.createProducer(topic)

        this.producers.set(topic, {
            producer,
            lastUsed: Date.now(),
        })

        return producer
    }

    private async removeProducer(topic: string): Promise<void> {
        const existing = this.producers.get(topic)

        if (existing) {
            await existing.producer.close()

            this.producers.delete(topic)
        }
    }

    public async add<Params>(
        jobName: string,
        details: JobDetails<Params>,
    ): Promise<void> {
        const producer = await this.getProducer(jobName)

        await producer.schedule(details)
    }

    public async remove(jobName: string, internalId: string) {
        const { consumer } = await this.getConsumer(jobName)

        if (isCancelableConsumer(consumer)) {
            await consumer.cancel(internalId)
        } else {
            throw new Error('Cancelation is not supported by this queue driver')
        }
    }

    // Consumer API

    private async getConsumer(topic: string): Promise<ConsumerRecord> {
        if (this.consumers.has(topic)) return this.consumers.get(topic)

        const handlers = new Set<JobHandler>()
        const consumer = await this.createConsumer(topic, async (job) => {
            for (const handler of handlers) {
                await handler(job) // in sequence or parallel?
            }
        })

        const record = {
            consumer,
            handlers,
        }

        this.consumers.set(topic, record)

        return record
    }

    public async addHandler<Params>(
        jobName: string,
        handler: JobHandler<Params>,
    ): Promise<void> {
        const consumer = await this.getConsumer(jobName)

        consumer.handlers.add(handler)
    }

    public async removeHandler<Params>(
        jobName: string,
        listener: JobHandler<Params>,
    ): Promise<void> {
        const consumer = this.consumers.get(jobName)

        if (!consumer) return

        consumer.handlers.delete(listener)

        if (consumer.handlers.size === 0) {
            await consumer.consumer.close()

            this.consumers.delete(jobName)
        }
    }

    public async destroy() {
        clearInterval(this.cleanupTimer)

        const promises = []

        for (const [_, { producer }] of this.producers) {
            promises.push(producer.close())
        }

        for (const [_, { consumer }] of this.consumers) {
            promises.push(consumer.close())
        }

        await Promise.all(promises)

        this.producers.clear()
        this.consumers.clear()
    }
}
