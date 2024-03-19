import { Redis } from 'ioredis'
import { MessageBus } from '.'

interface RedisNodeMessage<T> {
    event: string
    data: T
}

type Listener<T> = (message: T) => void

export class RedisMessageBus implements MessageBus {
    private publisher: Redis
    private subscriber: Redis

    private identity: string
    private listeners: Map<string, Set<Listener<unknown>>>
    private messageListeners: Map<string, Set<Listener<unknown>>>

    constructor(identity: string, redis: Redis) {
        this.handleMessage = this.handleMessage.bind(this)
        this.identity = identity

        this.publisher = redis
        this.subscriber = redis.duplicate()

        this.subscriber.on('message', (channel, rawMessage) => {
            const message = JSON.parse(rawMessage)
            const listeners = this.listeners.get(channel)

            for (const listener of listeners.values()) {
                listener(message)
            }
        })

        this.addListener(this.channelFor(this.identity), this.handleMessage)
    }

    private channelFor(event: string) {
        return `jobs.events.${event}`
    }

    private eventFrom(channel: string): string {
        return channel.slice(12)
    }

    private addListener<T>(channel: string, listener: Listener<T>): void {
        if (!this.listeners.has(channel)) {
            this.listeners.set(channel, new Set([listener]))
            this.subscriber.subscribe(channel)
        } else {
            this.listeners.get(channel).add(listener)
        }
    }

    private removeListener<T>(channel: string, listener: Listener<T>): void {
        const listeners = this.listeners.get(channel)

        listeners.delete(listener)

        if (listeners.size === 0) {
            this.listeners.delete(channel)
            this.subscriber.unsubscribe(channel)
        }
    }

    private addMessageListener<T>(event: string, listener: Listener<T>): void {
        if (!this.messageListeners.has(event)) {
            this.messageListeners.set(event, new Set([listener]))
        } else {
            this.messageListeners.get(event).add(listener)
        }
    }

    private removeMessageListener<T>(
        event: string,
        listener: Listener<T>,
    ): void {
        const listeners = this.messageListeners.get(event)

        listeners.delete(listener)

        if (listeners.size === 0) {
            this.messageListeners.delete(event)
        }
    }

    private handleMessage(message: RedisNodeMessage<unknown>) {
        const listeners = this.messageListeners.get(message.event)

        for (const listener of listeners.values()) {
            listener(message.data)
        }
    }

    async broadcast<T>(event: string, message?: T): Promise<void> {
        await this.publisher.publish(
            this.channelFor(event),
            JSON.stringify(message),
        )
    }

    sendTo<T>(identity: string, event: string, message?: T): Promise<void> {
        return this.broadcast(identity, <RedisNodeMessage<T>>{
            event,
            data: message,
        })
    }

    on<T>(event: string, listener: Listener<T>): void {
        this.addListener<T>(this.channelFor(event), listener)
    }

    off<T>(event: string, listener: Listener<T>): void {
        this.removeListener<T>(this.channelFor(event), listener)
    }

    onMessage<T>(event: string, listener: Listener<T>): void {
        this.addMessageListener<T>(event, listener)
    }
}
