export interface MessageBus {
    broadcast<T>(event: string, message?: T): Promise<void>
    sendTo<T>(identity: string, event: string, message?: T): Promise<void>
    on<T>(event: string, listener: (message: T) => void): void
    off<T>(event: string, listener: (message: T) => void): void
    onMessage<T>(event: string, listener: (message: T) => void): void
}
