export type EventType = 'error'
type EventPayload = Error
export type EventHandler = (payload: EventPayload) => void

export class EventEmitter {
    private listeners: Record<EventType, Set<EventHandler>> = {
        error: new Set(),
    }

    public on(event: EventType, handler: EventHandler) {
        this.listeners[event].add(handler)
    }

    public off(event: EventType, handler: EventHandler) {
        this.listeners[event].delete(handler)
    }

    public emit(event: EventType, payload: Error) {
        for (const listener of this.listeners[event]) {
            listener(payload)
        }
    }
}
