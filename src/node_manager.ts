import { MessageBus } from './message-bus'

export const ASSIGN_PARTITIONS = 'assign-partitions'
export const HEARTBEAT = 'heartbeat'
export const PING = 'ping'

class Node {
    public readonly identity: string
    public lastHeartbeat = Date.now()

    private bus: MessageBus

    constructor(identity: string, bus: MessageBus) {
        this.identity = identity
        this.bus = bus
    }

    send<T>(event: string, message?: T): Promise<void> {
        return this.bus.sendTo<T>(this.identity, event, message)
    }
}

export class NodeManager {
    private bus: MessageBus
    private rebalanceNeeded = false

    private heartbeatDeadline: number // in seconds
    private heartbeatTimer: NodeJS.Timeout

    private partitionCount: number
    private partitions: number[] = []

    private nodes = new Map<string, Node>()

    constructor() {
        this.addNode = this.addNode.bind(this)

        for (let i = 0; i < this.partitionCount; i++) {
            this.partitions.push(i)
        }
    }

    private addNode(identity: string) {
        this.nodes.set(identity, new Node(identity, this.bus))
        this.rebalanceNeeded = true
    }

    private removeNode(identity: string) {
        this.nodes.delete(identity)
        this.rebalanceNeeded = true
    }

    private onHeartbeat(identity: string) {
        const node = this.nodes.get(identity)

        if (!node) {
            this.addNode(identity)
        } else {
            node.lastHeartbeat = Date.now()
        }
    }

    private async rebalancePartitions() {
        this.rebalanceNeeded = false

        const totalNodes = this.nodes.size
        const totalPartitions = this.partitionCount

        // Calculate average partitions per node
        const avgPartitionsPerNode = Math.floor(totalPartitions / totalNodes)
        const remainder = totalPartitions % totalNodes

        let i = 0
        let partitionIndex = 0

        for (const node of this.nodes.values()) {
            const numPartitions = avgPartitionsPerNode + (i < remainder ? 1 : 0)
            const assignedPartitions = this.partitions.slice(
                partitionIndex,
                partitionIndex + numPartitions,
            )

            await node.send('assign-partitions', assignedPartitions)

            partitionIndex += numPartitions
            i++
        }
    }

    private startHeartbeat() {
        this.heartbeatTimer = setInterval(async () => {
            const now = Date.now()

            for (const node of this.nodes.values()) {
                if (node.lastHeartbeat < now - this.heartbeatDeadline) {
                    this.removeNode(node.identity)
                }
            }

            if (this.rebalanceNeeded) {
                await this.rebalancePartitions()
            }
        }, this.heartbeatDeadline)
    }

    private stopHeartbeat() {
        clearInterval(this.heartbeatTimer)
    }

    start() {
        this.bus.on('heartbeat', this.onHeartbeat)
        this.bus.broadcast('ping')

        this.startHeartbeat()
    }

    stop() {
        this.bus.off('heartbeat', this.onHeartbeat)
        this.nodes.clear()

        this.stopHeartbeat()
    }
}
