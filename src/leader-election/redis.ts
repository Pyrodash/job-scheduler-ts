import { Callback, Redis, Result } from 'ioredis'
import {
    LeaderElectionConfig,
    LeaderElectionService,
    ERROR,
    STARTED_LEADING,
    STOPPED_LEADING,
} from '.'
import { sleep } from '~/utils'

const renewScript = `
local identity = ARGV[1]
local expiry = ARGV[2]

local currId = redis.call("GET", KEYS[1])

if currId == identity then
	redis.call("SET", KEYS[1], identity, "EX", expiry)

	return 1
end

return 0
`

const releaseScript = `
local identity = ARGV[1]
local currId = redis.call("GET", KEYS[1])

if currId == identity then
    redis.call("DEL", KEYS[1])

    return 1
end

return 0
`

declare module 'ioredis' {
    interface RedisCommander<Context> {
        renewLease(
            key: string,
            identity: string,
            expiryInSeconds: number,
            callback?: Callback<number>,
        ): Result<number, Context>
        releaseLease(
            key: string,
            identity: string,
            callback?: Callback<number>,
        ): Result<number, Context>
    }
}

export class RedisLeaderElectionService extends LeaderElectionService {
    private static key = 'jobs.leader'

    private client: Redis
    private active: boolean

    constructor(config: LeaderElectionConfig, client: Redis) {
        super(config)

        this.client = client
        this.client.defineCommand('renewLease', {
            numberOfKeys: 1,
            lua: renewScript,
        })

        this.client.defineCommand('releaseLease', {
            numberOfKeys: 1,
            lua: releaseScript,
        })
    }

    public async start(): Promise<void> {
        this.active = true

        while (this.active) {
            await this.obtain()
            await this.maintain()
        }
    }

    public async stop() {
        this.active = false

        await this.release()
    }

    private async release() {
        await this.client.releaseLease(
            RedisLeaderElectionService.key,
            this.config.identity,
        )
    }

    private async obtain(): Promise<void> {
        while (this.active) {
            try {
                const res = await this.client.call(
                    'set',
                    RedisLeaderElectionService.key,
                    this.config.identity,
                    'NX',
                    'EX',
                    this.config.leaseDuration,
                )

                if (res === 'OK') {
                    break
                }
            } catch (err) {
                this.emit(ERROR, err)
            }

            await sleep(this.config.retryRate * 1000)
        }

        this.emit(STARTED_LEADING)
    }

    private async maintain(): Promise<void> {
        while (this.active) {
            await sleep(this.config.renewRate * 1000)

            try {
                const res = await this.client.renewLease(
                    RedisLeaderElectionService.key,
                    this.config.identity,
                    this.config.leaseDuration,
                )

                if (res !== 1) {
                    break
                }
            } catch (err) {
                this.emit(ERROR, err)

                break
            }
        }

        this.emit(STOPPED_LEADING)
    }
}
