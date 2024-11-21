# @pyrodash/job-scheduler

A distributed job scheduler for Node.js. There are currently two drivers being wrapped by this library, one for Apache Pulsar and the other for Redis.

## Apache Pulsar
### Setup
1. [Install Apache Pulsar](https://pulsar.apache.org/docs/4.0.x/getting-started-home/)
2. Install the library
```sh
npm install @pyrodash/job-scheduler pulsar-client
```

### Usage
```ts
import { PulsarScheduler, JobContext, Job } from '@pyrodash/job-scheduler'

interface MyParams {
    name: string
}

class MyJob extends Job<MyParams> {
    handle(ctx: JobContext<MyParams>): void {
        console.log(ctx)
    }
}

const scheduler = new PulsarScheduler({
    url: 'pulsar://localhost:6650',
    topic: 'persistent://public/default',
    producerTimeoutMs: 0, // How long before inactive producers are terminated
    cleanupIntervalMs: 0, // How often producer garbage collection should run (0 for never)
})

// Registering a worker
await scheduler.register(new MyJob()) // Job name will be the class name in kebab-case

// This syntax is also supported
await scheduler.handle('my-job', (ctx: JobContext<MyParams>) => {
    console.log(ctx)
})

// Scheduling a job
await scheduler.schedule(
    MyJob
        .withId('hello-world')
        .withParams({ name: 'Pyrodash' })
        .at(Date.now() + 5000) // 5 seconds from now
        .recurring(1000), // every second
)
// This syntax is also supported
await scheduler.schedule('my-job', {
    id: 'hello-world',
    params: { name: 'Pyrodash' },
    schedule: {
        deliverAt: Date.now() + 5000,
        rate: 1000,
    },
})
```

## Redis
### Setup
1. [Install Redis](https://redis.io/docs/latest/operate/oss_and_stack/install/install-redis/)
2. Install the library
```sh
npm install @pyrodash/job-scheduler bull ioredis
```

### Usage
```ts
import { RedisScheduler } from '@pyrodash/job-scheduler'

const scheduler = new RedisScheduler({
    ... ioredis config
})

// Usage is identical to the Pulsar scheduler
// The Redis Scheduler supports cancelation out of the box
await scheduler.cancel('my-job', 'job id')
```