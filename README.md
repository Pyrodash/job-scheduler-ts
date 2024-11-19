# @pyrodash/job-scheduler

A distributed job scheduler for Node.js based on Apache Pulsar.

## Setup
1. [Install Apache Pulsar](https://pulsar.apache.org/docs/4.0.x/getting-started-home/)
2. Install the library
```sh
npm install @pyrodash/job-scheduler
```

## Usage
```ts
import { JobScheduler, JobContext, Job } from '@pyrodash/job-scheduler'

interface MyParams {
    name: string
}

class MyJob extends Job<MyParams> {
    handle(ctx: JobContext<MyParams>): void {
        console.log(ctx)
    }
}

const scheduler = new JobScheduler({
    queue: {
        url: 'pulsar://localhost:6650',
        topic: 'persistent://public/default',
        producerTimeoutMs: 0, // How long before inactive producers are terminated
        cleanupIntervalMs: 0, // How often producer garbage collection should run (0 for never)
    },
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
        .withParams({ name: 'Pyrodash' })
        .at(Date.now() + 5000) // 5 seconds from now
        .recurring(1000), // every second
)
// This syntax is also supported
await scheduler.schedule('my-job', {
    params: { name: 'Pyrodash' },
    schedule: {
        deliverAt: Date.now() + 5000,
        rate: 1000,
    },
})
```

## Cancelation
Job cancelation is supported, but you need to implement the Repository interface and pass it to the config.
```ts
interface Repository {
    // This should set recurring to true
    cancel(id: string): Promise<void>

    // This should delete your DB entry if the item is non-recurring or canceled
    // Example query: DELETE FROM Job WHERE ID = 'id' AND (Recurring = false or Canceled = true) RETURNING Canceled;
    // You can also just use Redis Sets
    isCanceled(id: string): Promise<boolean>
}
```
```ts
const myRepo = new MyRepository()
const scheduler = new JobScheduler({
    queue: { ... },
    repository: myRepo,
})

const jobId = await scheduler.schedule('my-job', {
    params: { name: 'Pyrodash' },
    schedule: {
        deliverAt: Date.now() + 5000,
        rate: 1000,
    },
})

await myRepo.add({ jobId, recurring: true, canceled: false })
await scheduler.cancel(jobId)
```