export function sleep(ms: number): Promise<void> {
    return new Promise((resolve) => setTimeout(resolve, ms))
}

export function rand(min: number, max: number) {
    // min and max included
    return Math.floor(Math.random() * (max - min + 1) + min)
}

export function getName(instance: unknown) {
    return Object.getPrototypeOf(instance).constructor.name
}
