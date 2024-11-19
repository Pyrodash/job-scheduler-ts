export interface Repository {
    cancel(id: string): Promise<void>

    // This should delete your DB entry if the item is non-recurring and/or canceled
    // Example query: DELETE FROM Job WHERE ID = 'id' AND (Recurring = false or Canceled = true) RETURNING Canceled;
    isCanceled(id: string): Promise<boolean>
}
