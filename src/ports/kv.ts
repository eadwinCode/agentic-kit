/** Hot key-value port: thread state cache, HITL handoff keys, seq & attempt
 *  counters, per-thread run locks (§3.2). Backed by Redis in the reference
 *  adapters — any KV works. */
export interface Kv {
  get(key: string): Promise<string | null>;
  /** Returns true iff the value was written. With `onlyIfNotExists` (SET NX),
   *  returns false when the key already existed — this is the atomic
   *  primitive behind the per-thread run lock (§3.4). */
  set(
    key: string,
    value: string,
    opts?: { exSeconds?: number; onlyIfNotExists?: boolean },
  ): Promise<boolean>;
  del(key: string): Promise<void>;
  incr(key: string): Promise<number>;
}
