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
  /** Compare-and-set: writes `value` only while the key still holds
   *  `expected` and has not expired, and returns whether it did. The renewal
   *  half of the run lock (§3.4): a worker that lost its lock to another can
   *  never extend the other's. Must be ONE atomic step (a Lua script on
   *  Redis), never a get followed by a set. */
  setIfValue(
    key: string,
    expected: string,
    value: string,
    opts?: { exSeconds?: number },
  ): Promise<boolean>;
  /** Compare-and-delete: removes the key only while it holds `expected`, and
   *  returns whether it did. The release half of the run lock: a worker never
   *  frees a lock another worker took after its own lapsed. Atomic, like
   *  `setIfValue`. */
  delIfValue(key: string, expected: string): Promise<boolean>;
}
