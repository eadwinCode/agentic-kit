/** Hot key-value port: thread state cache, HITL handoff keys, seq & attempt
 *  counters (§3.2). Backed by Redis in the reference adapter — any KV works. */
export interface Kv {
  get(key: string): Promise<string | null>;
  set(key: string, value: string, opts?: { exSeconds?: number }): Promise<void>;
  del(key: string): Promise<void>;
  incr(key: string): Promise<number>;
}
