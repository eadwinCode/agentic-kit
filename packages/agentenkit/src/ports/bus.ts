import type { AgentEvent } from '../core/types.js';

/** Pub/sub port: live fan-out + death notices (§2.2, §2.5).
 *
 *  Delivery is at-most-once — the §2.5 death-notice/heartbeat/watchdog pattern
 *  exists precisely because pub/sub drops. Stronger buses may simplify the
 *  heartbeat but reclamation stays (§3.4). */
export interface EventBus {
  publish(threadId: string, event: AgentEvent): Promise<void>;
  /** Returns an unsubscribe function. The reference adapter runs the §2.5
   *  heartbeat while any subscriber is attached. */
  subscribe(threadId: string, handler: (event: AgentEvent) => void): Promise<() => void>;
}
