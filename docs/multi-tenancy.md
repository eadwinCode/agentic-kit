# Multi-tenancy

One deployment, many customers, and no customer able to see another's data.

The whole mechanism is [run state](./run-state.md): whatever you attach to a run
reaches every storage call it makes, every tool, and every nested run —
including in a worker that picks the job up hours later, in another process,
after an approval. The platform never reads it.

Scoping then happens in **your** storage layer, where your database's rules
already live, rather than being threaded through prompts or re-derived per
query.

---

## 1. Type the state

```ts
// types/agentrun.d.ts
declare module 'agentrun' {
  interface AgentRunState {
    orgId: string;
    userId: string;
  }
}
```

Declaration merging rather than a `<TState>` parameter, which would otherwise
have to appear on `Storage`, `AgentCore`, every tool and every subagent before
reaching the one place it is read.

Now `ctx.state.orgId` is typed everywhere and a typo is a compile error.

## 2. Put the tenant in your schema

```prisma
model Thread {
  id        String   @id @default(cuid())
  orgId     String
  state     String
  model     String
  updatedAt DateTime @updatedAt
  messages  Message[]

  @@index([orgId, updatedAt])
}

model Message {
  id       String  @id @default(cuid())
  threadId String
  orgId    String          // denormalized so a message query never joins to check
  agentId  String?
  role     String
  content  Json
  createdAt DateTime @default(now())

  @@index([threadId, createdAt])
  @@index([orgId])
}
```

Carrying `orgId` on every table rather than joining up to the thread is worth
the denormalization: it means a mistake is a *missing row*, not a leaked one.

## 3. Scope your Storage

Every method receives the context as its last argument.

```ts
export class TenantStorage implements Storage {
  constructor(private db: PrismaClient) {}

  threads = {
    get: async (threadId: string, ctx: StorageContext) =>
      this.db.thread.findFirst({
        where: { id: threadId, orgId: ctx.state.orgId },   // ← the whole game
      }),

    create: async (init: { model?: string } | undefined, ctx: StorageContext) =>
      this.db.thread.create({
        data: { orgId: ctx.state.orgId, model: init?.model ?? 'gpt-4o', state: 'IDLE' },
      }),

    list: async (ctx: StorageContext) =>
      this.db.thread.findMany({
        where: { orgId: ctx.state.orgId },
        orderBy: { updatedAt: 'desc' },
      }),

    setState: async (threadId, state, ctx) => {
      await this.db.thread.updateMany({
        where: { id: threadId, orgId: ctx.state.orgId },
        data: { state },
      });
    },

    // ONE conditional update. Exactly one caller wins.
    claimState: async (threadId, from, to, ctx) => {
      const { count } = await this.db.thread.updateMany({
        where: { id: threadId, orgId: ctx.state.orgId, state: from },
        data: { state: to },
      });
      return count === 1;
    },

    delete: async (threadId, ctx) => {
      await this.db.thread.deleteMany({ where: { id: threadId, orgId: ctx.state.orgId } });
    },
  };

  messages = {
    append: async (threadId, message, ctx) =>
      this.db.message.create({
        data: { threadId, orgId: ctx.state.orgId, ...message },
      }),

    list: async (threadId, opts, ctx) =>
      this.db.message.findMany({
        where: {
          threadId,
          orgId: ctx.state.orgId,
          // Scope by producer when asked — unscoped, a subagent's turns leak
          // into the parent's prompt.
          ...(opts && 'agentId' in opts ? { agentId: opts.agentId } : {}),
        },
        orderBy: { createdAt: 'asc' },
      }),

    // …events and usage follow the same shape.
  };
}
```

`findFirst` with `orgId` rather than `findUnique` by id is deliberate. A thread
id from the wrong tenant returns `null`, and the runtime treats that as "thread
not found" — which is the correct answer to give someone asking about a thread
that is not theirs.

## 4. Attach the tenant at the edge

The tenant comes from your session, never from the request body.

```ts
export async function POST(req: NextRequest) {
  const session = await requireSession(req);          // your auth
  const { threadId, prompt, model } = await req.json();

  const result = await chat.run({
    threadId,
    prompt,
    model,
    state: { orgId: session.orgId, userId: session.userId },
  });

  if (!result.accepted) return NextResponse.json(result, { status: 409 });
  return NextResponse.json(result, { status: 202 });
}
```

> **Never read the tenant from the client.** A body field named `orgId` is a
> request to read someone else's data.

## 5. Pass it to reads and operations too

A run carries its state on the dispatch ticket. A **read** has no ticket, so it
takes the state directly:

```ts
await runtime.listThreads({ orgId });
await runtime.getThreadSnapshot(threadId, { orgId });
await runtime.getThreadUsage(threadId, { orgId });
await runtime.deleteThread(threadId, { orgId });
await runtime.hitl.respond({ threadId, toolCallId, approved, state: { orgId } });
await chat.stop(threadId, { orgId });
```

Forget one and that call reaches your storage with an empty context. Write your
storage so an absent `orgId` returns nothing rather than everything:

```ts
const orgId = ctx.state.orgId;
if (!orgId) throw new Error('storage called without a tenant');
```

Failing loudly here is the single highest-value line in a multi-tenant
integration. It turns a silent cross-tenant read into a stack trace.

## 6. Tools inherit it

```ts
import { agentTool } from 'agentrun';

const lookupInvoice = agentTool({
  parameters: z.object({ invoiceId: z.string() }),
  execute: async ({ invoiceId }, { state }) =>
    db.invoice.findFirst({ where: { id: invoiceId, orgId: state.orgId } }),
});
```

`agentTool` rather than the SDK's `tool()` because the latter cannot type
`state` — see [Agents and tools](./agents-and-tools.md#tools-see-the-runs-state).

The model cannot reach outside the tenant even if it asks to, because the tool —
not the prompt — decides what it can see.

Subagents inherit the same state, so a delegated run is scoped identically.

---

## What survives what

| Boundary | State survives? | How |
| :--- | :--- | :--- |
| `run()` → worker | yes | on the dispatch ticket |
| step → step | yes | same worker, same input |
| worker dies → redrive | yes | the ticket is re-delivered |
| park → approval → resume | yes | persisted with the park, rebuilt on resume |
| approval expiry | yes | the expiry job carries it |
| parent → subagent | yes | inherited |
| a read (`listThreads`, snapshot…) | only if you pass it | no ticket exists |

The park row is worth dwelling on. A parked approval can outlive the process
that created it by days, so the state has to travel on the ticket rather than
sit in a closure that is long gone. It does — but this is exactly the kind of
thing to assert in your own test suite, because the failure is silent.

## Testing your isolation

The test that matters is not "does the happy path work" but "does the wrong
tenant get nothing":

```ts
it('does not let one tenant read another thread', async () => {
  const a = await chat.run({ prompt: 'hi', state: { orgId: 'a', userId: 'u1' } });
  await runtime.worker.handleJob(queue.items.at(-1)!);

  expect(await runtime.getThreadSnapshot(a.threadId!, { orgId: 'b', userId: 'u2' })).toBeNull();
  expect(await runtime.listThreads({ orgId: 'b', userId: 'u2' })).toEqual([]);
});
```

And the one people forget — that isolation holds **after** an approval, which is
where a resumed run could otherwise lose its scope:

```ts
it('keeps the tenant across a park and resume', async () => {
  // …park on a marked tool, then:
  await runtime.hitl.respond({ threadId, toolCallId, approved: true, state: { orgId: 'a' } });
  await runtime.worker.handleJob(queue.items.at(-1)!);

  const unscoped = storageCalls.filter((c) => c.ctx.state.orgId !== 'a');
  expect(unscoped).toEqual([]);
});
```

A spying `Storage` wrapper that records the context of every call makes both
tests easy, and is far more convincing than asserting on results alone. The
package's own suite does exactly this in `test/run-state.test.ts`.

## Operational data is not tenant-scoped

The [operational store](./observability.md) is the platform's, not yours. Run
records, step timings and the thread index live in `agentic_` tables, and
`runtime.admin.*` reads **across all tenants**.

That is right for an operator dashboard and wrong for a customer-facing one.

- **Keep `runtime.admin.*` behind operator authorization.** Never expose it to a
  tenant.
- If a customer needs their own run history, build it from *your* tables — you
  have `orgId` on every row.
- With `recordPayloads` on, the run's state (and therefore `orgId`) is recorded
  with the run, along with prompts and tool payloads. Decide whether that
  belongs in an operational database:

```ts
config: { recordPayloads: false }
```

## Per-tenant limits

`billingPreCheck` runs before anything is written, so a tenant over its limit
costs nothing:

```ts
config: {
  billingPreCheck: async (threadId) => {
    const org = await orgForThread(threadId);
    if (!org) return { ok: false, error: 'Unknown thread' };
    if (org.creditsRemaining <= 0) return { ok: false, error: 'Out of credits' };
    return { ok: true };
  },
}
```

Per-tenant token ceilings ride on the run:

```ts
await chat.run({ prompt, state, tokenBudget: org.plan.perRunTokenBudget });
```

## Isolating further

Run state is row-level isolation, which suits most products. Two stronger
options, both implemented in your `Storage` rather than in the library:

**A database per tenant.** Route on the state:

```ts
private db(ctx: StorageContext) {
  return this.pools.get(ctx.state.orgId) ?? this.fail(ctx);
}
```

Watch your connection count — a pool per tenant does not scale to thousands.

**A separate deployment per tenant.** Total isolation, highest cost. Do not do
this with one runtime *per tenant inside one process*: each runtime has its own
connections, and that multiplies by your customer count.

## Checklist

- [ ] `AgentRunState` augmented with your tenant fields
- [ ] `orgId` on every table the storage touches
- [ ] Every `Storage` method filters by `ctx.state.orgId`
- [ ] Storage **throws** when the tenant is absent
- [ ] `claimState` is one atomic conditional update, scoped by tenant
- [ ] Tenant read from the session at the edge, never from the request body
- [ ] State passed to `listThreads`, `getThreadSnapshot`, `getThreadUsage`, `deleteThread`, `stop`, `hitl.respond`
- [ ] Tools scope their own queries by `state.orgId`
- [ ] `runtime.admin.*` behind operator authorization only
- [ ] `recordPayloads` decided deliberately
- [ ] A test proving the wrong tenant gets nothing — including after an approval
