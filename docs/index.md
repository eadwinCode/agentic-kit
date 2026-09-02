---
layout: home

hero:
  name: agentic-kit
  text: A durable runtime for AI agent runs
  tagline: >-
    Not an agent framework. It owns the lifecycle of a run — that it outlives
    the request that started it, survives a worker dying mid-step, can be
    stopped, parked for a human, resumed exactly where it stopped, nested,
    metered, and watched by several people at once.
  actions:
    - theme: brand
      text: Get started
      link: /getting-started
    - theme: alt
      text: Core concepts
      link: /concepts
    - theme: alt
      text: GitHub
      link: https://github.com/eadwinCode/agentic-kit

features:
  - title: Runs outlive the request
    details: >-
      run() persists, enqueues and returns in milliseconds. The model call
      happens somewhere else, later, in a worker that can die and resume from
      the last committed step.
  - title: A human in the middle
    details: >-
      A parked approval is a durable state holding no process — no worker, no
      lock, no memory. It expires on a queue timer whether anyone is watching
      or not.
  - title: Subagents are runs
    details: >-
      Same loop, same table, same persistence. So a subagent can use tools,
      park for a human, and be resumed — none of it built twice.
  - title: Your database
    details: >-
      Four small interfaces stand between the engine and your stack. Postgres,
      Mongo, Dynamo, SQLite — the engine imports no driver.
  - title: Every client agrees
    details: >-
      Hydrate from the durable log, tail the bus from a cursor. Two tabs, a
      reload, and a mid-run reconnect all converge on the same conversation.
  - title: Bring your own UI
    details: >-
      use-agentkit is a React hook with every route, label and transport under
      your control — and the event log is a public contract you can build any
      client over.
---
