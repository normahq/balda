# Actorlayer

`pkg/actorlayer` is Balda's reusable actor runtime library boundary. It is
transport-agnostic and independent of Balda product packages: code in this
package does not import Balda handlers, state stores, channel adapters, NATS, or
provider runtimes.

Actorlayer is currently part of the root Go module, so consumers import it as
`github.com/normahq/balda/pkg/actorlayer/...`. It is package-independent, but it
is not separately versioned from Balda unless it is later split into its own Go
module or repository.

## Package Layout

- `actorlayer`: envelope, actor address, payload, retry, and error
  classification primitives.
- `actorlayer/dispatch`: actor registration and exact/wildcard address
  resolution.
- `actorlayer/engine`: transport-agnostic delivery execution, lane
  serialization, retry handling, lifecycle events, and settlement.
- `actorlayer/transport`: small interfaces for dispatching commands, publishing
  events, consuming events, and draining transports.
- `actorlayer/transport/memory`: in-memory transport implementation for tests,
  examples, and local standalone use.

The production dependency direction is one-way:

```text
transport/memory -> engine -> dispatch -> actorlayer
transport/memory -> transport -> actorlayer
```

The root `actorlayer` package imports only the Go standard library.

## Runtime Model

Actorlayer processes work as envelopes delivered by a transport adapter:

```text
Envelope -> Delivery -> Runtime -> Registry -> Actor.Handle -> Ack/Retry/DeadLetter
```

An `Envelope` carries identity, addressing, idempotency, metadata, and an
encoded JSON payload. Transport adapters expose incoming messages as
`engine.Delivery` values. A runtime validates the envelope, resolves a lane key
for per-lane serialization, invokes the registered actor, then settles the
delivery through transport-owned `Ack`, `Retry`, or `DeadLetter` hooks.

The engine does not own broker behavior. Redelivery, durable storage, publish
semantics, and dead-letter persistence belong to the concrete transport
implementation.

## Extension Points

Implement these interfaces to connect actorlayer to another queue or broker:

- `transport.Dispatcher` to send command envelopes.
- `engine.Source` to stream command deliveries into a runtime.
- `engine.Delivery` to expose message metadata and settlement operations.
- `transport.EventPublisher` and `transport.EventConsumer` for lifecycle or
  domain event streams.
- `transport.Drainer` for graceful transport shutdown.

Use `dispatch.Registry` when actor lookup needs a custom implementation. The
provided `dispatch.MemoryRegistry` is suitable for process-local actors and
supports wildcard fallback addresses such as `session:*`.

## Error and Retry Semantics

Actor errors are classified as transient, permanent, decode, policy, actor not
found, or external delivery failures. `actorlayer.IsRetryableError` treats
transient, actor-not-found, and external-delivery errors as retryable by default;
permanent, decode, and policy errors are terminal unless a host supplies a
different retry policy.

Runtime retry behavior is configured through `engine.RetryPolicy`. The policy
decides whether an error is retryable, how long to delay before retrying, and
when attempts are exhausted.

## What Belongs Outside Actorlayer

Keep product and infrastructure policy outside this package:

- broker subjects, stream names, queue names, and headers;
- database schemas, task projections, and read models;
- channel/provider concepts such as Slack, Telegram, Zulip, sessions, goals, or
  Balda users;
- app lifecycle wiring, observability policy, and configuration loading.

Balda-specific runtime policy currently lives under `internal/apps/balda/swarm`
and concrete durable transport adaptation lives under
`internal/apps/balda/eventbus/nats`.
