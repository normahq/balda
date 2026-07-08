// Package engine provides transport-agnostic actor delivery execution.
//
// Runtime validates envelope input, serializes deliveries by resolver lane,
// calls the supplied actor handler, and settles each delivery through Ack,
// Retry, or DeadLetter. Lifecycle events include delivery identity and lane
// metadata and terminal events are emitted only after settlement succeeds.
// InProgress is a delivery hook for host adapters that own heartbeat cadence;
// Runtime exposes EmitInProgress for event publication but does not start a
// heartbeat loop on its own.
package engine
