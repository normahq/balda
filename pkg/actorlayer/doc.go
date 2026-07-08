// Package actorlayer contains reusable actor runtime primitives.
//
// The package is intentionally transport-agnostic and independent of Balda
// product packages so it can serve as the library boundary for generic actor
// envelopes, errors, retry helpers, and runtime contracts.
//
// Actorlayer is organized as a small set of composable packages:
//   - actorlayer defines the wire-safe Envelope model, actor addresses,
//     payload helpers, and error classification.
//   - dispatch registers typed actors and resolves exact or wildcard actor
//     addresses.
//   - engine executes transport deliveries, serializes work by lane, applies
//     retry policy, and settles deliveries through transport-owned hooks.
//   - transport defines dispatcher, event publisher, event consumer, and
//     drain contracts for broker or queue adapters.
//   - transport/memory provides an in-memory implementation for tests,
//     examples, and lightweight standalone use.
//
// Actor addresses are normalized as case-insensitive full address strings in
// registry and engine dispatch paths. PayloadJSON carries an encoded JSON
// payload string; MarshalPayload and UnmarshalPayload provide the standard
// helpers for that field. ReportTo is optional, but when present it must be a
// valid actor address.
package actorlayer
