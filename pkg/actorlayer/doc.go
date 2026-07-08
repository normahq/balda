// Package actorlayer contains reusable actor runtime primitives.
//
// The package is intentionally transport-agnostic and independent of Balda
// product packages so it can serve as the library boundary for generic actor
// envelopes, errors, retry helpers, and runtime contracts.
//
// Actor addresses are normalized as case-insensitive full address strings in
// registry and engine dispatch paths. PayloadJSON carries an encoded JSON
// payload string; MarshalPayload and UnmarshalPayload provide the standard
// helpers for that field. ReportTo is optional, but when present it must be a
// valid actor address.
package actorlayer
