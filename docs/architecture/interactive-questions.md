# Interactive questions

Owner: Balda maintainers  
Status: active

## Problem

Balda needs a way for running actor flows to ask the user for input and
continue later after a reply arrives.

The asking actor is not always the session actor itself. A session-scoped flow
may activate downstream product actors such as Goalkeeper, and those actors may
also need to ask the user a question in the same conversation.

If question handling is modeled as a handler-owned feature, a transport-owned
timer, or a hidden suspended tool frame, Balda would mix ingress, delivery,
runtime continuation, and durable state ownership across the wrong layers.

## Decision

Balda models questions as a session-scoped interaction primitive with
actor-resume semantics.

- Any product actor may ask a question if it has a valid interaction context.
- Question delivery uses the existing delivery actor boundary.
- A pending question stores both conversation scope and actor resume target.
- User replies are matched to pending questions through durable delivery
  correlation.
- Timeouts reuse one-shot scheduled jobs.
- A reply or timeout starts a new continuation command; Balda does not keep an
  old model/tool turn alive across time.

Synchronous agent permission requests are the one bounded exception. The
ADK-facing permission callback must receive a decision before the active run
can continue, so the permission application service waits while that run
remains active. The request is still provider-neutral, persisted and delivered
through the question boundary, has a configured deadline, and resumes through
a dedicated permission actor. Ordinary conversational questions must continue
to use the actor-resume rule above.

## Core contracts

Balda uses shared question contracts in the dedicated contract package
`questioncmd`.

### Interaction context

`InteractionContext` describes where the question belongs:

- session id;
- channel kind;
- delivery target / locator;
- conversation or thread scope;
- requesting user identity;
- origin metadata for the root turn/job/run.

This context is carried through actor chains explicitly. Product actors must not
reconstruct it indirectly from actor addresses or ambient runtime state.

### Resume target

`ResumeTarget` describes who should continue when the answer arrives:

- actor address;
- subject / namespace;
- correlation id;
- optional metadata.

This is an actor/runtime contract, not a callback function pointer.

### Question request and answer

Shared contracts also define:

- question prompt and options;
- timeout metadata;
- normalized inbound reply payload;
- normalized structured selection payload;
- settled answer payload;
- continuation payloads such as `QuestionAnswered` and
  `QuestionTimedOut`.

When a product workflow needs to request user input based on structured agent
output rather than an ordinary reply, that upstream agent/result contract
should live in its own dedicated contract package. For Goalkeeper this contract
currently lives in `goalresultcmd`; `questioncmd` remains focused on the user
question lifecycle itself.

## Ownership

### Contracts

Transport-neutral shared question contracts belong in a dedicated contract
package, not in `session` or `channel/*`.

### Application

The `questions` application package owns:

- pending question lifecycle;
- durable settlement rules;
- reply correlation policy;
- structured option validation against the persisted request;
- timeout orchestration;
- ports for persistence, delivery publication, channel-control cleanup, and
  scheduled wake-up.

### Domain actors

Product actors own:

- deciding to ask a question;
- supplying `InteractionContext`;
- supplying `ResumeTarget`;
- continuing work after `QuestionAnswered` or `QuestionTimedOut`.

### Ingress

Handlers may:

- parse transport reply or control semantics;
- authenticate the user and resolve session context;
- normalize inbound replies into a shared contract;
- delegate to the question application service or publish a product command.

Handlers must not:

- own pending-question state;
- decide settlement rules;
- own continuation policy.

### Delivery

Delivery remains the external side-effect boundary.

Question prompts are sent through the existing delivery actor path. Concrete
transport packages may project generic options into channel-native controls and
extract transport-specific reply references. They must not own option validity,
pending-question matching, responder policy, or question lifecycle rules.

Control cleanup is also a delivery side effect. After a question settles or
times out, the question application publishes a generic clear-controls request.
Composition routes it through delivery, and only the concrete transport adapter
knows how to remove its controls. Cleanup is best effort and idempotent; it must
not roll back an already durable answer.

### State

State owns durable records for:

- pending questions;
- provider delivery correlation needed to match user replies;
- atomic transitions such as `pending -> answered` or
  `pending -> timed_out`.

## Flow

### Ask path

`actor with InteractionContext -> questions service -> persist pending question -> delivery actor sends prompt -> provider message reference stored`

The asking actor may be the session actor or a downstream actor such as
Goalkeeper.

### Reply path

`ingress -> normalize inbound reply -> questions service resolves pending question -> settle answer atomically -> publish continuation command to ResumeTarget`

If the inbound message does not match a pending question, ingress continues
through the ordinary conversational path.

### Structured selection path

`transport callback -> normalize question id, option coordinate, user and conversation -> questions service reloads persisted request -> validate responder/context/option -> settle answer atomically -> clear channel controls through delivery -> publish continuation command`

Callback payloads are routing hints, not authority. The questions service maps
the selection to the persisted option set and performs the same responder and
conversation checks used by text replies. Repeated or expired selections do not
create another continuation; ingress only acknowledges that the request is no
longer active.

### Timeout path

`questions service -> create one-shot scheduled job -> scheduled wake-up arrives through session ingress -> session actor resolves timeout record -> questions service marks timed out -> publish QuestionTimedOut continuation`

This reuses the existing wait/scheduled-job architecture and the same
session-scoped scheduled wake-up path already used for other delayed work.
Timeout handling does not create a transport-owned timer and does not resume an
old turn frame.

For the bounded agent permission exception, the permission application service
also owns an in-process deadline so a blocked ADK-facing callback fails closed even if
the session actor cannot process the scheduled wake-up yet. The durable
one-shot timeout remains the restart-safe source of truth; its later delivery
is an idempotent no-op after settlement.

## Required properties

- Any actor in a session-scoped activation chain may ask a question by using an
  explicit interaction context.
- Question routing remains conversation-scoped even when actor ownership shifts.
- Provider-specific reply ids are durable and restart-safe.
- A question answer or timeout becomes new actor work, not hidden suspended
  runtime state.
- Permission replies may be restricted to the user who initiated the active
  turn. Unauthorized and invalid replies are consumed but do not settle the
  question.
- Text replies and channel-native selections share one atomic settlement rule.
- Channel controls are removed after answer or timeout without making cleanup
  part of durable settlement.
- Delivery, ingress, and question lifecycle stay in separate owners.

## Consequences

- Session actor ownership does not become a bottleneck for all interactive
  follow-up questions.
- Goalkeeper and other downstream actors can ask questions without importing
  channel-specific behavior.
- Restart safety stays aligned with Balda's scheduled-job wake-up model.
- Architecture enforcement should keep dedicated placement for question
  contracts and the question application package.

## Current scope

The current scope stays narrow:

- channel-appropriate permission presentation and generic question prompts;
- free-text answers and transport-neutral numbered options;
- Telegram inline controls for any question carrying options, with one option
  per row and a numbered text fallback if controlled delivery fails;
- durable correlation by provider reply reference;
- one answer per pending question;
- optional timeout via one-shot scheduled job;
- continuation to explicit actor resume target;
- agent permission policy modes `allow_all`, `ask`, and `deny_all`; `ask` is
  supported for Telegram and Slack Agent and fails closed elsewhere.

Additional channel-control projections, richer validation, and multiple
simultaneous question policies may be added later without changing the generic
selection contract.

## Update triggers

- Introduction of dedicated question packages or contracts.
- Changes to timeout ownership or scheduling flow.
- Changes to actor continuation semantics for interactive user input.
- New transport reply-correlation requirements.
