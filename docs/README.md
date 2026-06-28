# Distributed Systems Simulator (DSS)

The **Distributed Systems Simulator (DSS)** models and lets you experiment with core distributed-consensus protocols — Paxos, Multi-Paxos, and Raft — over a simulated network with virtual nodes, configurable message loss/delay, and real crash-stop node failures.

## Introduction to Distributed Systems

A distributed system is a network of independent computers that work together as a single system to achieve a common goal. Key challenges include:
- **Consensus and Coordination:** Ensuring that multiple nodes agree on a common state despite failures or delays.
- **Fault Tolerance:** Detecting and recovering from node failures without compromising system integrity.
- **Scalability and Heterogeneity:** Handling growing numbers of nodes and diverse network conditions.

In DSS, these challenges are explored by simulating how nodes interact using different consensus protocols while accounting for message routing, loss, delay, and node failure/recovery.

## Project Purpose

The primary objectives of DSS are:
- **Experimentation:** To serve as a testbed for comparing consensus algorithms under controlled, adversarial conditions (message loss, delay, node crashes).
- **Education:** To provide detailed, runnable insight into how consensus is actually achieved — not just described — in distributed systems.
- **Simulation:** To mimic real-world distributed behavior using virtual nodes, a central message router, and schedulers for periodic tasks such as heartbeats, election timeouts, and metrics collection.

## Per-Algorithm Implementation Status

Implementation depth is intentionally uneven across protocols. This is a scope decision, not an oversight — Raft is the protocol this project goes deep on; Paxos and Multi-Paxos demonstrate correct quorum-based agreement under crash-stop.

| Algorithm | Happy path | Crash-stop semantics | Leader-failure recovery |
|---|---|---|---|
| **Raft** | ✅ | ✅ a failed node stops processing and heartbeating entirely until recovered | ✅ randomized election timeout (150–300ms, jittered per node), real leader failure triggers a new election, vote granting includes a log-completeness check, and a recovered follower catches up automatically via the leader's existing `AppendEntries` conflict-backoff logic |
| **Paxos** | ✅ globally unique proposal numbers, full Learner phase (every acceptor learns the chosen value via a COMMIT broadcast, not just the proposer) | ✅ Paxos has no leader, so a crashed non-quorum node doesn't block progress | N/A — no leader to fail over |
| **Multi-Paxos** | ✅ same proposal-number/Learner-phase guarantees as Paxos, plus a stable leader to skip repeated Prepare phases | ✅ a crashed acceptor/learner that isn't the leader doesn't block progress | ❌ **not implemented.** If the Multi-Paxos leader crashes, there is no automatic re-election — this is a documented limitation (see below), not a bug |

For Paxos and Multi-Paxos, a recovered crashed node simply rejoins and participates in future rounds — there's no ordered log to retroactively catch it up on, unlike Raft.

## Network Model

- **Real crash-stop failures.** A `FAILED` node is deaf and mute: it stops processing inbound messages and stops sending heartbeats immediately, and stays that way until explicitly recovered. `MessageRouter` also drops any message to or from a `FAILED` node (tracked via a `droppedMessageCount` metric) rather than delivering it.
- **Configurable message loss and delay.** `MessageRouter` exposes a live-adjustable random loss rate and a delay range; both are settable at simulation start and updatable mid-run via `PUT /api/simulations/{id}/network-conditions` (the dashboard exposes these as sliders). Loss and delay are global probabilities applied to every message, not per node-pair — there's no way to model an asymmetric or partial network partition (e.g., "node A can't reach node B but everyone else can").
- **Topology is visualization metadata, not enforcement.** Selecting RING/STAR/TREE/etc. produces a neighbor map the dashboard uses to draw the node graph, but it does **not** constrain message delivery — every consensus protocol here needs full connectivity for its quorum math, so a topology-constrained router would silently break all of them. Real multi-hop/gossip-style routing is explicitly out of scope (see Known Limitations).

## Dashboard

A minimal React dashboard is wired to the backend's REST and WebSocket APIs:
- **Node grid** — one card per node showing live status, protocol role (Raft `LEADER`/`FOLLOWER`/`CANDIDATE`, Multi-Paxos `LEADER`/`FOLLOWER`, Paxos `ACCEPTOR`), the value that node has committed, and a protocol-specific state line (e.g. Raft's `term N · committed K/M`), polled from the live node-status endpoint.
- **Topology graph** — an SVG node graph drawn from the simulation's adjacency map (`MESH`/`RING`/`STAR`/`TREE`), colored live from the same node-status poll so a node turns red the instant it fails and the leader is highlighted as leadership moves. (Topology is a visualization of the chosen layout; it does not constrain message routing — see Network Model.)
- **Live event log** — a STOMP/SockJS client subscribed to the simulation's WebSocket topics, showing the run as a narrative: `LEADER_ELECTED`, `VALUE_PROPOSED`, `VALUE_COMMITTED`, `NODE_FAILED`, `NODE_RECOVERED`.
- **Metrics panel** — live counts of messages delivered, proposals, commits (one per cluster-agreed value), and dropped messages.
- **Controls** — start/stop a simulation, fail/recover a specific node, and propose a value.
- **Network sliders** — debounced controls for the loss rate and delay range described above.

It's intentionally plain (fetch + basic CSS + inline SVG, no component library) — the goal is a real, demonstrable simulation, not a polished product.

## Security

DSS is an **unauthenticated local demo / educational tool**. There is no login, no user accounts, and no token handling — every endpoint is open, and `SecurityConfig` simply permits all requests (CSRF is disabled because the API is stateless and is only driven by the bundled dashboard).

This is a deliberate choice for what the project is: a single-user simulator you run on your own machine to watch consensus protocols behave. **Do not expose it on an untrusted network.** Authentication is intentionally out of scope.

## Known Limitations

These are deliberate scope decisions for this milestone, not bugs:

- **Multi-Paxos has no leader-failure detection or re-election.** A backup node could reuse the existing per-node phi-accrual failure detector to trigger a new Prepare round, but that's future work.
- **Topology selection doesn't constrain routing.** It's visualization metadata only (see Network Model above). Multi-hop/gossip forwarding that would make topology actually matter is out of scope.
- **No durable or persistent log/term storage.** Every protocol's state (logs, terms, proposal numbers) is in-memory only and resets on restart. This applies uniformly across all three algorithms.
- **`GET /api/simulations/{id}/events` always returns 500.** Events are published to the WebSocket topic but are never persisted back onto the `Simulation` document, so this REST endpoint has nothing to read. The dashboard's event log consumes the WebSocket topic directly and never calls this endpoint, so the gap is dead code, not a blocking bug — but it should be fixed or removed rather than left as a 500.
- **No authentication.** The app is an unauthenticated local demo tool (see Security above); there is no login, user store, or access control. This is intentional, not a gap to be worked around.
- **Message loss/delay are global, not per-link.** There's no way to simulate an asymmetric network partition (some node pairs cut off, others fine) — only a uniform random chance applied to every message in the simulation.
- **No dynamic membership.** The set of nodes in a simulation is fixed once it starts; nodes can fail and recover, but none can be added or removed mid-run.

## Project Structure

The codebase is organized into several top-level directories:

- **backend/**
  Spring Boot application with packages for configuration, consensus algorithms, messaging, simulation orchestration, and REST controllers.

- **frontend/**
  React dashboard (node grid, live event log, controls, network sliders) — plain fetch + CSS, no component library.

- **deployment/**
  Docker Compose (backend + frontend + MongoDB, all built from source) and Kubernetes manifests (resource limits, liveness/readiness probes via Spring Actuator, and a Secret for the Mongo URI) for running the backend and its dependencies.

- **docs/**
  Architecture, sequence diagrams, the simulation lifecycle, and additional design decisions:
  - [consensus-architecture.md](consensus-architecture.md)
  - [consensus-sequence-diagrams.md](consensus-sequence-diagrams.md)
  - [high-level-architecture.md](high-level-architecture.md)
  - [simulation-lifecycle.md](simulation-lifecycle.md)
  - [simulation-node-state.md](simulation-node-state.md)

## Running Locally

**Backend** (requires MongoDB running locally on `27017`):
```bash
cd backend
./mvnw spring-boot:run
```

**Frontend:**
```bash
cd frontend
npm install
npm start   # proxies API calls to localhost:8080
```

**Full stack via Docker Compose** (backend + frontend + MongoDB, all built from source):
```bash
cd deployment
docker compose up --build
```
Then open the dashboard at <http://localhost:3000> (nginx serves the built frontend and reverse-proxies `/api` and `/ws` to the backend).

## Conclusion

The Distributed Systems Simulator provides a real (if intentionally uneven) environment for studying how consensus protocols behave under node failure and network unreliability — not just on the happy path. Raft is implemented end-to-end, including crash-stop recovery; Paxos and Multi-Paxos are correct under crash-stop short of leader failure. For deeper design detail, see the docs listed above.
