# Manual Smoke Tests

Manual curl-based checklist for verifying simulation behavior by hand, before
the dashboard exists (Phase 4) and while the failure/election mechanics are
still being made real (Phases 1-3). Endpoints and payloads below were read
directly from the controller/DTO/model classes, not guessed.

Run the backend first:

```bash
cd backend
./mvnw spring-boot:run
```

Default port is `8080` (no `server.port` override in `application.properties`).
Security is disabled by default (`app.security.disable=true`), so no auth
headers are needed against a fresh checkout.

## Important gotcha before you start

`SimulationService.runSimulation()` calls `nodeRepository.findAll()` — it pulls
**every** `Node` document in the database into the simulation, regardless of
the `nodeCount` set in the simulation's config. There is no per-simulation node
scoping. If you've created nodes from a previous test run and don't delete
them, they'll silently join your next simulation too. For a clean run, either
delete all nodes between tests (`GET /api/nodes` then `DELETE /api/nodes/{id}`
for each) or just keep track of exactly which nodes exist before calling `run`.

## 1. Create nodes

Create three nodes that will participate in the simulation (`Node.status` is
the `NodeStatus` enum: `ACTIVE | INACTIVE | FAILED`):

```bash
curl -s -X POST http://localhost:8080/api/nodes \
  -H "Content-Type: application/json" \
  -d '{"id": "node1", "address": "10.0.0.1", "status": "ACTIVE"}'

curl -s -X POST http://localhost:8080/api/nodes \
  -H "Content-Type: application/json" \
  -d '{"id": "node2", "address": "10.0.0.2", "status": "ACTIVE"}'

curl -s -X POST http://localhost:8080/api/nodes \
  -H "Content-Type: application/json" \
  -d '{"id": "node3", "address": "10.0.0.3", "status": "ACTIVE"}'
```

Confirm they exist:

```bash
curl -s http://localhost:8080/api/nodes | python3 -m json.tool
```

## 2. Create a simulation

`ConsensusAlgorithmType`: `PAXOS | RAFT | MULTI_PAXOS | VIEW_STAMPED_REPLICATION | ZAB`
`TopologyType`: `STAR | MESH | RING | TREE` (per the Phase 1 review note: this
field is recorded but does not currently affect message routing — every
algorithm broadcasts to the full node set regardless of topology).

```bash
curl -s -X POST http://localhost:8080/api/simulations \
  -H "Content-Type: application/json" \
  -d '{
    "name": "raft-smoke-test",
    "config": {
      "algorithmType": "RAFT",
      "nodeCount": 3,
      "topologyType": "MESH",
      "failurePercentage": 0,
      "metricsToCapture": ["latency", "throughput"],
      "tlsEnabled": false
    }
  }' | python3 -m json.tool
```

Copy the `"id"` from the response — every command below needs it as `{simId}`.

## 3. Start the simulation

```bash
curl -s -X POST http://localhost:8080/api/simulations/{simId}/run
```

Expect: `Simulation started with ID: {simId}`. Tail the application log —
you should see `Topology mapping: {...}` from `SimulationOrchestrator`, then
heartbeat/election/proposal log lines depending on the algorithm.

## 4. Check simulation status

```bash
curl -s http://localhost:8080/api/simulations/{simId} | python3 -m json.tool
```

Expect `"status": "RUNNING"`. Also check events and metrics:

```bash
curl -s http://localhost:8080/api/simulations/{simId}/events | python3 -m json.tool
curl -s http://localhost:8080/api/simulations/{simId}/metrics | python3 -m json.tool
```

## 5. Fail a node

```bash
curl -s -X POST http://localhost:8080/api/simulations/{simId}/failNode/node1
```

Expect: `Node node1 failed in simulation {simId}`.

**Known current limitation (Phase 2 target):** as of this baseline, failing a
node only flips its `NodeStatus` to `FAILED` — it does not stop the node's
message-processing loop or its heartbeat, and `MessageRouter` does not yet
check status before delivering. So at this point in the project, a "failed"
node will keep behaving exactly like a healthy one. Re-run this smoke test
after Phase 2.1/2.2 land to confirm the node actually goes silent.

## 6. Recover a node

**No endpoint exists for this yet.** There is no `recoverNode`/"un-fail" REST
endpoint anywhere in `NodeController` or `SimulationController` — only
`failNode`. `VirtualNode.recoverNode()` exists as a method but nothing in the
web layer calls it. This is exactly the gap Phase 2.1 is meant to close
(implement real stop/restart semantics) and Phase 4.1 calls out (expose a
recover control in the dashboard). Once an endpoint is added — e.g.
`POST /api/simulations/{id}/recoverNode/{nodeId}` mirroring `failNode`'s
shape — add the matching curl command here.

## 7. Propose a value

**No endpoint exists for this yet either.** There's no REST path that calls
`ConsensusAlgorithm.propose(...)` on a running simulation's leader/proposer.
`AlgorithmController` only exposes `GET /api/algorithms` (the list of
available algorithm types) — nothing simulation-instance-specific. This is
the gap Phase 4.1 flags ("add a `POST /api/simulations/{id}/propose`
endpoint if one doesn't already exist" — confirmed here: it doesn't). Add the
curl command here once that endpoint is built.

## 8. Check node status

```bash
curl -s http://localhost:8080/api/nodes/node1 | python3 -m json.tool
curl -s http://localhost:8080/api/nodes | python3 -m json.tool
```

Note: this returns the `Node` document's status field as stored via
`NodeService`/`NodeRepository` — confirm whether it reflects the live
in-memory `VirtualNode` status or only what was last written to Mongo via
`failNode`'s side effects. If they diverge, that's worth noting once the
dashboard (Phase 4) needs a single source of truth for node state.

## 9. Stop the simulation

```bash
curl -s -X POST http://localhost:8080/api/simulations/{simId}/stop
```

Expect: `Simulation stopped for ID: {simId}`, and the simulation's status to
become `COMPLETED` on a follow-up `GET /api/simulations/{simId}`.

## 10. Clean up

```bash
curl -s -X DELETE http://localhost:8080/api/simulations/{simId}
curl -s -X DELETE http://localhost:8080/api/nodes/node1
curl -s -X DELETE http://localhost:8080/api/nodes/node2
curl -s -X DELETE http://localhost:8080/api/nodes/node3
```

Remember the gotcha from the top: leftover nodes silently join the next
simulation you start, so clean up before your next run.
