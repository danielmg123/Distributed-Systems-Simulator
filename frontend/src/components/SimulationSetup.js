import { useEffect, useState } from "react";
import { api } from "../api";

const TOPOLOGY_TYPES = ["MESH", "RING", "STAR", "TREE"];

// Creates the node pool and the simulation, then runs it. The backend currently
// initializes a simulation with every persisted node (not just nodeCount of them --
// see SimulationService.runSimulation), so this seeds exactly nodeCount fresh nodes
// each time to keep the demo predictable.
export default function SimulationSetup({ onSimulationStarted }) {
  const [algorithms, setAlgorithms] = useState([]);
  const [name, setName] = useState("Demo Simulation");
  const [nodeCount, setNodeCount] = useState(5);
  const [algorithmType, setAlgorithmType] = useState("RAFT");
  const [topologyType, setTopologyType] = useState("MESH");
  const [busy, setBusy] = useState(false);
  const [error, setError] = useState(null);

  useEffect(() => {
    api
      .getAlgorithms()
      .then((list) => {
        setAlgorithms(list);
        if (list.length > 0) {
          setAlgorithmType(list[0].type);
        }
      })
      .catch((err) => setError(err.message));
  }, []);

  async function handleSubmit(e) {
    e.preventDefault();
    setBusy(true);
    setError(null);
    try {
      // Clear out nodes from any previous demo run -- runSimulation() currently pulls
      // in every persisted node regardless of nodeCount, so leftovers would silently
      // join this new simulation too.
      const existing = await api.getNodes();
      await Promise.all(existing.map((n) => api.deleteNode(n.id)));

      for (let i = 1; i <= nodeCount; i++) {
        await api.createNode({ id: `node${i}`, address: `10.0.0.${i}`, status: "ACTIVE" });
      }

      const simulation = await api.createSimulation({
        name,
        config: {
          algorithmType,
          nodeCount: Number(nodeCount),
          topologyType,
          failurePercentage: 0,
          messageLossRate: 0,
          minMessageDelayMs: 0,
          maxMessageDelayMs: 0,
        },
      });

      await api.runSimulation(simulation.id);
      onSimulationStarted(simulation.id);
    } catch (err) {
      setError(err.message);
    } finally {
      setBusy(false);
    }
  }

  return (
    <form className="setup-form" onSubmit={handleSubmit}>
      <h2>Start a Simulation</h2>
      <label>
        Name
        <input value={name} onChange={(e) => setName(e.target.value)} required />
      </label>
      <label>
        Node count
        <input
          type="number"
          min={1}
          max={20}
          value={nodeCount}
          onChange={(e) => setNodeCount(e.target.value)}
          required
        />
      </label>
      <label>
        Algorithm
        <select value={algorithmType} onChange={(e) => setAlgorithmType(e.target.value)}>
          {algorithms.map((a) => (
            <option key={a.type} value={a.type}>
              {a.name}
            </option>
          ))}
        </select>
      </label>
      <label>
        Topology (visualization only -- doesn't constrain routing)
        <select value={topologyType} onChange={(e) => setTopologyType(e.target.value)}>
          {TOPOLOGY_TYPES.map((t) => (
            <option key={t} value={t}>
              {t}
            </option>
          ))}
        </select>
      </label>
      <button type="submit" disabled={busy}>
        {busy ? "Starting..." : "Create & Run"}
      </button>
      {error && <p className="error">{error}</p>}
    </form>
  );
}
