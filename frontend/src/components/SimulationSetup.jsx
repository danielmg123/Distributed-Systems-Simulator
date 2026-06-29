import { useEffect, useState } from "react";
import { api } from "../api";

const TOPOLOGY_TYPES = ["MESH", "RING", "STAR", "TREE"];

// Creates a simulation from the form config and runs it. The backend builds the
// simulation's nodes from config.nodeCount (see SimulationService.runSimulation),
// so the UI just creates and runs -- no separate node seeding needed.
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
        Topology (shapes the graph view; doesn't constrain message routing)
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
