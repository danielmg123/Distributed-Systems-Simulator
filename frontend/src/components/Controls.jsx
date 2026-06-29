import { useState } from "react";

// Propose-value form and the stop-simulation button. Proposals broadcast to every
// active node; each node's algorithm decides whether to act on it (e.g. only the
// current Raft leader does) -- the user doesn't need to know who that is.
export default function Controls({ onPropose, onStop }) {
  const [value, setValue] = useState("");

  function handleSubmit(e) {
    e.preventDefault();
    if (!value.trim()) {
      return;
    }
    onPropose(value);
    setValue("");
  }

  return (
    <div className="controls">
      <form onSubmit={handleSubmit} className="controls__propose">
        <input
          placeholder="Value to propose"
          value={value}
          onChange={(e) => setValue(e.target.value)}
        />
        <button type="submit">Propose</button>
      </form>
      <button className="controls__stop" onClick={onStop}>
        Stop Simulation
      </button>
    </div>
  );
}
