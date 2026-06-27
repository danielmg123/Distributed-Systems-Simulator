import { useEffect, useRef, useState } from "react";

const DEBOUNCE_MS = 300;

// Two range inputs for live-tuning simulated message loss and delivery delay. Slider
// drags fire many onChange events; debouncing avoids hammering the network-conditions
// endpoint on every pixel of drag.
export default function NetworkSliders({ onChange }) {
  const [lossRate, setLossRate] = useState(0);
  const [maxDelayMs, setMaxDelayMs] = useState(0);
  const debounceRef = useRef(null);

  useEffect(() => {
    clearTimeout(debounceRef.current);
    debounceRef.current = setTimeout(() => {
      onChange({ messageLossRate: lossRate, minDelayMs: 0, maxDelayMs });
    }, DEBOUNCE_MS);
    return () => clearTimeout(debounceRef.current);
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [lossRate, maxDelayMs]);

  return (
    <div className="network-sliders">
      <h3>Network Conditions</h3>
      <label>
        Message loss rate: {(lossRate * 100).toFixed(0)}%
        <input
          type="range"
          min={0}
          max={1}
          step={0.01}
          value={lossRate}
          onChange={(e) => setLossRate(Number(e.target.value))}
        />
      </label>
      <label>
        Max delivery delay: {maxDelayMs}ms
        <input
          type="range"
          min={0}
          max={2000}
          step={50}
          value={maxDelayMs}
          onChange={(e) => setMaxDelayMs(Number(e.target.value))}
        />
      </label>
    </div>
  );
}
