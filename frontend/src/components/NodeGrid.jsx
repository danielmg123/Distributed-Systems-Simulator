// One card per node: id, live status, and (Raft-only) role label. Fail/Recover
// buttons hit the simulation's failNode/recoverNode endpoints directly.
export default function NodeGrid({ nodes, onFailNode, onRecoverNode }) {
  if (nodes.length === 0) {
    return <p>No nodes yet.</p>;
  }

  return (
    <div className="node-grid">
      {nodes.map((node) => (
        <div key={node.id} className={`node-card node-card--${node.status?.toLowerCase()}`}>
          <div className="node-card__id">{node.id}</div>
          <div className="node-card__status">{node.status}</div>
          {node.roleLabel && <div className="node-card__role">{node.roleLabel}</div>}
          {node.committedValue != null && (
            <div className="node-card__committed">committed: {node.committedValue}</div>
          )}
          {node.detail && <div className="node-card__detail">{node.detail}</div>}
          <div className="node-card__actions">
            {node.status === "FAILED" ? (
              <button onClick={() => onRecoverNode(node.id)}>Recover</button>
            ) : (
              <button onClick={() => onFailNode(node.id)}>Fail</button>
            )}
          </div>
        </div>
      ))}
    </div>
  );
}
