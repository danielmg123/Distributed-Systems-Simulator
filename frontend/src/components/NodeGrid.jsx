// One card per node: id, live state, committed value, and detail, plus a Fail/Recover
// button. Each card is colored by the same state logic as the topology hero, so a node
// reads the same in both places -- cyan leader, green follower/acceptor, amber candidate,
// red failed.

const STATE_COLOR = {
  leader: "var(--status-leader)",
  candidate: "var(--status-candidate)",
  active: "var(--status-active)",
  failed: "var(--status-failed)",
};

function stateOf(node) {
  if (!node || node.status === "FAILED") return "failed";
  if (node.roleLabel === "LEADER") return "leader";
  if (node.roleLabel === "CANDIDATE") return "candidate";
  return "active"; // FOLLOWER / ACCEPTOR / anything else healthy
}

export default function NodeGrid({ nodes, onFailNode, onRecoverNode }) {
  if (nodes.length === 0) {
    return <p className="node-grid__empty">No nodes yet.</p>;
  }

  return (
    <div className="node-grid">
      {nodes.map((node) => {
        const st = stateOf(node);
        const failed = node.status === "FAILED";
        return (
          <div key={node.id} className={`node-card node-card--${st}`} style={{ color: STATE_COLOR[st] }}>
            <div className="node-card__head">
              <span className="node-card__id">{node.id}</span>
              <span className="node-card__dot" aria-label={node.status} title={node.status} />
            </div>
            <div className="node-card__role">{node.roleLabel || node.status}</div>
            {node.committedValue != null && (
              <div className="node-card__committed">
                <span className="node-card__committed-label">committed</span>
                <span className="node-card__committed-value">{node.committedValue}</span>
              </div>
            )}
            {node.detail && <div className="node-card__detail">{node.detail}</div>}
            <div className="node-card__actions">
              {failed ? (
                <button
                  className="node-card__btn node-card__btn--recover"
                  onClick={() => onRecoverNode(node.id)}
                >
                  Recover
                </button>
              ) : (
                <button
                  className="node-card__btn node-card__btn--fail"
                  onClick={() => onFailNode(node.id)}
                >
                  Fail
                </button>
              )}
            </div>
          </div>
        );
      })}
    </div>
  );
}
