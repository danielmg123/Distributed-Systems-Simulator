// Thin fetch wrapper for the backend REST API. Plain fetch, no axios -- this project
// has no other HTTP-client dependency and the API surface is small enough not to need
// one. The CRA dev server proxies requests to http://localhost:8080 (see "proxy" in
// package.json), so these paths are relative and CORS-free in dev.

async function request(path, options = {}) {
  const res = await fetch(path, {
    headers: { "Content-Type": "application/json" },
    ...options,
  });
  if (!res.ok) {
    const text = await res.text().catch(() => "");
    throw new Error(`${options.method || "GET"} ${path} failed: ${res.status} ${text}`);
  }
  const contentType = res.headers.get("content-type") || "";
  if (contentType.includes("application/json")) {
    return res.json();
  }
  return res.text();
}

const json = (body) => ({ body: JSON.stringify(body) });

export const api = {
  // Algorithms
  getAlgorithms: () => request("/api/algorithms"),

  // Simulations
  createSimulation: (simulation) =>
    request("/api/simulations", { method: "POST", ...json(simulation) }),
  getSimulation: (id) => request(`/api/simulations/${id}`),
  runSimulation: (id) => request(`/api/simulations/${id}/run`, { method: "POST" }),
  stopSimulation: (id) => request(`/api/simulations/${id}/stop`, { method: "POST" }),
  failNode: (id, nodeId) =>
    request(`/api/simulations/${id}/failNode/${nodeId}`, { method: "POST" }),
  recoverNode: (id, nodeId) =>
    request(`/api/simulations/${id}/recoverNode/${nodeId}`, { method: "POST" }),
  propose: (id, value) =>
    request(`/api/simulations/${id}/propose`, { method: "POST", ...json({ value }) }),
  getNodeStatuses: (id) => request(`/api/simulations/${id}/nodes`),
  getTopology: (id) => request(`/api/simulations/${id}/topology`),
  getMetrics: (id) => request(`/api/simulations/${id}/metrics`),
  getEvents: (id) => request(`/api/simulations/${id}/events`),
  updateNetworkConditions: (id, conditions) =>
    request(`/api/simulations/${id}/network-conditions`, { method: "PUT", ...json(conditions) }),
};
