import { render, screen } from "@testing-library/react";
import App from "./App";

beforeEach(() => {
  global.fetch = jest.fn().mockResolvedValue({
    ok: true,
    headers: { get: () => "application/json" },
    json: async () => [],
  });
});

test("renders the simulation setup form when no simulation is running", async () => {
  render(<App />);
  expect(await screen.findByText(/start a simulation/i)).toBeInTheDocument();
});
