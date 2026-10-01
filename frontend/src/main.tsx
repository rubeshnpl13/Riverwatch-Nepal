import {
  StrictMode,
} from "react";

import {
  createRoot,
} from "react-dom/client";

import "leaflet/dist/leaflet.css";

import "./index.css";

import App from "./App";

import {
  DashboardErrorBoundary,
} from "./components/DashboardErrorBoundary";


createRoot(
  document.getElementById(
    "root",
  )!,
).render(
  <StrictMode>
    <DashboardErrorBoundary>
      <App />
    </DashboardErrorBoundary>
  </StrictMode>,
);