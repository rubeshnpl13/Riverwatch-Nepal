import {
  useEffect,
  useState,
} from "react";

import {
  getHealth,
} from "./api/health";
import {
  NetworkOverview,
} from "./components/NetworkOverview";

import "./App.css";
import {
  StationTable,
} from "./components/StationTable";

import {
  StationMap,
} from "./components/StationMap";

type ApiStatus =
  | "checking"
  | "online"
  | "offline";


function App() {
  const [
    apiStatus,
    setApiStatus,
  ] = useState<ApiStatus>(
    "checking",
  );

  useEffect(() => {
    const controller =
      new AbortController();

    void getHealth(
      controller.signal,
    )
      .then(() => {
        setApiStatus(
          "online",
        );
      })
      .catch(
        (
          error: unknown,
        ) => {
          if (
            error
              instanceof DOMException
            && error.name
              === "AbortError"
          ) {
            return;
          }

          setApiStatus(
            "offline",
          );
        },
      );

    return () => {
      controller.abort();
    };
  }, []);

  return (
    <main className="app-shell">
      <header className="topbar">
        <div>
          <p className="eyebrow">
            RiverWatch Nepal
          </p>

          <h1>
            River Monitoring Dashboard
          </h1>
        </div>

        <div
          className={
            `api-status api-status--${apiStatus}`
          }
        >
          <span
            className="status-dot"
            aria-hidden="true"
          />

          {apiStatus
            === "checking"
            && "Checking API"}

          {apiStatus
            === "online"
            && "API connected"}

          {apiStatus
            === "offline"
            && "API unavailable"}
        </div>
      </header>

      <NetworkOverview />
      <StationMap />
      <StationTable />
    </main>
  );
}


export default App;