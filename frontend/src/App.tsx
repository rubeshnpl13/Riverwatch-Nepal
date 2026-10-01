import { lazy, Suspense, useEffect, useState } from 'react';

import { getHealth } from './api/health';

import { NetworkOverview } from './components/NetworkOverview';

import { StationTable } from './components/StationTable';

import './App.css';
const BasinAnalytics = lazy(async () => {
  const module = await import('./components/BasinAnalytics');

  return {
    default: module.BasinAnalytics,
  };
});

const StationMap = lazy(async () => {
  const module = await import('./components/StationMap');

  return {
    default: module.StationMap,
  };
});

const StationDetail = lazy(async () => {
  const module = await import('./components/StationDetail');

  return {
    default: module.StationDetail,
  };
});

type ApiStatus = 'checking' | 'online' | 'offline';

function App() {
  const [apiStatus, setApiStatus] = useState<ApiStatus>('checking');

  const [selectedStationId, setSelectedStationId] = useState<string | null>(
    null,
  );

  useEffect(() => {
    const controller = new AbortController();

    void getHealth(controller.signal)
      .then(() => {
        setApiStatus('online');
      })
      .catch((error: unknown) => {
        if (error instanceof DOMException && error.name === 'AbortError') {
          return;
        }

        setApiStatus('offline');
      });

    return () => {
      controller.abort();
    };
  }, []);

  function selectStation(stationId: string) {
    setSelectedStationId(stationId);

    window.requestAnimationFrame(() => {
      const prefersReducedMotion = window.matchMedia(
        '(prefers-reduced-motion: reduce)',
      ).matches;

      document.getElementById('station-detail')?.scrollIntoView({
        behavior: prefersReducedMotion ? 'auto' : 'smooth',

        block: 'start',
      });
    });
  }

  return (
    <>
      <a className="skip-link" href="#dashboard-content">
        Skip to dashboard content
      </a>

      <main className="app-shell">
        <header className="topbar">
          <div>
            <p className="eyebrow">RiverWatch Nepal</p>

            <h1>River Monitoring Dashboard</h1>
          </div>

          <div
            className={`api-status api-status--${apiStatus}`}
            role="status"
            aria-live="polite"
          >
            <span className="status-dot" aria-hidden="true" />

            {apiStatus === 'checking' && 'Checking API'}

            {apiStatus === 'online' && 'API connected'}

            {apiStatus === 'offline' && 'API unavailable'}
          </div>
        </header>

        <div id="dashboard-content" tabIndex={-1}>
          <NetworkOverview />

          <Suspense
            fallback={
              <section className="state-card">
                <p className="section-label">Basin analytics</p>

                <h2>Loading analytics…</h2>
              </section>
            }
          >
            <BasinAnalytics />
          </Suspense>

          <Suspense
            fallback={
              <section className="state-card">
                <p className="section-label">Station map</p>

                <h2>Loading map…</h2>
              </section>
            }
          >
            <StationMap
              selectedStationId={selectedStationId}
              onSelectStation={selectStation}
            />
          </Suspense>

          <Suspense
            fallback={
              <section className="state-card">
                <p className="section-label">Station detail</p>

                <h2>Loading station detail…</h2>
              </section>
            }
          >
            <StationDetail stationId={selectedStationId} />
          </Suspense>

          <StationTable
            selectedStationId={selectedStationId}
            onSelectStation={selectStation}
          />
        </div>
      </main>
    </>
  );
}

export default App;
