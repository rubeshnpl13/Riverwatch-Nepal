import { lazy, Suspense, useEffect, useState } from 'react';

import { getHealth } from './api/health';
import { NetworkOverview } from './components/NetworkOverview';
import { StationTable } from './components/StationTable';

import './App.css';
import './dashboard-refresh.css';

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

type Theme = 'light' | 'dark';

const THEME_STORAGE_KEY = 'riverwatch-theme';

function initialTheme(): Theme {
  const storedTheme = window.localStorage.getItem(THEME_STORAGE_KEY);

  if (storedTheme === 'light' || storedTheme === 'dark') {
    return storedTheme;
  }

  return window.matchMedia('(prefers-color-scheme: dark)').matches
    ? 'dark'
    : 'light';
}

function ThemeIcon({ theme }: { theme: Theme }) {
  if (theme === 'dark') {
    return (
      <svg viewBox="0 0 24 24" aria-hidden="true">
        <circle cx="12" cy="12" r="4" />

        <path d="M12 2v2" />
        <path d="M12 20v2" />
        <path d="m4.93 4.93 1.42 1.42" />
        <path d="m17.66 17.66 1.41 1.41" />
        <path d="M2 12h2" />
        <path d="M20 12h2" />
        <path d="m6.34 17.66-1.41 1.41" />
        <path d="m19.07 4.93-1.41 1.41" />
      </svg>
    );
  }

  return (
    <svg viewBox="0 0 24 24" aria-hidden="true">
      <path d={'M21 12.79A9 9 0 1 1 ' + '11.21 3 7 7 0 0 0 ' + '21 12.79Z'} />
    </svg>
  );
}

function SectionFallback({ label }: { label: string }) {
  return (
    <section className="state-card section-fallback">
      <div className="skeleton-line skeleton-line--short" />

      <div className="skeleton-line skeleton-line--title" />

      <span className="sr-only">Loading {label}</span>
    </section>
  );
}

function App() {
  const [apiStatus, setApiStatus] = useState<ApiStatus>('checking');

  const [selectedStationId, setSelectedStationId] = useState<string | null>(
    null,
  );

  const [theme, setTheme] = useState<Theme>(initialTheme);

  useEffect(() => {
    document.documentElement.dataset.theme = theme;

    window.localStorage.setItem(THEME_STORAGE_KEY, theme);
  }, [theme]);

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

  function toggleTheme() {
    setTheme((currentTheme) => (currentTheme === 'light' ? 'dark' : 'light'));
  }

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
    <div className="dashboard-page">
      <a className="skip-link" href="#dashboard-content">
        Skip to dashboard content
      </a>

      <header className="dashboard-header">
        <div className="dashboard-header-inner">
          <a
            className="brand-lockup"
            href="#overview"
            aria-label="RiverWatch Nepal dashboard"
          >
            <span className="brand-mark" aria-hidden="true">
              R
            </span>

            <span className="brand-copy">
              <strong>RiverWatch Nepal</strong>

              <span>River data observability</span>
            </span>
          </a>

          <nav className="dashboard-nav" aria-label="Dashboard sections">
            <a href="#overview">Overview</a>

            <a href="#map">Map</a>

            <a href="#basins">Basins</a>

            <a href="#stations">Stations</a>
          </nav>

          <div className="header-actions">
            <button
              className="theme-toggle"
              type="button"
              onClick={toggleTheme}
              aria-label={
                theme === 'light'
                  ? 'Switch to dark mode'
                  : 'Switch to light mode'
              }
              title={theme === 'light' ? 'Dark mode' : 'Light mode'}
            >
              <ThemeIcon theme={theme} />

              <span className="theme-toggle-label">
                {theme === 'light' ? 'Dark' : 'Light'}
              </span>
            </button>

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
          </div>
        </div>
      </header>

      <main className="app-shell">
        <section className="dashboard-hero">
          <div>
            <p className="hero-kicker">RiverWatch monitoring platform</p>

            <h1>River Monitoring Dashboard</h1>

            <p className="hero-description">
              Explore river monitoring coverage, station locations, basin-level
              data health and historical water-level observations across Nepal.
            </p>
          </div>

          <div className="hero-context">
            <span>Nepal</span>

            <span>Monitoring network</span>
          </div>
        </section>

        <div id="dashboard-content" tabIndex={-1}>
          <div id="overview" className="dashboard-anchor">
            <NetworkOverview />
          </div>

          <div id="map" className="dashboard-anchor">
            <Suspense fallback={<SectionFallback label="station map" />}>
              <StationMap
                selectedStationId={selectedStationId}
                onSelectStation={selectStation}
              />
            </Suspense>
          </div>

          <Suspense fallback={<SectionFallback label="station detail" />}>
            <StationDetail stationId={selectedStationId} />
          </Suspense>

          <div id="basins" className="dashboard-anchor">
            <Suspense fallback={<SectionFallback label="basin analytics" />}>
              <BasinAnalytics />
            </Suspense>
          </div>

          <div id="stations" className="dashboard-anchor">
            <StationTable
              selectedStationId={selectedStationId}
              onSelectStation={selectStation}
            />
          </div>
        </div>

        <footer className="dashboard-footer">
          <div>
            <strong>RiverWatch Nepal</strong>

            <span>River monitoring and analytical data platform</span>
          </div>

          <p>
            Data availability does not represent flood danger or official
            river-safety guidance.
          </p>
        </footer>
      </main>
    </div>
  );
}

export default App;
