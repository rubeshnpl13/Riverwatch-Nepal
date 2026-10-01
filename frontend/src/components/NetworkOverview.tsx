import {
  useEffect,
  useState,
} from "react";

import {
  getNetworkSummary,
  type CurrentNetworkSummary,
} from "../api/network";


const numberFormatter =
  new Intl.NumberFormat();


const percentFormatter =
  new Intl.NumberFormat(
    undefined,
    {
      style: "percent",
      maximumFractionDigits: 1,
    },
  );


function formatNumber(
  value: number,
): string {
  return numberFormatter.format(
    value,
  );
}


function formatPercent(
  value: number,
): string {
  return percentFormatter.format(
    value,
  );
}


function formatTimestamp(
  value: string | null,
): string {
  if (value === null) {
    return "Not available";
  }

  const date = new Date(
    value,
  );

  if (
    Number.isNaN(
      date.getTime(),
    )
  ) {
    return value;
  }

  return new Intl.DateTimeFormat(
    undefined,
    {
      year: "numeric",
      month: "short",
      day: "numeric",
      hour: "2-digit",
      minute: "2-digit",
      timeZone: "UTC",
      timeZoneName: "short",
    },
  ).format(
    date,
  );
}


function percentageWidth(
  value: number,
): string {
  const clamped = Math.min(
    Math.max(
      value,
      0,
    ),
    1,
  );

  return `${clamped * 100}%`;
}


interface MetricCardProps {
  label: string;
  value: string;
  detail: string;
}


function MetricCard({
  label,
  value,
  detail,
}: MetricCardProps) {
  return (
    <article className="metric-card">
      <p className="metric-label">
        {label}
      </p>

      <p className="metric-value">
        {value}
      </p>

      <p className="metric-detail">
        {detail}
      </p>
    </article>
  );
}


interface RatioCardProps {
  label: string;
  value: number;
  detail: string;
}


function RatioCard({
  label,
  value,
  detail,
}: RatioCardProps) {
  return (
    <article className="ratio-card">
      <div className="ratio-heading">
        <p className="metric-label">
          {label}
        </p>

        <strong>
          {formatPercent(
            value,
          )}
        </strong>
      </div>

      <div
        className="ratio-track"
        role="progressbar"
        aria-label={label}
        aria-valuemin={0}
        aria-valuemax={100}
        aria-valuenow={
          Math.round(
            value * 100,
          )
        }
      >
        <div
          className="ratio-fill"
          style={{
            width:
              percentageWidth(
                value,
              ),
          }}
        />
      </div>

      <p className="metric-detail">
        {detail}
      </p>
    </article>
  );
}


export function NetworkOverview() {
  const [
    summary,
    setSummary,
  ] = useState<
    CurrentNetworkSummary | null
  >(null);

  const [
    hasError,
    setHasError,
  ] = useState(
    false,
  );

  useEffect(() => {
    const controller =
      new AbortController();

    void getNetworkSummary(
      controller.signal,
    )
      .then(
        (
          networkSummary,
        ) => {
          setSummary(
            networkSummary,
          );

          setHasError(
            false,
          );
        },
      )
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

          setHasError(
            true,
          );
        },
      );

    return () => {
      controller.abort();
    };
  }, []);

  if (hasError) {
    return (
      <section
        className="state-card state-card--error"
        aria-live="polite"
      >
        <p className="section-label">
          Network overview
        </p>

        <h2>
          Network data unavailable
        </h2>

        <p>
          RiverWatch could not load
          the current network summary
          from the API.
        </p>
      </section>
    );
  }

  if (summary === null) {
    return (
      <section
        className="state-card"
        aria-live="polite"
      >
        <p className="section-label">
          Network overview
        </p>

        <h2>
          Loading network data…
        </h2>
      </section>
    );
  }

  return (
    <section>
      <div className="section-heading">
        <div>
          <p className="section-label">
            Network overview
          </p>

          <h2>
            Current RiverWatch snapshot
          </h2>
        </div>

        <p className="snapshot-time">
          Latest observation
          {" "}
          <strong>
            {formatTimestamp(
              summary
                .latest_observed_at,
            )}
          </strong>
        </p>
      </div>

      <div className="metrics-grid">
        <MetricCard
          label="Stations"
          value={
            formatNumber(
              summary
                .total_stations,
            )
          }
          detail="Stations in the current RiverWatch snapshot."
        />

        <MetricCard
          label="Basins"
          value={
            formatNumber(
              summary
                .basins_represented,
            )
          }
          detail="Distinct basin groups represented."
        />

        <MetricCard
          label="With observation"
          value={
            formatNumber(
              summary
                .stations_with_observation,
            )
          }
          detail="Stations with an observation in the current snapshot."
        />

        <MetricCard
          label="Without observation"
          value={
            formatNumber(
              summary
                .stations_without_observation,
            )
          }
          detail="Stations currently lacking an observation."
        />

        <MetricCard
          label="Water level available"
          value={
            formatNumber(
              summary
                .observations_with_water_level,
            )
          }
          detail="Current observations containing a water-level value."
        />

        <MetricCard
          label="Water level missing"
          value={
            formatNumber(
              summary
                .observations_without_water_level,
            )
          }
          detail="Current observations without a water-level value."
        />
      </div>

      <div className="ratio-grid">
        <RatioCard
          label="Observation coverage"
          value={
            summary
              .coverage_ratio
          }
          detail={
            `${formatNumber(
              summary
                .stations_with_observation,
            )} of ${formatNumber(
              summary
                .total_stations,
            )} stations have an observation.`
          }
        />

        <RatioCard
          label="Observation freshness"
          value={
            summary
              .freshness_ratio
          }
          detail={
            `${formatNumber(
              summary
                .fresh_observations,
            )} fresh and ${formatNumber(
              summary
                .stale_observations,
            )} stale observations.`
          }
        />
      </div>

      <article className="observation-window">
        <div>
          <p className="metric-label">
            Observation time range
          </p>

          <p>
            <strong>
              Oldest:
            </strong>
            {" "}
            {formatTimestamp(
              summary
                .oldest_observed_at,
            )}
          </p>

          <p>
            <strong>
              Latest:
            </strong>
            {" "}
            {formatTimestamp(
              summary
                .latest_observed_at,
            )}
          </p>
        </div>

        <div>
          <p className="metric-label">
            Timestamp assessment
          </p>

          <p>
            Future observations:
            {" "}
            <strong>
              {formatNumber(
                summary
                  .future_observations,
              )}
            </strong>
          </p>

          <p>
            Unassessable observations:
            {" "}
            <strong>
              {formatNumber(
                summary
                  .unassessable_observations,
              )}
            </strong>
          </p>
        </div>
      </article>

      <p className="dashboard-disclaimer">
        Freshness and coverage describe
        RiverWatch data availability and
        recency. They do not indicate
        whether a river is safe or
        dangerous and are not official
        flood warnings.
      </p>
    </section>
  );
}