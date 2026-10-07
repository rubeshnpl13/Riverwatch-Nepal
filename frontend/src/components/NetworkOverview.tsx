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

  const date =
    new Date(
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
  const clamped =
    Math.min(
      Math.max(
        value,
        0,
      ),
      1,
    );

  return `${clamped * 100}%`;
}


interface OverviewCardProps {
  label: string;
  value: string;
  detail: string;
  progress?: number;
  accent?: boolean;
}


function OverviewCard({
  label,
  value,
  detail,
  progress,
  accent = false,
}: OverviewCardProps) {
  return (
    <article
      className={
        accent
          ? (
            "overview-kpi-card "
            + "overview-kpi-card--accent"
          )
          : "overview-kpi-card"
      }
    >
      <div className="overview-kpi-top">
        <p className="overview-kpi-label">
          {label}
        </p>

        <span
          className="overview-kpi-indicator"
          aria-hidden="true"
        />
      </div>

      <strong className="overview-kpi-value">
        {value}
      </strong>

      {progress !== undefined && (
        <div
          className="overview-kpi-meter"
          role="progressbar"
          aria-label={label}
          aria-valuemin={0}
          aria-valuemax={100}
          aria-valuenow={
            Math.round(
              progress * 100,
            )
          }
        >
          <div
            className="overview-kpi-meter-fill"
            style={{
              width:
                percentageWidth(
                  progress,
                ),
            }}
          />
        </div>
      )}

      <p className="overview-kpi-detail">
        {detail}
      </p>
    </article>
  );
}


interface HealthItemProps {
  label: string;
  primary: string;
  secondary: string;
}


function HealthItem({
  label,
  primary,
  secondary,
}: HealthItemProps) {
  return (
    <article className="network-health-card">
      <p className="network-health-label">
        {label}
      </p>

      <strong className="network-health-primary">
        {primary}
      </strong>

      <p className="network-health-secondary">
        {secondary}
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
  >(
    null,
  );

  const [
    hasError,
    setHasError,
  ] = useState(
    false,
  );


  useEffect(
    () => {
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
    },
    [],
  );


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
    <section
      className="overview-section"
      aria-labelledby="network-overview-heading"
    >
      <div className="overview-heading">
        <div>
          <p className="section-label">
            Network overview
          </p>

          <h2 id="network-overview-heading">
            Current network snapshot
          </h2>

          <p className="section-description">
            A concise view of RiverWatch
            station coverage and data
            availability.
          </p>
        </div>

        <div className="snapshot-pill">
          <span>
            Latest observation
          </span>

          <strong>
            {formatTimestamp(
              summary.latest_observed_at,
            )}
          </strong>
        </div>
      </div>


      <div className="overview-kpi-grid">
        <OverviewCard
          label="Monitoring stations"
          value={
            formatNumber(
              summary.total_stations,
            )
          }
          detail={
            `${formatNumber(
              summary.basins_represented,
            )} basin groups represented`
          }
          accent
        />

        <OverviewCard
          label="Observation coverage"
          value={
            formatPercent(
              summary.coverage_ratio,
            )
          }
          progress={
            summary.coverage_ratio
          }
          detail={
            `${formatNumber(
              summary
                .stations_with_observation,
            )} of ${formatNumber(
              summary.total_stations,
            )} stations reporting`
          }
        />

        <OverviewCard
          label="Observation freshness"
          value={
            formatPercent(
              summary.freshness_ratio,
            )
          }
          progress={
            summary.freshness_ratio
          }
          detail={
            `${formatNumber(
              summary.fresh_observations,
            )} fresh · ${formatNumber(
              summary.stale_observations,
            )} stale`
          }
        />

        <OverviewCard
          label="Water-level coverage"
          value={
            formatNumber(
              summary
                .observations_with_water_level,
            )
          }
          detail={
            `${formatNumber(
              summary
                .observations_without_water_level,
            )} current observations missing a value`
          }
        />
      </div>


      <div className="network-health-grid">
        <HealthItem
          label="Station availability"
          primary={
            `${formatNumber(
              summary
                .stations_with_observation,
            )} reporting`
          }
          secondary={
            `${formatNumber(
              summary
                .stations_without_observation,
            )} without an observation`
          }
        />

        <HealthItem
          label="Observation window"
          primary={
            formatTimestamp(
              summary
                .latest_observed_at,
            )
          }
          secondary={
            `Oldest: ${formatTimestamp(
              summary
                .oldest_observed_at,
            )}`
          }
        />

        <HealthItem
          label="Timestamp quality"
          primary={
            `${formatNumber(
              summary
                .future_observations,
            )} future`
          }
          secondary={
            `${formatNumber(
              summary
                .unassessable_observations,
            )} unassessable`
          }
        />

        <HealthItem
          label="Network composition"
          primary={
            `${formatNumber(
              summary
                .basins_represented,
            )} basins`
          }
          secondary={
            `${formatNumber(
              summary
                .total_stations,
            )} monitoring stations`
          }
        />
      </div>


      <div className="dashboard-data-note">
        <span
          className="dashboard-data-note-icon"
          aria-hidden="true"
        >
          i
        </span>

        <p>
          Coverage and freshness describe
          RiverWatch data availability
          and recency. They do not indicate
          flood danger, river safety or
          official warning conditions.
        </p>
      </div>
    </section>
  );
}
