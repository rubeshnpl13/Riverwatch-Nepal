import {
  useEffect,
  useMemo,
  useState,
} from "react";

import {
  Bar,
  BarChart,
  CartesianGrid,
  Legend,
  ResponsiveContainer,
  Tooltip,
  XAxis,
  YAxis,
} from "recharts";

import {
  getBasinSummaries,
  type BasinCurrentSummary,
} from "../api/basins.tsx";


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


function clampRatio(
  value: number,
): number {
  return Math.min(
    Math.max(
      value,
      0,
    ),
    1,
  );
}


interface RatioDisplayProps {
  label: string;
  value: number;
}


function RatioDisplay({
  label,
  value,
}: RatioDisplayProps) {
  const normalized =
    clampRatio(
      value,
    );

  return (
    <div className="basin-ratio-display">
      <span>
        {formatPercent(
          value,
        )}
      </span>

      <div
        className="basin-mini-track"
        role="progressbar"
        aria-label={label}
        aria-valuemin={0}
        aria-valuemax={100}
        aria-valuenow={
          Math.round(
            normalized * 100,
          )
        }
      >
        <div
          className="basin-mini-fill"
          style={{
            width:
              `${
                normalized
                * 100
              }%`,
          }}
        />
      </div>
    </div>
  );
}


export function BasinAnalytics() {
  const [
    basins,
    setBasins,
  ] = useState<
    BasinCurrentSummary[] | null
  >(null);

  const [
    hasError,
    setHasError,
  ] = useState(
    false,
  );

  const [
    search,
    setSearch,
  ] = useState(
    "",
  );


  useEffect(() => {
    const controller =
      new AbortController();

    void getBasinSummaries(
      controller.signal,
    )
      .then(
        (
          summaries,
        ) => {
          setBasins(
            summaries,
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


  const filteredBasins =
    useMemo(
      () => {
        if (basins === null) {
          return [];
        }

        const query =
          search
            .trim()
            .toLowerCase();

        if (!query) {
          return basins;
        }

        return basins.filter(
          (
            basin,
          ) => (
            basin
              .basin_name
              .toLowerCase()
              .includes(
                query,
              )
          ),
        );
      },
      [
        basins,
        search,
      ],
    );


  const chartData =
    useMemo(
      () => {
        if (basins === null) {
          return [];
        }

        return [
          ...basins,
        ]
          .sort(
            (
              first,
              second,
            ) => (
              second
                .total_stations
              - first
                .total_stations
            ),
          )
          .slice(
            0,
            10,
          )
          .map(
            (
              basin,
            ) => ({
              basinName:
                basin
                  .basin_name,

              withObservation:
                basin
                  .stations_with_observation,

              withoutObservation:
                basin
                  .stations_without_observation,
            }),
          );
      },
      [
        basins,
      ],
    );


  const totalStations =
    useMemo(
      () => (
        basins?.reduce(
          (
            total,
            basin,
          ) => (
            total
            + basin.total_stations
          ),
          0,
        )
        ?? 0
      ),
      [
        basins,
      ],
    );


  const completeObservationBasins =
    useMemo(
      () => (
        basins?.filter(
          (
            basin,
          ) => (
            basin
              .stations_without_observation
            === 0
          ),
        ).length
        ?? 0
      ),
      [
        basins,
      ],
    );


  const unknownBasinStations =
    useMemo(
      () => (
        basins?.find(
          (
            basin,
          ) => (
            basin.basin_name
            === "Unknown"
          ),
        )?.total_stations
        ?? 0
      ),
      [
        basins,
      ],
    );


  if (hasError) {
    return (
      <section className="basin-section">
        <div
          className={
            "state-card "
            + "state-card--error"
          }
        >
          <p className="section-label">
            Basin analytics
          </p>

          <h2>
            Basin data unavailable
          </h2>

          <p>
            RiverWatch could not load
            the current basin
            summaries.
          </p>
        </div>
      </section>
    );
  }


  if (basins === null) {
    return (
      <section className="basin-section">
        <div className="state-card">
          <p className="section-label">
            Basin analytics
          </p>

          <h2>
            Loading basin data…
          </h2>
        </div>
      </section>
    );
  }


  return (
    <section className="basin-section">
      <div className="section-heading">
        <div>
          <p className="section-label">
            Basin analytics
          </p>

          <h2>
            Network by basin
          </h2>
        </div>

        <p className="station-count">
          <strong>
            {basins.length}
          </strong>
          {" "}
          basin groups
        </p>
      </div>


      <div className="basin-summary-grid">
        <article className="basin-summary-card">
          <p className="metric-label">
            Basin groups
          </p>

          <p className="metric-value">
            {
              formatNumber(
                basins.length,
              )
            }
          </p>

          <p className="metric-detail">
            Basin labels represented
            in the current snapshot.
          </p>
        </article>


        <article className="basin-summary-card">
          <p className="metric-label">
            Station total
          </p>

          <p className="metric-value">
            {
              formatNumber(
                totalStations,
              )
            }
          </p>

          <p className="metric-detail">
            Stations represented across
            all basin groups.
          </p>
        </article>


        <article className="basin-summary-card">
          <p className="metric-label">
            No missing observations
          </p>

          <p className="metric-value">
            {
              formatNumber(
                completeObservationBasins,
              )
            }
          </p>

          <p className="metric-detail">
            Basin groups where every
            station has an observation.
          </p>
        </article>


        <article className="basin-summary-card">
          <p className="metric-label">
            Unknown basin
          </p>

          <p className="metric-value">
            {
              formatNumber(
                unknownBasinStations,
              )
            }
          </p>

          <p className="metric-detail">
            Stations whose upstream
            basin metadata is missing.
          </p>
        </article>
      </div>


      <article className="basin-chart-card">
        <div className="basin-chart-heading">
          <div>
            <p className="metric-label">
              Station distribution
            </p>

            <h3>
              Largest basin groups
            </h3>
          </div>

          <p>
            Top 10 by station count
          </p>
        </div>


        <div
          className="basin-chart"
          role="img"
          aria-label={
            "Station distribution "
            + "across the ten largest "
            + "RiverWatch basin groups"
          }
        >
          <ResponsiveContainer
            width="100%"
            height={460}
          >
            <BarChart
              data={chartData}
              layout="vertical"
              margin={{
                top: 8,
                right: 24,
                bottom: 8,
                left: 12,
              }}
            >
              <CartesianGrid
                strokeDasharray="3 3"
                horizontal={false}
              />

              <XAxis
                type="number"
                allowDecimals={false}
              />

              <YAxis
                type="category"
                dataKey="basinName"
                width={145}
                tick={{
                  fontSize: 12,
                }}
              />

              <Tooltip />

              <Legend />

              <Bar
                dataKey="withObservation"
                name="With observation"
                stackId="stations"
                fill="#397b69"
              />

              <Bar
                dataKey="withoutObservation"
                name="Without observation"
                stackId="stations"
                fill="#a5b1ad"
              />
            </BarChart>
          </ResponsiveContainer>
        </div>


        <p className="chart-note">
          The chart compares station
          data availability. It does not
          represent flood severity,
          river safety, or basin risk.
        </p>
      </article>


      <article className="basin-table-card">
        <div className="basin-table-toolbar">
          <div>
            <p className="metric-label">
              Basin details
            </p>

            <h3>
              Current basin summaries
            </h3>
          </div>

          <label className="basin-search">
            <span>
              Search basins
            </span>

            <input
              type="search"
              value={search}
              placeholder="Basin name"
              onChange={
                (
                  event,
                ) => {
                  setSearch(
                    event
                      .target
                      .value,
                  );
                }
              }
            />
          </label>
        </div>


        <div className="basin-result-count">
          Showing
          {" "}
          <strong>
            {
              filteredBasins.length
            }
          </strong>
          {" "}
          of
          {" "}
          <strong>
            {basins.length}
          </strong>
          {" "}
          basin groups
        </div>


        {
          filteredBasins.length
          === 0
            ? (
              <div className="table-empty">
                <strong>
                  No basins found
                </strong>

                <p>
                  Try changing your
                  search.
                </p>
              </div>
            )
            : (
              <div className="table-scroll">
                <table className="basin-table">
                  <thead>
                    <tr>
                      <th>
                        Basin
                      </th>

                      <th>
                        Stations
                      </th>

                      <th>
                        With obs.
                      </th>

                      <th>
                        Without obs.
                      </th>

                      <th>
                        Coverage
                      </th>

                      <th>
                        Freshness
                      </th>

                      <th>
                        Water level
                      </th>

                      <th>
                        Latest
                      </th>
                    </tr>
                  </thead>

                  <tbody>
                    {
                      filteredBasins.map(
                        (
                          basin,
                        ) => (
                          <tr
                            key={
                              basin
                                .basin_name
                            }
                          >
                            <td>
                              <strong>
                                {
                                  basin
                                    .basin_name
                                }
                              </strong>
                            </td>

                            <td>
                              {
                                formatNumber(
                                  basin
                                    .total_stations,
                                )
                              }
                            </td>

                            <td>
                              {
                                formatNumber(
                                  basin
                                    .stations_with_observation,
                                )
                              }
                            </td>

                            <td>
                              {
                                formatNumber(
                                  basin
                                    .stations_without_observation,
                                )
                              }
                            </td>

                            <td>
                              <RatioDisplay
                                label={
                                  `${
                                    basin
                                      .basin_name
                                  } observation coverage`
                                }
                                value={
                                  basin
                                    .coverage_ratio
                                }
                              />
                            </td>

                            <td>
                              <RatioDisplay
                                label={
                                  `${
                                    basin
                                      .basin_name
                                  } observation freshness`
                                }
                                value={
                                  basin
                                    .freshness_ratio
                                }
                              />
                            </td>

                            <td>
                              {
                                formatNumber(
                                  basin
                                    .observations_with_water_level,
                                )
                              }
                              {" / "}
                              {
                                formatNumber(
                                  basin
                                    .stations_with_observation,
                                )
                              }
                            </td>

                            <td className="basin-latest">
                              {
                                formatTimestamp(
                                  basin
                                    .latest_observed_at,
                                )
                              }
                            </td>
                          </tr>
                        ),
                      )
                    }
                  </tbody>
                </table>
              </div>
            )
        }
      </article>


      <p className="dashboard-disclaimer">
        Basin coverage and freshness
        are RiverWatch data-health
        metrics. They do not classify
        flood danger, river safety, or
        emergency conditions. Basin
        names are preserved from the
        upstream data rather than
        automatically merged.
      </p>
    </section>
  );
}