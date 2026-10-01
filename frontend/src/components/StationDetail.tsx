import {
  useEffect,
} from "react";

import {
  CartesianGrid,
  Line,
  LineChart,
  ResponsiveContainer,
  Tooltip,
  XAxis,
  YAxis,
} from "recharts";

import {
  getStation,
  getStationHistory,
  type CurrentRiverStation,
  type StationObservation,
} from "../api/stations";

import {
  useState,
} from "react";


interface StationDetailProps {
  stationId: string | null;
}


interface LoadedStationData {
  stationId: string;
  station: CurrentRiverStation;
  history: StationObservation[];
}


interface ChartPoint {
  observedAt: string;
  waterLevel: number | null;
}


const numberFormatter =
  new Intl.NumberFormat(
    undefined,
    {
      maximumFractionDigits: 3,
    },
  );


function stationName(
  station: CurrentRiverStation,
): string {
  const value =
    station.station_name?.trim();

  return value
    ? value
    : "Unnamed station";
}


function basinName(
  station: CurrentRiverStation,
): string {
  const value =
    station.basin_name?.trim();

  return value
    ? value
    : "Unknown";
}


function formatWaterLevel(
  value: number | null,
): string {
  if (value === null) {
    return "Not available";
  }

  return `${
    numberFormatter.format(
      value,
    )
  } m`;
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


function formatShortDate(
  value: string,
): string {
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
      month: "short",
      day: "numeric",
      year: "2-digit",
      timeZone: "UTC",
    },
  ).format(
    date,
  );
}


function formatCoordinate(
  value: number | null,
): string {
  if (value === null) {
    return "Not available";
  }

  return value.toFixed(
    5,
  );
}


function formatElevation(
  value: number | null,
): string {
  if (value === null) {
    return "Not available";
  }

  return `${
    numberFormatter.format(
      value,
    )
  } m`;
}


export function StationDetail({
  stationId,
}: StationDetailProps) {
  const [
    loadedData,
    setLoadedData,
  ] = useState<
    LoadedStationData | null
  >(null);

  const [
    errorStationId,
    setErrorStationId,
  ] = useState<
    string | null
  >(null);


  useEffect(() => {
    if (
      stationId === null
    ) {
      return;
    }

    const controller =
      new AbortController();

    void Promise.all([
      getStation(
        stationId,
        controller.signal,
      ),
      getStationHistory(
        stationId,
        controller.signal,
      ),
    ])
      .then(
        ([
          station,
          history,
        ]) => {
          setLoadedData({
            stationId,
            station,
            history,
          });

          setErrorStationId(
            null,
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

          setErrorStationId(
            stationId,
          );
        },
      );

    return () => {
      controller.abort();
    };
  }, [
    stationId,
  ]);


  if (
    stationId === null
  ) {
    return (
      <section
        id="station-detail"
        className="station-detail-section"
      >
        <div className="station-detail-empty">
          <p className="section-label">
            Station detail
          </p>

          <h2>
            Explore a station
          </h2>

          <p>
            Select a station from the
            map or station table to view
            its current observation and
            historical water-level data.
          </p>
        </div>
      </section>
    );
  }


  if (
    errorStationId
    === stationId
  ) {
    return (
      <section
        id="station-detail"
        className="station-detail-section"
      >
        <div
          className={
            "state-card "
            + "state-card--error"
          }
        >
          <p className="section-label">
            Station detail
          </p>

          <h2>
            Station details unavailable
          </h2>

          <p>
            RiverWatch could not load
            this station or its
            historical observations.
          </p>
        </div>
      </section>
    );
  }


  if (
    loadedData === null
    || loadedData.stationId
      !== stationId
  ) {
    return (
      <section
        id="station-detail"
        className="station-detail-section"
      >
        <div
          className="state-card"
          aria-live="polite"
        >
          <p className="section-label">
            Station detail
          </p>

          <h2>
            Loading station…
          </h2>
        </div>
      </section>
    );
  }


  const {
    station,
    history,
  } = loadedData;


  const chartData:
    ChartPoint[] =
    history.map(
      (
        observation,
      ) => ({
        observedAt:
          observation
            .observed_at,

        waterLevel:
          observation
            .water_level_m,
      }),
    );


  const waterLevelCount =
    history.filter(
      (
        observation,
      ) => (
        observation
          .water_level_m
        !== null
      ),
    ).length;


  const firstObservation =
    history.length > 0
      ? history[0]
      : null;


  const latestObservation =
    history.length > 0
      ? history[
        history.length - 1
      ]
      : null;


  return (
    <section
      id="station-detail"
      className="station-detail-section"
    >
      <div className="section-heading">
        <div>
          <p className="section-label">
            Station detail
          </p>

          <h2>
            {
              stationName(
                station,
              )
            }
          </h2>
        </div>

        <div className="station-detail-id">
          Station
          {" "}
          <strong>
            {
              station.station_id
              ?? "Unknown"
            }
          </strong>
        </div>
      </div>


      <div className="station-detail-grid">
        <article className="station-info-card">
          <p className="metric-label">
            Station information
          </p>

          <dl className="station-info-list">
            <div>
              <dt>
                Basin
              </dt>

              <dd>
                {
                  basinName(
                    station,
                  )
                }
              </dd>
            </div>

            <div>
              <dt>
                Latitude
              </dt>

              <dd>
                {
                  formatCoordinate(
                    station.latitude,
                  )
                }
              </dd>
            </div>

            <div>
              <dt>
                Longitude
              </dt>

              <dd>
                {
                  formatCoordinate(
                    station.longitude,
                  )
                }
              </dd>
            </div>

            <div>
              <dt>
                Elevation
              </dt>

              <dd>
                {
                  formatElevation(
                    station.elevation_m,
                  )
                }
              </dd>
            </div>
          </dl>
        </article>


        <article className="station-info-card">
          <p className="metric-label">
            Current observation
          </p>

          <dl className="station-info-list">
            <div>
              <dt>
                Status
              </dt>

              <dd>
                <span
                  className={
                    station
                      .has_observation
                      ? (
                        "status-badge "
                        + "status-badge--available"
                      )
                      : (
                        "status-badge "
                        + "status-badge--missing"
                      )
                  }
                >
                  {
                    station
                      .has_observation
                      ? "Available"
                      : "Missing"
                  }
                </span>
              </dd>
            </div>

            <div>
              <dt>
                Water level
              </dt>

              <dd>
                {
                  formatWaterLevel(
                    station
                      .water_level_m,
                  )
                }
              </dd>
            </div>

            <div>
              <dt>
                Observed at
              </dt>

              <dd>
                {
                  formatTimestamp(
                    station
                      .observed_at,
                  )
                }
              </dd>
            </div>
          </dl>
        </article>


        <article className="station-info-card">
          <p className="metric-label">
            Historical data
          </p>

          <dl className="station-info-list">
            <div>
              <dt>
                Observations
              </dt>

              <dd>
                {
                  history.length
                }
              </dd>
            </div>

            <div>
              <dt>
                Water-level values
              </dt>

              <dd>
                {
                  waterLevelCount
                }
              </dd>
            </div>

            <div>
              <dt>
                First
              </dt>

              <dd>
                {
                  formatTimestamp(
                    firstObservation
                      ?.observed_at
                    ?? null,
                  )
                }
              </dd>
            </div>

            <div>
              <dt>
                Latest
              </dt>

              <dd>
                {
                  formatTimestamp(
                    latestObservation
                      ?.observed_at
                    ?? null,
                  )
                }
              </dd>
            </div>
          </dl>
        </article>
      </div>


      <article className="history-chart-card">
        <div className="history-chart-heading">
          <div>
            <p className="metric-label">
              Historical observations
            </p>

            <h3>
              Water level over time
            </h3>
          </div>

          <p>
            {
              waterLevelCount
            }
            {" "}
            water-level values
          </p>
        </div>


        {
          waterLevelCount === 0
            ? (
              <div className="chart-empty">
                <strong>
                  No water-level values
                </strong>

                <p>
                  Historical records
                  exist, but none contain
                  a water-level value
                  that can be plotted.
                </p>
              </div>
            )
            : (
              <div
                className="history-chart"
                role="img"
                aria-label={
                  `Historical water-level chart for ${
                    stationName(
                      station,
                    )
                  }`
                }
              >
                <ResponsiveContainer
                  width="100%"
                  height={330}
                >
                  <LineChart
                    data={
                      chartData
                    }
                    margin={{
                      top: 10,
                      right: 16,
                      bottom: 10,
                      left: 8,
                    }}
                  >
                    <CartesianGrid
                      strokeDasharray="3 3"
                      vertical={false}
                    />

                    <XAxis
                      dataKey="observedAt"
                      tickFormatter={
                        (
                          value,
                        ) => (
                          formatShortDate(
                            String(
                              value,
                            ),
                          )
                        )
                      }
                      minTickGap={32}
                    />

                    <YAxis
                      width={62}
                      unit=" m"
                      domain={[
                        "auto",
                        "auto",
                      ]}
                    />

                    <Tooltip
                      labelFormatter={
                        (
                          value,
                        ) => (
                          formatTimestamp(
                            String(
                              value,
                            ),
                          )
                        )
                      }
                    />

                    <Line
                      type="monotone"
                      dataKey="waterLevel"
                      name="Water level (m)"
                      stroke="#397b69"
                      strokeWidth={2}
                      dot={false}
                      activeDot={{
                        r: 5,
                      }}
                      connectNulls={false}
                      isAnimationActive={
                        false
                      }
                    />
                  </LineChart>
                </ResponsiveContainer>
              </div>
            )
        }


        <p className="chart-note">
          The chart displays
          observations stored by
          RiverWatch. Missing
          water-level values create
          gaps. Values are analytical
          observations and are not
          flood-safety classifications.
        </p>
      </article>
    </section>
  );
}