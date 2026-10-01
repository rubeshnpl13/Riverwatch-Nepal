import {
  useEffect,
  useMemo,
  useState,
} from "react";

import {
  getStations,
  type CurrentRiverStation,
} from "../api/stations";


const PAGE_SIZE = 20;


type ObservationFilter =
  | "all"
  | "with"
  | "without";


type WaterLevelFilter =
  | "all"
  | "available"
  | "missing";


const numberFormatter =
  new Intl.NumberFormat(
    undefined,
    {
      maximumFractionDigits: 3,
    },
  );


const timestampFormatter =
  new Intl.DateTimeFormat(
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
  );


function basinLabel(
  station: CurrentRiverStation,
): string {
  const value =
    station.basin_name?.trim();

  return value
    ? value
    : "Unknown";
}


function stationName(
  station: CurrentRiverStation,
): string {
  const value =
    station.station_name?.trim();

  return value
    ? value
    : "Unnamed station";
}


function formatWaterLevel(
  value: number | null,
): string {
  if (value === null) {
    return "—";
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
    return "—";
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

  return timestampFormatter.format(
    date,
  );
}


export function StationTable() {
  const [
    stations,
    setStations,
  ] = useState<
    CurrentRiverStation[] | null
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

  const [
    basin,
    setBasin,
  ] = useState(
    "all",
  );

  const [
    observationFilter,
    setObservationFilter,
  ] = useState<
    ObservationFilter
  >(
    "all",
  );

  const [
    waterLevelFilter,
    setWaterLevelFilter,
  ] = useState<
    WaterLevelFilter
  >(
    "all",
  );

  const [
    page,
    setPage,
  ] = useState(
    1,
  );


  useEffect(() => {
    const controller =
      new AbortController();

    void getStations(
      controller.signal,
    )
      .then(
        (
          currentStations,
        ) => {
          setStations(
            currentStations,
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


  const basinOptions =
    useMemo(
      () => {
        if (stations === null) {
          return [];
        }

        return [
          ...new Set(
            stations.map(
              basinLabel,
            ),
          ),
        ].sort(
          (
            first,
            second,
          ) => {
            if (
              first === "Unknown"
            ) {
              return 1;
            }

            if (
              second === "Unknown"
            ) {
              return -1;
            }

            return first.localeCompare(
              second,
            );
          },
        );
      },
      [
        stations,
      ],
    );


  const filteredStations =
    useMemo(
      () => {
        if (stations === null) {
          return [];
        }

        const query =
          search
            .trim()
            .toLowerCase();

        return stations.filter(
          (
            station,
          ) => {
            const stationBasin =
              basinLabel(
                station,
              );

            if (query) {
              const searchableText = [
                station.station_id
                  ?? "",
                stationName(
                  station,
                ),
                stationBasin,
              ]
                .join(
                  " ",
                )
                .toLowerCase();

              if (
                !searchableText.includes(
                  query,
                )
              ) {
                return false;
              }
            }

            if (
              basin !== "all"
              && stationBasin
                !== basin
            ) {
              return false;
            }

            if (
              observationFilter
                === "with"
              && !station
                .has_observation
            ) {
              return false;
            }

            if (
              observationFilter
                === "without"
              && station
                .has_observation
            ) {
              return false;
            }

            if (
              waterLevelFilter
                === "available"
              && station
                .water_level_m
                === null
            ) {
              return false;
            }

            if (
              waterLevelFilter
                === "missing"
              && (
                !station
                  .has_observation
                || station
                  .water_level_m
                  !== null
              )
            ) {
              return false;
            }

            return true;
          },
        );
      },
      [
        basin,
        observationFilter,
        search,
        stations,
        waterLevelFilter,
      ],
    );


  // useEffect(() => {
  //   setPage(
  //     1,
  //   );
  // }, [
  //   basin,
  //   observationFilter,
  //   search,
  //   waterLevelFilter,
  // ]);


  const totalPages =
    Math.max(
      1,
      Math.ceil(
        filteredStations.length
        / PAGE_SIZE,
      ),
    );

  const currentPage =
    Math.min(
      page,
      totalPages,
    );

  const startIndex =
    (
      currentPage
      - 1
    )
    * PAGE_SIZE;

  const visibleStations =
    filteredStations.slice(
      startIndex,
      startIndex
        + PAGE_SIZE,
    );


  function clearFilters() {
    setSearch(
      "",
    );

    setBasin(
      "all",
    );

    setObservationFilter(
      "all",
    );

    setWaterLevelFilter(
      "all",
    );

    setPage(
      1,
    );
  }


  if (hasError) {
    return (
      <section
        className="station-section"
      >
        <div
          className={
            "state-card "
            + "state-card--error"
          }
        >
          <p className="section-label">
            Stations
          </p>

          <h2>
            Station data unavailable
          </h2>

          <p>
            RiverWatch could not load
            the current station
            snapshot.
          </p>
        </div>
      </section>
    );
  }


  if (stations === null) {
    return (
      <section
        className="station-section"
      >
        <div className="state-card">
          <p className="section-label">
            Stations
          </p>

          <h2>
            Loading stations…
          </h2>
        </div>
      </section>
    );
  }


  return (
    <section
      className="station-section"
    >
      <div className="section-heading">
        <div>
          <p className="section-label">
            Stations
          </p>

          <h2>
            Current station snapshot
          </h2>
        </div>

        <p className="station-count">
          Showing
          {" "}
          <strong>
            {
              filteredStations
                .length
            }
          </strong>
          {" "}
          of
          {" "}
          <strong>
            {
              stations.length
            }
          </strong>
          {" "}
          stations
        </p>
      </div>


      <div className="station-toolbar">
        <label
          className="station-filter station-search"
        >
          <span>
            Search
          </span>

          <input
            type="search"
            value={search}
            placeholder={
              "Station name, ID, or basin"
            }
            onChange={
              (
                event,
              ) => {
                setSearch(
                  event
                    .target
                    .value,
                );
                setPage(
                    1,
                );
              }
            }
          />
        </label>


        <label className="station-filter">
          <span>
            Basin
          </span>

          <select
            value={basin}
            onChange={
              (
                event,
              ) => {
                setBasin(
                  event
                    .target
                    .value,
                );
                setPage(
                    1,
                );
              }
            }
          >
            <option value="all">
              All basins
            </option>

            {
              basinOptions.map(
                (
                  option,
                ) => (
                  <option
                    key={option}
                    value={option}
                  >
                    {option}
                  </option>
                ),
              )
            }
          </select>
        </label>


        <label className="station-filter">
          <span>
            Observation
          </span>

          <select
            value={
              observationFilter
            }
            onChange={
              (
                event,
              ) => {
                setObservationFilter(
                  event
                    .target
                    .value as ObservationFilter,
                );
                setPage(
                    1,
                );
              }
            }
          >
            <option value="all">
              All stations
            </option>

            <option value="with">
              With observation
            </option>

            <option value="without">
              Without observation
            </option>
          </select>
        </label>


        <label className="station-filter">
          <span>
            Water level
          </span>

          <select
            value={
              waterLevelFilter
            }
            onChange={
              (
                event,
              ) => {
                setWaterLevelFilter(
                  event
                    .target
                    .value as WaterLevelFilter,
                );
                setPage(
                    1,
                );
              }
            }
          >
            <option value="all">
              All values
            </option>

            <option value="available">
              Available
            </option>

            <option value="missing">
              Missing
            </option>
          </select>
        </label>


        <button
          className="clear-filters"
          type="button"
          onClick={
            clearFilters
          }
        >
          Clear filters
        </button>
      </div>


      <div className="station-table-card">
        {
          filteredStations
            .length === 0
            ? (
              <div className="table-empty">
                <strong>
                  No stations found
                </strong>

                <p>
                  Try changing your
                  search or filters.
                </p>
              </div>
            )
            : (
              <>
                <div className="table-scroll">
                  <table className="station-table">
                    <thead>
                      <tr>
                        <th>
                          ID
                        </th>

                        <th>
                          Station
                        </th>

                        <th>
                          Basin
                        </th>

                        <th>
                          Observation
                        </th>

                        <th>
                          Water level
                        </th>

                        <th>
                          Observed at
                        </th>
                      </tr>
                    </thead>

                    <tbody>
                      {
                        visibleStations.map(
                          (
                            station,
                            index,
                          ) => (
                            <tr
                              key={
                                station
                                  .station_id
                                ?? `${
                                  station
                                    .station_name
                                  ?? "station"
                                }-${
                                  startIndex
                                  + index
                                }`
                              }
                            >
                              <td
                                className="station-id"
                              >
                                {
                                  station
                                    .station_id
                                  ?? "—"
                                }
                              </td>

                              <td>
                                <strong>
                                  {
                                    stationName(
                                      station,
                                    )
                                  }
                                </strong>
                              </td>

                              <td>
                                {
                                  basinLabel(
                                    station,
                                  )
                                }
                              </td>

                              <td>
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
                              </td>

                              <td>
                                {
                                  formatWaterLevel(
                                    station
                                      .water_level_m,
                                  )
                                }
                              </td>

                              <td>
                                {
                                  formatTimestamp(
                                    station
                                      .observed_at,
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


                <div className="table-pagination">
                  <p>
                    {
                      startIndex
                      + 1
                    }
                    –
                    {
                      Math.min(
                        startIndex
                        + PAGE_SIZE,
                        filteredStations
                          .length,
                      )
                    }
                    {" "}
                    of
                    {" "}
                    {
                      filteredStations
                        .length
                    }
                  </p>

                  <div className="pagination-actions">
                    <button
                      type="button"
                      disabled={
                        currentPage
                        === 1
                      }
                      onClick={
                        () => {
                          setPage(
                            (
                              current,
                            ) => (
                              Math.max(
                                1,
                                current
                                - 1,
                              )
                            ),
                          );
                        }
                      }
                    >
                      Previous
                    </button>

                    <span>
                      Page
                      {" "}
                      {
                        currentPage
                      }
                      {" "}
                      of
                      {" "}
                      {
                        totalPages
                      }
                    </span>

                    <button
                      type="button"
                      disabled={
                        currentPage
                        === totalPages
                      }
                      onClick={
                        () => {
                          setPage(
                            (
                              current,
                            ) => (
                              Math.min(
                                totalPages,
                                current
                                + 1,
                              )
                            ),
                          );
                        }
                      }
                    >
                      Next
                    </button>
                  </div>
                </div>
              </>
            )
        }
      </div>
    </section>
  );
}