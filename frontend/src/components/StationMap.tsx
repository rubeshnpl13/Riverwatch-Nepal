import {
  useEffect,
  useMemo,
  useState,
} from "react";

import {
  CircleMarker,
  MapContainer,
  Popup,
  TileLayer,
  useMap,
} from "react-leaflet";

import type {
  LatLngBoundsExpression,
} from "leaflet";

import {
  getStations,
  type CurrentRiverStation,
} from "../api/stations";


const NEPAL_CENTER:
  [number, number] = [
    28.2,
    84.0,
  ];


interface StationMapProps {
  selectedStationId:
    string | null;

  onSelectStation:
    (
      stationId: string,
    ) => void;
}


function isValidCoordinate(
  station: CurrentRiverStation,
): station is CurrentRiverStation & {
  latitude: number;
  longitude: number;
} {
  const {
    latitude,
    longitude,
  } = station;

  if (
    latitude === null
    || longitude === null
  ) {
    return false;
  }

  return (
    Number.isFinite(
      latitude,
    )
    && Number.isFinite(
      longitude,
    )
    && latitude >= -90
    && latitude <= 90
    && longitude >= -180
    && longitude <= 180
  );
}


function stationName(
  station: CurrentRiverStation,
): string {
  const name =
    station.station_name?.trim();

  return name
    ? name
    : "Unnamed station";
}


function basinName(
  station: CurrentRiverStation,
): string {
  const basin =
    station.basin_name?.trim();

  return basin
    ? basin
    : "Unknown";
}


function formatWaterLevel(
  value: number | null,
): string {
  if (value === null) {
    return "Not available";
  }

  return `${value.toLocaleString(
    undefined,
    {
      maximumFractionDigits: 3,
    },
  )} m`;
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


interface FitStationBoundsProps {
  stations: Array<
    CurrentRiverStation & {
      latitude: number;
      longitude: number;
    }
  >;
}


function FitStationBounds({
  stations,
}: FitStationBoundsProps) {
  const map = useMap();

  useEffect(() => {
    if (
      stations.length === 0
    ) {
      return;
    }

    const bounds:
      LatLngBoundsExpression =
      stations.map(
        (
          station,
        ) => [
          station.latitude,
          station.longitude,
        ],
      );

    map.fitBounds(
      bounds,
      {
        padding: [
          30,
          30,
        ],
        maxZoom: 9,
      },
    );
  }, [
    map,
    stations,
  ]);

  return null;
}


export function StationMap({
  selectedStationId,
  onSelectStation,
}: StationMapProps) {
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


  const mappedStations =
    useMemo(
      () => (
        stations === null
          ? []
          : stations.filter(
              isValidCoordinate,
            )
      ),
      [
        stations,
      ],
    );


  if (hasError) {
    return (
      <section className="map-section">
        <div
          className={
            "state-card "
            + "state-card--error"
          }
        >
          <p className="section-label">
            Station map
          </p>

          <h2>
            Map data unavailable
          </h2>

          <p>
            RiverWatch could not load
            the station coordinates.
          </p>
        </div>
      </section>
    );
  }


  if (stations === null) {
    return (
      <section className="map-section">
        <div className="state-card">
          <p className="section-label">
            Station map
          </p>

          <h2>
            Loading station map…
          </h2>
        </div>
      </section>
    );
  }


  return (
    <section className="map-section">
      <div className="section-heading">
        <div>
          <p className="section-label">
            Station map
          </p>

          <h2>
            River monitoring network
          </h2>
        </div>

        <p className="station-count">
          Mapped
          {" "}
          <strong>
            {mappedStations.length}
          </strong>
          {" "}
          of
          {" "}
          <strong>
            {stations.length}
          </strong>
          {" "}
          stations
        </p>
      </div>


      <div className="map-card">
        <div className="map-legend">
          <span className="legend-item">
            <span
              className={
                "legend-dot "
                + "legend-dot--observation"
              }
              aria-hidden="true"
            />

            Has observation
          </span>

          <span className="legend-item">
            <span
              className={
                "legend-dot "
                + "legend-dot--missing"
              }
              aria-hidden="true"
            />

            No observation
          </span>
        </div>


        <MapContainer
          center={NEPAL_CENTER}
          zoom={7}
          scrollWheelZoom
          className="station-map"
        >
          <TileLayer
            attribution={
              "&copy; OpenStreetMap contributors"
            }
            url={
              "https://tile.openstreetmap.org/{z}/{x}/{y}.png"
            }
          />

          <FitStationBounds
            stations={
              mappedStations
            }
          />

          {
            mappedStations.map(
              (
                station,
                index,
              ) => (
                <CircleMarker
                  key={
                    station.station_id
                    ?? `${
                      station.station_name
                      ?? "station"
                    }-${index}`
                  }
                  center={[
                    station.latitude,
                    station.longitude,
                  ]}
                  radius={
                    station.station_id
                      === selectedStationId
                      ? 9
                      : 6
                  }
                  pathOptions={{
                    color:
                      station.station_id
                        === selectedStationId
                        ? "#17211e"
                        : station
                            .has_observation
                          ? "#247a5c"
                          : "#6f7d78",

                    fillColor:
                      station
                        .has_observation
                        ? "#3a9272"
                        : "#9aa6a2",

                    fillOpacity: 0.8,

                    weight:
                      station.station_id
                        === selectedStationId
                        ? 3
                        : 1.5,
                  }}
                >
                  <Popup>
                    <div className="station-popup">
                      <strong className="station-popup-title">
                        {
                          stationName(
                            station,
                          )
                        }
                      </strong>

                      <dl>
                        <div>
                          <dt>
                            Station ID
                          </dt>

                          <dd>
                            {
                              station
                                .station_id
                              ?? "Not available"
                            }
                          </dd>
                        </div>

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
                            Observation
                          </dt>

                          <dd>
                            {
                              station
                                .has_observation
                                ? "Available"
                                : "Missing"
                            }
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

                      <button
                        type="button"
                        className="popup-detail-button"
                        aria-pressed={
                          station.station_id
                          === selectedStationId
                        }
                        disabled={
                          station.station_id
                          === null
                        }
                        onClick={
                          () => {
                            if (
                              station.station_id
                              !== null
                            ) {
                              onSelectStation(
                                station.station_id,
                              );
                            }
                          }
                        }
                      >
                        {
                          station.station_id
                            === selectedStationId
                            ? "Viewing details"
                            : "View station details"
                        }
                      </button>
                    </div>
                  </Popup>
                </CircleMarker>
              ),
            )
          }
        </MapContainer>
      </div>


      <p className="map-note">
        Marker status represents data
        availability only. It does not
        indicate flood severity or river
        safety.
      </p>
    </section>
  );
}