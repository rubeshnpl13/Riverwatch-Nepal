import {
  apiGet,
} from "./client";


export interface CurrentRiverStation {
  station_id: string | null;
  station_name: string | null;
  basin_name: string | null;

  latitude: number | null;
  longitude: number | null;
  elevation_m: number | null;

  source_record_id: string | null;

  observed_at: string | null;
  water_level_m: number | null;

  observation_ingested_at:
    string | null;

  has_observation: boolean;

  observation_age_hours:
    number | null;
}


export function getStations(
  signal?: AbortSignal,
): Promise<
  CurrentRiverStation[]
> {
  return apiGet<
    CurrentRiverStation[]
  >(
    "/stations",
    signal,
  );
}

export interface StationObservation {
  source_record_id: string | null;

  station_id: string;

  observed_at: string;

  water_level_m: number | null;

  endpoint: string;
  run_id: string;

  ingested_at: string | null;
}


export function getStation(
  stationId: string,
  signal?: AbortSignal,
): Promise<CurrentRiverStation> {
  return apiGet<
    CurrentRiverStation
  >(
    `/stations/${
      encodeURIComponent(
        stationId,
      )
    }`,
    signal,
  );
}


export function getStationHistory(
  stationId: string,
  signal?: AbortSignal,
): Promise<
  StationObservation[]
> {
  return apiGet<
    StationObservation[]
  >(
    `/stations/${
      encodeURIComponent(
        stationId,
      )
    }/history`,
    signal,
  );
}