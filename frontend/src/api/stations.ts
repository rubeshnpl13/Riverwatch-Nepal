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