import {
  apiGet,
} from "./client";


export interface BasinCurrentSummary {
  basin_name: string;

  total_stations: number;

  stations_with_observation: number;
  stations_without_observation: number;

  fresh_observations: number;
  stale_observations: number;
  future_observations: number;
  unassessable_observations: number;

  observations_with_water_level: number;
  observations_without_water_level: number;

  coverage_ratio: number;
  freshness_ratio: number;

  oldest_observed_at:
    string | null;

  latest_observed_at:
    string | null;
}


export function getBasinSummaries(
  signal?: AbortSignal,
): Promise<
  BasinCurrentSummary[]
> {
  return apiGet<
    BasinCurrentSummary[]
  >(
    "/basins",
    signal,
  );
}