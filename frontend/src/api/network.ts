import {
  apiGet,
} from "./client";


export interface CurrentNetworkSummary {
  basins_represented: number;

  total_stations: number;

  stations_with_observation: number;
  stations_without_observation: number;

  fresh_observations: number;
  stale_observations: number;
  future_observations: number;
  unassessable_observations: number;

  observations_with_water_level: number;
  observations_without_water_level: number;

  oldest_observed_at: string | null;
  latest_observed_at: string | null;

  coverage_ratio: number;
  freshness_ratio: number;
}


export function getNetworkSummary(
  signal?: AbortSignal,
): Promise<CurrentNetworkSummary> {
  return apiGet<CurrentNetworkSummary>(
    "/network/summary",
    signal,
  );
}