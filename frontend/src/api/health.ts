import {
  apiGet,
} from "./client";


export interface HealthResponse {
  service: "riverwatch";
  status: "ok";
}


export function getHealth(
  signal?: AbortSignal,
): Promise<HealthResponse> {
  return apiGet<HealthResponse>(
    "/health/live",
    signal,
  );
}