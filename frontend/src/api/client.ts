import {
  API_BASE_URL,
} from "./config";


export class ApiError extends Error {
  readonly status: number;

  constructor(
    status: number,
    message: string,
  ) {
    super(message);

    this.name = "ApiError";
    this.status = status;
  }
}


export async function apiGet<T>(
  path: string,
  signal?: AbortSignal,
): Promise<T> {
  const response = await fetch(
    `${API_BASE_URL}${path}`,
    {
      method: "GET",
      headers: {
        Accept: "application/json",
      },
      signal,
    },
  );

  if (!response.ok) {
    throw new ApiError(
      response.status,
      `API request failed with status ${
        response.status
      }`,
    );
  }

  return response.json() as Promise<T>;
}