import {
  render,
  screen,
} from "@testing-library/react";

import {
  beforeEach,
  describe,
  expect,
  it,
  vi,
} from "vitest";

import {
  getNetworkSummary,
  type CurrentNetworkSummary,
} from "../api/network";

import {
  NetworkOverview,
} from "./NetworkOverview";


vi.mock(
  "../api/network",
  () => ({
    getNetworkSummary:
      vi.fn(),
  }),
);


const summary:
  CurrentNetworkSummary = {
    basins_represented: 25,

    total_stations: 284,

    stations_with_observation:
      279,

    stations_without_observation:
      5,

    fresh_observations: 188,

    stale_observations: 91,

    future_observations: 0,

    unassessable_observations:
      0,

    observations_with_water_level:
      275,

    observations_without_water_level:
      4,

    oldest_observed_at:
      "2020-07-21T01:55:00Z",

    latest_observed_at:
      "2026-09-28T21:45:00Z",

    coverage_ratio:
      0.9823943661971831,

    freshness_ratio:
      0.6738351254480287,
  };


describe(
  "NetworkOverview",
  () => {
    beforeEach(
      () => {
        vi.mocked(
          getNetworkSummary,
        ).mockResolvedValue(
          summary,
        );
      },
    );


    it(
      "renders the current network summary",
      async () => {
        render(
          <NetworkOverview />,
        );


        expect(
          await screen.findByText(
            "Current RiverWatch snapshot",
          ),
        ).toBeInTheDocument();


        expect(
          screen.getByText(
            "284",
          ),
        ).toBeInTheDocument();

        expect(
          screen.getByText(
            "25",
          ),
        ).toBeInTheDocument();

        expect(
          screen.getByText(
            "98.2%",
          ),
        ).toBeInTheDocument();

        expect(
          screen.getByText(
            "67.4%",
          ),
        ).toBeInTheDocument();
      },
    );


    it(
      "renders an error state when loading fails",
      async () => {
        vi.mocked(
          getNetworkSummary,
        ).mockRejectedValueOnce(
          new Error(
            "API unavailable",
          ),
        );

        render(
          <NetworkOverview />,
        );


        expect(
          await screen.findByText(
            "Network data unavailable",
          ),
        ).toBeInTheDocument();
      },
    );
  },
);