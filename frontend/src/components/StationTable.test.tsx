
import {
  cleanup,
  render,
  screen,
} from "@testing-library/react";

import userEvent from "@testing-library/user-event";

import {
  afterEach,
  beforeEach,
  describe,
  expect,
  it,
  vi,
} from "vitest";

import {
  getStations,
  type CurrentRiverStation,
} from "../api/stations";

import {
  StationTable,
} from "./StationTable";


vi.mock(
  "../api/stations",
  () => ({
    getStations:
      vi.fn(),
  }),
);


const stations:
  CurrentRiverStation[] = [
    {
      station_id: "1",

      station_name:
        "Alpha River",

      basin_name:
        "Koshi",

      latitude:
        27.5,

      longitude:
        87.2,

      elevation_m:
        100,

      source_record_id:
        "source-1",

      observed_at:
        "2026-09-28T20:00:00Z",

      water_level_m:
        null,

      observation_ingested_at:
        "2026-09-28T21:00:00Z",

      has_observation:
        true,

      observation_age_hours:
        1,
    },

    {
      station_id: "2",

      station_name:
        "Bravo River",

      basin_name:
        "Narayani",

      latitude:
        28.0,

      longitude:
        84.0,

      elevation_m:
        200,

      source_record_id:
        null,

      observed_at:
        null,

      water_level_m:
        null,

      observation_ingested_at:
        null,

      has_observation:
        false,

      observation_age_hours:
        null,
    },

    {
      station_id: "3",

      station_name:
        "Charlie River",

      basin_name:
        "Bagmati",

      latitude:
        27.7,

      longitude:
        85.3,

      elevation_m:
        300,

      source_record_id:
        "source-3",

      observed_at:
        "2026-09-28T21:00:00Z",

      water_level_m:
        2.5,

      observation_ingested_at:
        "2026-09-28T21:05:00Z",

      has_observation:
        true,

      observation_age_hours:
        0.75,
    },
  ];


afterEach(
  () => {
    cleanup();
    vi.clearAllMocks();
  },
);


describe(
  "StationTable",
  () => {
    beforeEach(
      () => {
        vi.mocked(
          getStations,
        ).mockResolvedValue(
          stations,
        );
      },
    );


    it(
      "filters stations with missing water levels correctly",
      async () => {
        const user =
          userEvent.setup();

        render(
          <StationTable
            selectedStationId={
              null
            }
            onSelectStation={
              vi.fn()
            }
          />,
        );


        expect(
          await screen.findByText(
            "Alpha River",
          ),
        ).toBeInTheDocument();


        await user.selectOptions(
          screen.getByLabelText(
            "Water level",
          ),
          "missing",
        );


        expect(
          screen.getByText(
            "Alpha River",
          ),
        ).toBeInTheDocument();

        expect(
          screen.queryByText(
            "Bravo River",
          ),
        ).not
          .toBeInTheDocument();

        expect(
          screen.queryByText(
            "Charlie River",
          ),
        ).not
          .toBeInTheDocument();
      },
    );


    it(
      "selects a station from the table",
      async () => {
        const user =
          userEvent.setup();

        const onSelectStation =
          vi.fn();


        render(
          <StationTable
            selectedStationId={
              null
            }
            onSelectStation={
              onSelectStation
            }
          />,
        );


        expect(
          await screen.findByText(
            "Alpha River",
          ),
        ).toBeInTheDocument();


        const alphaRow =
          screen
            .getByText(
              "Alpha River",
            )
            .closest("tr");

        expect(
          alphaRow,
        ).not.toBeNull();


        const viewButton =
          alphaRow!.querySelector(
            "button.station-view-button",
          );

        expect(
          viewButton,
        ).not.toBeNull();

        expect(
          viewButton,
        ).not.toBeDisabled();


        await user.click(
          viewButton!,
        );


        expect(
          onSelectStation,
        ).toHaveBeenCalledTimes(
          1,
        );

        expect(
          onSelectStation,
        ).toHaveBeenCalledWith(
          "1",
        );
      },
    );
  },
);
