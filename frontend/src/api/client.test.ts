
import {
  afterEach,
  describe,
  expect,
  it,
  vi,
} from "vitest";

import {

  apiGet,
} from "./client";


describe(
  "apiGet",
  () => {
    afterEach(
      () => {
        vi.unstubAllGlobals();
      },
    );


    it(
      "returns parsed JSON for a successful request",
      async () => {
        const body = {
          status: "ok",
        };

        const fetchMock =
          vi.fn().mockResolvedValue({
            ok: true,
            status: 200,

            json:
              vi.fn()
                .mockResolvedValue(
                  body,
                ),
          });

        vi.stubGlobal(
          "fetch",
          fetchMock,
        );


        await expect(
          apiGet<{
            status: string;
          }>(
            "/health/live",
          ),
        ).resolves.toEqual(
          body,
        );


        expect(
          fetchMock,
        ).toHaveBeenCalledTimes(
          1,
        );


        const requestUrl =
          String(
            fetchMock
              .mock
              .calls[0][0],
          );

        expect(
          requestUrl,
        ).toMatch(
          /\/api\/v1\/health\/live$/,
        );
      },
    );


    it(
      "throws ApiError for a non-success response",
      async () => {
        const fetchMock =
          vi.fn().mockResolvedValue({
            ok: false,
            status: 503,
          });

        vi.stubGlobal(
          "fetch",
          fetchMock,
        );


        const request =
          apiGet(
            "/network/summary",
          );

        await expect(
          request,
        ).rejects.toEqual(
          expect.objectContaining({
            name:
              "ApiError",

            status:
              503,
          }),
        );
      },
    );
  },
);
