import "@testing-library/jest-dom/vitest";

import {
  vi,
} from "vitest";


class ResizeObserverMock {
  observe() {
    // Test environment no-op.
  }

  unobserve() {
    // Test environment no-op.
  }

  disconnect() {
    // Test environment no-op.
  }
}


vi.stubGlobal(
  "ResizeObserver",
  ResizeObserverMock,
);


Object.defineProperty(
  window,
  "matchMedia",
  {
    writable: true,
    configurable: true,

    value: vi.fn().mockImplementation(
      (
        query: string,
      ) => ({
        matches: false,
        media: query,

        onchange: null,

        addListener:
          vi.fn(),

        removeListener:
          vi.fn(),

        addEventListener:
          vi.fn(),

        removeEventListener:
          vi.fn(),

        dispatchEvent:
          vi.fn(),
      }),
    ),
  },
);


Object.defineProperty(
  Element.prototype,
  "scrollIntoView",
  {
    writable: true,

    value:
      vi.fn(),
  },
);