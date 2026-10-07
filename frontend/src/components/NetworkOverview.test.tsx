import { render, screen } from '@testing-library/react';

import { beforeEach, describe, expect, it, vi } from 'vitest';

import { getNetworkSummary, type CurrentNetworkSummary } from '../api/network';

import { NetworkOverview } from './NetworkOverview';

vi.mock('../api/network', () => ({
  getNetworkSummary: vi.fn(),
}));

const summary: CurrentNetworkSummary = {
  basins_represented: 25,

  total_stations: 284,

  stations_with_observation: 279,

  stations_without_observation: 5,

  fresh_observations: 188,

  stale_observations: 91,

  future_observations: 0,

  unassessable_observations: 0,

  observations_with_water_level: 275,

  observations_without_water_level: 4,

  oldest_observed_at: '2020-07-21T01:55:00Z',

  latest_observed_at: '2026-09-28T21:45:00Z',

  coverage_ratio: 0.9823943661971831,

  freshness_ratio: 0.6738351254480287,
};

describe('NetworkOverview', () => {
  beforeEach(() => {
    vi.mocked(getNetworkSummary).mockResolvedValue(summary);
  });

  it('renders the current network summary', async () => {
    render(<NetworkOverview />);

    expect(
      await screen.findByRole('heading', {
        name: 'Current network snapshot',
      }),
    ).toBeInTheDocument();

    expect(screen.getByText('Monitoring stations')).toBeInTheDocument();

    expect(screen.getByText('284')).toBeInTheDocument();

    expect(screen.getByText('25 basin groups represented')).toBeInTheDocument();

    expect(screen.getByText('98.2%')).toBeInTheDocument();

    expect(
      screen.getByText('279 of 284 stations reporting'),
    ).toBeInTheDocument();

    expect(screen.getByText('67.4%')).toBeInTheDocument();

    expect(screen.getByText('188 fresh · 91 stale')).toBeInTheDocument();

    expect(screen.getByText('275')).toBeInTheDocument();

    expect(
      screen.getByText('4 current observations missing a value'),
    ).toBeInTheDocument();

    expect(
      screen.getByRole('progressbar', {
        name: 'Observation coverage',
      }),
    ).toHaveAttribute('aria-valuenow', '98');

    expect(
      screen.getByRole('progressbar', {
        name: 'Observation freshness',
      }),
    ).toHaveAttribute('aria-valuenow', '67');

    expect(screen.getByText('279 reporting')).toBeInTheDocument();

    expect(screen.getByText('5 without an observation')).toBeInTheDocument();

    expect(screen.getByText('25 basins')).toBeInTheDocument();

    expect(screen.getByText('284 monitoring stations')).toBeInTheDocument();
  });

  it('renders an error state when loading fails', async () => {
    vi.mocked(getNetworkSummary).mockRejectedValueOnce(
      new Error('API unavailable'),
    );

    render(<NetworkOverview />);

    expect(
      await screen.findByRole('heading', {
        name: 'Network data unavailable',
      }),
    ).toBeInTheDocument();

    expect(
      screen.getByText(
        'RiverWatch could not load the current network summary from the API.',
      ),
    ).toBeInTheDocument();
  });
});
