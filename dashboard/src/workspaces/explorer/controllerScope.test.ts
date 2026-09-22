import { describe, expect, it } from 'vitest';
import {
  explorerSessionContextUrl,
  explorerSessionSizeUrl,
} from './controller.ts';
import { scopedQueryKey, scopedUrl, type DashboardScope } from '../../data/scope/store.ts';

const PROJECT_ALPHA: DashboardScope = {
  kind: 'project',
  projectId: 'project.alpha',
  label: 'Alpha',
  activation: 'active',
};
const PROJECT_BETA: DashboardScope = {
  kind: 'project',
  projectId: 'project.beta',
  label: 'Beta',
  activation: 'active',
};

describe('Explorer session identity routing', () => {
  it('binds provider and project scope into the URL and React Query key', () => {
    const url = explorerSessionContextUrl('session.shared', 'claude');
    const sizeUrl = explorerSessionSizeUrl('session.shared', 'claude');

    expect(scopedUrl(PROJECT_ALPHA, url)).toBe(
      '/api/projects/project.alpha/explorer/sessions/session.shared/read-context?limit=25&offset=0&order=asc&provider=claude',
    );
    expect(scopedUrl(PROJECT_ALPHA, sizeUrl)).toBe(
      '/api/projects/project.alpha/explorer/sessions/session.shared/size?provider=claude',
    );

    const alphaClaude = scopedQueryKey(
      PROJECT_ALPHA,
      ['explorer', 'read-context', 'session.shared', 'claude'],
      url,
    );
    const alphaCodex = scopedQueryKey(
      PROJECT_ALPHA,
      ['explorer', 'read-context', 'session.shared', 'codex'],
      explorerSessionContextUrl('session.shared', 'codex'),
    );
    const betaClaude = scopedQueryKey(
      PROJECT_BETA,
      ['explorer', 'read-context', 'session.shared', 'claude'],
      url,
    );

    expect(alphaClaude).not.toEqual(alphaCodex);
    expect(alphaClaude).not.toEqual(betaClaude);
  });
});
