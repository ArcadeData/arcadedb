/**
 * Test Utils Entry Point
 *
 * Exports all shared test utilities for easy importing in test files.
 */

export {
  ArcadeStudioTestHelper,
  assertGraphState,
  assertContextMenu,
  assertPerformanceMetrics,
  getElementWithFallback,
  isCI,
  getEnvironmentConfig
} from './test-utils';

export {
  TEST_CONFIG,
  getTestCredentials,
  getAdaptiveTimeout,
  getMemoryTestSizes,
  getAdaptiveTestSizes,
  getPerformanceThresholds,
  getPerformanceBudget,
  getTestDatabase,
  getTestConfig,
  validateTestEnvironment
} from './test-config';

export type {
  TestCredentials,
  TestTimeouts,
  PerformanceThresholds
} from './test-config';

export type {
  GraphInfo,
  ContextMenuInfo
} from './test-utils';
