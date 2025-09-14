///
/// Copyright © 2021-present Arcade Data Ltd (info@arcadedata.com)
///
/// Licensed under the Apache License, Version 2.0 (the "License");
/// you may not use this file except in compliance with the License.
/// You may obtain a copy of the License at
///
///     http://www.apache.org/licenses/LICENSE-2.0
///
/// Unless required by applicable law or agreed to in writing, software
/// distributed under the License is distributed on an "AS IS" BASIS,
/// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
/// See the License for the specific language governing permissions and
/// limitations under the License.
///

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
