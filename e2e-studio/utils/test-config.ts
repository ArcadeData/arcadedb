/**
 * Test Configuration and Environment Variables
 *
 * Provides centralized configuration for ArcadeDB Studio e2e tests
 * with support for environment variables and CI-friendly defaults.
 */

export interface TestCredentials {
  username: string;
  password: string;
}

export interface TestTimeouts {
  login: number;
  query: number;
  graph: number;
  networkIdle: number;
  graphReady: number;
}

export interface PerformanceThresholds {
  queryTime: number;
  renderTime: number;
  totalTime: number;
  memoryGrowth: number;
  maxMemoryGrowth: number;
  zoomOperationTime: number;
  panOperationTime: number;
  selectionTime: number;
  layoutTime: number;
  accessTime: number;
  responseTime: number;
  timePerNode: number;
  memoryPerNode: number;
}

/**
 * Main test configuration with environment variable support
 */
export const TEST_CONFIG = {
  credentials: {
    username: process.env.ARCADE_TEST_USERNAME || 'root',
    password: process.env.ARCADE_TEST_PASSWORD || 'playwithdata'
  } as TestCredentials,

  database: process.env.ARCADE_TEST_DATABASE || 'Beer',

  timeouts: {
    login: parseInt(process.env.ARCADE_LOGIN_TIMEOUT || '15000'),
    query: parseInt(process.env.ARCADE_QUERY_TIMEOUT || '30000'),
    graph: parseInt(process.env.ARCADE_GRAPH_TIMEOUT || '10000'),
    networkIdle: parseInt(process.env.ARCADE_NETWORK_TIMEOUT || '5000'),
    graphReady: parseInt(process.env.ARCADE_GRAPH_READY_TIMEOUT || '10000')
  } as TestTimeouts,

  /**
   * Performance thresholds that adapt to CI vs local environments
   */
  performance: {
    queryTime: process.env.CI ? 30000 : 10000,
    renderTime: process.env.CI ? 20000 : 5000,
    totalTime: process.env.CI ? 60000 : 30000,
    memoryGrowth: process.env.CI ? 20 * 1024 * 1024 : 10 * 1024 * 1024,
    maxMemoryGrowth: process.env.CI ? 20 * 1024 * 1024 : 10 * 1024 * 1024,
    zoomOperationTime: process.env.CI ? 10000 : 5000,
    panOperationTime: process.env.CI ? 5000 : 3000,
    selectionTime: process.env.CI ? 5000 : 3000,
    layoutTime: process.env.CI ? 30000 : 20000,
    accessTime: process.env.CI ? 1000 : 500,
    responseTime: process.env.CI ? 200 : 100,
    timePerNode: process.env.CI ? 200 : 100,
    memoryPerNode: process.env.CI ? 102400 : 51200,
    zoomIterations: process.env.CI ? 3 : 5,
    maxTestDuration: process.env.CI ? 5000 : 2000
  } as PerformanceThresholds,

  /**
   * Test data configuration
   */
  testData: {
    defaultNodeLimit: parseInt(process.env.ARCADE_DEFAULT_NODE_LIMIT || '5'),
    memoryTestSizes: process.env.CI ? [10, 20, 30] : [25, 50, 75],
    performanceIterations: process.env.CI ? 3 : 5,
    maxTestDuration: process.env.CI ? 5000 : 2000
  },

  /**
   * CI/Environment detection
   */
  environment: {
    isCI: !!process.env.CI,
    isCIEnvironment: () => !!process.env.CI,
    getEnvironmentType: () => process.env.CI ? 'ci' : 'local'
  }
};

/**
 * Get test credentials with environment variable support
 * @returns TestCredentials object with username and password
 */
export const getTestCredentials = (): TestCredentials => {
  return {
    username: process.env.ARCADE_TEST_USERNAME || 'root',
    password: process.env.ARCADE_TEST_PASSWORD || 'playwithdata'
  };
};

/**
 * Get adaptive timeout values based on environment
 * @param baseTimeout - Base timeout in milliseconds
 * @returns Adjusted timeout for current environment
 */
export const getAdaptiveTimeout = (baseTimeout: number): number => {
  const multiplier = process.env.CI ? 2 : 1;
  return baseTimeout * multiplier;
};

/**
 * Get memory test sizes adapted for current environment
 * @returns Array of node counts for memory testing
 */
export const getMemoryTestSizes = (): number[] => {
  const baseSize = process.env.CI ? 10 : 25;
  return [baseSize, baseSize * 2, baseSize * 3];
};

/**
 * Get adaptive test sizes for performance testing based on environment
 * @returns Array of node counts optimized for current environment
 */
export const getAdaptiveTestSizes = (): number[] => {
  return TEST_CONFIG.testData.memoryTestSizes;
};

/**
 * Get environment-aware performance thresholds
 * @returns PerformanceThresholds object optimized for current environment
 */
export const getPerformanceThresholds = (): PerformanceThresholds => {
  return TEST_CONFIG.performance;
};

/**
 * Get performance budget for a given node count
 * @param nodeCount - Number of nodes being tested
 * @returns Performance budget with time and memory constraints
 */
export const getPerformanceBudget = (nodeCount: number) => {
  return {
    maxTime: nodeCount * TEST_CONFIG.performance.timePerNode,
    maxMemory: nodeCount * TEST_CONFIG.performance.memoryPerNode
  };
};

/**
 * Environment-aware query for selecting test database
 * @param customDatabase - Optional custom database name
 * @returns Database name to use for tests
 */
export const getTestDatabase = (customDatabase?: string): string => {
  return customDatabase || process.env.ARCADE_TEST_DATABASE || 'Beer';
};

/**
 * Get test configuration for specific test type
 * @param testType - Type of test (unit, integration, performance, etc.)
 * @returns Tailored configuration for the test type
 */
export const getTestConfig = (testType: 'unit' | 'integration' | 'performance' | 'e2e' = 'e2e') => {
  const baseConfig = TEST_CONFIG;

  switch (testType) {
    case 'performance':
      return {
        ...baseConfig,
        timeouts: {
          ...baseConfig.timeouts,
          query: baseConfig.timeouts.query * 2,
          graph: baseConfig.timeouts.graph * 2
        }
      };
    case 'integration':
      return {
        ...baseConfig,
        timeouts: {
          ...baseConfig.timeouts,
          login: baseConfig.timeouts.login * 1.5
        }
      };
    default:
      return baseConfig;
  }
};

/**
 * Validate test environment configuration
 * @throws Error if critical configuration is missing
 */
export const validateTestEnvironment = (): void => {
  const config = TEST_CONFIG;

  if (!config.credentials.username || !config.credentials.password) {
    throw new Error('Test credentials are required. Set ARCADE_TEST_USERNAME and ARCADE_TEST_PASSWORD environment variables.');
  }

  if (!config.database) {
    throw new Error('Test database is required. Set ARCADE_TEST_DATABASE environment variable.');
  }

  // Validate timeout values
  Object.entries(config.timeouts).forEach(([key, value]) => {
    if (value <= 0 || value > 300000) { // Max 5 minutes
      throw new Error(`Invalid timeout value for ${key}: ${value}ms. Must be between 1ms and 300000ms.`);
    }
  });
};
