import { defineConfig, devices } from '@playwright/test';

export default defineConfig({
  testDir: './tests',
  globalSetup: './global-setup.ts',
  fullyParallel: false, // Disable parallel execution to avoid conflicts
  forbidOnly: !!process.env.CI,
  retries: process.env.CI ? 2 : 0,
  workers: 1, // Use single worker to avoid database conflicts
  reporter: process.env.CI
    ? [['html'], ['junit', { outputFile: 'test-results/junit-report.xml' }]]
    : 'html',
  use: {
    baseURL: 'http://localhost:2480',
    trace: 'on-first-retry',
    headless: true,
    screenshot: 'only-on-failure',
    video: 'retain-on-failure',
  },

  projects: [
    {
      name: 'chromium',
      use: { ...devices['Desktop Chrome'] },
    },
  ],

  webServer: process.env.CI ? undefined : {
    command: 'echo "No server management in CI - assuming external server"',
    port: 2480,
    reuseExistingServer: true,
  },
});
