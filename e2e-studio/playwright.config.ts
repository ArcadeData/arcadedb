/**
* Copyright © 2021-present Arcade Data Ltd (info@arcadedata.com)
*
* Licensed under the Apache License, Version 2.0 (the "License");
* you may not use this file except in compliance with the License.
* You may obtain a copy of the License at
*
*     http://www.apache.org/licenses/LICENSE-2.0
*
* Unless required by applicable law or agreed to in writing, software
* distributed under the License is distributed on an "AS IS" BASIS,
* WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
* See the License for the specific language governing permissions and
* limitations under the License.
*
*/

import { defineConfig, devices } from '@playwright/test';

export default defineConfig({
  testDir: './tests',
  globalSetup: './global-setup.ts',
  globalTeardown: './global-teardown.ts',
  fullyParallel: false, // Disable parallel execution to avoid conflicts
  forbidOnly: !!process.env.CI,
  retries: process.env.CI ? 2 : 0,
  workers: 1, // Use single worker to avoid database conflicts
  reporter: process.env.CI
    ? [['html'], ['junit', { outputFile: 'reports/playwright-junit.xml' }]]
    : 'html',
  use: {
    baseURL: process.env.ARCADEDB_BASE_URL || 'http://localhost:2480',
    trace: 'on-first-retry',
    headless: true,
    screenshot: 'only-on-failure',
    video: 'retain-on-failure',
  },

  projects: [
    {
      name: 'chromium',
      use: {
        ...devices['Desktop Chrome'],
        // GitHub Actions runners give Chromium a small /dev/shm; when a rendering-heavy page (the
        // graph canvas in particular) runs out of shared memory, the tab crashes or its renderer never
        // finishes painting, which shows up as every visibility assertion timing out with no JS error
        // and no server-side symptom - the failure is in the browser process, not the page or the app.
        // Chromium falls back to disk-backed shared memory with this flag; it is Google's own documented
        // workaround for containerized CI (https://developer.chrome.com/blog/chrome-docker) and costs
        // nothing outside that failure mode.
        launchOptions: {
          args: ['--disable-dev-shm-usage'],
        },
      },
    },
  ],
});
