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

import { chromium, FullConfig } from '@playwright/test';
import { GenericContainer, StartedTestContainer, Wait } from 'testcontainers';

let arcadedbContainer: StartedTestContainer | null = null;

async function globalSetup(config: FullConfig) {
  // Use environment variable to specify image, default to 'latest' for CI/CD
  // For local testing with unreleased changes, set ARCADEDB_DOCKER_IMAGE=arcadedata/arcadedb:26.2.1-SNAPSHOT
  const dockerImage = process.env.ARCADEDB_DOCKER_IMAGE || 'arcadedata/arcadedb:latest';
  console.log(`🚀 Starting ArcadeDB container for e2e tests using image: ${dockerImage}`);

  try {
    arcadedbContainer = await new GenericContainer(dockerImage)
      .withExposedPorts(2480)
      .withEnvironment({
        JAVA_OPTS: '-Darcadedb.server.rootPassword=playwithdata -Darcadedb.server.defaultDatabases=Beer[root]{import:https://github.com/ArcadeData/arcadedb-datasets/raw/main/orientdb/OpenBeer.gz}'
      })
      .withStartupTimeout(180000)
      .withWaitStrategy(Wait.forHttp("/api/v1/ready", 2480).forStatusCodeMatching((statusCode) => statusCode === 204))
      .start();

    const host = arcadedbContainer.getHost();
    const port = arcadedbContainer.getMappedPort(2480);
    const baseURL = `http://${host}:${port}`;

    console.log(`✅ ArcadeDB container started at ${baseURL}`);

    // Store container info for tests and teardown
    global.arcadedbContainer = arcadedbContainer;
    global.arcadedbBaseURL = baseURL;

    // Set environment variable for Playwright baseURL
    process.env.ARCADEDB_BASE_URL = baseURL;

    // Verify Studio is accessible
    const browser = await chromium.launch();
    const page = await browser.newPage();

    try {
      await page.goto(baseURL, { timeout: 10000 });
      console.log('✅ ArcadeDB Studio is accessible');
    } catch (error) {
      console.error('❌ Failed to access ArcadeDB Studio:', error);
      throw error;
    } finally {
      await browser.close();
    }

    console.log('🚀 Global setup completed successfully');
  } catch (error) {
    console.error('❌ Failed to start ArcadeDB container:', error);
    if (arcadedbContainer) {
      await arcadedbContainer.stop();
    }
    throw error;
  }
}

export default globalSetup;
