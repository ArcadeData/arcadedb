/*
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
 * SPDX-FileCopyrightText: 2021-present Arcade Data Ltd (info@arcadedata.com)
 * SPDX-License-Identifier: Apache-2.0
 */
package com.arcadedb.server.ha;

import com.arcadedb.GlobalConfiguration;
import com.arcadedb.exception.ConfigurationException;
import com.arcadedb.server.BaseGraphServerTest;
import com.arcadedb.server.ServerException;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.fail;

class HAConfigurationIT extends BaseGraphServerTest {
  protected int getServerCount() {
    return 3;
  }

  @Override
  protected String getServerAddresses() {
    return "192.168.0.1:2424,192.168.0.1:2425,localhost:2424";
  }

  @BeforeEach
  @Override
  public void beginTest() {
    // Don't run automatic setup - the test method controls startup manually
    setTestConfiguration();
  }

  @AfterEach
  @Override
  public void endTest() {
    GlobalConfiguration.resetAll();
  }

  @Test
  void replication() {
    try {
      deleteDatabaseFolders();
      prepareDatabase();
      startServers();
      fail("Expected exception for invalid server list");
    } catch (final ServerException | ConfigurationException e) {
      assertThat(e.getMessage()).containsAnyOf("Cannot find local server", "Found a localhost");
    }
  }

  // Expose prepareDatabase for this test
  private void prepareDatabase() {
    // Minimal: just set up config, don't create databases
  }
}
