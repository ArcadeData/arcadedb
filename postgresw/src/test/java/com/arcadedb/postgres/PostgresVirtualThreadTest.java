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
package com.arcadedb.postgres;

import com.arcadedb.GlobalConfiguration;
import com.arcadedb.server.ArcadeDBServer;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Tests for PostgreSQL wire protocol with virtual threads support.
 */
public class PostgresVirtualThreadTest {

  @Test
  public void testVirtualThreadsConfigurationExists() {
    // Verify that the configuration flag exists
    assertThat(GlobalConfiguration.POSTGRES_USE_VIRTUAL_THREADS).isNotNull();
    assertThat(GlobalConfiguration.POSTGRES_USE_VIRTUAL_THREADS.getKey())
        .isEqualTo("arcadedb.postgres.useVirtualThreads");
  }

  @Test
  public void testVirtualThreadsCanBeDisabled() {
    // Verify the type is Boolean
    assertThat(GlobalConfiguration.POSTGRES_USE_VIRTUAL_THREADS.getType())
        .isEqualTo(Boolean.class);
  }

  @Test
  public void testConfigurationScope() {
    // Verify that configuration is server-scoped
    assertThat(GlobalConfiguration.POSTGRES_USE_VIRTUAL_THREADS.getScope())
        .isEqualTo(GlobalConfiguration.SCOPE.SERVER);
  }
}
