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
package com.arcadedb.server.gremlin;

import com.arcadedb.database.Database;
import com.arcadedb.database.DatabaseFactory;
import com.arcadedb.gremlin.ArcadeGraph;
import com.arcadedb.gremlin.ArcadeGraphFactory;
import com.arcadedb.remote.RemoteServer;
import com.arcadedb.server.BaseGraphServerTest;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;

import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.fail;

public class LocalGremlinFactoryIT {
  private static final String DATABASE_NAME = "local-database-factory";

  @Test
  public void okPoolRelease() {
    try (ArcadeGraphFactory pool = ArcadeGraphFactory.withLocal(DATABASE_NAME)) {
      for (int i = 0; i < 1_000; i++) {
        final ArcadeGraph instance = pool.get();
        assertThat(instance).isNotNull();
        instance.close();
      }

      assertThat(pool.getTotalInstancesCreated()).isEqualTo(1);
    }
  }

  @Test
  public void errorPoolRelease() {
    try (ArcadeGraphFactory pool = ArcadeGraphFactory.withLocal(DATABASE_NAME)) {
      for (int i = 0; i < pool.getMaxInstances(); i++) {
        final ArcadeGraph instance = pool.get();
        assertThat(instance).isNotNull();
      }

      try {
        pool.get();
        fail("");
      } catch (IllegalArgumentException e) {
        // EXPECTED
      }

      assertThat(pool.getTotalInstancesCreated()).isEqualTo(pool.getMaxInstances());
    }
  }

  @BeforeAll
  public static void beginTest() {
    try (DatabaseFactory factory = new DatabaseFactory(DATABASE_NAME)) {
      if (!factory.exists())
        factory.create().close();
    }
  }

  @AfterAll
  public static void endTest() {
    DatabaseFactory.getActiveDatabaseInstances().stream().map(d -> {
      d.drop();
      return null;
    });
  }
}
