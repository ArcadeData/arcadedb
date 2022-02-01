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
package com.arcadedb.e2e;

import com.arcadedb.query.sql.executor.ResultSet;
import com.arcadedb.remote.RemoteDatabase;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

public class RemoteDatabaseQueriesTest extends ArcadeContainerTemplate {
  private RemoteDatabase database;

  @BeforeEach
  void setUp() {
    database = new RemoteDatabase(host, httpPort, "beer", "root", "playwithdata");
    // ENLARGE THE TIMEOUT TO PASS THESE TESTS ON CI (GITHUB ACTIONS)
    database.setTimeout(60_000);
  }

  @AfterEach
  void tearDown() {
    database.close();
  }

  @Test
  void simpleSQLQuery() {
    database.transaction(() -> {
      ResultSet result = database.query("SQL", "select from Beer limit 10");
      assertThat(result.countEntries()).isEqualTo(10);
    });
  }

  @Test
  void simpleGremlinQuery() {
    database.transaction(() -> {
      ResultSet result = database.query("gremlin", "g.V().limit(10)");
      assertThat(result.countEntries()).isEqualTo(10);
    });
  }

  @Test
  void simpleCypherQuery() {
    database.transaction(() -> {
      ResultSet result = database.query("cypher", "MATCH(p:Beer) RETURN * LIMIT 10");
      assertThat(result.countEntries()).isEqualTo(10);
    });
  }
}
