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
import com.arcadedb.remote.grpc.RemoteGrpcDatabase;
import com.arcadedb.remote.grpc.RemoteGrpcServer;
import com.arcadedb.utility.CollectionUtils;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

class RemoteGrpcDatabaseTest extends ArcadeContainerTemplate {

  private RemoteGrpcDatabase database;

  @BeforeEach
  void setUp() {

    RemoteGrpcServer server = new RemoteGrpcServer(host, grpcPort, "root", "playwithdata", true, List.of());
    database = new RemoteGrpcDatabase(server, host, grpcPort, httpPort, "beer", "root", "playwithdata");
    // ENLARGE THE TIMEOUT TO PASS THESE TESTS ON CI (GITHUB ACTIONS)
    database.setTimeout(60_000);
  }

  @AfterEach
  void tearDown() {
    if (database != null)
      database.close();
  }

  @Test
  void simpleSQLQuery() {
    final ResultSet result = database.query("SQL", "select * from Beer limit 10");
    assertThat(CollectionUtils.countEntries(result)).isEqualTo(10);
  }

  @Test
  @Disabled("Gremlin not supported yet")
  void simpleGremlinQuery() {
    final ResultSet result = database.query("gremlin", "g.V().limit(10)");
    assertThat(CollectionUtils.countEntries(result)).isEqualTo(10);
  }

  @Test
  @Disabled("Cypher not supported yet")
  void simpleCypherQuery() {
    final ResultSet result = database.query("cypher", "MATCH(p:Beer) RETURN * LIMIT 10");
    assertThat(CollectionUtils.countEntries(result)).isEqualTo(10);
  }
}
