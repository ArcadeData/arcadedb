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

import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.wait.strategy.Wait;
import org.testcontainers.containers.wait.strategy.WaitAllStrategy;
import org.testcontainers.containers.wait.strategy.WaitStrategyTarget;

import java.time.*;

public abstract class ArcadeContainerTemplate {
  static final GenericContainer ARCADE;

  static {
    ARCADE = new GenericContainer("arcadedata/arcadedb:latest")
        .withExposedPorts(2480, 6379, 5432, 8182)
        .withStartupTimeout(Duration.ofSeconds(90))
        .withEnv("JAVA_OPTS", "-Darcadedb.server.rootPassword=playwithdata "
            + "-Darcadedb.server.defaultDatabases=beer[root]{import:https://github.com/ArcadeData/arcadedb-datasets/raw/main/orientdb/OpenBeer.gz} "
            + "-Darcadedb.server.plugins=Postgres:com.arcadedb.postgres.PostgresProtocolPlugin,GremlinServer:com.arcadedb.server.gremlin.GremlinServerPlugin")
        .waitingFor(Wait.forHttp("/api/v1/ready").forPort(2480).forStatusCode(204));
    ARCADE.start();
  }

  protected String host        = ARCADE.getHost();
  protected int    httpPort    = ARCADE.getMappedPort(2480);
  protected int    redisPort   = ARCADE.getMappedPort(6379);
  protected int    pgsqlPort   = ARCADE.getMappedPort(5432);
  protected int    gremlinPort = ARCADE.getMappedPort(8182);
}
