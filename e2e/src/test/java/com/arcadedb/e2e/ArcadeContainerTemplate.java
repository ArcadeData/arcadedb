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

import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.api.extension.ExtensionContext;
import org.junit.jupiter.api.extension.TestWatcher;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.wait.strategy.Wait;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assumptions.assumeFalse;

/**
 * Starts one ArcadeDB container for the whole e2e battery. The image is a parameter, so the same tests run against the JVM
 * image and the GraalVM native one:
 * <ul>
 *   <li>{@code -Darcadedb.test.image=<name:tag>} (the property {@code load-tests} and {@code e2e-ha} already read), else</li>
 *   <li>the {@code ARCADEDB_DOCKER_IMAGE} environment variable (the convention of CI and the other e2e suites), else</li>
 *   <li>{@code arcadedata/arcadedb:latest}.</li>
 * </ul>
 * The server is configured through environment variables named after the settings ({@code arcadedb.server.rootPassword}
 * ...), which {@code GlobalConfiguration} reads in both images. {@code JAVA_OPTS} would not do: the native image has no
 * shell script to expand it, its entrypoint is the binary itself.
 */
@ExtendWith(ArcadeContainerTemplate.ContainerLogOnFailure.class)
public abstract class ArcadeContainerTemplate {
  static final String             IMAGE  = System.getProperty("arcadedb.test.image",
      System.getenv().getOrDefault("ARCADEDB_DOCKER_IMAGE", "arcadedata/arcadedb:latest"));
  /**
   * Whether the image runs the native binary: {@code -Darcadedb.test.native=true|false}, else guessed from the tag
   * ({@code latest-native}, {@code <version>-native-<arch>}). The native image does not bundle Gremlin.
   */
  static final boolean            NATIVE = Boolean.parseBoolean(
      System.getProperty("arcadedb.test.native", String.valueOf(IMAGE.contains("-native"))));
  static final GenericContainer<?> ARCADE;

  static {
    final List<Integer> ports = new ArrayList<>(List.of(2480, 6379, 5432, 7687, 27017, 50051));
    final StringBuilder plugins = new StringBuilder(
        "PostgresProtocolPlugin,GrpcServerPlugin,BoltProtocolPlugin,PrometheusMetricsPlugin,RedisProtocolPlugin,MongoDBProtocolPlugin");
    if (!NATIVE) {
      ports.add(8182);
      plugins.append(",GremlinServerPlugin");
    }

    ARCADE = new GenericContainer<>(IMAGE)
        .withExposedPorts(ports.toArray(new Integer[0]))
        .withStartupTimeout(Duration.ofSeconds(90))
        .withEnv(Map.of(
            "arcadedb.server.rootPassword", "playwithdata",
            "arcadedb.postgres.debug", "false",
            "arcadedb.grpc.enabled", "true",
            "arcadedb.grpc.port", "50051",
            "arcadedb.grpc.mode", "standard",
            "arcadedb.grpc.reflection.enabled", "true",
            "arcadedb.grpc.health.enabled", "true",
            "arcadedb.server.defaultDatabases",
            "beer[root]{import:https://github.com/ArcadeData/arcadedb-datasets/raw/main/orientdb/OpenBeer.gz}",
            "arcadedb.server.plugins", plugins.toString()))
        .waitingFor(Wait.forHttp("/api/v1/ready").forPort(2480).forStatusCode(204));
    System.out.println("ArcadeDB e2e image: " + IMAGE + (NATIVE ? " (native)" : ""));
    ARCADE.start();
  }

  protected String host      = ARCADE.getHost();
  protected int    httpPort  = ARCADE.getMappedPort(2480);
  protected int    redisPort = ARCADE.getMappedPort(6379);
  protected int    pgsqlPort = ARCADE.getMappedPort(5432);
  protected int    mongoPort = ARCADE.getMappedPort(27017);
  protected int    grpcPort  = ARCADE.getMappedPort(50051);
  protected int    boltPort  = ARCADE.getMappedPort(7687);

  /** Gremlin is bundled in the JVM image only. */
  protected static void assumeGremlin() {
    assumeFalse(NATIVE, "the native image does not bundle Gremlin");
  }

  /**
   * Dumps the ArcadeDB container's server log when a test fails. Wire-protocol drivers surface only a
   * generic message (e.g. "Error executing Cypher command"), so the server-side cause is otherwise
   * invisible in CI output. Best-effort: never fails the test itself.
   */
  static class ContainerLogOnFailure implements TestWatcher {
    @Override
    public void testFailed(final ExtensionContext context, final Throwable cause) {
      try {
        System.out.println("===== ArcadeDB container log (failed: " + context.getDisplayName() + ") =====");
        System.out.println(ARCADE.getLogs());
        System.out.println("===== end ArcadeDB container log =====");
      } catch (final RuntimeException ignored) {
        // best-effort diagnostic only
      }
    }
  }
}
