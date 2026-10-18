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
package com.arcadedb.server;

import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Map;
import java.util.TreeSet;
import java.util.regex.Pattern;
import java.util.stream.Stream;

import static java.util.Map.entry;
import static org.assertj.core.api.Assertions.assertThat;

/**
 * Issue #10235: every class of the main sources that admits requests through the query admission gate is a surface, and
 * every surface has a conformance test built on {@link AdmissionSlotConformance}. A surface added later - or one that moves
 * its admission to another class - fails here until its test exists, instead of shipping a statement whose release of the
 * slot before a wait silently gives nothing back (#10140).
 * <p>
 * The census is textual, so it guards against the likely mistake and no more: a surface is a class of the main sources
 * that names {@code QueryAdmissionGate} and calls {@code .admit(}, and its test passes when it calls the check. Which entry
 * points of a surface are driven is the per-surface test's business: each one lists its own.
 */
class AdmissionSurfaceCensusTest {
  /**
   * The reactor root: the parent of the {@code server} module. Surefire sets {@code basedir} to the module; a runner that
   * does not (an IDE) starts in the module directory too. Either way the assertion on {@link #GATE_CLASS} fails loudly if not.
   */
  private static final Path    REACTOR_ROOT = Path.of(System.getProperty("basedir", System.getProperty("user.dir"))).resolve("..")
      .normalize();
  private static final String  GATE_CLASS   = "engine/src/main/java/com/arcadedb/query/sql/executor/QueryAdmissionGate.java";
  // A CALL TO admit(), IN A CLASS THAT USES THE GATE: WHATEVER THE NAME OF THE VARIABLE THAT HOLDS IT. THE OTHER admit() OF
  // THE CODE BASE (VECTOR GROUPS, PATH EXPANSION) LIVE IN CLASSES THAT NEVER NAME THE GATE
  private static final Pattern ADMIT        = Pattern.compile("\\.admit\\(");

  /**
   * Each surface, and the conformance test that drives it. The census checks that the test calls the check, not which entry
   * points of the surface it drives: the test of each surface lists those.
   */
  private static final Map<String, String> SURFACES = Map.ofEntries(//
      entry("server/src/main/java/com/arcadedb/server/http/handler/DatabaseAbstractHandler.java",
          "server/src/test/java/com/arcadedb/server/http/handler/AdmissionSlotConformanceHttpTest.java"),
      entry("server/src/main/java/com/arcadedb/server/http/handler/PostBatchHandler.java",
          "server/src/test/java/com/arcadedb/server/http/handler/AdmissionSlotConformanceHttpTest.java"),
      entry("bolt/src/main/java/com/arcadedb/bolt/BoltNetworkExecutor.java",
          "bolt/src/test/java/com/arcadedb/bolt/AdmissionSlotConformanceBoltIT.java"),
      entry("gremlin/src/main/java/com/arcadedb/server/gremlin/ArcadeGraphManager.java",
          "gremlin/src/test/java/com/arcadedb/server/gremlin/AdmissionSlotConformanceGremlinTest.java"),
      entry("grpcw/src/main/java/com/arcadedb/server/grpc/GrpcAdmissionInterceptor.java",
          "grpcw/src/test/java/com/arcadedb/server/grpc/AdmissionSlotConformanceGrpcIT.java"),
      entry("mcp/src/main/java/com/arcadedb/mcp/MCPDispatcher.java",
          "mcp/src/test/java/com/arcadedb/mcp/AdmissionSlotConformanceMcpIT.java"),
      entry("mongodbw/src/main/java/com/arcadedb/mongo/MongoDBBackend.java",
          "mongodbw/src/test/java/com/arcadedb/mongo/AdmissionSlotConformanceMongoTest.java"),
      entry("postgresw/src/main/java/com/arcadedb/postgres/PostgresNetworkExecutor.java",
          "postgresw/src/test/java/com/arcadedb/postgres/AdmissionSlotConformancePostgresIT.java"),
      entry("redisw/src/main/java/com/arcadedb/redis/RedisNetworkExecutor.java",
          "redisw/src/test/java/com/arcadedb/redis/AdmissionSlotConformanceRedisTest.java"));

  @Test
  void everyClassThatAdmitsThroughTheGateHasAConformanceTest() throws IOException {
    assertThat(REACTOR_ROOT.resolve(GATE_CLASS)).as("the scan must start at the reactor root, or it checks nothing").isRegularFile();

    final TreeSet<String> admitting = new TreeSet<>();
    try (final Stream<Path> modules = Files.list(REACTOR_ROOT)) {
      for (final Path module : modules.filter(Files::isDirectory).toList()) {
        final Path mainSources = module.resolve("src").resolve("main").resolve("java");
        if (!Files.isDirectory(mainSources))
          continue;
        try (final Stream<Path> files = Files.walk(mainSources)) {
          for (final Path file : files.filter(p -> p.toString().endsWith(".java")).toList()) {
            final String path = REACTOR_ROOT.relativize(file).toString().replace('\\', '/');
            if (path.equals(GATE_CLASS))
              continue;
            final String source = Files.readString(file, StandardCharsets.UTF_8);
            if (source.contains("QueryAdmissionGate") && ADMIT.matcher(source).find())
              admitting.add(path);
          }
        }
      }
    }

    assertThat(admitting).as("""
        the classes that admit requests through the QueryAdmissionGate. A new one is a new surface: give it a conformance \
        test built on AdmissionSlotConformance.assertGivesItsSlotBack(), then list both in SURFACES""")
        .containsExactlyInAnyOrderElementsOf(SURFACES.keySet());

    for (final Map.Entry<String, String> surface : SURFACES.entrySet()) {
      final Path test = REACTOR_ROOT.resolve(surface.getValue());
      assertThat(test).as("the conformance test of " + surface.getKey()).isRegularFile();
      assertThat(Files.readString(test, StandardCharsets.UTF_8)).as(surface.getValue())
          .contains("AdmissionSlotConformance.assertGivesItsSlotBack(");
    }
  }
}
