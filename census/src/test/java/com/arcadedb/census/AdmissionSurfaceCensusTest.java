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
package com.arcadedb.census;

import org.junit.jupiter.api.Test;

import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.TreeSet;

import static java.util.Map.entry;
import static org.assertj.core.api.Assertions.assertThat;

/**
 * Every class that admits requests through the query admission gate is a surface, and two things hold of every surface:
 * <ul>
 *   <li>it has a conformance test built on {@code AdmissionSlotConformance} (#10235), so a surface added later - or one that
 *   moves its admission to another class - fails here until its test exists, instead of shipping a statement whose release
 *   of the slot before a wait silently gives nothing back (#10140);</li>
 *   <li>it opens a running-query entry (#9774): the gate's definition of "a request that does work" and the running-query
 *   registry's ({@code list queries}, {@code terminate query}, {@code SHOW TRANSACTIONS}) are the same set by design, but
 *   the two were wired by hand at every protocol, and each protocol that took a slot without opening an entry had to be
 *   found on its own: work an administrator could neither see nor stop, on a server refusing requests because of it.</li>
 * </ul>
 * The census ranges over the authority (#11288): a surface is a class with a call of {@code QueryAdmissionGate.admit}, or a
 * method reference to it, in the compiled classes of every module ({@link CompiledClasses}); it opens an entry when one of
 * its methods reaches {@code RunningQueryRegistry.register} or {@code open}, directly or through a helper, inherited ones included. The two
 * source-text censuses it replaces counted a file as a surface when its text named the gate and called {@code .admit(},
 * and as registering when its text called a method named {@code registerRunningQuery} or {@code getRunningQueries}, which
 * a helper that registered nothing satisfied. A surface is a top-level class: its nested classes and lambdas are its own.
 * <p>
 * The conformance tests are named files read as text, which is what they are here: the test classes of other modules are
 * not on this module's classpath, and the check is that a known file calls the check, not a census of a population.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class AdmissionSurfaceCensusTest {
  private static final String GATE     = "com/arcadedb/query/sql/executor/QueryAdmissionGate";
  private static final String ADMIT    = "admit";
  private static final String REGISTRY = "com/arcadedb/query/RunningQueryRegistry";
  /** Both open an entry: {@code register} publishes it on the calling thread, {@code open} for work bound elsewhere (#9689). */
  private static final Set<String> OPENS_AN_ENTRY = Set.of("register", "open");

  /**
   * Each surface, as its top-level class keyed as {@link CompiledClasses} keys it, and the conformance test that drives it.
   * The census checks that the test calls the check, not which entry points of the surface it drives: the test of each
   * surface lists those.
   */
  private static final Map<String, String> SURFACES = new TreeMap<>(Map.ofEntries(//
      entry("server/http/handler/DatabaseAbstractHandler",
          "server/src/test/java/com/arcadedb/server/http/handler/AdmissionSlotConformanceHttpTest.java"),
      entry("server/http/handler/PostBatchHandler",
          "server/src/test/java/com/arcadedb/server/http/handler/AdmissionSlotConformanceHttpTest.java"),
      entry("bolt/BoltNetworkExecutor", "bolt/src/test/java/com/arcadedb/bolt/AdmissionSlotConformanceBoltIT.java"),
      entry("server/gremlin/ArcadeGraphManager",
          "gremlin/src/test/java/com/arcadedb/server/gremlin/AdmissionSlotConformanceGremlinTest.java"),
      entry("server/grpc/GrpcAdmissionInterceptor", "grpcw/src/test/java/com/arcadedb/server/grpc/AdmissionSlotConformanceGrpcIT.java"),
      entry("mcp/MCPDispatcher", "mcp/src/test/java/com/arcadedb/mcp/AdmissionSlotConformanceMcpIT.java"),
      entry("mongo/MongoDBBackend", "mongodbw/src/test/java/com/arcadedb/mongo/AdmissionSlotConformanceMongoTest.java"),
      entry("postgres/PostgresNetworkExecutor", "postgresw/src/test/java/com/arcadedb/postgres/AdmissionSlotConformancePostgresIT.java"),
      entry("redis/RedisNetworkExecutor", "redisw/src/test/java/com/arcadedb/redis/AdmissionSlotConformanceRedisTest.java")));

  @Test
  void everyClassThatAdmitsThroughTheGateHasAConformanceTest() throws Exception {
    final Set<String> surfaces = surfacesOf(CompiledClasses.ofEveryModule());

    assertThat(surfaces).as("""
        the classes that admit requests through the QueryAdmissionGate. A new one is a new surface: give it a conformance \
        test built on AdmissionSlotConformance.assertGivesItsSlotBack(), then list both in SURFACES""")
        .containsExactlyInAnyOrderElementsOf(SURFACES.keySet());

    for (final Map.Entry<String, String> surface : SURFACES.entrySet()) {
      final Path test = CompiledClasses.REACTOR_ROOT.resolve(surface.getValue());
      assertThat(test).as("the conformance test of " + surface.getKey()).isRegularFile();
      assertThat(Files.readString(test, StandardCharsets.UTF_8)).as(surface.getValue())
          .contains("AdmissionSlotConformance.assertGivesItsSlotBack(");
    }
  }

  @Test
  void everyClassTakingAnAdmissionSlotOpensARunningQueryEntry() throws Exception {
    final Map<String, byte[]> classes = CompiledClasses.ofEveryModule();
    final Set<String> surfaces = surfacesOf(classes);
    assertThat(surfaces).as("the census must reach the protocol modules").contains("mcp/MCPDispatcher");

    assertThat(surfaces).as("classes that take an admission slot but are not listed by 'list queries': open a "
        + "RunningQueryRegistry entry for the admitted work").isSubsetOf(registeringClassesOf(classes));
  }

  /**
   * The control: a surface that admits and registers through an inherited helper passes, one that admits and calls a helper
   * merely NAMED like the registration does not, and an {@code admit} on another class makes no surface.
   */
  @Test
  void theCensusTellsARegistrationFromItsName() throws Exception {
    final Map<String, byte[]> classes = new TreeMap<>(CompiledClasses.ofEveryModule());
    classes.putAll(CompiledClasses.ofTheProbes());

    final Set<String> surfaces = surfacesOf(classes);
    assertThat(surfaces).contains("census/AdmitsAndRegistersProbe", "census/AdmitsAndRegistersNothingProbe")
        .doesNotContain("census/AdmitsOnAnotherGateProbe");

    final Set<String> registering = registeringClassesOf(classes);
    assertThat(registering).contains("census/AdmitsAndRegistersProbe")
        .doesNotContain("census/AdmitsAndRegistersNothingProbe");
  }

  /** The top-level classes with a call of the gate's {@code admit}. */
  private static Set<String> surfacesOf(final Map<String, byte[]> classes) {
    final Set<String> surfaces = new TreeSet<>();
    for (final String site : CompiledClasses.callSitesOf(classes, (owner, name, desc) -> GATE.equals(owner) && ADMIT.equals(name))
        .keySet())
      surfaces.add(topLevelOf(site));
    surfaces.remove(topLevelOf(CompiledClasses.keyOf(GATE + ".class")));
    return surfaces;
  }

  /** The top-level classes with a method that reaches the registry's {@code register}. */
  private static Set<String> registeringClassesOf(final Map<String, byte[]> classes) {
    final Set<String> classesReaching = new TreeSet<>();
    for (final String method : CompiledClasses.methodsReaching(classes,
        (owner, name, desc) -> REGISTRY.equals(owner) && OPENS_AN_ENTRY.contains(name)))
      classesReaching.add(topLevelOf(CompiledClasses.keyOf(method.substring(0, method.indexOf('.')) + ".class")));
    return classesReaching;
  }

  /** {@code a/C$Nested#m} -> {@code a/C}. */
  private static String topLevelOf(final String key) {
    final int hash = key.indexOf('#');
    final String className = hash < 0 ? key : key.substring(0, hash);
    final int nested = className.indexOf('$');
    return nested < 0 ? className : className.substring(0, nested);
  }
}
