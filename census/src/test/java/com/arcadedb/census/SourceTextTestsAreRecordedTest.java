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

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.TreeSet;
import java.util.regex.Pattern;
import java.util.stream.Stream;

import static java.util.Map.entry;
import static org.assertj.core.api.Assertions.assertThat;

/**
 * The census rule of the root {@code CLAUDE.md} (#11199) held the way it asks every other census to be held (#11288): every test
 * that reads {@code .java} source text is listed here with the reason it reads text, and a new one fails this test until it is
 * either a census over compiled classes in this module ({@link CompiledClasses}) or listed with its reason. When the rule shipped
 * nothing enumerated the censuses already in the tree: fifteen claimed to cover "every X" over a regular expression, and fifteen
 * more over the main sources were found by this test when it was written. The ones whose authority is a call, a type, a hierarchy
 * or the data flow of a call ({@link CallOperands}) now live in this module, and what is left reads text for one of the reasons
 * below.
 * <p>
 * This test itself ranges over source text, and says so: what it protects is a convention about test SOURCES - which of them
 * read source text - and the test classes of other modules are on no classpath this module has. A test is taken to read source
 * text when it names the {@code .java} suffix in a string literal and reads a file's content through {@code Files}; a test that
 * spells the suffix some other way passes it. That is the cost, stated rather than left for the reader to notice.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class SourceTextTestsAreRecordedTest {
  /** The {@code .java} suffix in a string literal: a filter, a path, a file name. */
  private static final Pattern JAVA_SUFFIX  = Pattern.compile("\\.java\"");
  /** Reading a file's content. */
  private static final Pattern READS_A_FILE = Pattern.compile("\\bFiles\\s*\\.\\s*(?:readString|readAllLines|readAllBytes|lines|newBufferedReader)\\b");

  /** A test reading one or a few files it names, about what those files say: a check of a named file, not a census. */
  private static final String NAMED_FILES  = "reads the named source files it checks: not a census of a population";
  /** A census over the test sources of every module, which no classpath carries. */
  private static final String TEST_SOURCES = "a census of TEST sources across modules: no module's classpath carries the test classes of the others";
  /** A census whose offender exists in source text only: javac folds it away or it is not code. */
  private static final String SOURCE_ONLY  = "the offender exists only in source text (a literal javac folds, a comment, a convention of spelling)";

  /** Every test that reads {@code .java} text for a reason that holds, by path from the reactor root, with the reason. */
  private static final Map<String, String> RECORDED = new TreeMap<>(Map.ofEntries(//
      entry("census/src/test/java/com/arcadedb/census/SourceTextTestsAreRecordedTest.java",
          "this test: the convention it holds is about test sources, as its javadoc says"),
      entry("census/src/test/java/com/arcadedb/census/CompiledClasses.java",
          "reads no source text: names a module's first source file only to find its class root, and reads pom.xml"),
      // NAMED FILES
      entry("census/src/test/java/com/arcadedb/census/AdmissionSurfaceCensusTest.java",
          NAMED_FILES + ": each surface's conformance test, in another module's test sources"),
      entry("bolt/src/test/java/com/arcadedb/bolt/Issue9870BoltFailureWiringTest.java", NAMED_FILES + ": BoltNetworkExecutor"),
      entry("grpc-client/src/test/java/com/arcadedb/remote/grpc/Issue10154AdminMappingTest.java", NAMED_FILES + ": RemoteGrpcServer"),
      entry("grpcw/src/test/java/com/arcadedb/server/grpc/Issue10158GrpcWrappedRefusalStatusTest.java",
          NAMED_FILES + ": the two gRPC services' status ladders"),
      entry("server/src/test/java/com/arcadedb/server/http/handler/Issue10573OpenApiNativeReflectionTest.java",
          NAMED_FILES + ": OpenApiModelReflectionFeature and the native pom"),
      entry("server/src/test/java/com/arcadedb/server/support/Issue10738FormerKeySupportSourceTest.java",
          NAMED_FILES + ": SupportDiagnostics"),
      // TEST SOURCES
      entry("server/src/test/java/com/arcadedb/server/HardcodedTestServerPortsTest.java", TEST_SOURCES + "; and ports are folded literals"),
      entry("server/src/test/java/com/arcadedb/server/InlineMocksOfJitWarmTypesTest.java", TEST_SOURCES),
      entry("server/src/test/java/com/arcadedb/server/NoNewMocksOfServerTypesTest.java", TEST_SOURCES),
      entry("server/src/test/java/com/arcadedb/server/Issue10533ClientsHonourTheMarksTest.java",
          TEST_SOURCES + "; the production classes are held by ClientsHonourTheMarksCensusTest"),
      entry("server/src/test/java/com/arcadedb/server/PluginPortFixtureScan.java",
          TEST_SOURCES + "; the shared scan of plugin fixtures' ports, folded literals"),
      entry("grpcw/src/test/java/com/arcadedb/server/grpc/Issue7496GrpcHaItsAllocateTheirPortsTest.java",
          TEST_SOURCES + "; and ports are folded literals"),
      entry("ha-raft/src/test/java/com/arcadedb/server/ha/raft/Issue8203RaftFixturePortsTest.java", TEST_SOURCES + "; and ports are folded literals"),
      entry("ha-raft/src/test/java/com/arcadedb/server/ha/raft/Issue8222StandaloneRaftFixturePortsTest.java",
          TEST_SOURCES + "; and ports are folded literals"),
      // SOURCE ONLY
      entry("grpcw/src/test/java/com/arcadedb/server/grpc/Issue7472EveryGrpcFailureIsConcealableTest.java", SOURCE_ONLY
          + ": an inlined static final String, and a throwable's message reaching a status is data flow; its call-site scan is "
          + "GrpcFailureConcealmentDecisionCensusTest"),
      entry("engine/src/test/java/com/arcadedb/serializer/Issue7638DateColumnPrecisionTest.java",
          SOURCE_ONLY + ": a division by an inlined constant, and a variable named timestamp")));

  @Test
  void everyTestThatReadsSourceTextIsRecorded() throws IOException {
    final Set<String> found = testsReadingSourceText();
    assertThat(found).as("the scan must reach the test sources of every module, this test's own included")
        .contains("census/src/test/java/com/arcadedb/census/SourceTextTestsAreRecordedTest.java");

    final Set<String> unrecorded = new TreeSet<>(found);
    unrecorded.removeAll(RECORDED.keySet());
    assertThat(unrecorded).as("a test that reads .java source text: if it is a census whose authority is a call, a type or a "
        + "hierarchy, write it in the census module over CompiledClasses (CLAUDE.md, 'Census tests enumerate from the authority "
        + "they protect'); otherwise list it in RECORDED with the reason it reads text").isEmpty();

    final Set<String> stale = new TreeSet<>(RECORDED.keySet());
    stale.removeAll(found);
    assertThat(stale).as("entries that no longer read source text (converted, or gone): remove them").isEmpty();
  }

  /** The test sources of every module, by path from the reactor root, that read {@code .java} text. */
  private static Set<String> testsReadingSourceText() throws IOException {
    final Set<String> found = new TreeSet<>();
    try (final Stream<Path> modules = Files.list(CompiledClasses.REACTOR_ROOT)) {
      for (final Path module : modules.filter(Files::isDirectory).sorted().toList()) {
        final Path sources = module.resolve("src").resolve("test").resolve("java");
        if (!Files.isDirectory(sources))
          continue;
        try (final Stream<Path> files = Files.walk(sources)) {
          for (final Path file : files.filter(p -> p.toString().endsWith(".java")).toList()) {
            final String text = Files.readString(file, StandardCharsets.UTF_8);
            if (JAVA_SUFFIX.matcher(text).find() && READS_A_FILE.matcher(text).find())
              found.add(CompiledClasses.REACTOR_ROOT.relativize(file).normalize().toString().replace('\\', '/'));
          }
        }
      }
    }
    return found;
  }
}
