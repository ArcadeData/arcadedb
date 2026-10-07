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

import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.TreeSet;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Stream;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Source-level guard for issue #9464: no test file may start mocking one of the concrete server/HA types with Mockito
 * (any mock maker) unless it is listed in {@code mocks-of-server-types-allowlist.txt}.
 * <p>
 * Mocks of these classes test how the code reached a result rather than the result, and the inline ones trigger the
 * GraalVM JIT problem behind #8021 and #8867. The replacement is a real server or database ({@code BaseGraphServerTest},
 * {@code TestHelper}) or a hand-written fake behind a narrow interface (see {@code FakeLeader}).
 * <p>
 * The allow-list only shrinks: a file that no longer mocks these types must be removed from it (the second test fails
 * otherwise), so every migrated file is locked in. Like {@link InlineMocksOfJitWarmTypesTest} this is a regular
 * expression approximation, not a parser: a mock built through a helper that receives the class as a variable is out of
 * its reach.
 */
class NoNewMocksOfServerTypesTest {

  private static final Path   REACTOR_ROOT             = Path.of("..");
  private static final String ALLOWLIST                = "mocks-of-server-types-allowlist.txt";
  private static final int    EXPECTED_MINIMUM_SOURCES = 1000;

  private static final List<String> SKIPPED_TOP_LEVEL = List.of(".worktrees", "e2e", "load-tests", "node_modules");

  private static final String GUARDED_TYPES =
      "ArcadeDBServer|RaftHAServer|HttpServer|ServerSecurity|ServerSecurityUser|RaftTransactionBroker|ArcadeStateMachine|ServerDatabase|"
          + "RaftHAPlugin|HttpAuthSession|HttpAuthSessionManager|ClusterMonitor";

  /** A guarded type by its simple or fully qualified name; group 1 is the simple name. */
  private static final String GUARDED_TYPE = "(?:[a-z_]\\w*\\.)*(" + GUARDED_TYPES + ")";

  /**
   * {@code mock(X.class...)}, {@code Mockito.mock(X.class...)}, {@code SubclassMocks.mock(X.class...)} (the {@code \\b}
   * before {@code mock} matches right after the dot), same for spy, mockStatic and mockConstruction. Group 1 is the simple type name.
   */
  private static final Pattern CALL = Pattern.compile("\\b(?:mock|spy|mockStatic|mockConstruction)\\(\\s*" + GUARDED_TYPE + "\\.class\\b");

  /** {@code spy(new X(...))}. Group 1 is the simple type name. */
  private static final Pattern SPY_NEW = Pattern.compile("\\bspy\\(\\s*new\\s+" + GUARDED_TYPE + "\\s*\\(");

  /** {@code @Mock X x;}. Group 1 is the simple type name. */
  private static final Pattern ANNOTATED = Pattern.compile("@(?:Mock|Spy)\\b[^;=]*?\\b(" + GUARDED_TYPES + ")\\s+\\w+\\s*[;=]");

  /** Read once: every test of this class scans the same tree. */
  private static Map<String, String> sources;

  @BeforeAll
  static void readSources() throws IOException {
    sources = readTestSources();
  }

  @Test
  void noNewTestMocksAServerType() throws IOException {
    assertThat(sources).as("the scan must find the test sources of every module").hasSizeGreaterThan(EXPECTED_MINIMUM_SOURCES);

    final Map<String, Set<String>> allowed = readAllowList();
    final List<String> added = new ArrayList<>();
    for (final Map.Entry<String, Set<String>> entry : mockedTypes(sources).entrySet())
      for (final String type : entry.getValue())
        if (!allowed.getOrDefault(entry.getKey(), Set.of()).contains(type))
          added.add(entry.getKey() + " mocks " + type);

    assertThat(added)
        .as("these files mock a concrete server/HA type. Use a real server/database or a hand-written fake instead "
            + "(issue #9464); do not add them to " + ALLOWLIST)
        .isEmpty();
  }

  @Test
  void theAllowListOnlyShrinks() throws IOException {
    final Map<String, Set<String>> mocked = mockedTypes(sources);
    final List<String> stale = new ArrayList<>();
    for (final Map.Entry<String, Set<String>> entry : readAllowList().entrySet())
      for (final String type : entry.getValue())
        if (!mocked.getOrDefault(entry.getKey(), Set.of()).contains(type))
          stale.add(entry.getKey() + " " + type);

    assertThat(stale)
        .as("these files no longer mock the listed server/HA type (or were deleted): remove the type, or the line, from "
            + ALLOWLIST + " so the migration stays locked in")
        .isEmpty();
  }

  @Test
  void theScanCatchesEveryShapeItGuards() {
    final Map<String, String> samples = new TreeMap<>();
    samples.put("a/Bare.java", "class Bare { void t() { ArcadeDBServer s = mock(ArcadeDBServer.class); } }");
    samples.put("a/Qualified.java", "class Qualified { void t() { Object s = Mockito.mock(RaftHAServer.class, RETURNS_DEEP_STUBS); } }");
    samples.put("a/Subclass.java", "class Subclass { void t() { Object s = SubclassMocks.mock(HttpServer.class); } }");
    samples.put("a/FullyQualified.java", "class FullyQualified { void t() { Object s = mock(com.arcadedb.server.security.ServerSecurity.class); } }");
    samples.put("a/SpyNew.java", "class SpyNew { void t() { Object s = spy(new ClusterMonitor(1)); } }");
    samples.put("a/Annotated.java", "class Annotated { @Mock private RaftTransactionBroker broker; }");
    samples.put("a/Two.java", "class Two { void t() { mock(ServerSecurityUser.class); Mockito.spy(ServerDatabase.class); } }");
    samples.put("a/Static.java", "class Static { void t() { try (var m = mockStatic(HttpAuthSessionManager.class)) { } } }");
    samples.put("a/Construction.java", "class Construction { void t() { try (var m = Mockito.mockConstruction(ArcadeStateMachine.class)) { } } }");
    samples.put("b/Unguarded.java", "class Unguarded { void t() { Object s = mock(HttpServerFactory.class); Object d = mock(Database.class); } }");
    samples.put("b/Real.java", "class Real { void t() { ArcadeDBServer s = new ArcadeDBServer(config); } }");

    assertThat(mockedTypes(samples)).isEqualTo(Map.of(
        "a/Annotated.java", Set.of("RaftTransactionBroker"),
        "a/Bare.java", Set.of("ArcadeDBServer"),
        "a/Construction.java", Set.of("ArcadeStateMachine"),
        "a/Static.java", Set.of("HttpAuthSessionManager"),
        "a/FullyQualified.java", Set.of("ServerSecurity"),
        "a/Qualified.java", Set.of("RaftHAServer"),
        "a/SpyNew.java", Set.of("ClusterMonitor"),
        "a/Subclass.java", Set.of("HttpServer"),
        "a/Two.java", Set.of("ServerSecurityUser", "ServerDatabase")));
  }

  /** File to the simple names of the guarded types it mocks or spies; files that mock none are absent. */
  static Map<String, Set<String>> mockedTypes(final Map<String, String> sources) {
    final Map<String, Set<String>> result = new TreeMap<>();
    for (final Map.Entry<String, String> entry : sources.entrySet()) {
      final Set<String> types = new TreeSet<>();
      for (final Pattern pattern : List.of(CALL, SPY_NEW, ANNOTATED)) {
        final Matcher matcher = pattern.matcher(entry.getValue());
        while (matcher.find())
          types.add(matcher.group(1));
      }
      if (!types.isEmpty())
        result.put(entry.getKey(), types);
    }
    return result;
  }

  /** Lines are {@code <path> <Type>[,<Type>...]}; blank lines and {@code #} comments are skipped. */
  private static Map<String, Set<String>> readAllowList() throws IOException {
    final Map<String, Set<String>> allowed = new TreeMap<>();
    try (final InputStream in = NoNewMocksOfServerTypesTest.class.getResourceAsStream("/" + ALLOWLIST)) {
      assertThat(in).as("the allow-list resource " + ALLOWLIST).isNotNull();
      for (final String line : new String(in.readAllBytes(), StandardCharsets.UTF_8).split("\n")) {
        final String trimmed = line.trim();
        if (trimmed.isEmpty() || trimmed.startsWith("#"))
          continue;
        final String[] parts = trimmed.split("\\s+", 2);
        assertThat(parts).as("allow-list line '%s' must be '<path> <Type>[,<Type>...]'", trimmed).hasSize(2);
        allowed.computeIfAbsent(parts[0], k -> new TreeSet<>()).addAll(List.of(parts[1].split(",")));
      }
    }
    return allowed;
  }

  private static Map<String, String> readTestSources() throws IOException {
    assertThat(REACTOR_ROOT.resolve("server").resolve("pom.xml")).as("the scan must start at the reactor root").isRegularFile();
    final Map<String, String> sources = new TreeMap<>();
    try (final Stream<Path> modules = Files.list(REACTOR_ROOT)) {
      for (final Path module : modules.filter(Files::isDirectory).toList()) {
        final String moduleName = module.getFileName().toString();
        if (SKIPPED_TOP_LEVEL.stream().anyMatch(moduleName::startsWith))
          continue;
        final Path testSources = module.resolve("src").resolve("test").resolve("java");
        if (!Files.isDirectory(testSources))
          continue;
        try (final Stream<Path> files = Files.walk(testSources)) {
          final List<Path> javaFiles = new ArrayList<>(files.filter(p -> p.toString().endsWith(".java"))
              .filter(p -> !p.getFileName().toString().equals(NoNewMocksOfServerTypesTest.class.getSimpleName() + ".java")).toList());
          for (final Path file : javaFiles)
            sources.put(REACTOR_ROOT.relativize(file).toString().replace('\\', '/'), Files.readString(file, StandardCharsets.UTF_8));
        }
      }
    }
    return sources;
  }
}
