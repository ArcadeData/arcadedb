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
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Stream;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Source-level guard that keeps every module's test servers off hand-picked HTTP ports.
 * <p>
 * The test base classes bind the first free port of a range and report the one they got through
 * {@link StaticBaseServerTest#getServerHttpPort(ArcadeDBServer)} / {@code getServerHttpUrl(...)}. A test that builds
 * {@code "http://127.0.0.1:248" + serverIndex} instead addresses whatever holds that port: when anything already
 * listens on 2480 - an IDE's server, a parallel build in another worktree, a previous run - every server of the
 * fixture shifts up by one and each index talks to its neighbour. The failure reads as a {@code 403} or "Too many
 * failed authentication attempts", never as a port conflict. A listener pinned to one number fails the same way from
 * the other side, as "Address already in use" in whichever test starts next.
 * <p>
 * No single test run can see a collision between two processes, so the rule is held by reading the sources of every
 * module. The scan runs from the {@code server} module and walks its siblings; {@link PluginPortFixtureScan} and the
 * ha-raft {@code Issue8203}/{@code Issue8222} guards cover the plugin and Raft ports.
 */
class HardcodedTestServerPortsTest {

  /** The reactor root, seen from the {@code server} module that surefire runs this class in. */
  private static final Path REACTOR_ROOT = Path.of("..");

  /**
   * Modules that are not scanned: the Testcontainers suites address the container-internal 2480 on purpose, and a
   * nested worktree is somebody else's checkout.
   */
  private static final List<String> SKIPPED_TOP_LEVEL = List.of(".worktrees", "e2e", "load-tests", "node_modules");

  /** {@code "http://127.0.0.1:248" + serverIndex}: a port computed from the index instead of read from the server. */
  private static final Pattern INDEX_DERIVED_URL = Pattern.compile(
      "\"(?:https?|wss?)://(?:localhost|127\\.0\\.0\\.1):2[45]\\d\"\\s*\\+");

  /** {@code http://localhost:2480/...} in a class that starts a server: the port is the one the server bound, or none. */
  private static final Pattern LITERAL_PORT_URL = Pattern.compile("://(?:localhost|127\\.0\\.0\\.1):2[45]\\d\\d\\b");

  /**
   * The HTTP/HTTPS port setting, or a Ratis gRPC server port, assigned a single value: captures the value expression.
   * A range ({@code "2480-2489"}) is not a single value: the server skips a held port and reports the one it bound.
   */
  private static final Pattern PORT_ASSIGNMENT = Pattern.compile(
      "(?:SERVER_HTTPS?_INCOMING_PORT(?:\\.getKey\\(\\))?\\s*,|Server\\.setPort\\(\\s*\\w+\\s*,)\\s*([^;]*?)\\)\\s*;");

  /** A class that starts an ArcadeDB server, directly or through one of the fixture bases. */
  private static final Pattern STARTS_SERVER = Pattern.compile(
      "\\bextends\\s+\\w*(?:ServerTest|RaftHA\\w*Test|MiniRaftTest|GremlinServerIT)\\b|new\\s+ArcadeDBServer\\(");

  /** Every module's test tree holds far more than this; below it the walk has silently lost its root. */
  private static final int EXPECTED_MINIMUM_SOURCES = 1000;

  @Test
  void noTestAddressesOrBindsAHandPickedHttpPort() throws IOException {
    assertThat(REACTOR_ROOT.resolve("server").resolve("pom.xml"))
        .as("the scan must start at the reactor root, or it checks nothing").isRegularFile();

    final Map<String, String> sources = readTestSources();
    assertThat(sources).as("the scan must find the test sources of every module").hasSizeGreaterThan(EXPECTED_MINIMUM_SOURCES);

    assertThat(offenders(sources))
        .as("tests must derive every URL from getServerHttpUrl(...)/getServerHttpPort(...) and bind ports drawn by "
            + "StaticBaseServerTest.allocateFreePorts, never a hand-picked one (see CLAUDE.md, 'Test server ports')")
        .isEmpty();
  }

  @Test
  void theScanCatchesEveryShapeItGuards() {
    final Map<String, String> sources = new TreeMap<>();
    sources.put("a/IndexUrl.java", """
        class IndexUrl extends BaseGraphServerTest { @Test void t() { url("http://127.0.0.1:248" + serverIndex + "/api"); } }""");
    sources.put("a/HttpsIndexUrl.java", """
        class HttpsIndexUrl extends BaseGraphServerTest { @Test void t() { url("https://localhost:249" + i); } }""");
    sources.put("a/LiteralUrl.java", """
        class LiteralUrl extends BaseGraphServerTest { @Test void t() { url("http://localhost:2480/prometheus"); } }""");
    sources.put("a/LiteralSetting.java", """
        class LiteralSetting { @Test void t() { new ArcadeDBServer(c); c.setValue(GlobalConfiguration.SERVER_HTTP_INCOMING_PORT, 2496); } }""");
    sources.put("a/ConstantSetting.java", """
        class ConstantSetting extends StaticBaseServerTest {
          private static final int HTTP_PORT = 2496;
          @Test void t() { c.setValue(GlobalConfiguration.SERVER_HTTP_INCOMING_PORT, HTTP_PORT); } }""");
    sources.put("a/BasePlusIndex.java", """
        abstract class BasePlusIndex {
          private static final int    BASE_HTTP_PORT = 12480;
          void setUp() { new ArcadeDBServer(c); c.setValue(GlobalConfiguration.SERVER_HTTP_INCOMING_PORT, BASE_HTTP_PORT + i); } }""");
    sources.put("a/RatisPort.java", """
        class RatisPort {
          private static final int BASE_PORT = 19860;
          @Test void t() { GrpcConfigKeys.Server.setPort(properties, BASE_PORT + i); } }""");

    // The shapes that are fine: a derived URL, a range, port 0, a drawn port, a main() launcher.
    sources.put("b/Fine.java", """
        class Fine extends BaseGraphServerTest {
          @Test void t() {
            url("http://127.0.0.1:" + getServerHttpPort(serverIndex) + "/api");
            c.setValue(GlobalConfiguration.SERVER_HTTP_INCOMING_PORT, "2490-2499");
            c.setValue(GlobalConfiguration.SERVER_HTTP_INCOMING_PORT, "0");
            c.setValue(GlobalConfiguration.SERVER_HTTP_INCOMING_PORT, allocateFreePorts(1)[0]);
            GrpcConfigKeys.Server.setPort(properties, ports[i]);
          } }""");
    sources.put("b/Launcher.java", """
        class Launcher { public static void main(String[] a) { new ArcadeDBServer(c); System.out.println("http://localhost:2480"); } }""");
    sources.put("b/UnitTest.java", """
        class UnitTest { @Test void t() { assertThat(url).isEqualTo("http://localhost:2480/api/v1/command"); } }""");

    final List<String> offenders = offenders(sources);
    for (final String name : List.of("IndexUrl", "HttpsIndexUrl", "LiteralUrl", "LiteralSetting", "ConstantSetting", "BasePlusIndex",
        "RatisPort"))
      assertThat(offenders).as("the scan must flag %s", name).anyMatch(o -> o.startsWith("a/" + name + ".java"));
    assertThat(offenders).as("the scan must not flag a shape that is fine").noneMatch(o -> o.startsWith("b/"));
  }

  private static Map<String, String> readTestSources() throws IOException {
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
          // Not this class: its own self-test spells out every shape it flags.
          for (final Path file : files.filter(p -> p.toString().endsWith(".java"))
              .filter(p -> !p.getFileName().toString().equals(HardcodedTestServerPortsTest.class.getSimpleName() + ".java")).toList())
            sources.put(REACTOR_ROOT.relativize(file).toString().replace('\\', '/'), Files.readString(file, StandardCharsets.UTF_8));
        }
      }
    }
    return sources;
  }

  /** One line per violation, {@code <file>:<line>: <what>}. */
  static List<String> offenders(final Map<String, String> sources) {
    final List<String> offenders = new ArrayList<>();
    for (final Map.Entry<String, String> entry : sources.entrySet()) {
      final String name = entry.getKey();
      final String source = entry.getValue();
      // A main() launcher for manual sessions (RaftClusterStarter) keeps the well-known ports on purpose: it never
      // runs in a shared fork.
      if (!source.contains("@Test") && !source.contains("@ParameterizedTest") && !source.contains("abstract class"))
        continue;

      report(offenders, name, source, INDEX_DERIVED_URL.matcher(source), "builds a URL from a port index instead of getServerHttpUrl(...)");

      // A literal URL is only a defect where a server is started: unit tests of the URL parsers use 2480 as data.
      if (STARTS_SERVER.matcher(source).find())
        report(offenders, name, source, LITERAL_PORT_URL.matcher(source), "addresses a hand-picked port instead of getServerHttpUrl(...)");

      final Matcher assignment = PORT_ASSIGNMENT.matcher(source);
      while (assignment.find())
        if (isHandPicked(assignment.group(1).trim(), source))
          offenders.add(location(name, source, assignment.start()) + ": binds the hand-picked port '" + assignment.group(1).trim()
              + "' instead of one drawn by allocateFreePorts");
    }
    return offenders;
  }

  /**
   * Whether a port value is a single number chosen by hand: a non-zero literal, or an expression that starts from an
   * {@code int} constant initialised with one ({@code HTTP_PORT}, {@code BASE_HTTP_PORT + i}). {@code 0} asks the
   * operating system; a range is scanned by the server; anything else is computed and outside this scan's reach.
   */
  private static boolean isHandPicked(final String value, final String source) {
    final String unquoted = value.startsWith("\"") && value.endsWith("\"") ? value.substring(1, value.length() - 1) : value;
    if (unquoted.matches("[1-9]\\d*"))
      return true;
    final Matcher head = Pattern.compile("^(\\w+)\\b").matcher(unquoted);
    return head.find() && Pattern.compile("\\bint\\s+" + Pattern.quote(head.group(1)) + "\\s*=\\s*[1-9]").matcher(source).find();
  }

  private static void report(final List<String> offenders, final String name, final String source, final Matcher matcher,
      final String what) {
    while (matcher.find())
      offenders.add(location(name, source, matcher.start()) + ": " + what);
  }

  private static String location(final String name, final String source, final int offset) {
    int line = 1;
    for (int i = 0; i < offset; i++)
      if (source.charAt(i) == '\n')
        line++;
    return name + ":" + line;
  }
}
