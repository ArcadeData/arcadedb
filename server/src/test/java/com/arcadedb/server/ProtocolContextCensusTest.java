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
import java.util.TreeMap;
import java.util.TreeSet;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Stream;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Source-level guard for the census of issue #10094: every module that serves clients on threads of its own must tag
 * them with {@code ProtocolContext.set(...)}.
 * <p>
 * The tag is what tells a client request from the engine's own work. Under HA, {@code RaftReplicatedDatabase} refuses a
 * CLIENT request on a database whose directory is being replaced from the leader's snapshot and exempts whatever reads
 * {@code ProtocolContext.INTERNAL}, which is the default of a thread nobody tagged. The query metrics and the trace
 * spans attribute the work by the same value. A listener that forgets the tag therefore inherits an exemption from an HA
 * safety gate and reports its traffic as the engine's, and nothing fails: MCP did both for as long as it existed.
 * <p>
 * A behavioural test cannot hold this rule across modules, because no single module has every listener on its test
 * classpath. So the rule is held by reading the sources: a module that depends on {@code arcadedb-server} in
 * {@code provided} scope is an extension of the server, and must either tag its threads or say, here, why it serves no
 * client of its own. A new module is therefore a decision, not an accident.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class ProtocolContextCensusTest {
  /** The reactor root, seen from the {@code server} module that surefire runs this class in. */
  private static final Path REACTOR_ROOT = Path.of("..");

  /** The tag, set on a thread that serves a client. */
  private static final String TAG_CALL = "ProtocolContext.set(";

  /** An {@code arcadedb-server} dependency declared {@code provided}: the module plugs into a server it does not own. */
  private static final Pattern PROVIDED_SERVER_DEPENDENCY = Pattern.compile(
      "<artifactId>arcadedb-server</artifactId>(?:(?!</dependency>).)*?<scope>provided</scope>", Pattern.DOTALL);

  /**
   * Modules that plug into the server and serve no client on a thread of their own, with the reason. Everything else
   * that plugs in is a listener and must tag.
   */
  private static final Map<String, String> NOT_A_LISTENER = Map.of(
      "graphql", "its handler extends AbstractServerHttpHandler, which tags the request thread http",
      "ha-raft", "the cluster's own machinery: its threads are the engine's (INTERNAL) and the module only reads the tag",
      "metrics", "reads the tag to attribute query metrics, serves no client",
      "tracing", "reads the tag to attribute trace spans, serves no client");

  /**
   * How many {@code ProtocolContext.set(} calls each listener module makes, and which entry points they cover (issue
   * #10390). A count above zero only separates "no tag anywhere" from "a tag somewhere": mongodbw passed on one call that
   * covered one of the three doors its library opens on a client request. So the number is pinned, and changing it means
   * coming here and saying what the new call covers - or, for a call that went away, which entry point lost its tag.
   * Where a module has more than one door, a test in that module enumerates them and proves each one tagged.
   */
  private static final Map<String, ExpectedTags> TAG_CALLS = Map.of(
      "bolt", new ExpectedTags(1, "BoltNetworkExecutor.run(), the one loop that serves a connection"),
      "gremlin", new ExpectedTags(1,
          "GremlinPrincipalPropagatingExecutorService.execute(), which submit()/invokeAll() funnel through"),
      "grpcw", new ExpectedTags(17, "4 in GrpcProtocolContextInterceptor (tag and restore around every call and its "
          + "listener callbacks), 13 in ArcadeDbGrpcService on the handlers whose work continues on a thread of its own"),
      "mcp", new ExpectedTags(2, "MCPDispatcher, around the dispatch of a request, and the restore of its enclosing tag"),
      "mongodbw", new ExpectedTags(2, "MongoDBBackend.tagAsMongo() and the set() branch of restoreProtocol() (its other "
          + "branch is a clear(), which this count does not see), called by every MongoBackend entry point the library makes "
          + "with a client's request: Issue10390MongoProtocolContextTest enumerates them by reflection"),
      "postgresw", new ExpectedTags(1, "PostgresNetworkExecutor.run(), the one loop that serves a connection"),
      "redisw", new ExpectedTags(1, "RedisNetworkExecutor.run(), the one loop that serves a connection"));

  private record ExpectedTags(int calls, String covering) {
  }

  @Test
  void everyListenerModuleTagsItsThreads() throws IOException {
    final Set<String> pluggedIn = modulesPluggedIntoTheServer();
    assertThat(pluggedIn).as("the scan found the modules that depend on the server").hasSizeGreaterThanOrEqualTo(8)
        .contains("bolt", "gremlin", "grpcw", "mcp", "mongodbw", "postgresw", "redisw");

    final Map<String, Integer> taggers = new TreeMap<>();
    for (final String module : pluggedIn)
      if (!NOT_A_LISTENER.containsKey(module))
        taggers.put(module, countTagCalls(REACTOR_ROOT.resolve(module).resolve("src/main/java")));

    assertThat(taggers).as("a module that serves clients on its own threads without tagging them inherits the "
        + "ProtocolContext.INTERNAL exemption of the HA safety gate, and its traffic is reported as the engine's. To resolve: "
        + "call ProtocolContext.set(<protocol>) where the module's request thread starts and clear or restore it in a finally; "
        + "if the module serves no client of its own, add it to NOT_A_LISTENER with the reason").allSatisfy(
        (module, calls) -> assertThat(calls).as("%s tags its threads", module).isPositive());

    assertThat(taggers.keySet()).as("every listener module states how many tag calls it makes, and what they cover, "
        + "in TAG_CALLS").isEqualTo(new TreeSet<>(TAG_CALLS.keySet()));
    taggers.forEach((module, calls) -> assertThat(calls).as("%s makes %d ProtocolContext.set( calls, covering %s. A different "
            + "count means an entry point gained or lost its tag: check every entry point of the module is still tagged, then "
            + "update TAG_CALLS with what the calls now cover", module, TAG_CALLS.get(module).calls(),
        TAG_CALLS.get(module).covering()).isEqualTo(TAG_CALLS.get(module).calls()));

    // The server itself: the HTTP handlers and the WebSocket insert sessions
    assertThat(countTagCalls(Path.of("src/main/java"))).as("server").isPositive();
  }

  @Test
  void theExemptionsAreNotStale() {
    final Set<String> pluggedIn = modulesPluggedIntoTheServer();
    assertThat(pluggedIn).as("every module named as not a listener still plugs into the server")
        .containsAll(NOT_A_LISTENER.keySet());
  }

  private static Set<String> modulesPluggedIntoTheServer() {
    final Set<String> modules = new TreeSet<>();
    try (final Stream<Path> children = Files.list(REACTOR_ROOT)) {
      children.filter(Files::isDirectory).forEach(dir -> {
        final Path pom = dir.resolve("pom.xml");
        if (Files.isRegularFile(pom) && PROVIDED_SERVER_DEPENDENCY.matcher(read(pom)).find())
          modules.add(dir.getFileName().toString());
      });
    } catch (final IOException e) {
      throw new IllegalStateException("Cannot list the modules of the reactor", e);
    }
    return modules;
  }

  private static int countTagCalls(final Path sources) throws IOException {
    if (!Files.isDirectory(sources))
      return 0;
    int calls = 0;
    try (final Stream<Path> files = Files.walk(sources)) {
      for (final Path file : (Iterable<Path>) files.filter(f -> f.toString().endsWith(".java"))::iterator) {
        final Matcher matcher = Pattern.compile(Pattern.quote(TAG_CALL)).matcher(read(file));
        while (matcher.find())
          calls++;
      }
    }
    return calls;
  }

  private static String read(final Path file) {
    try {
      return Files.readString(file, StandardCharsets.UTF_8);
    } catch (final IOException e) {
      throw new IllegalStateException("Cannot read " + file, e);
    }
  }
}
