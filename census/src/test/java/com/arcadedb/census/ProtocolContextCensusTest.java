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

import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.TreeSet;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * The census of issue #10094: every module that serves clients on threads of its own must tag them with
 * {@code ProtocolContext.set(...)}.
 * <p>
 * The tag is what tells a client request from the engine's own work. Under HA, {@code RaftReplicatedDatabase} refuses a
 * CLIENT request on a database whose directory is being replaced from the leader's snapshot and exempts whatever reads
 * {@code ProtocolContext.INTERNAL}, which is the default of a thread nobody tagged. The query metrics and the trace spans
 * attribute the work by the same value. A listener that forgets the tag therefore inherits an exemption from an HA safety
 * gate and reports its traffic as the engine's, and nothing fails: MCP did both for as long as it existed.
 * <p>
 * Two authorities, each read where it is defined (#11288). A module is an extension of the server when its {@code pom.xml}
 * declares {@code arcadedb-server} in {@code provided} scope, and must either tag its threads or say, here, why it serves
 * no client of its own: a new module is a decision, not an accident. A tag is a call of {@code ProtocolContext.set}, or a
 * method reference to it, in the compiled classes of that module ({@link CompiledClasses}), and every one is listed in
 * {@link #TAGS} by {@code class#method} with the entry point it covers (#10390): a count above zero only separates "no tag
 * anywhere" from "a tag somewhere", and mongodbw once passed on one call that covered one of the three doors its library
 * opens on a client request. The source-text census it replaces counted the spelling {@code ProtocolContext.set(} in the
 * module's text, so a static import or a method reference was no tag, a comment naming the call was one, and the pinned
 * count could stay the same while one tag moved from a door to another.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class ProtocolContextCensusTest {
  private static final String AUTHORITY = "com/arcadedb/database/ProtocolContext";
  private static final String TAG       = "set";


  /**
   * Modules that plug into the server and serve no client on a thread of their own, with the reason. Everything else that
   * plugs in is a listener and must tag.
   */
  private static final Map<String, String> NOT_A_LISTENER = Map.of(
      "graphql", "its handler extends AbstractServerHttpHandler, which tags the request thread http",
      "ha-raft", "the cluster's own machinery: its threads are the engine's (INTERNAL) and the module only reads the tag",
      "metrics", "reads the tag to attribute query metrics, serves no client",
      "tracing", "reads the tag to attribute trace spans, serves no client");

  private static final String GRPC_OWN_THREAD = "a gRPC handler whose work continues on a thread of its own";

  /**
   * Every tag of every listener module, by {@code class#method}, with the entry point it covers. Where a module has more
   * than one door, a test in that module enumerates them and proves each one tagged.
   */
  private static final Map<String, String> TAGS = new TreeMap<>(Map.ofEntries(//
      Map.entry("bolt/BoltNetworkExecutor#run", "the one loop that serves a connection"),
      Map.entry("postgres/PostgresNetworkExecutor#run", "the one loop that serves a connection"),
      Map.entry("redis/RedisNetworkExecutor#run", "the one loop that serves a connection"),
      Map.entry("server/gremlin/GremlinPrincipalPropagatingExecutorService#execute",
          "the executor the Gremlin server runs a request on: submit()/invokeAll() funnel through it"),
      Map.entry("mcp/MCPDispatcher#dispatch", "around the dispatch of a request, and the restore of its enclosing tag"),
      Map.entry("mongo/MongoDBBackend#tagAsMongo", "called by every MongoBackend entry point the library makes with a client's "
          + "request: Issue10390MongoProtocolContextTest enumerates them by reflection"),
      Map.entry("mongo/MongoDBBackend#restoreProtocol", "restores the tag the entry point found, after tagAsMongo"),
      Map.entry("server/grpc/GrpcProtocolContextInterceptor#taggedStart", "tags and restores around the start of every call"),
      Map.entry("server/grpc/GrpcProtocolContextInterceptor#tagged", "tags and restores around every listener callback of a call"),
      Map.entry("server/grpc/ArcadeDbGrpcService#asGrpcProtocol", GRPC_OWN_THREAD),
      Map.entry("server/grpc/ArcadeDbGrpcService#beginTransaction", GRPC_OWN_THREAD),
      Map.entry("server/grpc/ArcadeDbGrpcService#bulkInsert", GRPC_OWN_THREAD),
      Map.entry("server/grpc/ArcadeDbGrpcService#commitTransaction", GRPC_OWN_THREAD),
      Map.entry("server/grpc/ArcadeDbGrpcService#executeCommandInternal", GRPC_OWN_THREAD),
      Map.entry("server/grpc/ArcadeDbGrpcService#executeQueryInternal", GRPC_OWN_THREAD),
      Map.entry("server/grpc/ArcadeDbGrpcService#insertRows", GRPC_OWN_THREAD),
      Map.entry("server/grpc/ArcadeDbGrpcService#onNext", "the streaming insert's observer, whose callbacks run on the transport's threads"),
      Map.entry("server/grpc/ArcadeDbGrpcService#streamQuery", GRPC_OWN_THREAD),
      Map.entry("server/grpc/ArcadeDbGrpcService#submitToActiveTransaction", "the transaction's own executor thread"),
      Map.entry("server/grpc/ArcadeDbGrpcService#timeSeriesLatest", GRPC_OWN_THREAD),
      Map.entry("server/grpc/ArcadeDbGrpcService#timeSeriesQuery", GRPC_OWN_THREAD),
      Map.entry("server/grpc/ArcadeDbGrpcService#timeSeriesWrite", GRPC_OWN_THREAD)));

  @Test
  void everyListenerModuleTagsItsThreads() throws Exception {
    final Set<String> pluggedIn = CompiledClasses.modulesPluggedIntoTheServer();
    assertThat(pluggedIn).as("the scan found the modules that depend on the server").hasSizeGreaterThanOrEqualTo(8)
        .contains("bolt", "gremlin", "grpcw", "mcp", "mongodbw", "postgresw", "redisw");

    final Map<String, Map<String, byte[]>> modules = CompiledClasses.byModule();
    final Map<String, Set<String>> tags = new TreeMap<>();
    final Set<String> untagged = new TreeSet<>();
    for (final String module : pluggedIn)
      if (!NOT_A_LISTENER.containsKey(module)) {
        assertThat(modules).as("the classes of module " + module).containsKey(module);
        final Map<String, Set<String>> sites = tagsIn(modules.get(module));
        if (sites.isEmpty())
          untagged.add(module);
        tags.putAll(sites);
      }

    assertThat(untagged).as("a module that serves clients on its own threads without tagging them inherits the "
        + "ProtocolContext.INTERNAL exemption of the HA safety gate, and its traffic is reported as the engine's. To resolve: "
        + "call ProtocolContext.set(<protocol>) where the module's request thread starts and clear or restore it in a finally; "
        + "if the module serves no client of its own, add it to NOT_A_LISTENER with the reason").isEmpty();
    assertThat(CompiledClasses.unreviewed(tags, TAGS)).as("a tag not in TAGS: an entry point gained a tag or a tag moved; "
        + "check every entry point of the module is still tagged, then list the tag with what it covers").isEmpty();
    assertThat(CompiledClasses.stale(tags, TAGS)).as("TAGS entries whose tag is gone: which entry point lost it?").isEmpty();

    // The server itself: the HTTP handlers and the WebSocket insert sessions
    assertThat(tagsIn(modules.get("server"))).as("server").isNotEmpty();
  }

  @Test
  void theExemptionsAreNotStale() throws Exception {
    assertThat(CompiledClasses.modulesPluggedIntoTheServer()).as("every module named as not a listener still plugs into the server")
        .containsAll(NOT_A_LISTENER.keySet());
  }

  private static Map<String, Set<String>> tagsIn(final Map<String, byte[]> classes) {
    return CompiledClasses.callSitesOf(classes, (owner, name, desc) -> AUTHORITY.equals(owner) && TAG.equals(name));
  }
}
