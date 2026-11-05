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
 * Issue #10533: the restore reservation (and the {@code close database} mark) refused only HTTP. Postgres, Bolt, gRPC, Redis and
 * MongoDB resolved their database through {@code ArcadeDBServer.getDatabaseForOperator(String)}, which is also the entry point of
 * the operator's own steps ({@code open database}, the restore itself, the snapshot installer) and therefore cannot refuse. A
 * client of one of those protocols could open the copy an {@code overwrite} restore was about to destroy, write to it, be
 * acknowledged, and lose the writes to the swap. The fix is a second entry point, {@code getDatabaseForClient(String)}, that
 * every client surface shares with HTTP; the behaviour is pinned by {@code Issue10533ClientsHonourTheMarksTest} in the server
 * module, and this census holds the client surfaces to it.
 * <p>
 * The census ranges over the authority (#11288): every call of an entry point of {@code ArcadeDBServer} that sees past the
 * marks - {@code getDatabaseForOperator(String)}, its deprecated alias {@code getDatabase(String)} (#10802) and
 * {@code getOrCreateDatabase(String)}, each told from its overloads by its descriptor - or method reference to one, in the
 * compiled classes of every module ({@link CompiledClasses}). The source-text census it replaces matched the call on a receiver
 * whose name ended in {@code server}, and said so: a listener that kept the server in a field named otherwise ({@code srv},
 * {@code owner}) passed it.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class ClientsHonourTheMarksCensusTest {
  private static final String      AUTHORITY      = "com/arcadedb/server/ArcadeDBServer";
  private static final Set<String> PAST_THE_MARKS = Set.of("getDatabaseForOperator", "getDatabase", "getOrCreateDatabase");
  private static final String      BY_NAME_ALONE  = "(Ljava/lang/String;)Lcom/arcadedb/server/ServerDatabase;";
  private static final String      DEPRECATED     = "getDatabase";


  /**
   * Modules that plug into the server and resolve no database on behalf of a client, with the reason. Everything else that
   * plugs in serves clients and must resolve through {@code getDatabaseForClient}.
   */
  private static final Map<String, String> NOT_A_CLIENT_SURFACE = Map.of(
      "ha-raft", "the cluster's own machinery: the snapshot installer, the state machine and the bootstrap are the operator's steps",
      "metrics", "reads server state to publish gauges, serves no client",
      "tracing", "reads the protocol tag to attribute trace spans, serves no client");

  /**
   * The server module's own top-level classes that resolve a database through an entry point that sees past the marks, each the
   * operator's own step (issue #10690): the server module is the one with the most client surfaces (HTTP, {@code /ws}, the AI
   * handlers, the schema reader MCP shares), and every other class of it is held to {@code getDatabaseForClient}.
   */
  private static final Map<String, String> SERVER_OPERATOR_STEPS = Map.of(
      "server/ArcadeDBServer",
      "the registry itself: the entry point, its deprecated alias, the databases opened at startup and the startup restore",
      "server/ServerControlPlane",
      "the operator's commands: open, close, drop, restore and import database, and the cluster-wide steps they replicate",
      "server/security/ServerSecurity", "the security refresh, which updates the schema of every database the server holds");

  /**
   * A listener that resolves a database through an entry point that sees past the marks inherits the exemption the operator's
   * own steps need: no class of a module that plugs into the server calls one.
   */
  @Test
  void noListenerModuleResolvesADatabaseThroughTheEntryPointThatSeesPastTheMarks() throws Exception {
    final Set<String> pluggedIn = CompiledClasses.modulesPluggedIntoTheServer();
    assertThat(pluggedIn).as("the scan found the modules that depend on the server").hasSizeGreaterThanOrEqualTo(8)
        .contains("bolt", "grpcw", "mcp", "mongodbw", "postgresw", "redisw");
    assertThat(pluggedIn).as("every module named as not a client surface still plugs into the server")
        .containsAll(NOT_A_CLIENT_SURFACE.keySet());

    final Map<String, Map<String, byte[]>> modules = CompiledClasses.byModule();
    final Map<String, Set<String>> offenders = new TreeMap<>();
    for (final String module : pluggedIn)
      if (!NOT_A_CLIENT_SURFACE.containsKey(module)) {
        assertThat(modules).as("the classes of module " + module).containsKey(module);
        offenders.putAll(pastTheMarks(modules.get(module)));
      }

    assertThat(offenders).as("a listener must resolve the database a client names through ArcadeDBServer.getDatabaseForClient(), "
        + "which refuses a name an operator has taken away (a restore in progress, a 'close database'). getDatabaseForOperator(String) "
        + "is the operator's own entry point and sees past those marks. If the module serves no client, add it to "
        + "NOT_A_CLIENT_SURFACE with the reason").isEmpty();
  }

  /**
   * Issue #10690: the census above never walked the server module, and a {@code /ws} SUBSCRIBE frame resolved its database through
   * {@code getDatabase(String)}: one frame reopened a database an operator had closed, and cleared the mark for every client.
   */
  @Test
  void noClientSurfaceOfTheServerModuleResolvesADatabaseThroughTheEntryPointThatSeesPastTheMarks() throws Exception {
    final Set<String> offenders = new TreeSet<>();
    final Set<String> stepsSeen = new TreeSet<>();
    for (final Map.Entry<String, Set<String>> site : pastTheMarks(CompiledClasses.byModule().get("server")).entrySet()) {
      final String topLevel = topLevelOf(site.getKey());
      if (SERVER_OPERATOR_STEPS.containsKey(topLevel))
        stepsSeen.add(topLevel);
      else
        offenders.add(site.getKey() + " (" + String.join(", ", site.getValue()) + ")");
    }

    assertThat(offenders).as("a server handler or listener must resolve the database a client names through "
        + "ArcadeDBServer.getDatabaseForClient() (or getDatabaseForRequest() for HTTP), which refuses a name an operator has taken "
        + "away. getDatabaseForOperator(String) is the operator's own entry point and sees past those marks. A class that holds only "
        + "the operator's own steps goes in SERVER_OPERATOR_STEPS with the reason").isEmpty();
    assertThat(stepsSeen).as("every class named as an operator step still resolves through the operator's entry point: drop the "
        + "ones that no longer do, so the list cannot hide a new client surface").containsExactlyInAnyOrderElementsOf(
        SERVER_OPERATOR_STEPS.keySet());
  }

  /**
   * Issue #10802: {@code getDatabase(String)} became {@code getDatabaseForOperator(String)}, a name that says whom it serves, and
   * the old one is deprecated for removal, kept only for the embedded servers and plugins outside this repository. No production
   * class calls it; the test sources are held to the same by {@code Issue10533ClientsHonourTheMarksTest}.
   */
  @Test
  void noProductionClassCallsTheDeprecatedNameOfTheOperatorsEntryPoint() throws Exception {
    final Map<String, Set<String>> calls = CompiledClasses.callSitesOf(CompiledClasses.ofEveryModule(),
        (owner, name, desc) -> AUTHORITY.equals(owner) && DEPRECATED.equals(name) && BY_NAME_ALONE.equals(desc));

    assertThat(calls).as("call ArcadeDBServer.getDatabaseForOperator() for an operator's step, getDatabaseForClient() for a "
        + "database a client names; getDatabase(String) is deprecated for removal").isEmpty();
  }

  /** The control: a call on a receiver of any name, or a method reference, is a call site; an overload is not. */
  @Test
  void theCensusSeesTheCallWhateverTheReceiverIsNamed() throws Exception {
    final Map<String, Set<String>> sites = pastTheMarks(CompiledClasses.ofTheProbes());

    assertThat(sites).containsKeys("census/ShapeProbes$OperatorEntryPoint#onAFieldNamedOwner",
        "census/ShapeProbes$OperatorEntryPoint#methodReference", "census/ShapeProbes$OperatorEntryPoint#deprecated");
    assertThat(sites).doesNotContainKeys("census/ShapeProbes$OperatorEntryPoint#forAClient",
        "census/ShapeProbes$OperatorEntryPoint#theOverload");
  }

  private static Map<String, Set<String>> pastTheMarks(final Map<String, byte[]> classes) {
    return CompiledClasses.callSitesOf(classes,
        (owner, name, desc) -> AUTHORITY.equals(owner) && PAST_THE_MARKS.contains(name) && BY_NAME_ALONE.equals(desc));
  }

  /** {@code a/C$Nested#m} -> {@code a/C}. */
  private static String topLevelOf(final String key) {
    final String className = key.substring(0, key.indexOf('#'));
    final int nested = className.indexOf('$');
    return nested < 0 ? className : className.substring(0, nested);
  }
}
