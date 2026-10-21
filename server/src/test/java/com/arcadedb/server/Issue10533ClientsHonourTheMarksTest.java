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

import com.arcadedb.GlobalConfiguration;
import com.arcadedb.engine.ComponentFile;
import com.arcadedb.exception.DatabaseNotAvailableException;
import com.arcadedb.utility.FileUtils;
import org.junit.jupiter.api.Test;

import java.io.File;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.TreeSet;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Stream;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Issue #10533: the restore reservation (and the {@code close database} mark) refused only HTTP. Postgres, Bolt, gRPC, Redis and
 * MongoDB resolve their database through {@link ArcadeDBServer#getDatabase(String)}, which is also the entry point of the
 * operator's own steps ({@code open database}, the restore itself, the snapshot installer) and therefore cannot refuse. A client
 * of one of those protocols could open the copy an {@code overwrite} restore was about to destroy, write to it, be acknowledged,
 * and lose the writes to the swap.
 * <p>
 * The fix is a second entry point, {@link ArcadeDBServer#getDatabaseForClient(String)}, that every client surface shares with
 * HTTP, and a source-level census that holds the listener modules to it.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class Issue10533ClientsHonourTheMarksTest extends BaseGraphServerTest {
  private static final String NAME = "Issue10533Held";

  /** The reactor root, seen from the {@code server} module that surefire runs this class in. */
  private static final Path REACTOR_ROOT = Path.of("..");

  /** An {@code arcadedb-server} dependency declared {@code provided}: the module plugs into a server it does not own. */
  private static final Pattern PROVIDED_SERVER_DEPENDENCY = Pattern.compile(
      "<artifactId>arcadedb-server</artifactId>(?:(?!</dependency>).)*?<scope>provided</scope>", Pattern.DOTALL);

  /**
   * A resolution of a database by name through the entry point that sees past the marks: {@code getDatabase(name)} with the name
   * alone, or {@code getOrCreateDatabase(name)}. The three-argument form is a plugin initialising the database it owns at start.
   */
  private static final Pattern INTERNAL_ENTRY_POINT = Pattern.compile(
      "(?:\\b\\w*[sS]erver|\\bgetServer\\(\\))\\s*\\.\\s*(?:getDatabase\\((?:[^,()]|\\([^()]*\\))*\\)|getOrCreateDatabase\\()");

  /**
   * Modules that plug into the server and resolve no database on behalf of a client, with the reason. Everything else that plugs
   * in serves clients and must resolve through {@code getDatabaseForClient}.
   */
  private static final Map<String, String> NOT_A_CLIENT_SURFACE = Map.of(
      "ha-raft", "the cluster's own machinery: the snapshot installer, the state machine and the bootstrap are the operator's steps",
      "metrics", "reads server state to publish gauges, serves no client",
      "tracing", "reads the protocol tag to attribute trace spans, serves no client");

  @Test
  void aClientDuringARestoreDoesNotOpenTheTargetItIsAboutToReplace() {
    final ArcadeDBServer server = getServer(0);
    onDiskNotOpen(server);

    server.reserveDatabaseNameForRestore(NAME);
    try {
      assertThatThrownBy(() -> server.getDatabaseForRequest(NAME)).as("HTTP").isInstanceOf(DatabaseNotAvailableException.class)
          .hasMessageContaining("restore");
      assertThatThrownBy(() -> server.getDatabaseForClient(NAME)).as("Postgres, Bolt, gRPC, Redis, MongoDB")
          .isInstanceOf(DatabaseNotAvailableException.class).hasMessageContaining("restore");
      assertThat(server.existsDatabase(NAME)).as("the client opened nothing").isFalse();

      // The restore itself is the operator's step, and resolves the target it replaces through the entry point that is not refused
      assertThat(server.getDatabase(NAME).isOpen()).isTrue();
      // Once it is open it is served as before: the reservation takes no live database away from its readers
      assertThat(server.getDatabaseForClient(NAME).isOpen()).isTrue();
    } finally {
      server.releaseDatabaseNameReservedForRestore(NAME);
      dropIfPresent(server);
    }
  }

  @Test
  void aClientDoesNotReopenADatabaseACloseCommandClosed() {
    final ArcadeDBServer server = getServer(0);
    onDiskNotOpen(server);

    server.markClosedByCommand(NAME);
    try {
      assertThatThrownBy(() -> server.getDatabaseForRequest(NAME)).as("HTTP").isInstanceOf(DatabaseNotAvailableException.class)
          .hasMessageContaining("close database");
      assertThatThrownBy(() -> server.getDatabaseForClient(NAME)).as("Postgres, Bolt, gRPC, Redis, MongoDB")
          .isInstanceOf(DatabaseNotAvailableException.class).hasMessageContaining("close database");
      assertThat(server.existsDatabase(NAME)).as("the client opened nothing").isFalse();

      // 'open database' is the operator's step and is not refused; it ends the mark
      assertThat(server.getDatabase(NAME).isOpen()).isTrue();
      assertThat(server.getDatabaseForClient(NAME).isOpen()).isTrue();
    } finally {
      server.unmarkClosedByCommand(NAME);
      dropIfPresent(server);
    }
  }

  @Test
  void aClientOpensADatabaseOnDiskOnceNothingHoldsIt() {
    final ArcadeDBServer server = getServer(0);
    onDiskNotOpen(server);

    server.reserveDatabaseNameForRestore(NAME);
    server.releaseDatabaseNameReservedForRestore(NAME);
    try {
      assertThat(server.getDatabaseForClient(NAME).isOpen()).as("a released reservation holds nothing").isTrue();
    } finally {
      dropIfPresent(server);
    }
  }

  @Test
  void aClientKeepsTheAnswersOfAnUnknownOrInvalidName() {
    final ArcadeDBServer server = getServer(0);

    assertThatThrownBy(() -> server.getDatabaseForClient(null)).isInstanceOf(IllegalArgumentException.class);
    assertThatThrownBy(() -> server.getDatabaseForClient("  ")).isInstanceOf(IllegalArgumentException.class);
    assertThat(server.getDatabaseForClient(getDatabaseName()).isOpen()).as("an open database is served at once").isTrue();
  }

  /**
   * A listener that resolves a database through {@code server.getDatabase(name)} inherits the exemption the operator's own steps
   * need. The census reads the sources of every module that plugs into the server: the entry point that sees past the marks is
   * called from the server's operator steps and from the cluster module, and from no listener.
   */
  @Test
  void noListenerModuleResolvesADatabaseThroughTheEntryPointThatSeesPastTheMarks() throws IOException {
    final TreeSet<String> pluggedIn = new TreeSet<>();
    try (final Stream<Path> children = Files.list(REACTOR_ROOT)) {
      children.filter(Files::isDirectory).forEach(dir -> {
        final Path pom = dir.resolve("pom.xml");
        if (Files.isRegularFile(pom) && PROVIDED_SERVER_DEPENDENCY.matcher(read(pom)).find())
          pluggedIn.add(dir.getFileName().toString());
      });
    }
    assertThat(pluggedIn).as("the scan found the modules that depend on the server").hasSizeGreaterThanOrEqualTo(8)
        .contains("bolt", "grpcw", "mcp", "mongodbw", "postgresw", "redisw");
    assertThat(pluggedIn).as("every module named as not a client surface still plugs into the server")
        .containsAll(NOT_A_CLIENT_SURFACE.keySet());

    final List<String> offenders = new ArrayList<>();
    for (final String module : pluggedIn)
      if (!NOT_A_CLIENT_SURFACE.containsKey(module))
        collectOffenders(REACTOR_ROOT.resolve(module).resolve("src/main/java"), offenders);

    assertThat(offenders).as("a listener must resolve the database a client names through ArcadeDBServer.getDatabaseForClient(), "
        + "which refuses a name an operator has taken away (a restore in progress, a 'close database'). getDatabase(String) is "
        + "the operator's own entry point and sees past those marks. If the module serves no client, add it to "
        + "NOT_A_CLIENT_SURFACE with the reason").isEmpty();
  }

  /** The census is only as good as its pattern: the gRPC service names its server field {@code arcadeServer}, not {@code server}. */
  @Test
  void theCensusPatternRecognisesEveryNameAServerFieldGoes() {
    for (final String call : new String[] { "server.getDatabase(name)", "arcadeServer.getDatabase(databaseName)",
        "httpServer.getServer().getDatabase(name)", "getServer().getDatabase(name)", "this.server.getDatabase(name)",
        "server.getOrCreateDatabase(name)", "arcadeServer.getOrCreateDatabase(name)",
        "(DatabaseInternal) arcadeServer.getDatabase(databaseName)", "server.getDatabase(names.get(0))" })
      assertThat(INTERNAL_ENTRY_POINT.matcher(call).find()).as(call).isTrue();

    for (final String call : new String[] { "server.getDatabaseForClient(name)", "arcadeServer.getDatabaseForClient(databaseName)",
        "server.getDatabase(name, true, true)", "arcadeServer.getDatabase(name, false, false)", "getDatabase(name, credentials)",
        "database.getDatabase(name)" })
      assertThat(INTERNAL_ENTRY_POINT.matcher(call).find()).as(call).isFalse();
  }

  private static void collectOffenders(final Path sources, final List<String> offenders) throws IOException {
    if (!Files.isDirectory(sources))
      return;
    try (final Stream<Path> files = Files.walk(sources)) {
      for (final Path file : (Iterable<Path>) files.filter(f -> f.toString().endsWith(".java"))::iterator) {
        final String[] lines = read(file).split("\n");
        for (int i = 0; i < lines.length; i++) {
          final String line = lines[i].trim();
          if (line.startsWith("//") || line.startsWith("*") || line.startsWith("/*"))
            continue;
          final Matcher matcher = INTERNAL_ENTRY_POINT.matcher(line);
          if (matcher.find())
            offenders.add(file + ":" + (i + 1) + ": " + line);
        }
      }
    }
  }

  /** Leaves {@link #NAME} on disk, closed and deregistered: what an {@code overwrite} restore finds on its target. */
  private static void onDiskNotOpen(final ArcadeDBServer server) {
    final ServerDatabase created = server.createDatabase(NAME, ComponentFile.MODE.READ_WRITE);
    created.getEmbedded().close();
    server.removeDatabase(NAME);
    assertThat(server.existsDatabase(NAME)).isFalse();
  }

  private static void dropIfPresent(final ArcadeDBServer server) {
    if (server.existsDatabase(NAME))
      server.dropLocalDatabase(server.getDatabase(NAME).getEmbedded(), NAME);
    FileUtils.deleteRecursively(
        new File(server.getConfiguration().getValueAsString(GlobalConfiguration.SERVER_DATABASE_DIRECTORY), NAME));
  }

  private static String read(final Path file) {
    try {
      return Files.readString(file, StandardCharsets.UTF_8);
    } catch (final IOException e) {
      throw new IllegalStateException("Cannot read " + file, e);
    }
  }
}
