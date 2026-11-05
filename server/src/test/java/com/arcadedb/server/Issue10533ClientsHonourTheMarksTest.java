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
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Stream;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Issue #10533: the restore reservation (and the {@code close database} mark) refused only HTTP. Postgres, Bolt, gRPC, Redis and
 * MongoDB resolve their database through {@link ArcadeDBServer#getDatabaseForOperator(String)}, which is also the entry point of the
 * operator's own steps ({@code open database}, the restore itself, the snapshot installer) and therefore cannot refuse. A client
 * of one of those protocols could open the copy an {@code overwrite} restore was about to destroy, write to it, be acknowledged,
 * and lose the writes to the swap.
 * <p>
 * The fix is a second entry point, {@link ArcadeDBServer#getDatabaseForClient(String)}, that every client surface shares with
 * HTTP, and a census that holds the client surfaces to it: {@code ClientsHonourTheMarksCensusTest} in the census module, which
 * reads every call of the entry points that see past the marks in the compiled classes of every module (#11288).
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class Issue10533ClientsHonourTheMarksTest extends BaseGraphServerTest {
  private static final String NAME = "Issue10533Held";

  /** The deprecated name of the operator's entry point, {@code getDatabase(name)} with the name alone (issue #10802). */
  private static final String DEPRECATED_CALL = "getDatabase\\((?:[^,()]|\\([^()]*\\))*\\)";

  /** A call of the deprecated name on a receiver that names the server. */
  private static final Pattern DEPRECATED_ENTRY_POINT = Pattern.compile(
      "(?:\\b\\w*[sS]erver|\\bgetServer\\([^()]*\\)|\\bservers\\[[^\\]]*\\])\\s*\\.\\s*" + DEPRECATED_CALL);

  /** The reactor root, seen from the {@code server} module that surefire runs this class in. */
  private static final Path REACTOR_ROOT = Path.of(System.getProperty("basedir", "."), "..");

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
      assertThat(server.getDatabaseForOperator(NAME).isOpen()).isTrue();
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
      assertThat(server.getDatabaseForOperator(NAME).isOpen()).isTrue();
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
   * Issue #10802: {@code getDatabase(String)} became {@code getDatabaseForOperator(String)}, a name that says whom it serves, and
   * the old one is deprecated for removal. Kept only for the embedded servers and plugins outside this repository: no source of the
   * repository, main or test, calls it, so removing it is a one-line change.
   * <p>
   * The production classes are held to it by {@code ClientsHonourTheMarksCensusTest} in the census module, which reads every call
   * of the method in the compiled classes. This half reads the TEST sources of every module as text, because no module's classpath
   * carries the test classes of the others, and it says what that costs: the receiver must be named like a server (an identifier
   * ending in {@code server}, {@code getServer(..)} or {@code servers[..]}), and a test calling it on a receiver named otherwise
   * passes. A miss costs a test that keeps compiling against a deprecated method, not a client surface.
   */
  @Test
  void noTestSourceOfTheRepositoryCallsTheDeprecatedNameOfTheOperatorsEntryPoint() throws IOException {
    final List<String> offenders = new ArrayList<>();
    try (final Stream<Path> modules = Files.list(REACTOR_ROOT)) {
      for (final Path module : (Iterable<Path>) modules.filter(Files::isDirectory)::iterator)
        collect(module.resolve("src/test/java"), DEPRECATED_ENTRY_POINT, offenders);
    }
    offenders.removeIf(offender -> offender.contains(getClass().getSimpleName() + ".java:"));

    assertThat(offenders).as("call ArcadeDBServer.getDatabaseForOperator() for an operator's step, getDatabaseForClient() for a "
        + "database a client names; getDatabase(String) is deprecated for removal").isEmpty();
  }

  private static void collect(final Path sources, final Pattern pattern, final List<String> offenders) throws IOException {
    if (!Files.isDirectory(sources))
      return;
    try (final Stream<Path> files = Files.walk(sources)) {
      for (final Path file : (Iterable<Path>) files.filter(f -> f.toString().endsWith(".java"))::iterator) {
        final String[] lines = read(file).split("\n");
        for (int i = 0; i < lines.length; i++) {
          final String line = lines[i].trim();
          if (line.startsWith("//") || line.startsWith("*") || line.startsWith("/*"))
            continue;
          final Matcher matcher = pattern.matcher(line);
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
      server.dropLocalDatabase(server.getDatabaseForOperator(NAME).getEmbedded(), NAME);
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
