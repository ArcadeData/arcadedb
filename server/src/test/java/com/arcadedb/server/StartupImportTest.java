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

import com.arcadedb.ContextConfiguration;
import com.arcadedb.GlobalConfiguration;
import com.arcadedb.database.Database;
import com.arcadedb.database.DatabaseFactory;
import com.arcadedb.exception.CommandExecutionException;
import com.arcadedb.integration.importer.ImporterContext;
import com.arcadedb.utility.FileUtils;
import com.sun.net.httpserver.HttpServer;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import java.io.File;
import java.io.IOException;
import java.io.OutputStream;
import java.lang.reflect.Method;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.LongSupplier;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * The {@code import:} startup command of {@code arcadedb.server.defaultDatabases}: its failure, its network policy, its
 * progress counter, and a restore followed by an import.
 */
class StartupImportTest {
  /*
   * Issue #7483: {@code ServerControlPlane.scheduleImportCounters()} used to resolve the {@code AtomicLong} behind
   * {@code ImporterContext#parsed} once and sample it directly. That counter is per-phase - it is reset to zero by
   * {@code ImporterContext#beginPhase()} at every phase boundary of a multi-phase import (documents, then vertices,
   * then edges) - so a client watching the SSE/{@code GET /api/v1/progress} stream saw the reported {@code parsed}
   * figure climb, drop back towards zero at each boundary, then climb again.
   * <p>
   * {@code ImporterContext#getParsedTotal()} is the monotonic import-wide equivalent (phases already finished plus
   * the one still running), so {@code parsedTotalSupplier()} now prefers it, falling back to the raw {@code parsed}
   * field only for a server running against an {@code arcadedb-integration} build that predates it.
   *
   * @author Luca Garulli (l.garulli@arcadedata.com)
   */
  // Regression: #7483 the import progress counter never goes backwards across phases
  @Test
  void parsedTotalSupplierReadsTheMonotonicImportWideTotal() throws Exception {
    final ImporterContext context = new ImporterContext();
    context.parsed.set(500);

    final LongSupplier supplier = parsedTotalSupplier(context);
    assertThat(supplier.getAsLong()).isEqualTo(500);

    // End of the first phase: the raw per-phase counter resets to 0 here - this is exactly the moment the SSE
    // stream used to report a value smaller than the one before it.
    context.beginPhase();
    final long afterFirstPhaseBoundary = supplier.getAsLong();
    assertThat(afterFirstPhaseBoundary)
        .as("a phase boundary must not lose progress already reported")
        .isEqualTo(500);

    context.parsed.addAndGet(10);
    assertThat(supplier.getAsLong())
        .as("the second phase's own progress is added on top of the first phase's total")
        .isEqualTo(510);

    // A second phase boundary: same check from the other side, now with two phases already folded in.
    context.beginPhase();
    assertThat(supplier.getAsLong())
        .as("a phase boundary rolls 'parsed' into the accumulator; the reported total must never go backwards")
        .isGreaterThanOrEqualTo(afterFirstPhaseBoundary);
  }

  // Regression: #7483 the import progress counter never goes backwards across phases
  /**
   * A stand-in for an {@code ImporterContext} predating issues #7342/#7483: no {@code getParsedTotal()}, just the
   * raw per-phase field. {@code parsedTotalSupplier()} must still report something rather than schedule nothing.
   */
  @Test
  void fallsBackToTheRawPerPhaseCounterOnAnOlderIntegrationBuild() throws Exception {
    final LegacyImporterContext legacy = new LegacyImporterContext();
    legacy.parsed.set(42);

    assertThat(parsedTotalSupplier(legacy).getAsLong()).isEqualTo(42);
  }

  public static final class LegacyImporterContext {
    public final AtomicLong parsed = new AtomicLong();
  }

  private static LongSupplier parsedTotalSupplier(final Object context) throws Exception {
    final Method method = ServerControlPlane.class.getDeclaredMethod("parsedTotalSupplier", Class.class, Object.class);
    method.setAccessible(true);
    return (LongSupplier) method.invoke(null, context.getClass(), context);
  }

  // Regression: #7454 the real loadDefaultDatabases loop with a {restore:...,import:...} pair
  /**
   * The one startup path issue #7454's other tests do not drive: the real {@code loadDefaultDatabases()} loop,
   * with a {@code {restore:…,import:…}} pair.
   * <p>
   * #7454 moved the drop of the database a {@code restore:} command replaces out of that loop and into
   * {@code restoreDatabaseFromStartupCommand}, so the drop happens under the same maintenance slot as the
   * extraction. The loop's own {@code database} local is read once per iteration, before the command list is
   * walked, and a {@code restore:} invalidates it: the instance it names has been dropped, and the {@code import:}
   * case reuses that same local when it is non-null rather than resolving the database again. The loop therefore
   * re-resolves the handle after a restore.
   * <p>
   * This is not a regression the move introduced - the loop dropped the instance and kept the variable before it
   * too - but it is the only consequence of the move that {@code loadDefaultDatabases()} itself can show, so it is
   * pinned here rather than argued.
   *
   * @author Roberto Franchini (r.franchini@arcadedata.com)
   */
  @Nested
  class RestoreThenImport extends StaticBaseServerTest {
    private static final String DB_NAME         = "restorethenimport7454";
    private static final String SOURCE_DB       = "sourcerti7454";
    private static final String ARCHIVE_NAME    = "backup-rti-7454.zip";
    private static final String RESTORED_TYPE   = "Doc7454RTI";
    private static final int    RESTORED_COUNT  = 12;
    private static final String IMPORTED_TYPE   = "Document";
    private static final int    IMPORTED_COUNT  = 3;

    private ArcadeDBServer server;
    private File           csv;

    @BeforeEach
    @Override
    public void beginTest() {
      super.beginTest();
      // The startup commands read both URLs from the static global rather than from a ContextConfiguration.
      GlobalConfiguration.SERVER_RESTORE_IMPORT_ALLOW_LOCAL_URLS.setValue(true);
    }

    @AfterEach
    @Override
    public void endTest() {
      if (server != null && server.isStarted())
        server.stop();
      server = null;
      if (csv != null)
        csv.delete();
      FileUtils.deleteRecursively(new File("./target/databases/" + DB_NAME));
      FileUtils.deleteRecursively(new File("./target/backups"));
      super.endTest();
    }

    @Test
    @Timeout(180)
    void aRestoreFollowedByAnImportInOneStartupCommandListRunsBoth() throws Exception {
      final File archive = produceArchive();
      csv = writeCsv();

      final ContextConfiguration config = new ContextConfiguration();
      config.setValue(GlobalConfiguration.SERVER_ROOT_PATH, "./target");
      config.setValue(GlobalConfiguration.SERVER_ROOT_PASSWORD, DEFAULT_PASSWORD_FOR_TESTS);
      config.setValue(GlobalConfiguration.SERVER_HTTP_IO_THREADS, 2);
      config.setValue(GlobalConfiguration.TYPE_DEFAULT_BUCKETS, 2);
      config.setValue(GlobalConfiguration.SERVER_RESTORE_IMPORT_ALLOW_LOCAL_URLS, true);
      // Neither URL may contain a comma: loadDefaultDatabases() splits the '{...}' block on every comma before
      // any command is parsed, so a comma inside one would be read as the start of a second command.
      config.setValue(GlobalConfiguration.SERVER_DEFAULT_DATABASES,
          DB_NAME + "[root]{restore:file://" + archive.getAbsolutePath()
              + ",import:file://" + csv.getAbsolutePath() + "}");

      server = new ArcadeDBServer(config);
      server.start();

      assertThat(server.getStatus()).isEqualTo(ArcadeDBServer.STATUS.ONLINE);

      final Database database = server.getDatabaseForOperator(DB_NAME);
      assertThat(database.countType(RESTORED_TYPE, false))
          .as("the restore half of the startup command list did not land").isEqualTo(RESTORED_COUNT);
      assertThat(database.getSchema().existsType(IMPORTED_TYPE))
          .as("the import half ran against the handle the restore invalidated").isTrue();
      assertThat(database.countType(IMPORTED_TYPE, false)).isEqualTo(IMPORTED_COUNT);
    }

    private static File produceArchive() {
      FileUtils.deleteRecursively(new File("./target/backups"));
      final String directory = "./target/databases" + File.separator + SOURCE_DB;
      FileUtils.deleteRecursively(new File(directory));

      try (final DatabaseFactory factory = new DatabaseFactory(directory)) {
        try (final Database database = factory.create()) {
          database.getSchema().createDocumentType(RESTORED_TYPE);
          database.transaction(() -> {
            for (int i = 0; i < RESTORED_COUNT; i++)
              database.newDocument(RESTORED_TYPE).set("i", i).save();
          });
          database.command("sql", "backup database file://" + ARCHIVE_NAME).close();
          database.drop();
        }
      }

      final File produced = new File("./target/backups/" + SOURCE_DB + "/" + ARCHIVE_NAME);
      assertThat(produced).exists();
      return produced;
    }

    private static File writeCsv() throws Exception {
      final File file = new File("./target/import-7454-rti.csv").getAbsoluteFile();
      java.nio.file.Files.writeString(file.toPath(), "id,name\n1,a\n2,b\n3,c\n");
      return file;
    }
  }

  // Regression: #7484 a failed startup import fails the start instead of being thrown away
  /**
   * Issue #7484: the {@code import:} form of {@code arcadedb.server.defaultDatabases} ran the {@code IMPORT DATABASE}
   * SQL statement and threw its one-row result set away:
   * <pre>
   * try (final var rs = database.command("sql", "import database " + commandParams)) {
   *   // drain not needed: the import command produces no rows we consume here.
   * }
   * </pre>
   * {@code ImportDatabaseStatement} reports exactly one class of failure in band rather than by throwing - a failed
   * {@code probeOnly} probe, and (since issue #7461) a {@code WITH ...} setting value the importer refuses - both as
   * the single row {@code {"result":"FAIL","reason":...}}. Discarding that row meant a server booted with
   * {@code -Darcadedb.server.defaultDatabases="mydb[root]{import:file:///bad-source WITH probeOnly = true}"} came up
   * with {@code mydb} created and EMPTY, with nothing in the log saying the import was refused or why.
   * <p>
   * The fix reads the row and, on anything but {@code OK}, aborts startup the same way a failing {@code restore:}
   * startup command already does - the two commands now answer a bad source the same way, rather than one failing
   * loudly and the other succeeding silently with an empty database.
   * <p>
   * Security, the HTTP service and every plugin are already started by the point {@code loadDefaultDatabases()} runs,
   * so a caller that only rethrows leaves all of it running with {@code status} stuck at {@code STARTING} - the
   * exact state {@code isStarted() == false} cannot tell apart from a clean {@code OFFLINE}. The fix calls
   * {@code stop()} before rethrowing, the same recovery the {@code SERVER_UP} lifecycle event's own failure handler
   * already uses, so the server reaches {@code OFFLINE} with nothing left running.
   *
   * @author Luca Garulli (l.garulli@arcadedata.com)
   */
  @Nested
  class Failure extends StaticBaseServerTest {
    private static final String DB_NAME = "import7484db";

    private ArcadeDBServer server;

    @BeforeEach
    @Override
    public void beginTest() {
      super.beginTest();
    }

    @AfterEach
    @Override
    public void endTest() {
      if (server != null && server.isStarted())
        server.stop();
      server = null;
      super.endTest();
    }

    @Test
    void aFailingImportStartupCommandAbortsStartupWithTheReason() {
      server = newServerWithDefaultDatabaseImport(
          "file://target/does-not-exist-7484.csv WITH probeOnly = true");

      assertThatThrownBy(() -> server.start())
          .as("a probeOnly probe against a source that does not exist is the one failure ImportDatabaseStatement "
              + "reports in band rather than by throwing - discarding that row used to let startup succeed silently")
          .isInstanceOf(CommandExecutionException.class)
          .hasMessageContaining(DB_NAME)
          .hasMessageContaining("import:");

      assertThat(server.getStatus())
          .as("the server must fully unwind back to OFFLINE - security, the HTTP listener and every plugin already "
              + "started by this point stopped again - not merely fail to reach ONLINE while everything it already "
              + "brought up (and status == STARTING) is left running")
          .isEqualTo(ArcadeDBServer.STATUS.OFFLINE);
    }

    /**
     * The other of the two failures {@code ImportDatabaseStatement} reports in band (issue #7461): a {@code WITH ...}
     * setting value the importer refuses, as opposed to {@link #aFailingImportStartupCommandAbortsStartupWithTheReason}'s
     * unreadable-source probe. Both answer {@code {"result":"FAIL","reason":...}} through the same code path, but
     * pinning only one of the two leaves the other unverified against a fix that could, in principle, special-case
     * the probe failure and miss this one.
     * <p>
     * A single {@code WITH} setting, deliberately: {@code loadDefaultDatabases()}'s own {@code commands.split(",")}
     * tokenizes a {@code {...}} block's commands on every comma BEFORE the SQL statement ever sees the text, so a
     * multi-setting {@code WITH a = 1, b = 2} - fine inside a plain {@code IMPORT DATABASE} SQL command - would be
     * silently split into "a = 1" (this command) and a bogus second "command" ("b = 2", with no {@code type:} prefix,
     * logged and ignored) here.
     */
    @Test
    void aRefusedWithSettingAbortsStartupWithTheReason() throws Exception {
      final Path csv = Path.of("target", "import-7484-badsetting.csv").toAbsolutePath();
      Files.writeString(csv, "id,name\n1,a\n", StandardCharsets.UTF_8);

      try {
        server = newServerWithDefaultDatabaseImport("file://" + csv + " WITH documentsSkipEntries = 'not-a-number'");

        assertThatThrownBy(() -> server.start())
            .as("a WITH setting value the importer cannot use (issue #7461) is the other in-band FAIL this startup "
                + "command has to answer for, not just an unreadable source")
            .isInstanceOf(CommandExecutionException.class)
            .hasMessageContaining(DB_NAME)
            .hasMessageContaining("not-a-number");

        assertThat(server.getStatus()).isEqualTo(ArcadeDBServer.STATUS.OFFLINE);
      } finally {
        Files.deleteIfExists(csv);
      }
    }

    private static ArcadeDBServer newServerWithDefaultDatabaseImport(final String importCommandParams) {
      final ContextConfiguration config = new ContextConfiguration();
      config.setValue(GlobalConfiguration.SERVER_ROOT_PATH, "./target");
      config.setValue(GlobalConfiguration.SERVER_ROOT_PASSWORD, DEFAULT_PASSWORD_FOR_TESTS);
      config.setValue(GlobalConfiguration.SERVER_HTTP_IO_THREADS, 2);
      config.setValue(GlobalConfiguration.TYPE_DEFAULT_BUCKETS, 2);
      config.setValue(GlobalConfiguration.SERVER_DEFAULT_DATABASES, DB_NAME + "[root]{import:" + importCommandParams + "}");
      return new ArcadeDBServer(config);
    }
  }

  // Regression: #7632 the startup import honours the local-network URL policy
  /**
   * Issue #7632: the {@code import:} startup command of {@code arcadedb.server.defaultDatabases} ran the import as
   * <pre>database.command("sql", "import database " + commandParams)</pre>
   * and that overload builds a <b>fresh, empty</b> {@code ContextConfiguration} for the command, so when
   * {@code ImportDatabaseStatement} resolved the importer's local-network policy from
   * {@code context.getConfiguration()} it found nothing overlaid and read the process-wide static
   * {@link GlobalConfiguration#SERVER_SECURITY_IMPORT_BLOCK_LOCAL_NETWORKS} instead of this server's own answer.
   * <p>
   * That is the divergence #6474 closed for the client-issued {@code import database} verb and #7468 for the startup
   * {@code restore:} command: an operator who set the flag on one server instance's {@code ContextConfiguration} (an
   * embedded or multi-instance deployment) got one answer from that server's {@code IMPORT DATABASE} verb and a
   * different one from the same server's boot-time import. A database opened by the server inherits no
   * {@code ContextConfiguration} at all - {@code DatabaseFactory} is handed a path and nothing else - so the
   * configuration has to travel with the command.
   * <p>
   * Each test here sets the flag on the server's {@code ContextConfiguration} and on the static global to
   * <b>opposite</b> values, so only the per-server answer can satisfy the assertion: a fix that kept reading the
   * global fails both directions. The third test pins the other half of the contract - with no per-server override
   * the global is still what decides, which is what keeps this change invisible to every deployment that never
   * overrode the setting.
   * <p>
   * The source is served over {@code http://127.0.0.1}, not handed over as a {@code file://} path, on purpose:
   * {@code SourceDiscovery} gates only its remote branch on the flag, so a {@code file://} URL would exercise no
   * policy at all and pass against the unfixed code. {@code WITH probeOnly = true} keeps the import to the fetch
   * this test is about - the policy decision happens while opening the source, before a single row is parsed.
   *
   * @author Luca Garulli (l.garulli@arcadedata.com)
   */
  @Nested
  class LocalNetworkPolicy extends StaticBaseServerTest {
    private static final String DB_NAME  = "import7632db";
    private static final String CSV_NAME = "source-7632.csv";
    /**
     * The IPv4 loopback LITERAL. {@code localhost} resolves to {@code ::1} or {@code 127.0.0.1} depending on the
     * machine and the moment, and a fetch dialling a name that lands on the other stack reads as a connection
     * failure rather than as the policy decision this test is about.
     */
    private static final String LOOPBACK = "127.0.0.1";

    private ArcadeDBServer server;
    private HttpServer     sourceServer;
    private boolean        globalDefault;

    @AfterEach
    @Override
    public void endTest() {
      if (server != null && server.isStarted())
        server.stop();
      server = null;
      if (sourceServer != null) {
        sourceServer.stop(0);
        sourceServer = null;
      }
      try {
        super.endTest();
      } finally {
        GlobalConfiguration.SERVER_SECURITY_IMPORT_BLOCK_LOCAL_NETWORKS.setValue(globalDefault);
      }
    }

    /**
     * Override <b>blocks</b>, global <b>allows</b>: the operator hardened this server instance, so its own boot-time
     * import must refuse a loopback source even though the process-wide default permits it. This is the direction
     * that matters for the SSRF control - against the unfixed code the import read the global, saw "allow" and
     * fetched from loopback anyway.
     */
    @Test
    @Timeout(180)
    void aStartupImportFollowsAPerServerOverrideThatBlocksLocalNetworks() {
      globalDefault = GlobalConfiguration.SERVER_SECURITY_IMPORT_BLOCK_LOCAL_NETWORKS.getValueAsBoolean();
      GlobalConfiguration.SERVER_SECURITY_IMPORT_BLOCK_LOCAL_NETWORKS.setValue(false);

      server = newServer(config -> config.setValue(GlobalConfiguration.SERVER_SECURITY_IMPORT_BLOCK_LOCAL_NETWORKS, true));

      assertThatThrownBy(() -> server.start())
          .as("the per-server override blocks local networks, so this server's own startup import must refuse a "
              + "loopback source even though the static global allows it")
          .isInstanceOf(SecurityException.class)
          .hasMessageContaining("blocked request to a non-public or restricted address");

      assertThat(server.getStatus()).isEqualTo(ArcadeDBServer.STATUS.OFFLINE);
    }

    /**
     * Override <b>allows</b>, global <b>blocks</b>: the operator deliberately permitted local sources for this
     * server, so its own boot-time import must fetch one. Against the unfixed code the import read the global, saw
     * "block" and refused a source the operator had allowed - a startup failure rather than a security hole, but the
     * same single root cause, and pinning only the direction above would leave a fix free to hard-code a refusal.
     */
    @Test
    @Timeout(180)
    void aStartupImportFollowsAPerServerOverrideThatAllowsLocalNetworks() {
      globalDefault = GlobalConfiguration.SERVER_SECURITY_IMPORT_BLOCK_LOCAL_NETWORKS.getValueAsBoolean();
      GlobalConfiguration.SERVER_SECURITY_IMPORT_BLOCK_LOCAL_NETWORKS.setValue(true);

      server = newServer(config -> config.setValue(GlobalConfiguration.SERVER_SECURITY_IMPORT_BLOCK_LOCAL_NETWORKS, false));

      assertThatCode(() -> server.start())
          .as("the per-server override allows local networks, so the startup import must have fetched the source")
          .doesNotThrowAnyException();

      assertThat(server.isStarted()).isTrue();
      assertThat(server.existsDatabase(DB_NAME)).isTrue();
    }

    /**
     * No per-server override at all: the static global is still what decides, because
     * {@code ContextConfiguration.getValue(GlobalConfiguration)} falls through to it for every key the server did not
     * override. Without this the fix could have hard-coded an answer and the two tests above would not have noticed.
     */
    @Test
    @Timeout(180)
    void aStartupImportWithNoOverrideStillFollowsTheGlobal() {
      globalDefault = GlobalConfiguration.SERVER_SECURITY_IMPORT_BLOCK_LOCAL_NETWORKS.getValueAsBoolean();
      GlobalConfiguration.SERVER_SECURITY_IMPORT_BLOCK_LOCAL_NETWORKS.setValue(true);

      server = newServer(config -> {
        // deliberately no overlay for the setting
      });
      assertThat(server.getConfiguration().hasValue(GlobalConfiguration.SERVER_SECURITY_IMPORT_BLOCK_LOCAL_NETWORKS.getKey()))
          .as("this test is only meaningful while the server carries no overlay for the setting").isFalse();

      assertThatThrownBy(() -> server.start()).isInstanceOf(SecurityException.class);
      assertThat(server.getStatus()).isEqualTo(ArcadeDBServer.STATUS.OFFLINE);

      GlobalConfiguration.SERVER_SECURITY_IMPORT_BLOCK_LOCAL_NETWORKS.setValue(false);
      server = newServer(config -> {
      });
      assertThatCode(() -> server.start()).doesNotThrowAnyException();
      assertThat(server.existsDatabase(DB_NAME)).isTrue();
    }

    // ------------------------------------------------------------------------------------------------- HELPERS

    private ArcadeDBServer newServer(final java.util.function.Consumer<ContextConfiguration> customize) {
      if (server != null && server.isStarted())
        server.stop();
      if (sourceServer == null)
        sourceServer = serveSource();

      final ContextConfiguration config = new ContextConfiguration();
      config.setValue(GlobalConfiguration.SERVER_ROOT_PATH, "./target");
      config.setValue(GlobalConfiguration.SERVER_ROOT_PASSWORD, DEFAULT_PASSWORD_FOR_TESTS);
      config.setValue(GlobalConfiguration.SERVER_HTTP_IO_THREADS, 2);
      config.setValue(GlobalConfiguration.TYPE_DEFAULT_BUCKETS, 2);
      config.setValue(GlobalConfiguration.SERVER_DEFAULT_DATABASES,
          DB_NAME + "[root]{import:" + sourceUrl() + " WITH probeOnly = true}");
      customize.accept(config);
      return new ArcadeDBServer(config);
    }

    private String sourceUrl() {
      return "http://" + LOOPBACK + ":" + sourceServer.getAddress().getPort() + "/" + CSV_NAME;
    }

    /** Serves a two-row CSV from the IPv4 loopback literal, on an OS-assigned port. */
    private static HttpServer serveSource() {
      try {
        final byte[] bytes = "id,name\n1,a\n2,b\n".getBytes(StandardCharsets.UTF_8);
        final HttpServer httpServer = HttpServer.create(new InetSocketAddress(InetAddress.getByName(LOOPBACK), 0), 0);
        httpServer.createContext("/" + CSV_NAME, exchange -> {
          exchange.getResponseHeaders().add("Content-Type", "text/csv");
          exchange.sendResponseHeaders(200, bytes.length);
          try (final OutputStream out = exchange.getResponseBody()) {
            out.write(bytes);
          }
        });
        httpServer.start();
        return httpServer;
      } catch (final IOException e) {
        throw new RuntimeException("Cannot serve the test source", e);
      }
    }
  }
}
