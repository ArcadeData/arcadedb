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
import com.arcadedb.utility.FileUtils;
import com.sun.net.httpserver.HttpServer;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import java.io.File;
import java.io.IOException;
import java.io.OutputStream;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.nio.file.Files;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Issue #7468: the {@code restore:} startup command of {@code arcadedb.server.defaultDatabases} never called
 * {@code Restore.setAllowLocalUrls(...)}, so {@code RestoreSettings.allowLocalUrls} stayed {@code null} and
 * {@code FullRestoreFormat.openInputFile()} fell back to the <b>static</b>
 * {@link GlobalConfiguration#SERVER_RESTORE_IMPORT_ALLOW_LOCAL_URLS} value. Every other server-side
 * restore/import path resolves that flag through {@code ServerControlPlane.isRestoreImportLocalUrlsAllowed()},
 * which reads the server's own {@link ContextConfiguration}, so an operator who overrode the setting for one
 * server instance got one answer from that server's {@code restore database} verb and a different one from the
 * same server's boot-time restore.
 * <p>
 * Each test here sets the flag on the server's {@code ContextConfiguration} and the static global to
 * <b>opposite</b> values, so the assertion can only be satisfied by the per-server answer - a fix that kept
 * reading the global would fail every one of them in the direction the override went. The third test pins the
 * other half of the contract: with no per-server override the global is still what decides, because
 * {@link ContextConfiguration#getValueAsBoolean} falls through to it. That is what keeps this change invisible
 * to every deployment that never overrode the setting.
 * <p>
 * The archive is served over {@code http://127.0.0.1}, not handed over as a {@code file://} path, on purpose:
 * {@code FullRestoreFormat.openInputFile()} gates only its {@code http(s)} branch on {@code allowLocalUrls}, so
 * a {@code file://} URL would exercise no policy at all and pass against the unfixed code.
 */
class Issue7468StartupRestoreLocalUrlPolicyIT extends BaseGraphServerTest {
  private static final String SOURCE_DB      = "source7468";
  private static final String ARCHIVE_NAME   = "backup-7468.zip";
  private static final int    DOCUMENT_COUNT = 40;
  private static final String TYPE_NAME      = "Doc7468";
  /**
   * The IPv4 loopback LITERAL for both ends. {@code localhost} resolves to {@code ::1} or {@code 127.0.0.1}
   * depending on the machine and the moment, and a restore dialling a name that lands on the other stack reads
   * as a connection failure rather than as the policy decision this test is about.
   */
  private static final String LOOPBACK       = "127.0.0.1";

  private HttpServer archiveServer;
  private File       archive;
  private boolean    globalDefault;

  @Override
  protected boolean isCreateDatabases() {
    return false;
  }

  @Override
  protected void onServerConfiguration(final ContextConfiguration config) {
    super.onServerConfiguration(config);

    globalDefault = GlobalConfiguration.SERVER_RESTORE_IMPORT_ALLOW_LOCAL_URLS.getValueAsBoolean();
    archive = produceArchive();
    archiveServer = serveArchive(archive);

    // A dedicated port rather than the shared 2480-2489 range: this class needs its own server to be the one
    // answering, and a neighbouring listener would turn a policy assertion into an unrelated wall of red.
    config.setValue(GlobalConfiguration.SERVER_HTTP_INCOMING_HOST, LOOPBACK);
    config.setValue(GlobalConfiguration.SERVER_HTTP_INCOMING_PORT, String.valueOf(freePort()));
  }

  @AfterEach
  @Override
  public void endTest() {
    if (archiveServer != null)
      archiveServer.stop(0);
    try {
      super.endTest();
    } finally {
      GlobalConfiguration.SERVER_RESTORE_IMPORT_ALLOW_LOCAL_URLS.setValue(globalDefault);
      FileUtils.deleteRecursively(new File("./target/backups"));
    }
  }

  /**
   * Override <b>on</b>, global <b>off</b>: the operator deliberately allowed local URLs for this server, so its
   * own boot-time restore must fetch one. Against the unfixed code the restore read the global, saw {@code false}
   * and refused a URL the operator had allowed.
   */
  @Test
  @Timeout(180)
  void aStartupRestoreFollowsAPerServerOverrideThatAllowsLocalUrls() {
    GlobalConfiguration.SERVER_RESTORE_IMPORT_ALLOW_LOCAL_URLS.setValue(false);
    getServer(0).getConfiguration().setValue(GlobalConfiguration.SERVER_RESTORE_IMPORT_ALLOW_LOCAL_URLS, true);

    final String target = "allowed7468";
    getServer(0).restoreDatabaseFromStartupCommand(target, archiveUrl(), targetPath(target));

    assertThat(getServer(0).existsDatabase(target))
        .as("the per-server override allows local URLs, so the startup restore must have run").isTrue();
    assertThat(getServer(0).getDatabase(target).countType(TYPE_NAME, true)).isEqualTo(DOCUMENT_COUNT);
  }

  /**
   * Override <b>off</b>, global <b>on</b>: the operator disabled local URLs for this server, so its own boot-time
   * restore must refuse one even though the process-wide default permits it. This is the direction that matters
   * for the SSRF control - against the unfixed code the restore read the global, saw {@code true} and fetched
   * from loopback anyway.
   */
  @Test
  @Timeout(180)
  void aStartupRestoreFollowsAPerServerOverrideThatForbidsLocalUrls() {
    GlobalConfiguration.SERVER_RESTORE_IMPORT_ALLOW_LOCAL_URLS.setValue(true);
    getServer(0).getConfiguration().setValue(GlobalConfiguration.SERVER_RESTORE_IMPORT_ALLOW_LOCAL_URLS, false);

    final String target = "forbidden7468";
    assertThatThrownBy(() -> getServer(0).restoreDatabaseFromStartupCommand(target, archiveUrl(), targetPath(target)))
        .isInstanceOf(CommandExecutionException.class)
        .rootCause()
        .isInstanceOf(SecurityException.class)
        .hasMessageContaining("blocked request to a non-public or restricted address");

    assertThat(getServer(0).existsDatabase(target))
        .as("a refused startup restore must leave no database behind").isFalse();
  }

  /**
   * No per-server override at all: the static global is still what decides, so nothing changes for a deployment
   * that never overrode the setting. Without this the fix could have hard-coded an answer and the two tests above
   * would not have noticed.
   */
  @Test
  @Timeout(180)
  void aStartupRestoreWithNoOverrideStillFollowsTheGlobal() {
    assertThat(getServer(0).getConfiguration()
        .hasValue(GlobalConfiguration.SERVER_RESTORE_IMPORT_ALLOW_LOCAL_URLS.getKey()))
        .as("this test is only meaningful while the server carries no overlay for the setting").isFalse();

    GlobalConfiguration.SERVER_RESTORE_IMPORT_ALLOW_LOCAL_URLS.setValue(false);
    final String refused = "globaloff7468";
    assertThatThrownBy(() -> getServer(0).restoreDatabaseFromStartupCommand(refused, archiveUrl(), targetPath(refused)))
        .isInstanceOf(CommandExecutionException.class)
        .rootCause()
        .isInstanceOf(SecurityException.class);
    assertThat(getServer(0).existsDatabase(refused)).isFalse();

    GlobalConfiguration.SERVER_RESTORE_IMPORT_ALLOW_LOCAL_URLS.setValue(true);
    final String allowed = "globalon7468";
    getServer(0).restoreDatabaseFromStartupCommand(allowed, archiveUrl(), targetPath(allowed));
    assertThat(getServer(0).existsDatabase(allowed)).isTrue();
    assertThat(getServer(0).getDatabase(allowed).countType(TYPE_NAME, true)).isEqualTo(DOCUMENT_COUNT);
  }

  // ------------------------------------------------------------------------------------------------- HELPERS

  private String archiveUrl() {
    return "http://" + LOOPBACK + ":" + archiveServer.getAddress().getPort() + "/" + ARCHIVE_NAME;
  }

  private static String targetPath(final String databaseName) {
    return GlobalConfiguration.SERVER_DATABASE_DIRECTORY.getValueAsString() + "0" + File.separator + databaseName;
  }

  /** Creates a throwaway database, backs it up, drops it, and returns the archive. */
  private File produceArchive() {
    final String databaseDirectory = GlobalConfiguration.SERVER_DATABASE_DIRECTORY.getValueAsString() + "0";
    FileUtils.deleteRecursively(new File("./target/backups"));

    try (final DatabaseFactory factory = new DatabaseFactory(databaseDirectory + File.separator + SOURCE_DB)) {
      try (final Database database = factory.create()) {
        database.getSchema().createDocumentType(TYPE_NAME);
        database.transaction(() -> {
          for (int i = 0; i < DOCUMENT_COUNT; i++)
            database.newDocument(TYPE_NAME).set("i", i).set("payload", "x".repeat(256)).save();
        });
        database.command("sql", "backup database file://" + ARCHIVE_NAME).close();
        database.drop();
      }
    }

    final File produced = new File("./target/backups/" + SOURCE_DB + "/" + ARCHIVE_NAME);
    assertThat(produced).exists();
    return produced;
  }

  /** Serves the archive whole, from the IPv4 loopback literal, on an OS-assigned port. */
  private HttpServer serveArchive(final File file) {
    try {
      final byte[] bytes = Files.readAllBytes(file.toPath());
      final HttpServer server = HttpServer.create(new InetSocketAddress(InetAddress.getByName(LOOPBACK), 0), 0);
      server.createContext("/" + ARCHIVE_NAME, exchange -> {
        exchange.sendResponseHeaders(200, bytes.length);
        try (final OutputStream out = exchange.getResponseBody()) {
          out.write(bytes);
        }
      });
      server.start();
      return server;
    } catch (final IOException e) {
      throw new RuntimeException("Cannot serve the test archive", e);
    }
  }

  /** An ephemeral port the OS has just handed out, released again immediately so the server can take it. */
  private static int freePort() {
    try (final ServerSocket socket = new ServerSocket(0, 1, InetAddress.getByName(LOOPBACK))) {
      return socket.getLocalPort();
    } catch (final IOException e) {
      throw new RuntimeException("Cannot reserve a free port for the test server", e);
    }
  }
}
