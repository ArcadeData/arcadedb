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
import com.arcadedb.exception.CommandExecutionException;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

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
class Issue7484StartupImportFailureTest extends StaticBaseServerTest {
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
