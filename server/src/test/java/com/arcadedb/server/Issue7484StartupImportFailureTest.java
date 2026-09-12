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

    assertThat(server.isStarted())
        .as("startup must not report success for a default database whose import was refused")
        .isFalse();
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
