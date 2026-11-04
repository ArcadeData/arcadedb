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
package com.arcadedb.remote;

import com.arcadedb.database.BasicDatabase;
import com.arcadedb.exception.ArcadeDBException;
import com.arcadedb.query.sql.executor.ResultSet;
import com.arcadedb.server.BaseGraphServerTest;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.function.Consumer;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Issue #11045: a failing statement reached an embedded caller as the engine's exception and a remote caller as a
 * {@link RemoteException} that was not an {@link ArcadeDBException}, so the same {@code catch (ArcadeDBException)} caught it
 * embedded and let it escape over the driver.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class Issue11045RemoteCommandExceptionTypeIT extends BaseGraphServerTest {
  private static final String DATABASE_NAME = "remote-database-11045";

  private RemoteDatabase remote;

  @Override
  protected boolean isCreateDatabases() {
    return false;
  }

  @Test
  void aFailingStatementRaisesTheSameTypeEmbeddedAndRemote() {
    final BasicDatabase embedded = getServer(0).getDatabaseForClient(DATABASE_NAME);

    assertSameType(embedded, db -> db.query("sql", "insert into Doc set a = 1").close());
    assertSameType(embedded, db -> db.query("sql", "select from bucket:nosuchbucket").close());
    assertSameType(embedded, db -> db.query("sql", "selekt from Doc").close());
    assertSameType(embedded, db -> db.query("sql", "select nosuchfn(1) from Doc").close());
    assertSameType(embedded, db -> db.query("nosuchlang", "whatever").close());
    assertSameType(embedded, db -> db.command("sql", "insert into NoSuchType set a = 1").close());
  }

  @Test
  void aPortableHandlerCatchesASyntaxErrorOverTheDriver() {
    boolean caught = false;
    try (final ResultSet ignored = remote.query("sql", "selekt from Doc")) {
      // NOT REACHED
    } catch (final ArcadeDBException e) {
      caught = true;
      assertThat(e.getMessage()).startsWith("SQL syntax error");
    }
    assertThat(caught).isTrue();
  }

  private void assertSameType(final BasicDatabase embedded, final Consumer<BasicDatabase> statement) {
    final RuntimeException embeddedFailure = failureOf(embedded, statement);
    final RuntimeException remoteFailure = failureOf(remote, statement);
    assertThat(embeddedFailure).isNotNull();
    assertThat(remoteFailure).as("remote failure of what embedded raises as %s", embeddedFailure)
        .isExactlyInstanceOf(embeddedFailure.getClass());
    // The server writes the message on one line
    assertThat(remoteFailure.getMessage()).isEqualToIgnoringWhitespace(embeddedFailure.getMessage());
  }

  private static RuntimeException failureOf(final BasicDatabase database, final Consumer<BasicDatabase> statement) {
    try {
      statement.accept(database);
      return null;
    } catch (final RuntimeException e) {
      return e;
    }
  }

  @BeforeEach
  public void beginTest() {
    super.beginTest();
    final RemoteServer server = new RemoteServer("127.0.0.1", getServerHttpPort(), "root", DEFAULT_PASSWORD_FOR_TESTS);
    if (!server.exists(DATABASE_NAME))
      server.create(DATABASE_NAME);
    remote = new RemoteDatabase("127.0.0.1", getServerHttpPort(), DATABASE_NAME, "root", DEFAULT_PASSWORD_FOR_TESTS);
    remote.command("sql", "CREATE DOCUMENT TYPE Doc");
  }

  @AfterEach
  public void endTest() {
    if (remote != null)
      remote.close();
    final RemoteServer server = new RemoteServer("127.0.0.1", getServerHttpPort(), "root", DEFAULT_PASSWORD_FOR_TESTS);
    if (server.exists(DATABASE_NAME))
      server.drop(DATABASE_NAME);
    super.endTest();
  }
}
