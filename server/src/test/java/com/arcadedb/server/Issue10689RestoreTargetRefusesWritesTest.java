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
import com.arcadedb.database.LocalDatabase;
import com.arcadedb.engine.ComponentFile;
import com.arcadedb.exception.DatabaseNotAvailableException;
import com.arcadedb.exception.ErrorCategory;
import com.arcadedb.exception.NeedRetryException;
import com.arcadedb.utility.FileUtils;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.File;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Issue #10689: a database an {@code overwrite} restore has claimed stayed open and registered for the whole download, and every
 * resolver served it from its lock-free fast path, above the check of the claim. A client writing in that window was acknowledged,
 * and the swap then deleted the directory its rows were in.
 * <p>
 * The claim now refuses every WRITE to the database under its name, in the database itself: reads go on (the readers of a live
 * database keep it until the swap, as #10533 decided), and every commit carrying changes and every schema change is answered "not
 * available", whichever way the client reached the database - a resolution made now, a handle a session resolved before the
 * restore began (a Postgres connection keeps one for its life), or a transaction begun before the claim and committed inside it.
 * The resolution alone could not have done that: a refusal at resolution is not seen by a handle resolved earlier.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class Issue10689RestoreTargetRefusesWritesTest extends BaseGraphServerTest {
  private static final String NAME = "Issue10689Target";

  private ArcadeDBServer server;

  @BeforeEach
  void createTarget() {
    server = getServer(0);
    final ServerDatabase created = server.createDatabase(NAME, ComponentFile.MODE.READ_WRITE);
    created.getSchema().createDocumentType("T");
    created.transaction(() -> created.command("sql", "INSERT INTO T SET v = 1"));
  }

  @AfterEach
  void dropTarget() {
    server.releaseDatabaseNameReservedForRestore(NAME);
    if (server.existsDatabase(NAME))
      server.dropLocalDatabase(server.getDatabase(NAME).getEmbedded(), NAME);
    FileUtils.deleteRecursively(new File(server.getConfiguration().getValueAsString(GlobalConfiguration.SERVER_DATABASE_DIRECTORY), NAME));
  }

  /** The report's own sequence: claimed while open and registered, resolved by a client, written to. */
  @Test
  void aClientWriteToTheOpenTargetOfARestoreIsRefused() {
    server.reserveDatabaseNameForRestore(NAME);

    for (final ServerDatabase resolved : new ServerDatabase[] { server.getDatabaseForClient(NAME), server.getDatabaseForRequest(NAME) }) {
      assertThat(count(resolved)).as("its readers keep it until the swap").isEqualTo(1L);

      assertThatThrownBy(() -> resolved.transaction(() -> resolved.command("sql", "INSERT INTO T SET v = 2")))
          .isInstanceOf(DatabaseNotAvailableException.class).hasMessageContaining("restore");
      assertThatThrownBy(() -> resolved.getSchema().createDocumentType("Other")).as("a schema change is a write too")
          .isInstanceOf(DatabaseNotAvailableException.class);
    }
    assertThat(count(server.getDatabaseForClient(NAME))).as("nothing was written").isEqualTo(1L);
  }

  /**
   * A handle resolved BEFORE the claim, as a Postgres session holds one for its life and a Bolt connection across its requests: no
   * check at resolution can reach it, the refusal in the database does.
   */
  @Test
  void aHandleResolvedBeforeTheRestoreBeganIsRefusedToo() {
    final ServerDatabase session = server.getDatabaseForClient(NAME);

    server.reserveDatabaseNameForRestore(NAME);

    assertThatThrownBy(() -> session.transaction(() -> session.command("sql", "INSERT INTO T SET v = 2")))
        .isInstanceOf(DatabaseNotAvailableException.class);
    assertThat(ErrorCategory.of(catchWrite(session))).as("what every wire protocol maps to its own 'not available'")
        .isEqualTo(ErrorCategory.UNAVAILABLE);
  }

  /** A transaction begun before the claim and committed inside it would be acknowledged and then destroyed just the same. */
  @Test
  void aTransactionBegunBeforeTheRestoreCannotCommitInsideIt() {
    final ServerDatabase session = server.getDatabaseForClient(NAME);
    session.begin();
    try {
      session.command("sql", "INSERT INTO T SET v = 2");

      server.reserveDatabaseNameForRestore(NAME);

      assertThatThrownBy(session::commit).isInstanceOf(DatabaseNotAvailableException.class);
    } finally {
      if (session.isTransactionActive())
        session.rollback();
    }
    assertThat(count(session)).isEqualTo(1L);
  }

  /** The operator's own step may open the target while the claim stands: what it opens refuses writes as well. */
  @Test
  void aTargetOpenedWhileTheRestoreHoldsItRefusesWrites() {
    server.getDatabase(NAME).getEmbedded().close();
    server.removeDatabase(NAME);

    server.reserveDatabaseNameForRestore(NAME);
    assertThatThrownBy(() -> server.getDatabaseForClient(NAME)).as("a client still cannot open it")
        .isInstanceOf(DatabaseNotAvailableException.class);

    final ServerDatabase opened = server.getDatabase(NAME);
    assertThat(count(opened)).isEqualTo(1L);
    assertThatThrownBy(() -> opened.transaction(() -> opened.command("sql", "INSERT INTO T SET v = 2")))
        .isInstanceOf(DatabaseNotAvailableException.class);
  }

  /** A restore that ends - failed, so the target is still the one it found - gives the database its writes back. */
  @Test
  void theEndOfTheRestoreGivesTheWritesBack() {
    server.reserveDatabaseNameForRestore(NAME);
    server.releaseDatabaseNameReservedForRestore(NAME);

    final ServerDatabase resolved = server.getDatabaseForClient(NAME);
    resolved.transaction(() -> resolved.command("sql", "INSERT INTO T SET v = 2"));
    assertThat(count(resolved)).isEqualTo(2L);
  }

  /**
   * The restore's refusal and the one an HA server puts on a database before its plugin wraps it (issue #8270) are lifted each by
   * its own owner: ending one must not end the other, in either order.
   */
  @Test
  void theRestoreRefusalAndAnotherOneAreLiftedIndependently() {
    final LocalDatabase local = (LocalDatabase) server.getDatabase(NAME).getEmbedded();
    final String other = "another operation refuses writes";
    try {
      local.refuseWrites(other);
      server.reserveDatabaseNameForRestore(NAME);

      local.acceptWrites(other);
      assertThat(local.getWriteRefusal()).as("the restore's refusal outlives the other one").isEqualTo(ArcadeDBServer.RESTORE_WRITE_REFUSAL);
      assertThatThrownBy(local::checkWritesAccepted).isInstanceOf(DatabaseNotAvailableException.class);

      local.refuseWrites(other);
      server.releaseDatabaseNameReservedForRestore(NAME);
      assertThat(local.getWriteRefusal()).as("the end of the restore leaves the other refusal in place").isEqualTo(other);
      assertThatThrownBy(local::checkWritesAccepted).isInstanceOf(NeedRetryException.class)
          .isNotInstanceOf(DatabaseNotAvailableException.class);
    } finally {
      local.acceptWrites(other);
    }
    assertThat(local.getWriteRefusal()).isNull();
  }

  private static long count(final ServerDatabase database) {
    return database.query("sql", "SELECT count(*) AS c FROM T").next().<Number>getProperty("c").longValue();
  }

  private static RuntimeException catchWrite(final ServerDatabase database) {
    try {
      database.transaction(() -> database.command("sql", "INSERT INTO T SET v = 3"));
    } catch (final RuntimeException e) {
      return e;
    }
    throw new AssertionError("the write was accepted");
  }
}
