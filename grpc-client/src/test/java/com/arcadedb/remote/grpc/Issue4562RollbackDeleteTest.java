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
package com.arcadedb.remote.grpc;

import com.arcadedb.GlobalConfiguration;
import com.arcadedb.database.Database;
import com.arcadedb.exception.RecordNotFoundException;
import com.arcadedb.graph.MutableVertex;
import com.arcadedb.query.sql.executor.ResultSet;
import com.arcadedb.remote.RemoteDatabase;
import com.arcadedb.server.BaseGraphServerTest;

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;

import java.util.List;
import java.util.Map;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Reproduces issue #4562: when a transaction fails on commit (e.g. unique key violation), any
 * record deleted inside the same transaction must NOT be deleted, because the whole transaction is
 * rolled back. This must behave identically on gRPC and HTTP.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
class Issue4562RollbackDeleteTest extends BaseGraphServerTest {

  static final String TYPE = "SimpleVertexEx";

  private RemoteGrpcServer   grpcServer;
  private RemoteGrpcDatabase grpc;
  private RemoteDatabase     httpDb;

  @Override
  public void setTestConfiguration() {
    super.setTestConfiguration();
    GlobalConfiguration.SERVER_PLUGINS.setValue("GRPC:com.arcadedb.server.grpc.GrpcServerPlugin");
  }

  @AfterEach
  @Override
  public void endTest() {
    GlobalConfiguration.SERVER_PLUGINS.setValue("");
    super.endTest();
  }

  @BeforeAll
  void ensureDatabaseExists() {
    grpcServer = new RemoteGrpcServer("localhost", 50051, "root", DEFAULT_PASSWORD_FOR_TESTS, true, List.of());
  }

  @AfterAll
  void teardownServer() {
    if (grpcServer != null)
      grpcServer.close();
  }

  @BeforeEach
  void open() {
    grpc = new RemoteGrpcDatabase(this.grpcServer, "localhost", 50051, 2480, getDatabaseName(), "root", DEFAULT_PASSWORD_FOR_TESTS);
    httpDb = new RemoteDatabase("localhost", 2480, getDatabaseName(), "root", DEFAULT_PASSWORD_FOR_TESTS);

    grpc.command("sql", "CREATE VERTEX TYPE `" + TYPE + "` IF NOT EXISTS", Map.of());
    grpc.command("sql", "CREATE PROPERTY `" + TYPE + "`.svex IF NOT EXISTS STRING", Map.of());
    grpc.command("sql", "CREATE PROPERTY `" + TYPE + "`.svuuid IF NOT EXISTS STRING", Map.of());
    grpc.command("sql", "CREATE INDEX IF NOT EXISTS ON `" + TYPE + "` (svuuid) UNIQUE", Map.of());
    grpc.command("sql", "DELETE FROM `" + TYPE + "`", Map.of());
  }

  @AfterEach
  void close() {
    if (grpc != null) {
      try {
        grpc.rollback();
      } catch (Throwable ignore) {
      }
      grpc.close();
    }
    if (httpDb != null) {
      try {
        httpDb.rollback();
      } catch (Throwable ignore) {
      }
      httpDb.close();
      httpDb = null;
    }
  }

  /**
   * The core of issue #4562: a record deleted inside a transaction that later fails on commit must
   * survive (the whole transaction is rolled back).
   */
  @Test
  void deleteIsRolledBackWhenCommitFailsOnUniqueViolation_grpc() {
    runScenario(grpc);
  }

  /**
   * Same scenario over HTTP, used as the reference behaviour to confirm gRPC parity.
   */
  @Test
  void deleteIsRolledBackWhenCommitFailsOnUniqueViolation_http() {
    runScenario(httpDb);
  }

  private void runScenario(final RemoteDatabase db) {
    // 1) create svt1 with a unique svuuid
    db.begin(Database.TRANSACTION_ISOLATION_LEVEL.REPEATABLE_READ);
    final MutableVertex svt1 = db.newVertex(TYPE);
    final String uuid1 = UUID.randomUUID().toString();
    svt1.set("svex", uuid1);
    svt1.set("svuuid", uuid1);
    svt1.save();
    db.commit();

    // 2) create svt2 (unique uuid2) and svToDelete, then commit
    db.begin(Database.TRANSACTION_ISOLATION_LEVEL.REPEATABLE_READ);
    final MutableVertex svt2 = db.newVertex(TYPE);
    final String uuid2 = UUID.randomUUID().toString();
    svt2.set("svex", uuid2);
    svt2.set("svuuid", uuid2);
    svt2.save();

    final MutableVertex svToDelete = db.newVertex(TYPE);
    svToDelete.set("svex", "ToDelete");
    svToDelete.save();
    final String toDelete = svToDelete.getIdentity().toString();
    db.commit();

    assertThat(recordExists(db, toDelete)).as("svToDelete must exist after second commit").isTrue();
    assertThat(countAll(db)).as("3 records after second commit").isEqualTo(3);

    // 3) generate a duplicate (svt2.svuuid = uuid1), create svt3, delete svToDelete, then commit -> must FAIL
    db.begin(Database.TRANSACTION_ISOLATION_LEVEL.REPEATABLE_READ);
    svt2.set("svuuid", uuid1);
    svt2.save();

    final MutableVertex svt3 = db.newVertex(TYPE);
    svt3.set("svex", "svt3");
    svt3.save();

    db.deleteRecord(svToDelete);

    boolean failed = false;
    try {
      db.commit();
    } catch (Exception ex) {
      failed = true;
    }

    assertThat(failed).as("commit must fail because of the unique key violation").isTrue();
    assertThat(db.isTransactionActive()).as("transaction must not be active after a failed commit").isFalse();

    // The whole transaction was rolled back, so the deleted record must still exist and the duplicate
    // update on svt2 must have been undone.
    assertThat(recordExists(db, toDelete)).as("svToDelete must still exist after the rolled-back transaction").isTrue();
    assertThat(countAll(db)).as("still 3 records after the rolled-back transaction").isEqualTo(3);
    assertThat(countByUuid(db, uuid1)).as("only svt1 must own uuid1 after rollback").isEqualTo(1);

    // 4) control: a delete inside a transaction that commits successfully MUST persist
    db.begin(Database.TRANSACTION_ISOLATION_LEVEL.REPEATABLE_READ);
    db.deleteRecord(db.lookupByRID(svToDelete.getIdentity(), false));
    db.commit();

    assertThat(recordExists(db, toDelete)).as("svToDelete must be gone after a successful delete+commit").isFalse();
    assertThat(countAll(db)).as("2 records after the successful delete").isEqualTo(2);
  }

  /**
   * Runs the full scenario from the issue (including the recovery block) on both transports and
   * asserts the resulting database state is identical, proving gRPC/HTTP parity end to end.
   */
  @Test
  void fullIssueScenarioHasParityBetweenGrpcAndHttp() {
    final long grpcCount = runFullIssueFlow(grpc);
    grpc.command("sql", "DELETE FROM `" + TYPE + "`", Map.of());
    final long httpCount = runFullIssueFlow(httpDb);

    assertThat(grpcCount).as("final record count must match between gRPC and HTTP").isEqualTo(httpCount);
  }

  /**
   * Mirrors the method posted in issue #4562 as closely as possible and returns the final record count.
   */
  private long runFullIssueFlow(final RemoteDatabase db) {
    db.begin(Database.TRANSACTION_ISOLATION_LEVEL.REPEATABLE_READ);
    final MutableVertex svt1 = db.newVertex(TYPE);
    final String uuid1 = UUID.randomUUID().toString();
    svt1.set("svex", uuid1);
    svt1.set("svuuid", uuid1);
    svt1.save();
    db.commit();

    db.begin(Database.TRANSACTION_ISOLATION_LEVEL.REPEATABLE_READ);
    final MutableVertex svt2 = db.newVertex(TYPE);
    final String uuid2 = UUID.randomUUID().toString();
    svt2.set("svex", uuid2);
    svt2.set("svuuid", uuid2);
    svt2.save();

    final MutableVertex svToDelete = db.newVertex(TYPE);
    svToDelete.set("svex", "ToDelete");
    svToDelete.save();
    db.commit();

    // generate a duplicate, then delete a record, then commit -> must FAIL and roll back the delete
    db.begin(Database.TRANSACTION_ISOLATION_LEVEL.REPEATABLE_READ);
    svt2.set("svuuid", uuid1);
    svt2.save();
    final MutableVertex svt3 = db.newVertex(TYPE);
    svt3.set("svex", "svt3");
    svt3.save();
    db.deleteRecord(svToDelete);
    try {
      db.commit();
    } catch (Exception ex) {
      // expected: duplicate key
    }

    // recovery: fix the duplicate and delete the record for good
    db.begin(Database.TRANSACTION_ISOLATION_LEVEL.REPEATABLE_READ);
    svt2.set("svuuid", uuid2);
    svt2.save();
    db.deleteRecord(db.lookupByRID(svToDelete.getIdentity(), false));
    try {
      db.commit();
    } catch (Exception ex) {
      // tolerate (kept identical between transports)
    }
    if (db.isTransactionActive())
      db.rollback();

    return countAll(db);
  }

  /**
   * Saving a record whose RID no longer exists (e.g. it was deleted, or created in a rolled-back transaction) must fail
   * with {@link RecordNotFoundException} on both gRPC and HTTP, matching the embedded engine (issue #4562).
   */
  @Test
  void saveOfDeletedRecordThrowsOnGrpc() {
    assertSaveOfDeletedRecordThrows(grpc);
  }

  @Test
  void saveOfDeletedRecordThrowsOnHttp() {
    assertSaveOfDeletedRecordThrows(httpDb);
  }

  /**
   * After a transaction is rolled back (here by a failed commit on a unique violation), a record created in that
   * transaction must have its identity reset to provisional, so re-saving the same in-memory object cleanly INSERTs a
   * new record instead of being treated as an update of a now-missing record. Must match the embedded engine on both
   * transports (issue #4562 follow-up).
   */
  @Test
  void recreateAfterFailedCommitInsertsCleanlyOnGrpc() {
    assertRecreateAfterFailedCommitInsertsCleanly(grpc);
  }

  @Test
  void recreateAfterFailedCommitInsertsCleanlyOnHttp() {
    assertRecreateAfterFailedCommitInsertsCleanly(httpDb);
  }

  private void assertRecreateAfterFailedCommitInsertsCleanly(final RemoteDatabase db) {
    // seed: svt1 owns uuid1
    db.begin(Database.TRANSACTION_ISOLATION_LEVEL.REPEATABLE_READ);
    final MutableVertex svt1 = db.newVertex(TYPE);
    final String uuid1 = UUID.randomUUID().toString();
    svt1.set("svex", uuid1);
    svt1.set("svuuid", uuid1);
    svt1.save();
    db.commit();

    // tx that fails on commit: svt2 duplicates uuid1, svt3 is a fresh record created in the same tx
    db.begin(Database.TRANSACTION_ISOLATION_LEVEL.REPEATABLE_READ);
    final MutableVertex svt2 = db.newVertex(TYPE);
    svt2.set("svex", "svt2");
    svt2.set("svuuid", uuid1); // duplicate -> commit fails
    svt2.save();

    final MutableVertex svt3 = db.newVertex(TYPE);
    svt3.set("svex", "svt3");
    svt3.set("svuuid", UUID.randomUUID().toString());
    svt3.save();
    assertThat(svt3.getIdentity()).as("svt3 has an optimistic RID before the failed commit").isNotNull();

    boolean failed = false;
    try {
      db.commit();
    } catch (Exception ex) {
      failed = true;
    }
    assertThat(failed).as("commit must fail on the unique violation").isTrue();

    // both records created in the rolled-back tx must be provisional again
    assertThat(svt2.getIdentity()).as("svt2 identity reset after the rolled-back commit").isNull();
    assertThat(svt3.getIdentity()).as("svt3 identity reset after the rolled-back commit").isNull();

    // re-saving the same svt3 object must cleanly INSERT, not fail with RecordNotFoundException
    db.begin(Database.TRANSACTION_ISOLATION_LEVEL.REPEATABLE_READ);
    svt3.save();
    assertThat(svt3.getIdentity()).as("svt3 gets a fresh RID on re-insert").isNotNull();
    db.commit();

    assertThat(countAll(db)).as("svt1 + re-inserted svt3").isEqualTo(2);
  }

  private void assertSaveOfDeletedRecordThrows(final RemoteDatabase db) {
    db.begin();
    final MutableVertex v = db.newVertex(TYPE);
    v.set("svex", "ghost");
    v.set("svuuid", UUID.randomUUID().toString());
    v.save();
    db.commit();

    // delete it for good
    db.begin();
    db.deleteRecord(db.lookupByRID(v.getIdentity(), false));
    db.commit();

    // re-saving the now-stale object must fail, not silently no-op
    db.begin();
    v.set("svex", "resurrected");
    assertThatThrownBy(v::save).isInstanceOf(RecordNotFoundException.class);
    db.rollback();
  }

  private boolean recordExists(final RemoteDatabase db, final String rid) {
    try (final ResultSet rs = db.query("sql", "SELECT FROM " + rid)) {
      return rs.hasNext();
    } catch (Exception e) {
      return false;
    }
  }

  private long countAll(final RemoteDatabase db) {
    try (final ResultSet rs = db.query("sql", "SELECT count(*) AS c FROM `" + TYPE + "`")) {
      return rs.hasNext() ? rs.next().<Number>getProperty("c").longValue() : 0;
    }
  }

  private long countByUuid(final RemoteDatabase db, final String uuid) {
    try (final ResultSet rs = db.query("sql", "SELECT count(*) AS c FROM `" + TYPE + "` WHERE svuuid = :u",
        Map.of("u", uuid))) {
      return rs.hasNext() ? rs.next().<Number>getProperty("c").longValue() : 0;
    }
  }
}
