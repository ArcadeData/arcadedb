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
import com.arcadedb.database.MutableDocument;
import com.arcadedb.database.RID;
import com.arcadedb.event.BeforeRecordCreateListener;
import com.arcadedb.event.BeforeRecordUpdateListener;
import com.arcadedb.exception.RecordNotFoundException;
import com.arcadedb.exception.RecordVetoedException;
import com.arcadedb.graph.Edge;
import com.arcadedb.graph.MutableEdge;
import com.arcadedb.remote.RemoteDatabase;
import com.arcadedb.schema.DocumentType;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;

import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Issue #10161: a record update a server-side listener vetoed was answered over gRPC as a success. {@code saveRecord} discarded
 * the {@code success} flag of the reply, so {@code save()} returned normally, {@code updateRecord} was counted and the caller
 * believed the record was written. The HTTP driver raised a {@link RecordNotFoundException} for the same refusal, which named the
 * wrong cause. Every remote driver now raises {@link RecordVetoedException} from {@code save()} and answers {@code false} from
 * {@code saveReportingVeto()}, for a create and an update alike, and counts only the writes the server applied. Driven from one
 * list of drivers, so a driver cannot drift from the others again.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
public class Issue10161RemoteSaveVetoIT extends BaseGrpcClientServerTest {

  private static final String TYPE      = "Doc10161";
  private static final String VERTEX    = "V10161";
  private static final String EDGE      = "E10161";

  private RemoteGrpcServer           grpcServer;
  private RemoteGrpcDatabase         grpc;
  private RemoteDatabase             http;
  private BeforeRecordUpdateListener updateVeto;
  private BeforeRecordCreateListener createVeto;

  enum Driver {HTTP, GRPC}

  @Override
  public void setTestConfiguration() {
    super.setTestConfiguration();
    GlobalConfiguration.SERVER_PLUGINS.setValue("GRPC:com.arcadedb.server.grpc.GrpcServerPlugin");
  }

  @AfterEach
  @Override
  public void endTest() {
    // HERE AND NOT IN A SEPARATE @AfterEach: THE ORDER OF TWO OF THEM IN ONE CLASS IS UNDEFINED, AND super.endTest() DROPS THE DATABASE
    for (final String typeName : new String[] { TYPE, EDGE }) {
      final DocumentType type = getServerDatabase(0, getDatabaseName()).getSchema().getType(typeName);
      type.getEvents().unregisterListener(updateVeto);
      type.getEvents().unregisterListener(createVeto);
    }
    if (http != null)
      http.close();
    if (grpc != null)
      grpc.close();
    if (grpcServer != null)
      grpcServer.close();
    GlobalConfiguration.SERVER_PLUGINS.setValue("");
    super.endTest();
  }

  @BeforeEach
  void openAndPrepare() {
    grpcServer = new RemoteGrpcServer("localhost", getServerGrpcPort(), "root", DEFAULT_PASSWORD_FOR_TESTS, true, List.of());
    grpc = new RemoteGrpcDatabase(grpcServer, "localhost", getServerGrpcPort(), getServerHttpPort(), getDatabaseName(), "root",
        DEFAULT_PASSWORD_FOR_TESTS);
    http = new RemoteDatabase("localhost", getServerHttpPort(), getDatabaseName(), "root", DEFAULT_PASSWORD_FOR_TESTS);
    grpc.command("sql", "CREATE DOCUMENT TYPE `" + TYPE + "` IF NOT EXISTS");
    grpc.command("sql", "CREATE VERTEX TYPE `" + VERTEX + "` IF NOT EXISTS");
    grpc.command("sql", "CREATE EDGE TYPE `" + EDGE + "` IF NOT EXISTS");

    updateVeto = record -> !"refused".equals(record.asDocument().getString("v"));
    createVeto = record -> !"refused".equals(record.asDocument().getString("v"));
    for (final String typeName : new String[] { TYPE, EDGE }) {
      final DocumentType type = getServerDatabase(0, getDatabaseName()).getSchema().getType(typeName);
      type.getEvents().registerListener(updateVeto);
      type.getEvents().registerListener(createVeto);
    }
  }

  @ParameterizedTest
  @EnumSource(Driver.class)
  void aVetoedUpdateIsRaisedAndNotCounted(final Driver driver) {
    final RemoteDatabase db = db(driver);
    final RID rid = db.newDocument(TYPE).set("v", "original").save().getIdentity();
    final long updates = stat(db, "updateRecord");

    final MutableDocument doc = db.lookupByRID(rid).asDocument().modify();
    doc.set("v", "refused");
    final long reads = stat(db, "readRecord");
    final long exists = stat(db, "existsRecord");
    final long queries = stat(db, "queries");
    assertThatThrownBy(doc::save).isInstanceOf(RecordVetoedException.class);

    assertThat(stat(db, "updateRecord")).as("a refused update is not counted").isEqualTo(updates);
    // TELLING A VETO FROM A MISSING RECORD IS THE DRIVER'S OWN BUSINESS: NO READ THE CALLER DID NOT MAKE IS COUNTED
    assertThat(stat(db, "readRecord")).isEqualTo(reads);
    assertThat(stat(db, "existsRecord")).isEqualTo(exists);
    assertThat(stat(db, "queries")).isEqualTo(queries);
    assertThat(db.lookupByRID(rid).asDocument().getString("v")).as("the record was not written").isEqualTo("original");
  }

  @ParameterizedTest
  @EnumSource(Driver.class)
  void aVetoedUpdateIsReportedBySaveReportingVeto(final Driver driver) {
    final RemoteDatabase db = db(driver);
    final RID rid = db.newDocument(TYPE).set("v", "original").save().getIdentity();
    final long updates = stat(db, "updateRecord");

    final MutableDocument refused = db.lookupByRID(rid).asDocument().modify();
    refused.set("v", "refused");
    assertThat(refused.saveReportingVeto()).as("the update was vetoed").isFalse();
    assertThat(stat(db, "updateRecord")).isEqualTo(updates);

    final MutableDocument accepted = db.lookupByRID(rid).asDocument().modify();
    accepted.set("v", "changed");
    assertThat(accepted.saveReportingVeto()).as("the update was applied").isTrue();
    assertThat(stat(db, "updateRecord")).isEqualTo(updates + 1);
    assertThat(db.lookupByRID(rid).asDocument().getString("v")).isEqualTo("changed");
  }

  @ParameterizedTest
  @EnumSource(Driver.class)
  void aVetoedCreateIsRaisedAndNotCounted(final Driver driver) {
    final RemoteDatabase db = db(driver);
    final long creates = stat(db, "createRecord");

    final MutableDocument doc = db.newDocument(TYPE).set("v", "refused");
    assertThatThrownBy(doc::save).isInstanceOf(RecordVetoedException.class);
    assertThat(doc.saveReportingVeto()).as("the create was vetoed").isFalse();
    assertThat(doc.getIdentity()).isNull();

    assertThat(stat(db, "createRecord")).as("a refused create is not counted").isEqualTo(creates);
    assertThat(db.countType(TYPE, false)).isZero();
  }

  @ParameterizedTest
  @EnumSource(Driver.class)
  void aVetoedEdgeUpdateIsRaisedAndNotCounted(final Driver driver) {
    final RemoteDatabase db = db(driver);
    final RID edgeRid = db.command("sql", "CREATE EDGE " + EDGE + " FROM (CREATE VERTEX " + VERTEX + ") TO (CREATE VERTEX " + VERTEX
        + ") SET v = 'original'").next().getIdentity().get();
    final long updates = stat(db, "updateRecord");

    final MutableEdge refused = db.lookupByRID(edgeRid).asEdge().modify();
    refused.set("v", "refused");
    assertThatThrownBy(refused::save).isInstanceOf(RecordVetoedException.class);
    assertThat(refused.saveReportingVeto()).isFalse();
    assertThat(stat(db, "updateRecord")).as("a refused update is not counted").isEqualTo(updates);
    assertThat(db.lookupByRID(edgeRid).asEdge().getString("v")).isEqualTo("original");

    final MutableEdge accepted = db.lookupByRID(edgeRid).asEdge().modify();
    accepted.set("v", "changed");
    assertThat(accepted.saveReportingVeto()).isTrue();
    assertThat(stat(db, "updateRecord")).isEqualTo(updates + 1);
    final Edge reloaded = db.lookupByRID(edgeRid).asEdge();
    assertThat(reloaded.getString("v")).isEqualTo("changed");
    assertThat(reloaded.getOut()).as("the endpoints survive the update").isEqualTo(accepted.getOut());
    assertThat(reloaded.getIn()).isEqualTo(accepted.getIn());
  }

  @ParameterizedTest
  @EnumSource(Driver.class)
  void anUpdateOfADeletedRecordIsStillNotFound(final Driver driver) {
    final RemoteDatabase db = db(driver);
    final RID rid = db.newDocument(TYPE).set("v", "original").save().getIdentity();
    final MutableDocument doc = db.lookupByRID(rid).asDocument().modify();
    db.command("sql", "DELETE FROM " + rid);

    doc.set("v", "changed");
    assertThatThrownBy(doc::save).isInstanceOf(RecordNotFoundException.class);
  }

  @ParameterizedTest
  @EnumSource(Driver.class)
  void anUpdateOfADeletedEdgeIsStillNotFound(final Driver driver) {
    final RemoteDatabase db = db(driver);
    final RID edgeRid = db.command("sql", "CREATE EDGE " + EDGE + " FROM (CREATE VERTEX " + VERTEX + ") TO (CREATE VERTEX " + VERTEX
        + ") SET v = 'original'").next().getIdentity().get();
    final MutableEdge edge = db.lookupByRID(edgeRid).asEdge().modify();
    db.command("sql", "DELETE FROM " + edgeRid);

    edge.set("v", "changed");
    assertThatThrownBy(edge::save).isInstanceOf(RecordNotFoundException.class);
  }

  /** A save that changes nothing is still an applied update: it must never read as a veto. */
  @ParameterizedTest
  @EnumSource(Driver.class)
  void anUnchangedSaveIsAppliedAndNotReadAsAVeto(final Driver driver) {
    final RemoteDatabase db = db(driver);
    final RID rid = db.newDocument(TYPE).set("v", "original").save().getIdentity();
    final long updates = stat(db, "updateRecord");

    final MutableDocument unchanged = db.lookupByRID(rid).asDocument().modify();
    unchanged.save();
    assertThat(unchanged.saveReportingVeto()).isTrue();
    assertThat(stat(db, "updateRecord")).isEqualTo(updates + 2);
    assertThat(db.lookupByRID(rid).asDocument().getString("v")).isEqualTo("original");
  }

  /** Inside a remote transaction the veto is told apart in the caller's session, which sees the transaction's own records. */
  @ParameterizedTest
  @EnumSource(Driver.class)
  void aVetoInsideATransactionIsRaisedAndAMissingRecordIsNot(final Driver driver) {
    final RemoteDatabase db = db(driver);
    db.begin();
    try {
      // CREATED IN THIS TRANSACTION: ONLY THE TRANSACTION'S OWN SESSION CAN SEE IT
      final RID rid = db.newDocument(TYPE).set("v", "original").save().getIdentity();
      final MutableDocument refused = db.lookupByRID(rid).asDocument().modify();
      refused.set("v", "refused");
      assertThatThrownBy(refused::save).isInstanceOf(RecordVetoedException.class);

      final MutableDocument deleted = db.lookupByRID(rid).asDocument().modify();
      db.command("sql", "DELETE FROM " + rid);
      deleted.set("v", "changed");
      assertThatThrownBy(deleted::save).isInstanceOf(RecordNotFoundException.class);
    } finally {
      db.rollback();
    }
  }

  @ParameterizedTest
  @EnumSource(Driver.class)
  void anExistingEdgeStillCannotBeSavedIntoABucket(final Driver driver) {
    final RemoteDatabase db = db(driver);
    final RID edgeRid = db.command("sql", "CREATE EDGE " + EDGE + " FROM (CREATE VERTEX " + VERTEX + ") TO (CREATE VERTEX " + VERTEX
        + ") SET v = 'original'").next().getIdentity().get();
    final MutableEdge edge = db.lookupByRID(edgeRid).asEdge().modify();
    assertThatThrownBy(() -> edge.save(EDGE)).isInstanceOf(IllegalStateException.class);
  }

  private RemoteDatabase db(final Driver driver) {
    return driver == Driver.HTTP ? http : grpc;
  }

  private static long stat(final RemoteDatabase db, final String name) {
    return ((Number) db.getStats().get(name)).longValue();
  }
}
