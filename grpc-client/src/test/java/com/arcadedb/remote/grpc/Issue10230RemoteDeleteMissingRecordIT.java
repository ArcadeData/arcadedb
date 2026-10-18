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
import com.arcadedb.database.Document;
import com.arcadedb.database.MutableDocument;
import com.arcadedb.database.RID;
import com.arcadedb.event.BeforeRecordDeleteListener;
import com.arcadedb.exception.RecordNotFoundException;
import com.arcadedb.exception.RecordVetoedException;
import com.arcadedb.graph.Edge;
import com.arcadedb.graph.Vertex;
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
 * Issue #10230: over HTTP {@code RemoteDatabase.deleteRecord()} of a record that no longer exists sent {@code delete from <rid>},
 * matched nothing and returned normally, while the embedded engine and the gRPC driver raise {@link RecordNotFoundException}. A
 * delete a before-delete listener vetoed returned normally too, on both drivers, the gRPC one even counting it as a delete. Every
 * remote driver now raises {@link RecordNotFoundException} for a missing record and {@link RecordVetoedException} for a vetoed
 * delete, as {@code save()} does for an update (issue #10161), from every way a remote record can be deleted. Driven from one list
 * of drivers, so a driver cannot drift from the others again.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
public class Issue10230RemoteDeleteMissingRecordIT extends BaseGrpcClientServerTest {

  private static final String TYPE   = "Doc10230";
  private static final String VERTEX = "V10230";
  private static final String EDGE   = "E10230";

  private RemoteGrpcServer           grpcServer;
  private RemoteGrpcDatabase         grpc;
  private RemoteDatabase             http;
  private BeforeRecordDeleteListener deleteVeto;

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
    for (final String typeName : new String[] { TYPE, VERTEX, EDGE })
      getServerDatabase(0, getDatabaseName()).getSchema().getType(typeName).getEvents().unregisterListener(deleteVeto);
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

    deleteVeto = record -> !"kept".equals(record.asDocument().getString("v"));
    for (final String typeName : new String[] { TYPE, VERTEX, EDGE }) {
      final DocumentType type = getServerDatabase(0, getDatabaseName()).getSchema().getType(typeName);
      type.getEvents().registerListener(deleteVeto);
    }
  }

  @ParameterizedTest
  @EnumSource(Driver.class)
  void aSecondDeleteOfTheSameRecordIsNotFoundAndNotCounted(final Driver driver) {
    final RemoteDatabase db = db(driver);
    final MutableDocument doc = db.newDocument(TYPE).set("v", "gone");
    doc.save();
    final long deletes = stat(db, "deleteRecord");

    db.deleteRecord(doc);
    assertThat(stat(db, "deleteRecord")).as("the server deleted the record").isEqualTo(deletes + 1);

    final long reads = stat(db, "readRecord");
    final long exists = stat(db, "existsRecord");
    final long queries = stat(db, "queries");
    assertThatThrownBy(() -> db.deleteRecord(doc)).isInstanceOf(RecordNotFoundException.class);
    assertThat(stat(db, "deleteRecord")).as("a delete that matched no record is not counted").isEqualTo(deletes + 1);
    // TELLING A VETO FROM A MISSING RECORD IS THE DRIVER'S OWN BUSINESS: NO READ THE CALLER DID NOT MAKE IS COUNTED
    assertThat(stat(db, "readRecord")).isEqualTo(reads);
    assertThat(stat(db, "existsRecord")).isEqualTo(exists);
    assertThat(stat(db, "queries")).isEqualTo(queries);
  }

  @ParameterizedTest
  @EnumSource(Driver.class)
  void aDeleteOfARecordDeletedBySomeoneElseIsNotFound(final Driver driver) {
    final RemoteDatabase db = db(driver);
    final RID rid = db.newDocument(TYPE).set("v", "gone").save().getIdentity();
    final Document handle = db.lookupByRID(rid).asDocument();
    db.command("sql", "DELETE FROM " + rid);

    assertThatThrownBy(handle::delete).isInstanceOf(RecordNotFoundException.class);
    assertThatThrownBy(() -> handle.modify().delete()).isInstanceOf(RecordNotFoundException.class);
  }

  @ParameterizedTest
  @EnumSource(Driver.class)
  void aVetoedDeleteIsRaisedAndNotCounted(final Driver driver) {
    final RemoteDatabase db = db(driver);
    final MutableDocument doc = db.newDocument(TYPE).set("v", "kept");
    final RID rid = doc.save().getIdentity();
    final long deletes = stat(db, "deleteRecord");

    assertThatThrownBy(() -> db.deleteRecord(doc)).isInstanceOf(RecordVetoedException.class);
    assertThat(stat(db, "deleteRecord")).as("a refused delete is not counted").isEqualTo(deletes);
    assertThat(db.existsRecord(rid)).as("the record was not deleted").isTrue();
  }

  /** A vertex and an edge are deleted through their own handles, which must not bypass the driver's delete. */
  @ParameterizedTest
  @EnumSource(Driver.class)
  void aVertexAndAnEdgeDeletedTwiceAreNotFound(final Driver driver) {
    final RemoteDatabase db = db(driver);
    final RID edgeRid = db.command("sql", "CREATE EDGE " + EDGE + " FROM (CREATE VERTEX " + VERTEX + ") TO (CREATE VERTEX " + VERTEX
        + ")").next().getIdentity().get();
    final Edge edge = db.lookupByRID(edgeRid).asEdge();
    final Vertex vertex = db.lookupByRID(edge.getOut()).asVertex();
    final long deletes = stat(db, "deleteRecord");

    edge.delete();
    assertThat(db.existsRecord(edgeRid)).isFalse();
    assertThatThrownBy(edge::delete).isInstanceOf(RecordNotFoundException.class);

    vertex.delete();
    assertThat(db.existsRecord(vertex.getIdentity())).isFalse();
    assertThatThrownBy(vertex::delete).isInstanceOf(RecordNotFoundException.class);
    assertThatThrownBy(() -> vertex.modify().delete()).isInstanceOf(RecordNotFoundException.class);

    assertThat(stat(db, "deleteRecord")).as("each handle counts the one delete the server performed").isEqualTo(deletes + 2);
  }

  @ParameterizedTest
  @EnumSource(Driver.class)
  void aVetoedVertexOrEdgeDeleteIsRaised(final Driver driver) {
    final RemoteDatabase db = db(driver);
    final RID edgeRid = db.command("sql", "CREATE EDGE " + EDGE + " FROM (CREATE VERTEX " + VERTEX + " SET v = 'kept') TO (CREATE VERTEX "
        + VERTEX + ") SET v = 'kept'").next().getIdentity().get();
    final Edge edge = db.lookupByRID(edgeRid).asEdge();
    final Vertex vertex = db.lookupByRID(edge.getOut()).asVertex();

    assertThatThrownBy(edge::delete).isInstanceOf(RecordVetoedException.class);
    assertThatThrownBy(vertex::delete).isInstanceOf(RecordVetoedException.class);
    assertThat(db.existsRecord(edgeRid)).isTrue();
    assertThat(db.existsRecord(vertex.getIdentity())).isTrue();
  }

  /** Inside a remote transaction the veto is told apart in the caller's session, which sees the transaction's own records. */
  @ParameterizedTest
  @EnumSource(Driver.class)
  void insideATransactionAVetoIsRaisedAndAMissingRecordIsNotFound(final Driver driver) {
    final RemoteDatabase db = db(driver);
    db.begin();
    try {
      // CREATED IN THIS TRANSACTION: ONLY THE TRANSACTION'S OWN SESSION CAN SEE IT
      final MutableDocument kept = db.newDocument(TYPE).set("v", "kept");
      kept.save();
      assertThatThrownBy(() -> db.deleteRecord(kept)).isInstanceOf(RecordVetoedException.class);

      final MutableDocument gone = db.newDocument(TYPE).set("v", "gone");
      gone.save();
      db.deleteRecord(gone);
      assertThatThrownBy(() -> db.deleteRecord(gone)).isInstanceOf(RecordNotFoundException.class);
    } finally {
      db.rollback();
    }
  }

  private RemoteDatabase db(final Driver driver) {
    return driver == Driver.HTTP ? http : grpc;
  }

  private static long stat(final RemoteDatabase db, final String name) {
    return ((Number) db.getStats().get(name)).longValue();
  }
}
