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
import com.arcadedb.database.RID;
import com.arcadedb.event.BeforeRecordCreateListener;
import com.arcadedb.exception.RecordNotFoundException;
import com.arcadedb.exception.RecordVetoedException;
import com.arcadedb.graph.Edge;
import com.arcadedb.graph.MutableEdge;
import com.arcadedb.graph.Vertex;
import com.arcadedb.remote.RemoteDatabase;
import com.arcadedb.schema.DocumentType;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;

import java.util.List;
import java.util.NoSuchElementException;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.assertj.core.api.Assertions.catchThrowable;

/**
 * Issue #10373: a {@code vertex.newEdge(...)} a server-side before-create listener vetoed reached a remote caller as a bare
 * {@link java.util.NoSuchElementException}, where the embedded engine raises {@link RecordVetoedException}. The {@code CREATE EDGE}
 * statement answers no row for a vetoed edge, and the remote vertex read that answer with {@code next()} unguarded. Every remote
 * driver now raises {@link RecordVetoedException} naming the edge type and both endpoints, the message the engine builds. Driven
 * from one list of drivers, so a driver cannot drift from the others. The same shape in {@code moveTo}, which answered an empty
 * result with a {@link NullPointerException}, now raises {@link RecordNotFoundException}.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
public class Issue10373RemoteNewEdgeVetoIT extends BaseGrpcClientServerTest {

  private static final String VERTEX = "V10373";
  private static final String EDGE   = "E10373";
  private static final String TARGET = "T10373";

  private RemoteGrpcServer           grpcServer;
  private RemoteGrpcDatabase         grpc;
  private RemoteDatabase             http;
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
    // A SETUP THAT FAILED BEFORE THE EDGE TYPE OR THE LISTENER EXISTED MUST NOT KEEP THE SERVER UP AND THE PLUGIN SETTING SET
    try {
      if (createVeto != null && getServerDatabase(0, getDatabaseName()).getSchema().existsType(EDGE))
        getServerDatabase(0, getDatabaseName()).getSchema().getType(EDGE).getEvents().unregisterListener(createVeto);
      if (http != null)
        http.close();
      if (grpc != null)
        grpc.close();
      if (grpcServer != null)
        grpcServer.close();
    } finally {
      GlobalConfiguration.SERVER_PLUGINS.setValue("");
      super.endTest();
    }
  }

  @BeforeEach
  void openAndPrepare() {
    grpcServer = new RemoteGrpcServer("localhost", getServerGrpcPort(), "root", DEFAULT_PASSWORD_FOR_TESTS, true, List.of());
    grpc = new RemoteGrpcDatabase(grpcServer, "localhost", getServerGrpcPort(), getServerHttpPort(), getDatabaseName(), "root",
        DEFAULT_PASSWORD_FOR_TESTS);
    http = new RemoteDatabase("localhost", getServerHttpPort(), getDatabaseName(), "root", DEFAULT_PASSWORD_FOR_TESTS);
    grpc.command("sql", "CREATE VERTEX TYPE `" + VERTEX + "` IF NOT EXISTS");
    grpc.command("sql", "CREATE VERTEX TYPE `" + TARGET + "` IF NOT EXISTS");
    grpc.command("sql", "CREATE EDGE TYPE `" + EDGE + "` IF NOT EXISTS");

    createVeto = record -> !"refused".equals(record.asDocument().getString("v"));
    final DocumentType edgeType = getServerDatabase(0, getDatabaseName()).getSchema().getType(EDGE);
    edgeType.getEvents().registerListener(createVeto);
  }

  @ParameterizedTest
  @EnumSource(Driver.class)
  void aVetoedNewEdgeIsRaisedAsAVetoNamingTheEdgeAndBothEndpoints(final Driver driver) {
    final RemoteDatabase db = db(driver);
    final RID fromRid = newVertex(db);
    final RID toRid = newVertex(db);
    final Vertex from = db.lookupByRID(fromRid, true).asVertex();

    assertThatThrownBy(() -> from.newEdge(EDGE, toRid, "v", "refused")).isInstanceOf(RecordVetoedException.class)
        .hasMessageContaining(EDGE).hasMessageContaining(fromRid.toString()).hasMessageContaining(toRid.toString());

    assertThat(db.countType(EDGE, false)).as("nothing was stored").isZero();
    assertThat(db.lookupByRID(fromRid, true).asVertex().countEdges(Vertex.DIRECTION.OUT, EDGE)).isZero();
  }

  /**
   * An empty CREATE EDGE answer is read as a veto because every other refusal reaches the client as a server error: pinned here, so
   * a statement that fails for another reason (an edge type that does not exist) is never reported as a listener's veto.
   */
  @ParameterizedTest
  @EnumSource(Driver.class)
  void aRefusalThatIsNotAVetoIsNotReadAsOne(final Driver driver) {
    final RemoteDatabase db = db(driver);
    final RID fromRid = newVertex(db);
    final RID toRid = newVertex(db);
    final Vertex from = db.lookupByRID(fromRid, true).asVertex();

    assertThatThrownBy(() -> from.newEdge("NoSuchEdgeType10373", toRid)).isNotInstanceOf(RecordVetoedException.class)
        .isNotInstanceOf(NoSuchElementException.class);
  }

  /** The remote message is the one the embedded engine builds, so a caller reads the same refusal on every path. */
  @ParameterizedTest
  @EnumSource(Driver.class)
  void theRemoteVetoReadsAsTheEmbeddedOne(final Driver driver) {
    final RemoteDatabase db = db(driver);
    final RID fromRid = newVertex(db);
    final RID toRid = newVertex(db);

    final Throwable embedded = catchThrowable(() -> getServerDatabase(0, getDatabaseName()).transaction(
        () -> getServerDatabase(0, getDatabaseName()).lookupByRID(fromRid, true).asVertex().newEdge(EDGE, toRid, "v", "refused")));
    final Throwable remote = catchThrowable(() -> db.lookupByRID(fromRid, true).asVertex().newEdge(EDGE, toRid, "v", "refused"));

    assertThat(embedded).isInstanceOf(RecordVetoedException.class);
    assertThat(remote).isInstanceOf(RecordVetoedException.class).hasMessage(embedded.getMessage());
  }

  @ParameterizedTest
  @EnumSource(Driver.class)
  void anAcceptedNewEdgeIsStillCreated(final Driver driver) {
    final RemoteDatabase db = db(driver);
    final RID fromRid = newVertex(db);
    final RID toRid = newVertex(db);

    final MutableEdge edge = db.lookupByRID(fromRid, true).asVertex().newEdge(EDGE, toRid, "v", "accepted");

    assertThat(edge.getIdentity()).isNotNull();
    final Edge reloaded = db.lookupByRID(edge.getIdentity(), true).asEdge();
    assertThat(reloaded.getString("v")).isEqualTo("accepted");
    assertThat(reloaded.getOut()).isEqualTo(fromRid);
    assertThat(reloaded.getIn()).isEqualTo(toRid);
  }

  /** The veto is told apart in the caller's session too, so inside a remote transaction it is raised the same way. */
  @ParameterizedTest
  @EnumSource(Driver.class)
  void aVetoedNewEdgeInsideATransactionIsRaisedAndTheTransactionStaysUsable(final Driver driver) {
    final RemoteDatabase db = db(driver);
    db.begin();
    try {
      final RID fromRid = newVertex(db);
      final RID toRid = newVertex(db);
      final Vertex from = db.lookupByRID(fromRid, true).asVertex();

      assertThatThrownBy(() -> from.newEdge(EDGE, toRid, "v", "refused")).isInstanceOf(RecordVetoedException.class);

      final MutableEdge accepted = from.newEdge(EDGE, toRid, "v", "accepted");
      assertThat(accepted.getIdentity()).isNotNull();
    } finally {
      db.rollback();
    }
  }

  /**
   * Both remote vertices reach the server for a move. The immutable one called itself (a StackOverflowError) and the mutable one an
   * embedded database it does not have (a NullPointerException), and the bucket spelling the statement was built with was not one.
   */
  @ParameterizedTest
  @EnumSource(Driver.class)
  void aVertexIsMovedByTheServer(final Driver driver) {
    final RemoteDatabase db = db(driver);
    final RID rid = newVertex(db);

    final RID movedToType = db.lookupByRID(rid, true).asVertex().moveToType(TARGET);
    assertThat(movedToType).isNotEqualTo(rid);
    assertThat(db.lookupByRID(movedToType, true).asVertex().getTypeName()).isEqualTo(TARGET);
    assertThat(db.existsRecord(rid)).as("the old record is gone").isFalse();

    final RID movedBack = db.lookupByRID(movedToType, true).asVertex().modify().moveToType(VERTEX);
    assertThat(db.lookupByRID(movedBack, true).asVertex().getTypeName()).isEqualTo(VERTEX);

    final String bucketName = getServerDatabase(0, getDatabaseName()).getSchema().getType(TARGET).getBuckets(false).getLast().getName();
    final RID movedToBucket = db.lookupByRID(movedBack, true).asVertex().moveToBucket(bucketName);
    assertThat(db.lookupByRID(movedToBucket, true).asVertex().getTypeName()).isEqualTo(TARGET);
    assertThat(getServerDatabase(0, getDatabaseName()).getSchema().getBucketById(movedToBucket.getBucketId()).getName())
        .isEqualTo(bucketName);
  }

  /** {@code MOVE VERTEX} of a vertex that no longer exists answers no row: that is a missing record, not a null pointer. */
  @ParameterizedTest
  @EnumSource(Driver.class)
  void movingAMissingVertexIsRaisedAsAMissingRecord(final Driver driver) {
    final RemoteDatabase db = db(driver);
    final RID rid = newVertex(db);
    final Vertex vertex = db.lookupByRID(rid, true).asVertex();
    db.command("sql", "DELETE FROM " + rid);

    assertThatThrownBy(() -> vertex.moveToType(VERTEX)).isInstanceOf(RecordNotFoundException.class)
        .hasMessageContaining(rid.toString());
  }

  private RID newVertex(final RemoteDatabase db) {
    return db.command("sql", "CREATE VERTEX " + VERTEX).next().getIdentity().get();
  }

  private RemoteDatabase db(final Driver driver) {
    return driver == Driver.HTTP ? http : grpc;
  }
}
