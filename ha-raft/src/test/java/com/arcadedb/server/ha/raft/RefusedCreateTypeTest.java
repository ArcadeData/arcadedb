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
package com.arcadedb.server.ha.raft;

import com.arcadedb.database.DatabaseFactory;
import com.arcadedb.database.DatabaseInternal;
import com.arcadedb.database.LocalDatabase;
import com.arcadedb.exception.CommandExecutionException;
import com.arcadedb.exception.SchemaException;
import com.arcadedb.schema.DocumentType;
import com.arcadedb.server.TestServerHelper;
import org.apache.ratis.protocol.ClientId;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Path;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Issue #13092, the follow-up of #12958 for CREATE DOCUMENT TYPE: the type name was registered in the dictionary, a commit of its
 * own, right after the type was built and before its buckets and super types. A refusal only the super type linkage can give
 * (here: the super type has an EXTERNAL property and the paired {@code <bucket>_ext} bucket is the primary bucket of another type)
 * takes the type back, but the dictionary page stayed in the schema session's buffer, and on a Raft leader the session read it as a
 * change left on this node only and quarantined the leader (UNPUBLISHED_SCHEMA_CHANGE). The name is now registered last, once
 * nothing can refuse any more, so a refused create leaves nothing to publish.
 *
 * @author robfrank (ro.franchini@gmail.com)
 */
class RefusedCreateTypeTest {
  @TempDir
  Path tempDir;

  private LocalDatabase      db;
  private ArcadeStateMachine stateMachine;

  @BeforeEach
  void setUp() {
    db = (LocalDatabase) new DatabaseFactory(tempDir.resolve("issue13092").toString()).create();
    stateMachine = new ArcadeStateMachine() {
      @Override
      DatabaseInternal databaseFor(final String databaseName) {
        return db;
      }

      @Override
      ClientId localClientId() {
        return null;
      }

      @Override
      long currentRaftTerm() {
        return 7L;
      }
    };
    // A super type with an EXTERNAL property: a sub type needs a paired <bucket>_ext bucket, which only the linkage looks at
    db.command("sql", "CREATE DOCUMENT TYPE Holder13092");
    db.command("sql", "CREATE PROPERTY Holder13092.blob STRING (EXTERNAL true)");
  }

  @AfterEach
  void tearDown() {
    if (db != null && db.isOpen())
      db.drop();
  }

  // Regression for #13092: the refused create through the schema API
  @Test
  void aCreateTypeTheSuperTypeLinkageRefusesDoesNotQuarantineTheLeader() {
    squat("Child13092_0_ext");
    final FakeRaftTransactionBroker broker = new FakeRaftTransactionBroker();
    final RaftReplicatedDatabase replicated = replicated(leader(broker));

    assertThatThrownBy(() -> replicated.getSchema().buildDocumentType().withName("Child13092").withSuperType("Holder13092").create())
        .isInstanceOf(SchemaException.class).hasMessageContaining("Child13092_0_ext");

    assertNothingLeftBehind(broker, "Child13092");
  }

  /**
   * The statement creates the type and links the super types after it in one {@code bulkChange} frame (#10147), which publishes what
   * it applied even when a step refuses: the create and its take-back, and the dictionary page, travel in that one entry, so every
   * node holds the same thing and the leader has nothing of its own.
   */
  // Regression for #13092: the refused create as SQL
  @Test
  void createDocumentTypeExtendsRefusedByTheLinkageDoesNotQuarantineTheLeader() {
    squat("Sql13092_0_ext");
    final FakeRaftTransactionBroker broker = new FakeRaftTransactionBroker();
    final RaftReplicatedDatabase replicated = replicated(leader(broker));

    assertThatThrownBy(() -> replicated.command("sql", "CREATE DOCUMENT TYPE Sql13092 EXTENDS Holder13092"))
        .isInstanceOfAny(SchemaException.class, CommandExecutionException.class).hasStackTraceContaining("Sql13092_0_ext");

    assertThat(stateMachine.isDatabaseDiverged(db.getName())).as("a refused CREATE TYPE does not quarantine the leader").isFalse();
    assertThat(stateMachine.quarantineCause(db.getName())).isNull();
    assertThat(broker.calls("replicateSchema")).as("the statement's one frame is published, take-back included").hasSize(1);
    assertThat(db.getFileManager().getRecordedChanges()).as("the recording session is released").isNull();
    assertThat(db.getSchema().existsType("Sql13092")).isFalse();
    assertThat(db.getSchema().getType("Holder13092").getSubTypes()).isEmpty();
  }

  // Regression for #13092: a rename of a document type registers the new name only once the rename is complete
  @Test
  void aRenamedDocumentTypeRegistersItsNewName() {
    db.getSchema().createDocumentType("Old13092");
    final FakeRaftTransactionBroker broker = new FakeRaftTransactionBroker();
    final RaftReplicatedDatabase replicated = replicated(leader(broker));

    replicated.getSchema().getType("Old13092").rename("New13092");

    assertThat(db.getSchema().existsType("New13092")).isTrue();
    assertThat(db.getSchema().getDictionary().getIdByName("New13092", false)).isGreaterThanOrEqualTo(0);
    assertThat(stateMachine.isDatabaseDiverged(db.getName())).isFalse();
  }

  // Regression for #13092: the name is still registered when the create goes through, so an embedded document of the type works
  @Test
  void anAcceptedCreateTypeRegistersItsNameAndIsPublished() {
    final FakeRaftTransactionBroker broker = new FakeRaftTransactionBroker();
    final RaftReplicatedDatabase replicated = replicated(leader(broker));

    final DocumentType created = replicated.getSchema().buildDocumentType().withName("Ok13092").withSuperType("Holder13092").create();

    assertThat(created.getSuperTypes()).extracting(DocumentType::getName).containsExactly("Holder13092");
    assertThat(db.getSchema().getDictionary().getIdByName("Ok13092", false)).isGreaterThanOrEqualTo(0);
    assertThat(broker.calls("replicateSchema")).as("the create is published").hasSize(1);
    assertThat(stateMachine.isDatabaseDiverged(db.getName())).isFalse();
  }

  /** Makes {@code bucketName}, the paired external bucket the new type would adopt, the primary bucket of another type. */
  private void squat(final String bucketName) {
    db.command("sql", "CREATE BUCKET " + bucketName);
    db.command("sql", "CREATE DOCUMENT TYPE Squatter_" + bucketName + " BUCKET " + bucketName);
  }

  private void assertNothingLeftBehind(final FakeRaftTransactionBroker broker, final String typeName) {
    assertThat(stateMachine.isDatabaseDiverged(db.getName())).as("a refused CREATE TYPE does not quarantine the leader").isFalse();
    assertThat(stateMachine.quarantineCause(db.getName())).isNull();
    assertThat(broker.calls("replicateSchema")).as("a refused statement publishes nothing").isEmpty();
    assertThat(db.getFileManager().getRecordedChanges()).as("the recording session is released").isNull();
    assertThat(db.getSchema().existsType(typeName)).as("type %s", typeName).isFalse();
    assertThat(db.getSchema().getDictionary().getIdByName(typeName, false)).as("dictionary entry of %s", typeName).isEqualTo(-1);
    assertThat(db.getSchema().getType("Holder13092").getSubTypes()).isEmpty();
  }

  private FakeRaftHAServer leader(final FakeRaftTransactionBroker broker) {
    final FakeRaftHAServer raft = FakeRaftHAServer.detached().stateMachine(stateMachine).leader(true).currentTerm(7L)
        .transactionBroker(broker);
    stateMachine.setRaftHAServer(raft);
    return raft;
  }

  private RaftReplicatedDatabase replicated(final FakeRaftHAServer raft) {
    return new RaftReplicatedDatabase(TestServerHelper.unstartedServer(), db, raft);
  }
}
