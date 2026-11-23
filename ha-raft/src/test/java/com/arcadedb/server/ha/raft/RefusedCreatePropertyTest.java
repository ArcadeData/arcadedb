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
import com.arcadedb.schema.Type;
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
 * Issue #12958: {@code CREATE PROPERTY Doc.f EMBEDDED OF <missing type>} registered the property name in the dictionary, which
 * commits a transaction of its own, and only then refused the OF type. On a Raft leader that commit sat in the schema session's
 * buffer with the session's callback failing, so the session read it as a change left on this node only and quarantined the leader
 * (UNPUBLISHED_SCHEMA_CHANGE): one mistyped statement handed leadership over and cost a whole-database resync. The OF type is now
 * checked before anything is registered, so a refused statement changes nothing and has nothing to publish.
 */
class RefusedCreatePropertyTest {
  @TempDir
  Path tempDir;

  private LocalDatabase      db;
  private ArcadeStateMachine stateMachine;

  @BeforeEach
  void setUp() {
    db = (LocalDatabase) new DatabaseFactory(tempDir.resolve("issue12958").toString()).create();
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
    db.getSchema().createDocumentType("Doc");
  }

  @AfterEach
  void tearDown() {
    if (db != null && db.isOpen())
      db.drop();
  }

  // Regression for #12958: the reported statement, through the schema API
  @Test
  void anEmbeddedPropertyOfAMissingTypeIsRefusedWithoutQuarantiningTheLeader() {
    final FakeRaftTransactionBroker broker = new FakeRaftTransactionBroker();
    final RaftReplicatedDatabase replicated = replicated(leader(broker));

    final DocumentType doc = replicated.getSchema().getType("Doc");
    assertThatThrownBy(() -> doc.createProperty("f12958", Type.EMBEDDED, "NoSuchType12958")).isInstanceOf(SchemaException.class)
        .hasMessageContaining("NoSuchType12958");

    assertNothingLeftBehind(broker, "f12958");
  }

  // Regression for #12958: the reported statement, as SQL
  @Test
  void createPropertyEmbeddedOfAMissingTypeIsRefusedWithoutQuarantiningTheLeader() {
    final FakeRaftTransactionBroker broker = new FakeRaftTransactionBroker();
    final RaftReplicatedDatabase replicated = replicated(leader(broker));

    assertThatThrownBy(() -> replicated.command("sql", "CREATE PROPERTY Doc.g12958 EMBEDDED OF NoSuchType12958"))
        .isInstanceOfAny(SchemaException.class, CommandExecutionException.class).hasStackTraceContaining("NoSuchType12958");

    assertNothingLeftBehind(broker, "g12958");
  }

  // Regression for #12958: the other OF-type properties are checked before the name is registered too
  @Test
  void aLinkListAndMapPropertyOfAMissingTypeAreRefusedWithoutQuarantiningTheLeader() {
    final FakeRaftTransactionBroker broker = new FakeRaftTransactionBroker();
    final RaftReplicatedDatabase replicated = replicated(leader(broker));
    final DocumentType doc = replicated.getSchema().getType("Doc");

    assertThatThrownBy(() -> doc.createProperty("link12958", Type.LINK, "NoSuchType12958")).isInstanceOf(SchemaException.class);
    assertThatThrownBy(() -> doc.createProperty("list12958", Type.LIST, "NoSuchType12958")).isInstanceOf(SchemaException.class);
    assertThatThrownBy(() -> doc.createProperty("map12958", Type.MAP, "NoSuchType12958")).isInstanceOf(SchemaException.class);

    assertNothingLeftBehind(broker, "link12958", "list12958", "map12958");
  }

  /** The statement users meant still works: the property is created and published once the type exists. */
  // Regression for #12958: a refused statement does not get in the way of the corrected one
  @Test
  void theCorrectedStatementIsPublished() {
    final FakeRaftTransactionBroker broker = new FakeRaftTransactionBroker();
    final RaftReplicatedDatabase replicated = replicated(leader(broker));
    final DocumentType doc = replicated.getSchema().getType("Doc");

    assertThatThrownBy(() -> doc.createProperty("f12958", Type.EMBEDDED, "Sub12958")).isInstanceOf(SchemaException.class);
    replicated.getSchema().createDocumentType("Sub12958");
    final int publishedBefore = broker.calls("replicateSchema").size();

    doc.createProperty("f12958", Type.EMBEDDED, "Sub12958");

    assertThat(doc.existsProperty("f12958")).isTrue();
    assertThat(doc.getProperty("f12958").getOfType()).isEqualTo("Sub12958");
    assertThat(broker.calls("replicateSchema")).as("the property is published").hasSize(publishedBefore + 1);
    assertThat(stateMachine.isDatabaseDiverged(db.getName())).isFalse();
  }

  private void assertNothingLeftBehind(final FakeRaftTransactionBroker broker, final String... propertyNames) {
    assertThat(stateMachine.isDatabaseDiverged(db.getName())).as("a refused CREATE PROPERTY does not quarantine the leader").isFalse();
    assertThat(stateMachine.quarantineCause(db.getName())).isNull();
    assertThat(broker.calls("replicateSchema")).as("a refused statement publishes nothing").isEmpty();
    assertThat(db.getFileManager().getRecordedChanges()).as("the recording session is released").isNull();
    for (final String name : propertyNames) {
      assertThat(db.getSchema().getType("Doc").existsProperty(name)).as("property %s", name).isFalse();
      assertThat(db.getSchema().getDictionary().getIdByName(name, false)).as("dictionary entry of %s", name).isEqualTo(-1);
    }
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
