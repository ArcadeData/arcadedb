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
package com.arcadedb.query.sql.executor;

import com.arcadedb.TestHelper;
import com.arcadedb.database.RID;
import com.arcadedb.exception.ConcurrentModificationException;
import com.arcadedb.exception.DuplicatedKeyException;
import com.arcadedb.graph.MutableVertex;
import com.arcadedb.graph.Vertex;
import com.arcadedb.schema.Schema;
import com.arcadedb.schema.Type;
import com.arcadedb.schema.VertexType;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Issue #7096, engine side: with {@code setAutoTransaction(true)} and no transaction open, {@code UPDATE} on a
 * vertex type failed with "Transaction not active" while the same statement on a document type ran. The Postgres
 * wire plugin runs every connection in that mode, which is how the asymmetry surfaced to JDBC and Spark clients.
 * <p>
 * Two things changed. {@code ImmutableVertex.modify()} pinned the vertex to the transaction's page image even when
 * no transaction was open, which is where the error came from; it now only does so inside a transaction, as
 * {@code ImmutableDocument.modify()} always did. And every SQL write statement now runs in one statement-level
 * implicit transaction instead of one per record, so an auto-committed multi-record statement is atomic.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class UpdateAutoTransactionIssue7096Test extends TestHelper {

  @BeforeEach
  void createTypes() {
    database.transaction(() -> {
      database.getSchema().createVertexType("Character");
      database.getSchema().createDocumentType("Note");
      database.command("sql", "CREATE VERTEX Character SET name = 'Napoleon'").close();
      database.command("sql", "INSERT INTO Note SET text = 'a'").close();
    });
    database.setAutoTransaction(true);
    assertThat(database.isTransactionActive()).isFalse();
  }

  @AfterEach
  void resetAutoTransaction() {
    database.setAutoTransaction(false);
  }

  @Test
  void vertexUpdateRunsUnderAutoTransaction() {
    database.command("sql", "UPDATE Character SET name = 'Bonaparte', aliases = ['Emperor'], attrs = {'nation':'France'} "
        + "WHERE name = 'Napoleon'").close();
    assertThat(database.isTransactionActive()).as("the implicit transaction is committed by the statement").isFalse();

    try (final ResultSet rs = database.query("sql", "SELECT name, aliases, attrs FROM Character")) {
      final Result row = rs.next();
      assertThat(row.<String>getProperty("name")).isEqualTo("Bonaparte");
      assertThat(row.<Iterable<String>>getProperty("aliases")).containsExactly("Emperor");
      assertThat(row.<java.util.Map<String, Object>>getProperty("attrs")).containsEntry("nation", "France");
      assertThat(rs.hasNext()).isFalse();
    }
  }

  @Test
  void documentUpdateRunsUnderAutoTransactionAsBefore() {
    database.command("sql", "UPDATE Note SET text = 'b'").close();
    assertThat(database.isTransactionActive()).isFalse();

    try (final ResultSet rs = database.query("sql", "SELECT text FROM Note")) {
      assertThat(rs.next().<String>getProperty("text")).isEqualTo("b");
    }
  }

  /** Cypher SET goes through the same {@code modify()}, with no statement-level transaction of its own. */
  @Test
  void cypherSetOnAVertexRunsUnderAutoTransaction() {
    database.command("opencypher", "MATCH (n:Character {name: 'Napoleon'}) SET n.name = 'Bonaparte'").close();
    assertThat(database.isTransactionActive()).isFalse();

    try (final ResultSet rs = database.query("sql", "SELECT name FROM Character")) {
      assertThat(rs.next().<String>getProperty("name")).isEqualTo("Bonaparte");
    }
  }

  /** The Java API shape of the same defect: {@code modify()} on a vertex read outside any transaction. */
  @Test
  void vertexModifyAndSaveOutsideATransactionRunsUnderAutoTransaction() {
    final Vertex napoleon;
    try (final ResultSet rs = database.query("sql", "SELECT FROM Character")) {
      napoleon = rs.next().getVertex().get();
    }

    final MutableVertex mutable = napoleon.modify();
    mutable.set("name", "Bonaparte").save();
    assertThat(database.isTransactionActive()).isFalse();

    assertThat(database.lookupByRID(napoleon.getIdentity(), true).asVertex().getString("name")).isEqualTo("Bonaparte");
  }

  /** The read-then-write race that pinning guarded against (#6950) is still refused without the pin. */
  @Test
  void aVertexModifiedFromAReplacedImageIsStillRefused() throws Exception {
    final Vertex stale;
    try (final ResultSet rs = database.query("sql", "SELECT FROM Character")) {
      stale = rs.next().getVertex().get();
    }
    final MutableVertex mutable = stale.modify();

    final RID rid = stale.getIdentity();
    final ExecutorService other = Executors.newSingleThreadExecutor();
    try {
      other.submit(() -> database.transaction(
          () -> database.lookupByRID(rid, true).asVertex().modify().set("name", "Emperor").save())).get(60, TimeUnit.SECONDS);
    } finally {
      other.shutdownNow();
    }

    assertThatThrownBy(() -> mutable.set("name", "Bonaparte").save()).isInstanceOf(ConcurrentModificationException.class);
    assertThat(database.lookupByRID(rid, true).asVertex().getString("name")).isEqualTo("Emperor");
  }

  /** Autocommit is one transaction per statement, so a failure on the second record undoes the first. */
  @Test
  void anAutoCommittedMultiRecordUpdateIsAtomic() {
    database.transaction(() -> {
      final VertexType type = (VertexType) database.getSchema().getType("Character");
      type.createProperty("name", Type.STRING);
      type.createTypeIndex(Schema.INDEX_TYPE.LSM_TREE, true, "name");
      database.command("sql", "CREATE VERTEX Character SET name = 'Myriel'").close();
    });

    // The first record takes the name, the second one collides with it on the unique index.
    assertThatThrownBy(() -> database.command("sql", "UPDATE Character SET name = 'Anonymous'").close())
        .isInstanceOf(DuplicatedKeyException.class);
    assertThat(database.isTransactionActive()).as("the failed statement's implicit transaction is rolled back").isFalse();

    try (final ResultSet rs = database.query("sql", "SELECT name FROM Character ORDER BY name")) {
      assertThat(rs.next().<String>getProperty("name")).as("no record of the failed statement survives").isEqualTo("Myriel");
      assertThat(rs.next().<String>getProperty("name")).isEqualTo("Napoleon");
      assertThat(rs.hasNext()).isFalse();
    }
  }

  /**
   * {@code BATCH n} is the statement asking for chunked commits, so it is the one shape autocommit does not make atomic:
   * every chunk committed before the failure stays. With a per-record implicit transaction the clause had nothing to
   * chunk; now it commits the statement's transaction every {@code n} records, as it always did inside a caller's.
   */
  @Test
  void anAutoCommittedUpdateWithBatchCommitsEveryChunk() {
    database.transaction(() -> {
      final VertexType type = (VertexType) database.getSchema().getType("Character");
      type.createProperty("name", Type.STRING);
      type.createTypeIndex(Schema.INDEX_TYPE.LSM_TREE, true, "name");
      database.command("sql", "CREATE VERTEX Character SET name = 'Myriel'").close();
    });

    // Chunk 1 (the first record) is committed before chunk 2 collides with it on the unique index.
    assertThatThrownBy(() -> database.command("sql", "UPDATE Character SET name = 'Anonymous' BATCH 1").close())
        .isInstanceOf(DuplicatedKeyException.class);
    assertThat(database.isTransactionActive()).as("the failed chunk's transaction is rolled back").isFalse();

    try (final ResultSet rs = database.query("sql", "SELECT count(*) AS c FROM Character WHERE name = 'Anonymous'")) {
      assertThat(rs.next().<Long>getProperty("c")).as("the chunk committed before the failure survives").isEqualTo(1L);
    }
    try (final ResultSet rs = database.query("sql", "SELECT count(*) AS c FROM Character")) {
      assertThat(rs.next().<Long>getProperty("c")).isEqualTo(2L);
    }
  }

  @Test
  void deleteAndInsertRunUnderAutoTransactionToo() {
    database.command("sql", "INSERT INTO Character SET name = 'Myriel'").close();
    database.command("sql", "DELETE FROM Character WHERE name = 'Napoleon'").close();
    assertThat(database.isTransactionActive()).isFalse();

    try (final ResultSet rs = database.query("sql", "SELECT name FROM Character")) {
      assertThat(rs.next().<String>getProperty("name")).isEqualTo("Myriel");
      assertThat(rs.hasNext()).isFalse();
    }
  }

  /** A statement issued inside a caller's transaction joins it rather than committing on its own. */
  @Test
  void aStatementInsideACallerTransactionDoesNotCommitIt() {
    database.begin();
    try {
      database.command("sql", "UPDATE Character SET name = 'Bonaparte'").close();
      assertThat(database.isTransactionActive()).isTrue();
      database.rollback();
    } finally {
      if (database.isTransactionActive())
        database.rollback();
    }

    try (final ResultSet rs = database.query("sql", "SELECT name FROM Character")) {
      assertThat(rs.next().<String>getProperty("name")).isEqualTo("Napoleon");
    }
  }
}
