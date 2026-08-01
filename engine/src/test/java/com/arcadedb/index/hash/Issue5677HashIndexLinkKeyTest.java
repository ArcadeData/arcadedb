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
package com.arcadedb.index.hash;

import com.arcadedb.TestHelper;
import com.arcadedb.database.RID;
import com.arcadedb.exception.DuplicatedKeyException;
import com.arcadedb.graph.MutableVertex;
import com.arcadedb.index.Index;
import com.arcadedb.index.IndexCursor;
import com.arcadedb.index.IndexInternal;
import com.arcadedb.query.sql.executor.ResultSet;
import com.arcadedb.schema.EdgeType;
import com.arcadedb.schema.Schema;
import com.arcadedb.schema.Type;
import com.arcadedb.schema.VertexType;
import com.arcadedb.serializer.BinaryTypes;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Regression for issue #5677: a HASH index whose key is a LINK (a plain {@code LINK} property, or the
 * {@code @out}/{@code @in} endpoints of an edge type) was accepted at creation and then blew up on the first
 * insert with "Unsupported key type ... the index metadata or a bucket page is corrupted".
 *
 * <p>{@code Type.LINK} serializes as {@code BinaryTypes.TYPE_RID} (14), which the hash bucket's key-size,
 * compare and validation paths did not know about - only {@code TYPE_COMPRESSED_RID} (13) was handled - so
 * every key-length computation over such an entry fell through to the "corrupted" branch.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class Issue5677HashIndexLinkKeyTest extends TestHelper {

  @Test
  void uniqueHashIndexOnEdgeEndpointsDeduplicatesEdges() {
    database.transaction(() -> {
      database.getSchema().createVertexType("Account");
      final EdgeType edgeType = database.getSchema().createEdgeType("INITIATED");
      database.getSchema().buildTypeIndex("INITIATED", new String[] { "@out", "@in" })//
          .withType(Schema.INDEX_TYPE.HASH).withUnique(true).create();
      assertThat(edgeType.getAllIndexes(false)).hasSize(1);
    });

    final RID[] endpoints = new RID[2];
    database.transaction(() -> {
      final MutableVertex hub = database.newVertex("Account").set("id", 0).save();
      final MutableVertex leaf = database.newVertex("Account").set("id", 1).save();
      hub.newEdge("INITIATED", leaf).save();
      endpoints[0] = hub.getIdentity();
      endpoints[1] = leaf.getIdentity();
    });

    // THE SAME PAIR MUST BE REJECTED BY THE UNIQUE CONSTRAINT
    assertThatThrownBy(() -> database.transaction(() -> {
      final MutableVertex hub = database.lookupByRID(endpoints[0], true).asVertex().modify();
      hub.newEdge("INITIATED", database.lookupByRID(endpoints[1], true).asVertex()).save();
    })).isInstanceOf(DuplicatedKeyException.class);

    // AND THE INDEX MUST ANSWER A LOOKUP ON THE ENDPOINT PAIR
    database.transaction(() -> {
      final Index index = database.getSchema().getIndexByName("INITIATED[@out,@in]");

      try (final IndexCursor found = index.get(new Object[] { endpoints[0], endpoints[1] })) {
        assertThat(found.hasNext()).isTrue();
        found.next();
        assertThat(found.hasNext()).isFalse();
      }

      // THE REVERSED PAIR IS A DIFFERENT KEY
      try (final IndexCursor reversed = index.get(new Object[] { endpoints[1], endpoints[0] })) {
        assertThat(reversed.hasNext()).isFalse();
      }

      assertThat(database.countType("INITIATED", false)).isEqualTo(1);
    });
  }

  @Test
  void uniqueHashIndexOnEdgeEndpointsSurvivesManyDistinctPairs() {
    final int vertices = 500;

    database.transaction(() -> {
      database.getSchema().createVertexType("Account");
      database.getSchema().createEdgeType("INITIATED");
      database.getSchema().buildTypeIndex("INITIATED", new String[] { "@out", "@in" })//
          .withType(Schema.INDEX_TYPE.HASH).withUnique(true).create();
    });

    final RID[] rids = new RID[vertices];
    database.transaction(() -> {
      for (int i = 0; i < vertices; i++)
        rids[i] = database.newVertex("Account").set("id", i).save().getIdentity();
    });

    // FAN OUT FROM VERTEX 0: ENOUGH ENTRIES TO FORCE SEVERAL DIRECTORY DOUBLINGS AND BUCKET SPLITS
    database.transaction(() -> {
      final MutableVertex hub = database.lookupByRID(rids[0], true).asVertex().modify();
      for (int i = 1; i < vertices; i++)
        hub.newEdge("INITIATED", database.lookupByRID(rids[i], true).asVertex()).save();
    });

    database.transaction(() -> {
      final Index index = database.getSchema().getIndexByName("INITIATED[@out,@in]");
      for (int i = 1; i < vertices; i++)
        try (final IndexCursor cursor = index.get(new Object[] { rids[0], rids[i] })) {
          assertThat(cursor.hasNext()).as("missing entry for pair (0,%d)", i).isTrue();
        }

      assertThat(index.countEntries()).isEqualTo(vertices - 1L);
    });
  }

  @Test
  void uniqueHashIndexOnLinkPropertyEnforcesUniqueness() {
    database.transaction(() -> {
      final VertexType library = database.getSchema().createVertexType("Library");
      final VertexType book = database.getSchema().createVertexType("Book");
      book.createProperty("library", Type.LINK);
      database.getSchema().buildTypeIndex("Book", new String[] { "library" })//
          .withType(Schema.INDEX_TYPE.HASH).withUnique(true).create();
      assertThat(library.getName()).isEqualTo("Library");
    });

    final RID[] libraries = new RID[2];
    database.transaction(() -> {
      libraries[0] = database.newVertex("Library").set("name", "a").save().getIdentity();
      libraries[1] = database.newVertex("Library").set("name", "b").save().getIdentity();
      database.newVertex("Book").set("library", libraries[0]).save();
      database.newVertex("Book").set("library", libraries[1]).save();
    });

    assertThatThrownBy(() -> database.transaction(//
        () -> database.newVertex("Book").set("library", libraries[0]).save()))//
        .isInstanceOf(DuplicatedKeyException.class);

    database.transaction(() -> {
      final Index index = database.getSchema().getIndexByName("Book[library]");
      try (final IndexCursor cursor = index.get(new Object[] { libraries[1] })) {
        assertThat(cursor.hasNext()).isTrue();
      }
      // A VERTEX PASSED INSTEAD OF ITS RID MUST RESOLVE TO THE SAME KEY
      try (final IndexCursor cursor = index.get(new Object[] { database.lookupByRID(libraries[0], true) })) {
        assertThat(cursor.hasNext()).isTrue();
      }
    });
  }

  /**
   * A composite key where only SOME columns remap to a different storage encoding. The variable-width compressed RID
   * sits next to a variable-width STRING, in both orders, so a column whose declared and storage encodings disagree
   * has to be measured with the storage one for the following column to start at the right offset.
   */
  @Test
  void compositeKeyMixingALinkWithAStringIsReadBackAtTheRightOffsets() {
    database.transaction(() -> {
      database.getSchema().createVertexType("Library");

      final VertexType book = database.getSchema().createVertexType("Book");
      book.createProperty("library", Type.LINK);
      book.createProperty("title", Type.STRING);
      database.getSchema().buildTypeIndex("Book", new String[] { "library", "title" })//
          .withType(Schema.INDEX_TYPE.HASH).withUnique(true).create();

      // AND THE MIRROR ORDER, SO THE REMAPPED COLUMN IS THE TRAILING ONE TOO
      final VertexType loan = database.getSchema().createVertexType("Loan");
      loan.createProperty("borrower", Type.STRING);
      loan.createProperty("library", Type.LINK);
      database.getSchema().buildTypeIndex("Loan", new String[] { "borrower", "library" })//
          .withType(Schema.INDEX_TYPE.HASH).withUnique(true).create();
    });

    final RID[] libraries = new RID[2];
    // TITLES OF DELIBERATELY DIFFERENT LENGTHS: A MIS-MEASURED PRECEDING COLUMN WOULD MISALIGN THE STRING THAT FOLLOWS
    final String[] titles = { "a", "a much longer title than the first one", "" };

    database.transaction(() -> {
      libraries[0] = database.newVertex("Library").set("name", "a").save().getIdentity();
      libraries[1] = database.newVertex("Library").set("name", "b").save().getIdentity();

      for (final RID library : libraries)
        for (final String title : titles) {
          database.newVertex("Book").set("library", library).set("title", title).save();
          database.newVertex("Loan").set("borrower", title).set("library", library).save();
        }
    });

    database.transaction(() -> {
      final Index books = database.getSchema().getIndexByName("Book[library,title]");
      final Index loans = database.getSchema().getIndexByName("Loan[borrower,library]");

      for (final RID library : libraries)
        for (final String title : titles) {
          try (final IndexCursor cursor = books.get(new Object[] { library, title })) {
            assertThat(cursor.hasNext()).as("Book(%s, '%s')", library, title).isTrue();
            assertThat(cursor.next().asVertex().getString("title")).isEqualTo(title);
          }
          try (final IndexCursor cursor = loans.get(new Object[] { title, library })) {
            assertThat(cursor.hasNext()).as("Loan('%s', %s)", title, library).isTrue();
            assertThat(cursor.next().asVertex().<RID>get("library")).isEqualTo(library);
          }
        }

      assertThat(books.countEntries()).isEqualTo((long) libraries.length * titles.length);
      assertThat(loans.countEntries()).isEqualTo((long) libraries.length * titles.length);

      // SAME LINK, DIFFERENT STRING MUST NOT COLLIDE, AND NEITHER MUST THE REVERSE
      try (final IndexCursor cursor = books.get(new Object[] { libraries[0], "not indexed" })) {
        assertThat(cursor.hasNext()).isFalse();
      }
    });

    // AND THE UNIQUE CONSTRAINT STILL DISCRIMINATES ON THE LINK COLUMN ALONE
    assertThatThrownBy(() -> database.transaction(//
        () -> database.newVertex("Book").set("library", libraries[0]).set("title", titles[1]).save()))//
        .isInstanceOf(DuplicatedKeyException.class);
  }

  @Test
  void nonUniqueHashIndexOnLinkPropertyGroupsRecords() {
    database.transaction(() -> {
      database.getSchema().createVertexType("Library");
      database.getSchema().createVertexType("Book").createProperty("library", Type.LINK);
      database.getSchema().buildTypeIndex("Book", new String[] { "library" })//
          .withType(Schema.INDEX_TYPE.HASH).withUnique(false).create();
    });

    final RID[] libraries = new RID[2];
    database.transaction(() -> {
      libraries[0] = database.newVertex("Library").set("name", "a").save().getIdentity();
      libraries[1] = database.newVertex("Library").set("name", "b").save().getIdentity();
      for (int i = 0; i < 10; i++)
        database.newVertex("Book").set("library", libraries[i % 2]).set("id", i).save();
    });

    database.transaction(() -> {
      final Index index = database.getSchema().getIndexByName("Book[library]");
      for (final RID library : libraries) {
        int found = 0;
        try (final IndexCursor cursor = index.get(new Object[] { library })) {
          while (cursor.hasNext()) {
            cursor.next();
            ++found;
          }
        }
        assertThat(found).isEqualTo(5);
      }
    });
  }

  @Test
  void deletingAnEdgeRemovesItsEndpointKeyFromTheHashIndex() {
    database.transaction(() -> {
      database.getSchema().createVertexType("Account");
      database.getSchema().createEdgeType("INITIATED");
      database.getSchema().buildTypeIndex("INITIATED", new String[] { "@out", "@in" })//
          .withType(Schema.INDEX_TYPE.HASH).withUnique(true).create();
    });

    final RID[] endpoints = new RID[2];
    database.transaction(() -> {
      final MutableVertex hub = database.newVertex("Account").set("id", 0).save();
      final MutableVertex leaf = database.newVertex("Account").set("id", 1).save();
      hub.newEdge("INITIATED", leaf).save();
      endpoints[0] = hub.getIdentity();
      endpoints[1] = leaf.getIdentity();
    });

    database.transaction(() -> database.command("sql", "DELETE FROM INITIATED"));

    database.transaction(() -> {
      final Index index = database.getSchema().getIndexByName("INITIATED[@out,@in]");
      try (final IndexCursor cursor = index.get(new Object[] { endpoints[0], endpoints[1] })) {
        assertThat(cursor.hasNext()).isFalse();
      }

      // AND THE PAIR CAN BE RE-CREATED
      final MutableVertex hub = database.lookupByRID(endpoints[0], true).asVertex().modify();
      hub.newEdge("INITIATED", database.lookupByRID(endpoints[1], true).asVertex()).save();
    });
  }

  @Test
  void linkKeysSurviveAReopen() {
    database.transaction(() -> {
      database.getSchema().createVertexType("Library");
      database.getSchema().createVertexType("Book").createProperty("library", Type.LINK);
      database.getSchema().buildTypeIndex("Book", new String[] { "library" })//
          .withType(Schema.INDEX_TYPE.HASH).withUnique(false).create();
    });

    final RID[] libraries = new RID[2];
    database.transaction(() -> {
      libraries[0] = database.newVertex("Library").set("name", "a").save().getIdentity();
      libraries[1] = database.newVertex("Library").set("name", "b").save().getIdentity();
      for (int i = 0; i < 20; i++)
        database.newVertex("Book").set("library", libraries[i % 2]).set("id", i).save();
    });

    reopenDatabase();

    database.transaction(() -> {
      final IndexInternal index = (IndexInternal) database.getSchema().getIndexByName("Book[library]");
      // THE SCHEMA TYPE MUST BE REPORTED, NOT THE INTERNAL STORAGE ENCODING
      assertThat(index.getKeyTypes()).containsExactly(Type.LINK);
      assertThat(index.getBinaryKeyTypes()).containsExactly(BinaryTypes.TYPE_RID);
      assertThat(index.countEntries()).isEqualTo(20L);

      int found = 0;
      try (final IndexCursor cursor = index.get(new Object[] { libraries[0] })) {
        while (cursor.hasNext()) {
          cursor.next();
          ++found;
        }
      }
      assertThat(found).isEqualTo(10);
    });
  }

  @Test
  void sqlEqualityOnALinkPropertyUsesTheHashIndex() {
    database.transaction(() -> {
      database.getSchema().createVertexType("Library");
      database.getSchema().createVertexType("Book").createProperty("library", Type.LINK);
      database.getSchema().buildTypeIndex("Book", new String[] { "library" })//
          .withType(Schema.INDEX_TYPE.HASH).withUnique(false).create();
    });

    final RID[] libraries = new RID[1];
    database.transaction(() -> {
      libraries[0] = database.newVertex("Library").set("name", "a").save().getIdentity();
      for (int i = 0; i < 5; i++)
        database.newVertex("Book").set("library", libraries[0]).set("id", i).save();
    });

    database.transaction(() -> {
      // THE PLANNER MUST PICK THE HASH INDEX, OTHERWISE THE KEY COERCION BELOW WOULD NEVER BE EXERCISED
      assertThat(database.query("sql", "EXPLAIN SELECT FROM Book WHERE library = \"" + libraries[0] + "\"")//
          .next().<Object>getProperty("executionPlan").toString()).contains("FETCH FROM INDEX");

      // A RID PASSED AS A QUOTED STRING MUST BE COERCED TO THE DECLARED LINK KEY TYPE BEFORE THE LOOKUP
      try (final ResultSet rs = database.query("sql", "SELECT FROM Book WHERE library = \"" + libraries[0] + "\"")) {
        assertThat(rs.stream().count()).isEqualTo(5);
      }
      try (final ResultSet rs = database.query("sql", "SELECT FROM Book WHERE library = " + libraries[0])) {
        assertThat(rs.stream().count()).isEqualTo(5);
      }
    });
  }

  @Test
  void sqlCreatedEdgeEndpointHashIndexCanBeRebuilt() {
    database.command("sql", "CREATE VERTEX TYPE Account");
    database.command("sql", "CREATE EDGE TYPE INITIATED");
    database.command("sql", "CREATE INDEX ON INITIATED (`@out`, `@in`) UNIQUE_HASH");

    database.transaction(() -> {
      database.command("sql", "CREATE VERTEX Account SET id = 1");
      database.command("sql", "CREATE VERTEX Account SET id = 2");
      database.command("sql", "CREATE EDGE INITIATED FROM (SELECT FROM Account WHERE id = 1) "
          + "TO (SELECT FROM Account WHERE id = 2)");
    });

    // THE REBUILD REPOPULATES THE HASH BUCKET FROM THE STORED EDGES, EXERCISING THE KEY ENCODING AGAIN
    database.command("sql", "REBUILD INDEX *");

    database.transaction(() -> {
      final Index index = database.getSchema().getIndexByName("INITIATED[@out,@in]");
      assertThat(index.countEntries()).isEqualTo(1L);
    });

    assertThatThrownBy(() -> database.transaction(//
        () -> database.command("sql", "CREATE EDGE INITIATED FROM (SELECT FROM Account WHERE id = 1) "
            + "TO (SELECT FROM Account WHERE id = 2)")))//
        .isInstanceOf(DuplicatedKeyException.class);
  }

  @Test
  void hashIndexRejectsAnUnsupportedKeyTypeAtCreationTime() {
    database.transaction(() -> {
      database.getSchema().createDocumentType("Doc").createProperty("tags", Type.LIST);

      assertThatThrownBy(() -> database.getSchema().buildTypeIndex("Doc", new String[] { "tags" })//
          .withType(Schema.INDEX_TYPE.HASH).withUnique(false).create())//
          .hasMessageContaining("LIST")//
          .hasMessageContaining("HASH")//
          .hasMessageNotContainingAny("corrupt", "rebuild");

      assertThat(database.getSchema().getType("Doc").getAllIndexes(false)).isEmpty();
    });
  }
}
