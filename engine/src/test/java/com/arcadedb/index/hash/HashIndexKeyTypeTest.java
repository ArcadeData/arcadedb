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
import com.arcadedb.database.DatabaseInternal;
import com.arcadedb.database.RID;
import com.arcadedb.engine.ComponentFile;
import com.arcadedb.exception.DuplicatedKeyException;
import com.arcadedb.graph.MutableVertex;
import com.arcadedb.index.Index;
import com.arcadedb.index.IndexCursor;
import com.arcadedb.index.IndexException;
import com.arcadedb.index.IndexInternal;
import com.arcadedb.index.TypeIndex;
import com.arcadedb.index.lsm.LSMTreeIndexAbstract;
import com.arcadedb.query.sql.executor.ResultSet;
import com.arcadedb.schema.DocumentType;
import com.arcadedb.schema.EdgeType;
import com.arcadedb.schema.Schema;
import com.arcadedb.schema.Type;
import com.arcadedb.schema.VertexType;
import com.arcadedb.serializer.BinaryComparator;
import com.arcadedb.serializer.BinaryTypes;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.io.File;
import java.math.BigDecimal;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Hash index key type tests (#5677, #5724, #7767).
 */
class HashIndexKeyTypeTest extends TestHelper {
  // ===== Issue #5677 =====
  /*
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
  @Test
  @DisplayName("#5677 unique hash index on edge endpoints deduplicates edges")
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
  @DisplayName("#5677 unique hash index on edge endpoints survives many distinct pairs")
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
  @DisplayName("#5677 unique hash index on link property enforces uniqueness")
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
  @DisplayName("#5677 composite key mixing a link with a string is read back at the right offsets")
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
  @DisplayName("#5677 non unique hash index on link property groups records")
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
  @DisplayName("#5677 deleting an edge removes its endpoint key from the hash index")
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
  @DisplayName("#5677 link keys survive a reopen")
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
  @DisplayName("#5677 sql equality on a link property uses the hash index")
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
  @DisplayName("#5677 sql created edge endpoint hash index can be rebuilt")
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
  @DisplayName("#5677 hash index rejects an unsupported key type at creation time")
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

  // ===== Issue #5724 =====
  /*
   * Regression for issue #5724: the key-type refusal added by #5677 lived in {@code HashIndexFactoryHandler.create()},
   * which is not the only way to build a hash index - the creation constructor of {@link HashIndex} is public, so a
   * caller reaching it directly wrote a file whose declared key type the bucket cannot encode. The problem then only
   * surfaced on the next open, as {@code loadMetadata} reporting corruption of an index that was never valid.
   * <p>
   * The guard now sits in the bucket's creation constructor, so the direct path is refused too. The load constructor
   * does NOT run it: an index already on disk must keep opening, which is what the reopen test pins.
   */
  private static final String TYPE_NAME = "Doc";

  /**
   * The bypass the issue is about: constructing the index directly, without going through
   * {@code IndexFactory.createIndex()}. Before the guard moved into the constructor this returned a usable
   * {@link HashIndex} whose metadata page declared a key type no key could ever be encoded with.
   */
  @Test
  @DisplayName("#5724 direct construction refuses an unsupported key type")
  void directConstructionRefusesAnUnsupportedKeyType() {
    database.transaction(() -> {
      database.getSchema().createDocumentType(TYPE_NAME).createProperty("tags", Type.LIST);

      assertThatThrownBy(() -> newHashIndexDirectly("bypassed_list_key", new Type[] { Type.LIST }))//
          .isInstanceOf(IndexException.class)//
          .hasMessageContaining("LIST")//
          .hasMessageContaining("HASH")//
          .hasMessageNotContainingAny("corrupt", "rebuild");

      // the refusal must not register anything: the type is left without indexes
      assertThat(database.getSchema().getType(TYPE_NAME).getAllIndexes(false)).isEmpty();
      assertThat(database.getSchema().existsIndex("bypassed_list_key")).isFalse();
    });
  }

  /**
   * A composite key with a hole in it used to reach the constructor and die on {@code keyTypes[i].getBinaryType()}
   * with a bare {@link NullPointerException}. The guard names the problem instead.
   */
  @Test
  @DisplayName("#5724 direct construction refuses a key column without a type")
  void directConstructionRefusesAKeyColumnWithoutAType() {
    database.transaction(() -> {
      database.getSchema().createDocumentType(TYPE_NAME);

      assertThatThrownBy(() -> newHashIndexDirectly("bypassed_null_key", new Type[] { Type.STRING, null }))//
          .isInstanceOf(IndexException.class)//
          .hasMessageContaining("bypassed_null_key")//
          .hasMessageContaining("no type");
    });
  }

  /**
   * The constructor assigns {@code this.keyTypes} from the guard's return value, so the guard has to hand back the
   * very array it was given: a copy, or a null on the accepting path, would silently change what the index reports.
   */
  @Test
  @DisplayName("#5724 the guard returns the accepted key types unchanged")
  void theGuardReturnsTheAcceptedKeyTypesUnchanged() {
    final Type[] keyTypes = new Type[] { Type.STRING, Type.LINK };
    assertThat(HashIndexBucket.checkSupportedKeyTypes("accepted", keyTypes)).isSameAs(keyTypes);
  }

  /**
   * The upgrade case. The guard belongs to the creation constructor only: reopening a database whose hash indexes
   * were written by an earlier build must still work, so the load constructor must not consult it.
   */
  @Test
  @DisplayName("#5724 an existing index still opens after a reopen")
  void anExistingIndexStillOpensAfterAReopen() {
    database.transaction(() -> {
      database.getSchema().createVertexType("Library");
      database.getSchema().createVertexType("Book").createProperty("library", Type.LINK);
      database.getSchema().getType("Book").createProperty("title", Type.STRING);
      database.getSchema().buildTypeIndex("Book", new String[] { "library", "title" })//
          .withType(Schema.INDEX_TYPE.HASH).withUnique(true).create();
    });

    final RID[] library = new RID[1];
    database.transaction(() -> {
      library[0] = database.newVertex("Library").set("name", "central").save().getIdentity();
      for (int i = 0; i < 20; i++)
        database.newVertex("Book").set("library", library[0]).set("title", "title-" + i).save();
    });

    reopenDatabase();

    database.transaction(() -> {
      final IndexInternal index = (IndexInternal) database.getSchema().getIndexByName("Book[library,title]");
      assertThat(index.getKeyTypes()).containsExactly(Type.LINK, Type.STRING);
      assertThat(index.countEntries()).isEqualTo(20L);
      assertThat(index.checkIntegrity()).isEmpty();

      try (final IndexCursor cursor = index.get(new Object[] { library[0], "title-7" })) {
        assertThat(cursor.hasNext()).isTrue();
        assertThat(cursor.next().asVertex().getString("title")).isEqualTo("title-7");
      }
    });
  }

  /**
   * Builds a hash index the way the issue describes: straight through the public creation constructor, skipping
   * {@code HashIndexFactoryHandler} entirely.
   */
  private HashIndex newHashIndexDirectly(final String indexName, final Type[] keyTypes) {
    final DatabaseInternal db = (DatabaseInternal) database;
    return new HashIndex(db, indexName, true, db.getDatabasePath() + File.separator + indexName,
        ComponentFile.MODE.READ_WRITE, keyTypes, HashIndexBucket.DEF_PAGE_SIZE, LSMTreeIndexAbstract.NULL_STRATEGY.SKIP);
  }

  // ===== Issue #7767 =====
  /*
   * Regression test for issue #7767: the #7613 decimal-scale fix reached every comparator-based caller, and the HASH
   * index is not one. It serializes the key, hashes those bytes to route it and compares them RAW to settle equality,
   * and a {@link BigDecimal} serializes its scale - so {@code 5} and {@code 5.00} produced different bytes, a
   * different slot and a non-matching entry. A UNIQUE_HASH index on a DECIMAL property therefore did not enforce its
   * constraint across scales, and a lookup for one spelling of a number could not find a row stored under another.
   * <p>
   * Fixed by canonicalizing in {@code HashIndex.convertKeys()} - the single funnel every put/get/remove passes through
   * - with the rule that used to live privately inside the LSM index's bloom-filter guard and is now shared as
   * {@link BinaryComparator#canonicalizeForByteEquality}.
   * <p>
   * The LSM twin of each assertion is run alongside, as the control: it passed before this fix and must keep passing,
   * which is what identifies the defect as the site #7613 did not reach rather than a general property of DECIMAL
   * keys.
   *
   * @author Luca Garulli (l.garulli@arcadedata.com)
   */
  @Test
  @DisplayName("#7767 a unique hash index on a decimal refuses the same number spelled with another scale")
  void aUniqueHashIndexOnADecimalRefusesTheSameNumberSpelledWithAnotherScale() {
    assertScaleIsOneKey(Schema.INDEX_TYPE.HASH, "HashPrices");
  }

  @Test
  @DisplayName("#7767 the lsm twin behaves the same way")
  void theLsmTwinBehavesTheSameWay() {
    assertScaleIsOneKey(Schema.INDEX_TYPE.LSM_TREE, "LsmPrices");
  }

  /**
   * The upgrade consequence this fix carries, pinned rather than only described in the release note: an index
   * built on a type that already holds two scale-different spellings of one number is refused, because they ARE
   * one key. That is what "an existing UNIQUE_HASH index on a DECIMAL needs a rebuild, and the rebuild surfaces
   * whatever duplicates the constraint had been letting through" means in practice (code review on PR #7855).
   * <p>
   * NOT a regression test for the canonicalisation, and deliberately labelled so nobody later reads it as one: it
   * was checked against the unfixed {@code convertKeys} and passes there too, because the build-time duplicate
   * check catches the pair on its own. What it guards is the documented upgrade behaviour - that the rebuild an
   * operator is being told to perform REPORTS the duplicates rather than quietly admitting them.
   */
  @Test
  @DisplayName("#7767 building a unique hash index over rows that differ only in scale is refused")
  void buildingAUniqueHashIndexOverRowsThatDifferOnlyInScaleIsRefused() {
    assertDuplicatesAreSurfacedOnBuild(Schema.INDEX_TYPE.HASH, "HashRebuild");
  }

  @Test
  @DisplayName("#7767 the lsm twin refuses it too")
  void theLsmTwinRefusesItToo() {
    assertDuplicatesAreSurfacedOnBuild(Schema.INDEX_TYPE.LSM_TREE, "LsmRebuild");
  }

  @Test
  @DisplayName("#7767 canonicalization is shared and leaves everything else untouched")
  void canonicalizationIsSharedAndLeavesEverythingElseUntouched() {
    final Object five = new BigDecimal("5");
    assertThat(BinaryComparator.canonicalizeForByteEquality(new BigDecimal("5.00"))).isEqualTo(five);
    // Already canonical: the very same instance comes back, so the common path allocates nothing.
    assertThat(BinaryComparator.canonicalizeForByteEquality(five)).isSameAs(five);
    assertThat(BinaryComparator.canonicalizeForByteEquality("5.00")).isEqualTo("5.00");
    assertThat(BinaryComparator.canonicalizeForByteEquality((Object) null)).isNull();

    final Object[] untouched = { "a", 1L };
    assertThat(BinaryComparator.canonicalizeForByteEquality(untouched)).isSameAs(untouched);

    final Object[] composite = { "a", new BigDecimal("1.10") };
    final Object[] canonical = BinaryComparator.canonicalizeForByteEquality(composite);
    assertThat(canonical).isNotSameAs(composite);
    assertThat(canonical[1]).isEqualTo(new BigDecimal("1.1"));
    assertThat(composite[1]).as("the caller's array must not be rewritten").isEqualTo(new BigDecimal("1.10"));
  }

  private void assertDuplicatesAreSurfacedOnBuild(final Schema.INDEX_TYPE indexType, final String typeName) {
    database.transaction(() -> {
      final DocumentType type = database.getSchema().createDocumentType(typeName);
      type.createProperty("amount", Type.DECIMAL);

      // Written BEFORE the index exists, which is the state an index created before this fix left behind: two
      // rows the comparator has always called equal, sitting under a constraint that could not see them.
      database.newDocument(typeName).set("amount", new BigDecimal("5")).save();
      database.newDocument(typeName).set("amount", new BigDecimal("5.00")).save();
    });

    assertThatThrownBy(() -> database.getSchema().buildTypeIndex(typeName, new String[] { "amount" })
        .withType(indexType).withUnique(true).create())
        .as("%s: 5 and 5.00 are one key, so a UNIQUE index over both must refuse to build", indexType)
        .hasRootCauseInstanceOf(DuplicatedKeyException.class);

    assertThat(database.getSchema().existsIndex(typeName + "[amount]"))
        .as("a refused build must leave no index behind").isFalse();
  }

  private void assertScaleIsOneKey(final Schema.INDEX_TYPE indexType, final String typeName) {
    final TypeIndex[] holder = new TypeIndex[1];
    database.transaction(() -> {
      final DocumentType type = database.getSchema().createDocumentType(typeName);
      type.createProperty("amount", Type.DECIMAL);
      holder[0] = type.createTypeIndex(indexType, true, "amount");

      database.newDocument(typeName).set("amount", new BigDecimal("5")).save();
    });
    final TypeIndex index = holder[0];

    database.transaction(() -> {
      try (final IndexCursor byTheStoredSpelling = index.get(new Object[] { new BigDecimal("5") })) {
        assertThat(byTheStoredSpelling.hasNext()).isTrue();
      }
      try (final IndexCursor byAnotherScale = index.get(new Object[] { new BigDecimal("5.00") })) {
        assertThat(byAnotherScale.hasNext())
            .as("%s: 5.00 and 5 are one key to the comparator, so they must be one key to the index", indexType)
            .isTrue();
      }
    });

    assertThatThrownBy(() -> database.transaction(
        () -> database.newDocument(typeName).set("amount", new BigDecimal("5.00")).save()))
        .as("%s: a UNIQUE index must not admit the same number twice because it was spelled differently", indexType)
        .isInstanceOf(DuplicatedKeyException.class);

    assertThat(database.countType(typeName, false)).isEqualTo(1);
  }
}
