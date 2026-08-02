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
package com.arcadedb.index;

import com.arcadedb.TestHelper;
import com.arcadedb.database.RID;
import com.arcadedb.engine.Component;
import com.arcadedb.exception.DatabaseMetadataException;
import com.arcadedb.exception.DuplicatedKeyException;
import com.arcadedb.exception.SchemaException;
import com.arcadedb.schema.Schema;
import com.arcadedb.schema.Type;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Regression tests for issue #5765: the manual index path.
 * <p>
 * Two defects, one on top of the other. Underneath, {@code ManualIndexBuilder} registered the new index under
 * {@code index instanceof PaginatedComponent} - a test that never matched, since {@code LSMTreeIndex} and
 * {@code HashIndex} WRAP their component - so the file stayed unknown to the schema and the creating commit
 * failed resolving it PAST the WAL append, fencing the database. Every manual index creation ended there.
 * <p>
 * Above it, the guarded branch carried the two defects issue #5675 removed from {@code TypeIndexBuilder}: a
 * request that merely differed in uniqueness DROPPED the existing index and rebuilt it, and the index kind was
 * not compared at all. A manual index is worse off than a type index there: its entries are not derived from any
 * record, so the drop destroys the only copy and no rebuild can bring them back.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
public class Issue5765ManualIndexIfNotExistsTest extends TestHelper {

  /**
   * Real record identities: a manual index entry is checked against the record it points at (a dangling one is
   * repaired instead of failing the write), so a fabricated RID would not exercise the unique path at all.
   */
  private RID RID_1;
  private RID RID_2;

  @Override
  public void beginTest() {
    database.transaction(() -> {
      database.getSchema().createDocumentType("Doc");
      RID_1 = database.newDocument("Doc").set("name", "one").save().getIdentity();
      RID_2 = database.newDocument("Doc").set("name", "two").save().getIdentity();
    });
  }

  /**
   * The fix itself, asserted directly rather than inferred from the tests below happening to run: the index's file is
   * resolvable through the schema by its id.
   * <p>
   * That resolution IS the failure this PR is about. {@code TransactionContext.commit2ndPhase} calls
   * {@code getFileById} for every file whose page count the transaction changed, and it does so AFTER the WAL append -
   * so an unregistered file did not fail the creation, it fenced the whole database for recovery. Asserting the
   * registration pins the cause; asserting the database still commits afterwards pins the consequence, since every
   * operation on a fenced database throws.
   */
  @Test
  void aManualIndexRegistersItsFileAndLeavesTheDatabaseUsable() {
    final IndexInternal index = (IndexInternal) database.getSchema().buildManualIndex("manualIdx",
        new Type[] { Type.STRING }).withType(Schema.INDEX_TYPE.LSM_TREE).withUnique(false).create();

    final Component component = index.getComponent();
    assertThat(database.getSchema().getEmbedded().getFileById(component.getFileId()))
        .as("the index's file must be resolvable by id, which is what the creating commit does past the WAL append")
        .isSameAs(component);

    // Not fenced: a fenced database refuses every operation, so a plain write that commits is the proof.
    database.transaction(() -> database.newDocument("Doc").set("name", "after").save());
    assertThat(database.countType("Doc", false)).isEqualTo(3L);

    // And the index itself still works after that unrelated write.
    database.transaction(() -> index.put(new Object[] { "a" }, new RID[] { RID_1 }));
    database.transaction(() -> assertThat(index.get(new Object[] { "a" }).next()).isEqualTo(RID_1));
  }

  /**
   * The floor everything else stands on: creating a manual index works at all, its entries are readable, and the
   * database is still usable afterwards.
   */
  @Test
  void aManualIndexCanBeCreatedAndRead() {
    final Index index = database.getSchema().buildManualIndex("manualIdx", new Type[] { Type.STRING })
        .withType(Schema.INDEX_TYPE.LSM_TREE).withUnique(false).create();

    database.transaction(() -> {
      index.put(new Object[] { "a" }, new RID[] { RID_1 });
      index.put(new Object[] { "b" }, new RID[] { RID_2 });
    });

    database.transaction(() -> {
      assertThat(index.get(new Object[] { "a" }).next()).isEqualTo(RID_1);
      assertThat(index.get(new Object[] { "b" }).next()).isEqualTo(RID_2);
      assertThat(index.get(new Object[] { "c" }).hasNext()).isFalse();
    });

    // A HASH manual index is the other kind that can be built without a type.
    final Index hashIndex = database.getSchema().buildManualIndex("manualHashIdx", new Type[] { Type.STRING })
        .withType(Schema.INDEX_TYPE.HASH).withUnique(true).create();

    database.transaction(() -> hashIndex.put(new Object[] { "a" }, new RID[] { RID_1 }));
    database.transaction(() -> assertThat(hashIndex.get(new Object[] { "a" }).next()).isEqualTo(RID_1));
  }

  /**
   * The uniqueness constraint of a manual index is enforced at commit. That check resolved the index's type name
   * to reach the polymorphic index covering the same properties - a manual index has neither, so it threw a
   * NullPointerException and no unique manual index could commit an entry.
   */
  @Test
  void aUniqueManualIndexEnforcesItsConstraint() {
    final Index index = database.getSchema().buildManualIndex("manualIdx", new Type[] { Type.STRING })
        .withType(Schema.INDEX_TYPE.LSM_TREE).withUnique(true).create();

    database.transaction(() -> index.put(new Object[] { "a" }, new RID[] { RID_1 }));

    assertThatThrownBy(() -> database.transaction(() -> index.put(new Object[] { "a" }, new RID[] { RID_2 })))
        .isInstanceOf(DuplicatedKeyException.class);

    // The refused write must leave the original entry alone.
    database.transaction(() -> assertThat(index.get(new Object[] { "a" }).next()).isEqualTo(RID_1));

    // A different key still goes in.
    database.transaction(() -> index.put(new Object[] { "b" }, new RID[] { RID_2 }));
    database.transaction(() -> assertThat(index.get(new Object[] { "b" }).next()).isEqualTo(RID_2));
  }

  /**
   * The index and its entries have to survive a close/reopen cycle: a manual index is reloaded from its file, and
   * nothing else in the schema holds a copy of what it contains.
   */
  @Test
  void aManualIndexSurvivesAReopen() {
    final Index index = database.getSchema().buildManualIndex("manualIdx", new Type[] { Type.STRING })
        .withType(Schema.INDEX_TYPE.LSM_TREE).withUnique(false).create();

    database.transaction(() -> index.put(new Object[] { "a" }, new RID[] { RID_1 }));

    reopenDatabase();

    assertThat(database.getSchema().existsIndex("manualIdx")).isTrue();
    final Index reloaded = database.getSchema().getIndexByName("manualIdx");
    assertThat(reloaded.getType()).isEqualTo(Schema.INDEX_TYPE.LSM_TREE);
    database.transaction(() -> assertThat(reloaded.get(new Object[] { "a" }).next()).isEqualTo(RID_1));
  }

  /**
   * The reported case: a NOTUNIQUE manual index exists and the guarded request asks for UNIQUE. The existing
   * index cannot provide the constraint, so the request must be reported - and above all the entries already
   * stored in it must survive, since nothing else holds them.
   */
  @Test
  void guardedUniqueOverNotUniqueKeepsTheIndexAndItsEntries() {
    final Index existing = database.getSchema().buildManualIndex("manualIdx", new Type[] { Type.STRING })
        .withType(Schema.INDEX_TYPE.LSM_TREE).withUnique(false).create();

    database.transaction(() -> {
      existing.put(new Object[] { "a" }, new RID[] { RID_1 });
      existing.put(new Object[] { "b" }, new RID[] { RID_2 });
    });

    assertThatThrownBy(() -> database.getSchema().buildManualIndex("manualIdx", new Type[] { Type.STRING })
        .withType(Schema.INDEX_TYPE.LSM_TREE).withUnique(true).withIgnoreIfExists(true).create())
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("manualIdx");

    assertThat(database.getSchema().existsIndex("manualIdx")).isTrue();
    final Index survivor = database.getSchema().getIndexByName("manualIdx");
    assertThat(survivor.isUnique()).isFalse();

    database.transaction(() -> {
      assertThat(survivor.get(new Object[] { "a" }).next()).isEqualTo(RID_1);
      assertThat(survivor.get(new Object[] { "b" }).next()).isEqualTo(RID_2);
    });
  }

  /**
   * The kind was not compared at all, so an index of one kind answered a request for another and the caller was
   * handed something it cannot use while being told the index it asked for exists.
   */
  @Test
  void guardedDifferentKindIsReported() {
    database.getSchema().buildManualIndex("manualIdx", new Type[] { Type.STRING })
        .withType(Schema.INDEX_TYPE.HASH).withUnique(false).create();

    assertThatThrownBy(() -> database.getSchema().buildManualIndex("manualIdx", new Type[] { Type.STRING })
        .withType(Schema.INDEX_TYPE.LSM_TREE).withUnique(false).withIgnoreIfExists(true).create())
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("manualIdx")
        .hasMessageContaining("HASH");

    assertThat(database.getSchema().getIndexByName("manualIdx").getType()).isEqualTo(Schema.INDEX_TYPE.HASH);
  }

  /**
   * A UNIQUE index indexes exactly the keys a NOTUNIQUE one would, so it already provides what a NOTUNIQUE
   * request asks for: the guarded statement is a no-op that must not weaken the existing constraint.
   */
  @Test
  void guardedNotUniqueOverUniqueIsANoOp() {
    final Index existing = database.getSchema().buildManualIndex("manualIdx", new Type[] { Type.STRING })
        .withType(Schema.INDEX_TYPE.LSM_TREE).withUnique(true).create();

    final Index returned = database.getSchema().buildManualIndex("manualIdx", new Type[] { Type.STRING })
        .withType(Schema.INDEX_TYPE.LSM_TREE).withUnique(false).withIgnoreIfExists(true).create();

    assertThat(returned).isSameAs(existing);
    assertThat(database.getSchema().getIndexByName("manualIdx").isUnique()).isTrue();
  }

  /**
   * The plain idempotent case: same kind, same uniqueness, the existing index comes back untouched with its
   * entries still in it.
   */
  @Test
  void guardedIdenticalRequestReturnsTheExistingIndex() {
    final Index existing = database.getSchema().buildManualIndex("manualIdx", new Type[] { Type.STRING })
        .withType(Schema.INDEX_TYPE.LSM_TREE).withUnique(false).create();

    database.transaction(() -> existing.put(new Object[] { "a" }, new RID[] { RID_1 }));

    final Index returned = database.getSchema().buildManualIndex("manualIdx", new Type[] { Type.STRING })
        .withType(Schema.INDEX_TYPE.LSM_TREE).withUnique(false).withIgnoreIfExists(true).create();

    assertThat(returned).isSameAs(existing);
    database.transaction(() -> assertThat(returned.get(new Object[] { "a" }).next()).isEqualTo(RID_1));
  }

  /**
   * An unguarded request over an existing name keeps refusing with the long-standing message, whether or not the
   * definition matches.
   */
  @Test
  void unguardedRequestOverAnExistingNameIsRefused() {
    database.getSchema().buildManualIndex("manualIdx", new Type[] { Type.STRING })
        .withType(Schema.INDEX_TYPE.LSM_TREE).withUnique(false).create();

    assertThatThrownBy(() -> database.getSchema().buildManualIndex("manualIdx", new Type[] { Type.STRING })
        .withType(Schema.INDEX_TYPE.LSM_TREE).withUnique(false).create())
        .isInstanceOf(SchemaException.class)
        .hasMessageContaining("already exists");

    assertThat(database.getSchema().existsIndex("manualIdx")).isTrue();
  }

  /**
   * There is no implicit replacement on this path: a type index can be rebuilt from its records, a manual index
   * cannot, so the opt-in that upgrades a type index is refused outright at the call that asks for it.
   */
  @Test
  void replaceIfIncompatibleIsRefusedOnAManualIndex() {
    assertThatThrownBy(() -> database.getSchema().buildManualIndex("manualIdx", new Type[] { Type.STRING })
        .withType(Schema.INDEX_TYPE.LSM_TREE).withUnique(true).withReplaceIfIncompatible(true))
        .isInstanceOf(UnsupportedOperationException.class)
        .hasMessageContaining("manualIdx");

    // Setting it to false is meaningless but harmless: it is already the default.
    assertThat(database.getSchema().buildManualIndex("manualIdx", new Type[] { Type.STRING })
        .withReplaceIfIncompatible(false)).isNotNull();
  }

  /**
   * An index kind that needs a type is refused by name instead of failing inside the factory with a
   * NullPointerException (FULL_TEXT, GEOSPATIAL) or a ClassCastException (the vector kinds).
   */
  @Test
  void indexKindsThatNeedATypeAreRefused() {
    for (final Schema.INDEX_TYPE indexType : new Schema.INDEX_TYPE[] { Schema.INDEX_TYPE.FULL_TEXT,
        Schema.INDEX_TYPE.GEOSPATIAL, Schema.INDEX_TYPE.LSM_VECTOR, Schema.INDEX_TYPE.LSM_SPARSE_VECTOR }) {
      assertThatThrownBy(() -> database.getSchema().buildManualIndex("manualIdx", new Type[] { Type.STRING })
          .withType(indexType).create())
          .isInstanceOf(IllegalArgumentException.class)
          .hasMessageContaining(indexType.name());

      assertThat(database.getSchema().existsIndex("manualIdx")).isFalse();
    }
  }

  /**
   * A builder with no index type is refused by name rather than dereferencing the missing type in the factory.
   */
  @Test
  void aMissingIndexKindIsReported() {
    assertThatThrownBy(
        () -> database.getSchema().buildManualIndex("manualIdx", new Type[] { Type.STRING }).withUnique(false).create())
        .isInstanceOf(DatabaseMetadataException.class)
        .hasMessageContaining("indexType");
  }

  /**
   * The index type reaches the factory: the deprecated {@code createManualIndex} overload takes one and used to
   * drop it on the floor, so every call through it failed on a null index type.
   */
  @Test
  void deprecatedCreateManualIndexHonoursTheIndexType() {
    final Index index = database.getSchema()
        .createManualIndex(Schema.INDEX_TYPE.LSM_TREE, true, "manualIdx", new Type[] { Type.STRING }, 4096, null);

    assertThat(index.getType()).isEqualTo(Schema.INDEX_TYPE.LSM_TREE);
    assertThat(index.isUnique()).isTrue();
  }
}
