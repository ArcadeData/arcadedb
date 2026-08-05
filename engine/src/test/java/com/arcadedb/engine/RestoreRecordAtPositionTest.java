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
package com.arcadedb.engine;

import com.arcadedb.TestHelper;
import com.arcadedb.database.DatabaseInternal;
import com.arcadedb.database.MutableDocument;
import com.arcadedb.database.RID;
import com.arcadedb.exception.DatabaseOperationException;
import com.arcadedb.graph.MutableVertex;
import com.arcadedb.schema.VertexType;

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Regression tests for {@link LocalBucket#restoreRecordAtPosition(long, com.arcadedb.database.Record)}, the
 * low-level emergency repair primitive behind the production incident where a vertex record was deleted out of
 * band (skipping the graph cascade) while its edges survived, pointing at a RID that no longer exists.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class RestoreRecordAtPositionTest extends TestHelper {

  private static final String TYPE     = "RestoreTarget";
  private static final String DOC_TYPE = "RestoreDocTarget";

  @Override
  protected void beginTest() {
    database.transaction(() -> {
      final VertexType type = database.getSchema().createVertexType(TYPE);
      type.createProperty("name", com.arcadedb.schema.Type.STRING);
      database.getSchema().createDocumentType(DOC_TYPE);
    });
  }

  @Test
  void restoresAPlainDocumentTheSameWayAsAVertex() {
    // The primitive is record-kind agnostic: no graph logic is involved for a plain document restore.
    final DatabaseInternal db = (DatabaseInternal) database;
    final RID[] rid = new RID[1];

    database.transaction(() -> rid[0] = database.newDocument(DOC_TYPE).set("name", "original-doc").save().getIdentity());

    final LocalBucket bucket = (LocalBucket) db.getSchema().getBucketById(rid[0].getBucketId());
    database.transaction(() -> bucket.deleteRecord(rid[0]));
    assertThat(bucket.existsRecord(rid[0])).isFalse();

    database.transaction(() -> {
      final MutableDocument shell = database.newDocument(DOC_TYPE).set("name", "restored-doc");
      final RID restoredRid = bucket.restoreRecordAtPosition(rid[0].getPosition(), shell);
      assertThat(restoredRid).isEqualTo(rid[0]);
    });

    database.transaction(
        () -> assertThat(database.lookupByRID(rid[0], true).asDocument().getString("name")).isEqualTo("restored-doc"));
  }

  @Test
  void restoresARecordAtItsOriginalRidAfterARawDelete() {
    final DatabaseInternal db = (DatabaseInternal) database;
    final RID[] rid = new RID[1];

    database.transaction(() -> rid[0] = database.newVertex(TYPE).set("name", "original").save().getIdentity());

    // Raw, no-cascade delete - exactly what GraphDatabaseChecker's vertex arm and LocalBucket.check(fix=true) do:
    // deletes the ONE record's slot, nothing else.
    final LocalBucket bucket = (LocalBucket) db.getSchema().getBucketById(rid[0].getBucketId());
    database.transaction(() -> bucket.deleteRecord(rid[0]));

    assertThat(bucket.existsRecord(rid[0])).as("the slot must be free before restore").isFalse();

    database.transaction(() -> {
      final MutableVertex shell = database.newVertex(TYPE).set("name", "restored");
      final RID restoredRid = bucket.restoreRecordAtPosition(rid[0].getPosition(), shell);
      assertThat(restoredRid).isEqualTo(rid[0]);
    });

    database.transaction(() -> {
      assertThat(bucket.existsRecord(rid[0])).isTrue();
      assertThat(database.lookupByRID(rid[0], true).asVertex().getString("name")).isEqualTo("restored");
    });
  }

  @Test
  void refusesToOverwriteAnOccupiedSlot() {
    final DatabaseInternal db = (DatabaseInternal) database;
    final RID[] rid = new RID[1];

    database.transaction(() -> rid[0] = database.newVertex(TYPE).set("name", "still-here").save().getIdentity());

    final LocalBucket bucket = (LocalBucket) db.getSchema().getBucketById(rid[0].getBucketId());

    assertThatThrownBy(() -> database.transaction(() -> {
      final MutableVertex shell = database.newVertex(TYPE).set("name", "would-clobber");
      bucket.restoreRecordAtPosition(rid[0].getPosition(), shell);
    })).isInstanceOf(DatabaseOperationException.class).hasMessageContaining("occupied");

    // The live record must be completely untouched by the refused attempt.
    database.transaction(
        () -> assertThat(database.lookupByRID(rid[0], true).asVertex().getString("name")).isEqualTo("still-here"));
  }

  @Test
  void restoresAMultiPageRecordAtItsOriginalPosition() {
    final DatabaseInternal db = (DatabaseInternal) database;
    final RID[] rid = new RID[1];
    // A payload several pages long guarantees a multi-page (FIRST_CHUNK) record.
    final String bigData = "x".repeat(((LocalBucket) db.getSchema().getType(TYPE).getBuckets(false).get(0)).getPageSize() * 4);

    database.transaction(() -> {
      database.newVertex(TYPE).set("name", "seed").save();
      rid[0] = database.newVertex(TYPE).set("name", bigData).save().getIdentity();
    });

    final LocalBucket bucket = (LocalBucket) db.getSchema().getBucketById(rid[0].getBucketId());
    database.transaction(() -> bucket.deleteRecord(rid[0]));
    assertThat(bucket.existsRecord(rid[0])).isFalse();

    database.transaction(() -> {
      final MutableVertex shell = database.newVertex(TYPE).set("name", bigData);
      final RID restoredRid = bucket.restoreRecordAtPosition(rid[0].getPosition(), shell);
      assertThat(restoredRid).isEqualTo(rid[0]);
    });

    database.transaction(() -> {
      assertThat(bucket.existsRecord(rid[0])).isTrue();
      assertThat(database.lookupByRID(rid[0], true).asVertex().getString("name")).isEqualTo(bigData);
    });
  }

  @Test
  void restoresAMultiPageRecordAtPositionZeroToo() {
    // Position 0 (page 0, slot 0) of a bucket has a historical quirk: the space ALLOCATOR avoids ever CHOOSING it
    // for a multi-page record, because a continuation chunk's "next pointer" field uses the literal value 0 to
    // mean "no next chunk" - so a pointer that happened to target position 0 would be misread as chain-end. That
    // guard only fires when the allocator is free to pick a different slot for a CONTINUATION chunk (see
    // writeMultiPageRecord's own chunk-placement calls); it is never consulted for the HEAD/first chunk, whose
    // position is fixed by whatever RID the record already has - exactly the situation an UPDATE that grows an
    // existing position-0 record into a chunk chain is already in today, with no special-casing. This test proves
    // restoreRecordAtPosition is safe in the same way.
    final DatabaseInternal db = (DatabaseInternal) database;
    final int pageSize = ((LocalBucket) db.getSchema().getType(TYPE).getBuckets(false).get(0)).getPageSize();
    final String bigData = "x".repeat(pageSize * 4);

    final RID[] rid = new RID[1];
    database.transaction(() -> rid[0] = database.newVertex(TYPE).set("name", "position-zero-seed").save().getIdentity());
    assertThat(rid[0].getPosition()).isZero();

    final LocalBucket bucket = (LocalBucket) db.getSchema().getBucketById(rid[0].getBucketId());
    database.transaction(() -> bucket.deleteRecord(rid[0]));
    assertThat(bucket.existsRecord(rid[0])).isFalse();

    database.transaction(() -> {
      final RID restoredRid = bucket.restoreRecordAtPosition(rid[0].getPosition(), database.newVertex(TYPE).set("name", bigData));
      assertThat(restoredRid).isEqualTo(rid[0]);
    });

    database.transaction(
        () -> assertThat(database.lookupByRID(rid[0], true).asVertex().getString("name")).isEqualTo(bigData));
  }

  @Test
  void refusesWhenTheTargetPageDoesNotExist() {
    final DatabaseInternal db = (DatabaseInternal) database;
    final LocalBucket bucket;
    database.transaction(() -> {
      final RID seed = database.newVertex(TYPE).set("name", "seed").save().getIdentity();
      db.getSchema().getBucketById(seed.getBucketId());
    });
    bucket = (LocalBucket) db.getSchema().getType(TYPE).getBuckets(false).get(0);

    // A position far beyond any page this bucket has ever allocated.
    final long farPosition = 1_000_000L * bucket.getMaxRecordsInPage();

    assertThatThrownBy(() -> database.transaction(
        () -> bucket.restoreRecordAtPosition(farPosition, database.newVertex(TYPE).set("name", "nope")))).isInstanceOf(
        DatabaseOperationException.class).hasMessageContaining("does not exist");
  }
}
