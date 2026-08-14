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
import com.arcadedb.engine.UnreferencedFiles.MemoizedCount;
import com.arcadedb.schema.DocumentType;
import com.arcadedb.schema.Schema;
import com.arcadedb.schema.Type;

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Issue #6168, item 1: the unreferenced-file count is memoized behind a (file modification count, schema version)
 * gate, because the HA gauge that publishes it rebuilds the whole claimed-file set every 5 seconds for every open
 * replicated database.
 * <p>
 * A memoized diagnostic is only worth having if it cannot go stale, so the tests below are about the GATE and not
 * about the saving: one per half of it (a file appeared / a schema change stopped claiming one), each proving that
 * the OTHER half did not move, plus an end-to-end walk of a mutation sequence where the memoized answer has to agree
 * with a fresh one at every step.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class Issue6168MemoizedUnreferencedCountTest extends TestHelper {

  /** No underscore, so the derived-name rule can never attribute it to a type and call it claimed. */
  private static final String DETACHED_BUCKET = "detachedbucket";

  @Test
  void anUnchangedDatabaseIsAnsweredFromTheCache() {
    final MemoizedCount memoized = new MemoizedCount();

    assertThat(memoized.get(db())).as("a healthy database").isZero();
    assertThat(memoized.getRecomputations()).as("the first call has nothing cached").isEqualTo(1);

    for (int i = 0; i < 10; i++)
      assertThat(memoized.get(db())).isZero();

    assertThat(memoized.getRecomputations())
        .as("nothing changed, so the walk - one getFileIds() per index, each taking that index's read lock - must "
            + "not have run again")
        .isEqualTo(1);
  }

  /**
   * The first half of the gate. A standalone {@code CREATE BUCKET} leaves a file no type claims, and it is a FILE
   * that appeared, which is what {@code FileManager.getModificationCount()} answers for.
   */
  @Test
  void aNewFileInvalidatesTheEntry() {
    final MemoizedCount memoized = new MemoizedCount();
    assertThat(memoized.get(db())).isZero();

    database.getSchema().createBucket(DETACHED_BUCKET);
    try {
      assertThat(memoized.get(db())).as("the new file is claimed by nothing and must be counted").isEqualTo(1);
      assertThat(memoized.getRecomputations()).isEqualTo(2);
    } finally {
      database.getSchema().dropBucket(DETACHED_BUCKET);
    }

    assertThat(memoized.get(db())).as("and dropping it takes the count back down").isZero();
  }

  /**
   * The second half of the gate, and the one a file counter alone cannot cover: detaching a bucket from its type
   * creates and drops nothing, so the answer changes with NO file having moved. Asserting the file modification
   * count is unchanged across the mutation is what makes this a test of the schema-version half specifically.
   */
  @Test
  void aSchemaChangeThatStopsClaimingAFileInvalidatesTheEntry() {
    final MemoizedCount memoized = new MemoizedCount();

    final DocumentType type = database.getSchema().createDocumentType("Issue6168Owner");
    type.createProperty("id", Type.INTEGER);
    final Bucket detached = database.getSchema().createBucket(DETACHED_BUCKET);
    type.addBucket(detached);

    assertThat(memoized.get(db())).as("a bucket its type lists is claimed").isZero();

    final long filesBefore = db().getFileManager().getModificationCount();
    type.removeBucket(detached);
    assertThat(db().getFileManager().getModificationCount())
        .as("detaching a bucket creates and drops no file, so only the schema version can have moved")
        .isEqualTo(filesBefore);

    assertThat(memoized.get(db())).as("nothing claims the bucket any more").isEqualTo(1);
    assertThat(memoized.get(db())).isEqualTo(UnreferencedFiles.count(db()));

    database.getSchema().dropBucket(DETACHED_BUCKET);
    database.getSchema().dropType("Issue6168Owner");
  }

  /**
   * The property that actually matters, asserted over a sequence rather than at one point: whatever the gate does,
   * the memoized answer is the answer a fresh walk gives. A gate that missed a change would show up here as a
   * disagreement at the step after it.
   */
  @Test
  void theMemoizedAnswerAgreesWithAFreshWalkAtEveryStep() {
    final MemoizedCount memoized = new MemoizedCount();
    assertThatBothAgree(memoized);

    final DocumentType type = database.getSchema().createDocumentType("Issue6168Seq");
    type.createProperty("id", Type.INTEGER);
    assertThatBothAgree(memoized);

    type.createTypeIndex(Schema.INDEX_TYPE.LSM_TREE, true, "id");
    assertThatBothAgree(memoized);

    final Bucket detached = database.getSchema().createBucket(DETACHED_BUCKET);
    assertThatBothAgree(memoized);

    type.addBucket(detached);
    assertThatBothAgree(memoized);

    type.removeBucket(detached);
    assertThatBothAgree(memoized);

    database.getSchema().dropBucket(DETACHED_BUCKET);
    assertThatBothAgree(memoized);

    database.getSchema().dropType("Issue6168Seq");
    assertThatBothAgree(memoized);
  }

  private void assertThatBothAgree(final MemoizedCount memoized) {
    assertThat(memoized.get(db())).isEqualTo(UnreferencedFiles.count(db()));
  }

  /** The count reads the file manager and the schema registries, both of which live on the internal interface. */
  private DatabaseInternal db() {
    return (DatabaseInternal) database;
  }
}
