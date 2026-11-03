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
package com.arcadedb.index.vector;

import com.arcadedb.TestHelper;
import com.arcadedb.database.RID;
import com.arcadedb.index.IndexInternal;
import com.arcadedb.index.TypeIndex;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Issue #10993: a node that holds the vectors of an index created to take its dimensions from its first vector, but did not see
 * the size decided (a follower promoted by a failover: a commit of records ships no schema, so it never learned the size), has
 * to refuse a vector of another length as any sized index does, and not size the index a second time from it.
 */
class Issue10993SizeFromHeldVectorsBeforeAPutTest extends TestHelper {

  @Test
  void aVectorOfAnotherLengthIsRefusedByANodeThatDidNotSeeTheSizeDecided() {
    database.command("opencypher", "CREATE VECTOR INDEX emb FOR (n:Doc) ON n.embedding");
    database.transaction(() -> {
      for (int i = 0; i < 5; i++)
        database.newVertex("Doc").set("id", i, "embedding", new float[] { i + 1, 2, 3, 4 }).save();
    });

    final LSMVectorIndex index = indexOf();
    assertThat(index.getDimensions()).isEqualTo(4);
    // a follower that applied the replicated pages and never saw the put
    index.getMetadata().dimensions = 0;
    index.getMetadata().sizeOnFirstVector = true;

    assertThatThrownBy(() -> database.transaction(() -> database.newVertex("Doc").set("id", 100, "embedding", new float[] { 1, 2 }).save()))
        .hasStackTraceContaining("Vector dimension does not match index dimension 4");
    assertThat(index.getDimensions()).isEqualTo(4);

    database.transaction(() -> database.newVertex("Doc").set("id", 101, "embedding", new float[] { 1, 2, 3, 5 }).save());
    assertThat(index.getDimensions()).isEqualTo(4);
  }

  /**
   * Issue #11141: the batch entry point (commit replay, an index built over existing records) decides the size too, and must
   * derive it from the vectors the node holds before it lets the first vector of the batch decide.
   */
  @Test
  void aBatchOfAnotherLengthIsRefusedByANodeThatDidNotSeeTheSizeDecided() {
    database.command("opencypher", "CREATE VECTOR INDEX emb FOR (n:Doc) ON n.embedding");
    database.transaction(() -> {
      for (int i = 0; i < 5; i++)
        database.newVertex("Doc").set("id", i, "embedding", new float[] { i + 1, 2, 3, 4 }).save();
    });

    final LSMVectorIndex index = indexOf();
    final long before = index.countEntries();
    index.getMetadata().dimensions = 0;
    index.getMetadata().sizeOnFirstVector = true;

    index.putBatch(List.<Object[]>of(new Object[] { new float[] { 1, 2 } }), List.of(new RID(0, 999)));

    assertThat(index.getDimensions()).isEqualTo(4);
    assertThat(index.countEntries()).isEqualTo(before);
  }

  @Test
  void anIndexHoldingNoVectorIsStillSizedByTheFirstOneItReceives() {
    database.command("opencypher", "CREATE VECTOR INDEX emb FOR (n:Doc) ON n.embedding");
    assertThat(indexOf().getDimensions()).isZero();

    database.transaction(() -> database.newVertex("Doc").set("id", 1, "embedding", new float[] { 1, 2, 3 }).save());

    assertThat(indexOf().getDimensions()).isEqualTo(3);
  }

  private LSMVectorIndex indexOf() {
    for (final IndexInternal bucketIndex : ((TypeIndex) database.getSchema().getIndexByName("emb")).getIndexesOnBuckets())
      if (bucketIndex instanceof LSMVectorIndex vector)
        return vector;
    throw new AssertionError("no vector index behind emb");
  }
}
