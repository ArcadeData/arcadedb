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
package com.arcadedb.index.sparsevector;

import com.arcadedb.TestHelper;
import com.arcadedb.database.MutableDocument;
import com.arcadedb.database.RID;
import com.arcadedb.query.sql.executor.Result;
import com.arcadedb.query.sql.executor.ResultSet;
import com.arcadedb.schema.DocumentType;
import com.arcadedb.schema.Type;

import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Pins the behaviour of {@code LSMTreeIndexAbstract.compareKeys}: when an upper bound is supplied
 * as a prefix (one column shorter than the full key), the comparator must treat any posting whose
 * key starts with that prefix as equal to the bound, so an inclusive upper bound includes every
 * posting under the prefix. The WAND {@code seekTo} primitive in {@link LSMSparseVectorIndex}
 * relies on this; if a future comparator change broke the convention, postings after the seek
 * point in the same dimension would silently drop and WAND results would be missing rows.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class LSMSparseVectorIndexSeekTest extends TestHelper {

  private static final String TYPE_NAME = "SeekDoc";
  private static final String IDX_NAME  = "SeekDoc[tokens,weights]";

  /**
   * Inserts a tight cluster on dim 5 and asserts that a WAND query (which forces the cursor to
   * seek across RIDs within the dim) returns every doc in the dim, not just the one at the seek
   * point. This exercises the prefix-upper-bound path in {@code LSMTreeIndex.range}.
   */
  @Test
  void seekToInsideDimensionDoesNotDropTrailingPostings() {
    final List<RID> rids = new ArrayList<>();

    database.transaction(() -> {
      final DocumentType t = database.getSchema().createDocumentType(TYPE_NAME);
      t.createProperty("tokens", Type.ARRAY_OF_INTEGERS);
      t.createProperty("weights", Type.ARRAY_OF_FLOATS);
      database.getSchema()
          .buildTypeIndex(TYPE_NAME, new String[] { "tokens", "weights" })
          .withSparseVectorType()
          .withDimensions(50)
          .create();

      // Every doc shares dim 5 so the WAND scan must traverse all of them. Doc i additionally has
      // a unique dim (10 + i) with descending weight, so the WAND threshold rises monotonically
      // and forces seeking past previously-visited RIDs.
      for (int i = 0; i < 25; i++) {
        final MutableDocument d = database.newDocument(TYPE_NAME);
        d.set("tokens", new int[] { 5, 10 + i });
        d.set("weights", new float[] { 0.5f + 0.01f * i, 1.0f - 0.01f * i });
        d.save();
        rids.add(d.getIdentity());
      }
    });

    // Query covers dim 5 (all 25 docs) plus a few of the unique dims, with weights chosen so WAND
    // pruning will trigger seekTo on the dim-5 cursor multiple times.
    final ResultSet rs = database.query("sql",
        "SELECT expand(`vector.sparseNeighbors`(?, ?, ?, ?))",
        IDX_NAME,
        new int[]   { 5,    10,  15,  20 },
        new float[] { 1.0f, 0.5f, 0.5f, 0.5f },
        25);

    final HashSet<RID> returned = new HashSet<>();
    while (rs.hasNext()) {
      final Result row = rs.next();
      returned.add((RID) row.getProperty("@rid"));
    }

    // Every inserted doc shares dim 5 with the query, so all 25 must come back. If the seek upper
    // bound were too tight, some trailing RIDs would silently drop.
    assertThat(returned).as("WAND with seekTo must return every dim-5 posting").hasSize(25);
    assertThat(returned).containsAll(rids);
  }

  /**
   * Mirrors the worst case from the PR review: a posting list with a deliberate gap. Each inserted
   * doc lands in a different bucket position, and the test forces a WAND query whose threshold
   * causes the cursor to seek across the gap. If {@code seekTo}'s upper bound were "too tight" and
   * stopped at the gap point, the post-gap RIDs would silently disappear.
   */
  @Test
  void seekToAcrossPostingListGapStillReturnsTrailingRids() {
    final List<RID> ridsBefore = new ArrayList<>();
    final List<RID> ridsAfter  = new ArrayList<>();

    database.transaction(() -> {
      final DocumentType t = database.getSchema().createDocumentType(TYPE_NAME);
      t.createProperty("tokens", Type.ARRAY_OF_INTEGERS);
      t.createProperty("weights", Type.ARRAY_OF_FLOATS);
      database.getSchema()
          .buildTypeIndex(TYPE_NAME, new String[] { "tokens", "weights" })
          .withSparseVectorType()
          .withDimensions(200)
          .create();

      // Two clusters on dim 7, each with 5 docs; between them, 20 unrelated docs widen the RID
      // space to force seekTo to skip a non-trivial range.
      for (int i = 0; i < 5; i++) {
        final MutableDocument d = database.newDocument(TYPE_NAME);
        d.set("tokens", new int[] { 7, 30 + i });
        d.set("weights", new float[] { 0.4f + 0.05f * i, 1.0f });
        d.save();
        ridsBefore.add(d.getIdentity());
      }

      // Filler docs without dim 7. These sit between the two clusters in RID order and force a
      // visible "gap" in the dim-7 posting list.
      for (int i = 0; i < 20; i++) {
        final MutableDocument d = database.newDocument(TYPE_NAME);
        d.set("tokens", new int[] { 100 + i });
        d.set("weights", new float[] { 0.1f });
        d.save();
      }

      for (int i = 0; i < 5; i++) {
        final MutableDocument d = database.newDocument(TYPE_NAME);
        d.set("tokens", new int[] { 7, 40 + i });
        d.set("weights", new float[] { 0.7f + 0.05f * i, 1.0f });
        d.save();
        ridsAfter.add(d.getIdentity());
      }
    });

    // Query weights heavy on the post-gap unique dims so threshold rises after the first cluster
    // and forces seekTo on dim 7 across the gap.
    final ResultSet rs = database.query("sql",
        "SELECT expand(`vector.sparseNeighbors`(?, ?, ?, ?))",
        IDX_NAME,
        new int[]   { 7,    40,   41,   42 },
        new float[] { 1.0f, 1.0f, 1.0f, 1.0f },
        10);

    final HashSet<RID> returned = new HashSet<>();
    while (rs.hasNext())
      returned.add((RID) rs.next().getProperty("@rid"));

    // The post-gap docs must rank highest (their unique dims match the query). The pre-gap docs
    // are still under dim 7 so they must remain reachable too.
    assertThat(returned).as("post-gap RIDs must be returned").containsAll(ridsAfter);
  }
}
