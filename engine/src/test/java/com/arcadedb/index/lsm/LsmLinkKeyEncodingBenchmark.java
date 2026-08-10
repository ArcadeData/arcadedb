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
package com.arcadedb.index.lsm;

import com.arcadedb.database.Binary;
import com.arcadedb.database.Database;
import com.arcadedb.database.DatabaseFactory;
import com.arcadedb.database.DatabaseInternal;
import com.arcadedb.database.RID;
import com.arcadedb.graph.MutableVertex;
import com.arcadedb.graph.Vertex;
import com.arcadedb.index.IndexInternal;
import com.arcadedb.index.TypeIndex;
import com.arcadedb.schema.Schema;
import com.arcadedb.serializer.BinarySerializer;
import com.arcadedb.serializer.BinaryTypes;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * The measurement issue #5703 asked for before anything was changed: how much of an {@code LSM_TREE} index keyed on
 * {@code (@out,@in)} is spent on the LINK keys, and how much of that the varint {@code TYPE_COMPRESSED_RID} form
 * gives back.
 * <p>
 * It reports three things per run: what the keys cost under each encoding for the very RIDs that were indexed, what
 * the mutable index file actually occupies, and the resulting bytes per entry. The key figures are computed by
 * serializing the same RIDs both ways, so the comparison does not depend on which encoding the build under test
 * happens to use, and the run stays meaningful as a "did the saving hold?" check after the change.
 * <p>
 * <b>Measured answer (50k edges, one hub, destinations created in order):</b> keys cost 24.0 bytes/entry fixed-width
 * against 5.9 compressed - a 75% cut on the key bytes. The whole entry (4-byte pointer slot + 2 null flags + keys +
 * value count + compressed value RID) costs about 36.7 bytes under the fixed-width encoding, so the saving is
 * roughly half of everything the index occupies: the same 50k entries went from 7 mutable pages to 4. That is not
 * small relative to the per-entry overhead the LSM format carries, which is what decided #5703 in favour of making
 * the change rather than closing it on the numbers.
 * <p>
 * The absolute saving shrinks as a database grows - a position in the billions needs 5 varint bytes rather than 2 -
 * but the fixed form still costs 12 per column, so the compressed form stays ahead at every size.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
@Tag("benchmark")
class LsmLinkKeyEncodingBenchmark {
  private static final int EDGES        = Integer.parseInt(System.getProperty("edges", "50000"));
  private static final int EDGES_PER_TX = Integer.parseInt(System.getProperty("edgesPerTx", "100"));

  @Test
  void reportsWhatTheLinkKeysOfAnEndpointIndexCost() {
    final DatabaseFactory factory = new DatabaseFactory("./target/databases/lsmLinkKeyEncoding");
    if (factory.exists())
      factory.open().drop();

    final Database database = factory.create();
    try {
      final List<Vertex> leaves = new ArrayList<>(EDGES);
      final Vertex[] hub = new Vertex[1];

      database.transaction(() -> {
        database.getSchema().createVertexType("Account");
        database.getSchema().createEdgeType("INITIATED");
        database.getSchema().buildTypeIndex("INITIATED", new String[] { "@out", "@in" })
            .withType(Schema.INDEX_TYPE.LSM_TREE).withUnique(true).create();
      });

      database.transaction(() -> hub[0] = database.newVertex("Account").set("code", "HUB").save());
      for (int i = 0; i < EDGES; i += EDGES_PER_TX) {
        final int from = i;
        database.transaction(() -> {
          for (int j = from; j < Math.min(from + EDGES_PER_TX, EDGES); j++)
            leaves.add(database.newVertex("Account").set("code", "L" + j).save());
        });
      }
      for (int i = 0; i < EDGES; i += EDGES_PER_TX) {
        final int from = i;
        database.transaction(() -> {
          final MutableVertex source = hub[0].modify();
          for (int j = from; j < Math.min(from + EDGES_PER_TX, EDGES); j++)
            source.newEdge("INITIATED", leaves.get(j)).save();
        });
      }

      final BinarySerializer serializer = ((DatabaseInternal) database).getSerializer();
      final Binary scratch = new Binary(64);
      long fixedWidthKeyBytes = 0;
      long compressedKeyBytes = 0;
      for (int j = 0; j < EDGES; j++)
        for (final RID rid : new RID[] { hub[0].getIdentity(), leaves.get(j).getIdentity() }) {
          scratch.clear();
          serializer.serializeValue(database, scratch, BinaryTypes.TYPE_RID, rid);
          fixedWidthKeyBytes += scratch.size();
          scratch.clear();
          serializer.serializeValue(database, scratch, BinaryTypes.TYPE_COMPRESSED_RID, rid);
          compressedKeyBytes += scratch.size();
        }

      long mutablePages = 0;
      long indexed = 0;
      for (final IndexInternal sub : ((TypeIndex) database.getSchema().getIndexByName("INITIATED[@out,@in]")).getIndexesOnBuckets()) {
        final LSMTreeIndexMutable mutable = ((LSMTreeIndex) sub).getMutableIndex();
        mutablePages += mutable.getTotalPages();
        indexed += sub.countEntries();
      }

      // System.out, not LogManager: a benchmark's numbers ARE its output, and LogManager INFO does not surface in a
      // surefire run, which makes the measurement invisible.
      System.out.printf("%n===== #5703 LSM LINK key encoding (edges=%d) =====%n", EDGES);
      System.out.printf("  entries indexed        %d%n", indexed);
      System.out.printf("  key bytes fixed-width  %d (%.2f /entry)%n", fixedWidthKeyBytes, fixedWidthKeyBytes / (double) EDGES);
      System.out.printf("  key bytes compressed   %d (%.2f /entry)%n", compressedKeyBytes, compressedKeyBytes / (double) EDGES);
      System.out.printf("  saving on key bytes    %.1f%%%n",
          100.0 * (fixedWidthKeyBytes - compressedKeyBytes) / fixedWidthKeyBytes);
      System.out.printf("  mutable index pages    %d (%.2f bytes/entry)%n", mutablePages,
          mutablePages * (double) pageSizeOf(database) / EDGES);

      assertThat(indexed).isEqualTo(EDGES);
      assertThat(compressedKeyBytes).isLessThan(fixedWidthKeyBytes);
    } finally {
      database.drop();
    }
  }

  private static int pageSizeOf(final Database database) {
    return ((LSMTreeIndex) ((TypeIndex) database.getSchema().getIndexByName("INITIATED[@out,@in]")).getIndexesOnBuckets()[0])
        .getMutableIndex().getPageSize();
  }
}
