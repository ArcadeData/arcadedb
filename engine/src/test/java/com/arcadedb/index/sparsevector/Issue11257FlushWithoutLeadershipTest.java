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
import com.arcadedb.database.DatabaseInternal;
import com.arcadedb.database.LocalDatabase;
import com.arcadedb.database.RID;
import com.arcadedb.index.TypeIndex;
import com.arcadedb.schema.DocumentType;
import com.arcadedb.schema.Type;
import org.junit.jupiter.api.Test;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Proxy;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Issue #11257: a flush that seals the live memtable as it is takes it out of its log's slot. When the node turns out not to be
 * the leader any more, no segment is built: the memtable goes back into its slot, under the postings the slot received since,
 * instead of staying aside where it hid the other log's memtable from every read of a follower.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class Issue11257FlushWithoutLeadershipTest extends TestHelper {
  private static final String TYPE_NAME = "Issue11257Flush";
  private static final String IDX_NAME  = TYPE_NAME + "[tokens,weights]";

  @Test
  void aFlushThatLostTheLeadershipPutsItsMemtableBack() throws Exception {
    database.transaction(() -> {
      final DocumentType type = database.getSchema().createDocumentType(TYPE_NAME);
      type.createProperty("tokens", Type.ARRAY_OF_INTEGERS);
      type.createProperty("weights", Type.ARRAY_OF_FLOATS);
      database.getSchema().buildTypeIndex(TYPE_NAME, new String[] { "tokens", "weights" }).withSparseVectorType()
          .withDimensions(100).create();
    });
    final PaginatedSparseVectorEngine engine =
        ((LSMSparseVectorIndex) ((TypeIndex) database.getSchema().getIndexByName(IDX_NAME)).getIndexesOnBuckets()[0]).getEngine();

    // Written to the engine directly, so not logged: the active log holds nothing and the flush seals the live memtable as it is
    for (int i = 0; i < 5; i++)
      engine.put(1, new RID(0, i), i + 1f);

    final LocalDatabase real = (LocalDatabase) database;
    // A leader as far as the flush's first check goes, which loses the leadership before the segment is built; a posting lands
    // in the memtable that took the place of the one being sealed
    real.setWrappedDatabaseInstance((DatabaseInternal) Proxy.newProxyInstance(DatabaseInternal.class.getClassLoader(),
        new Class<?>[] { DatabaseInternal.class }, (proxy, method, args) -> {
          switch (method.getName()) {
          case "getWrappedDatabaseInstance":
            if (method.getParameterCount() == 0)
              return proxy;
            break;
          case "isReplicated", "isLeader":
            return true;
          case "runWithCompactionReplication":
            engine.put(1, new RID(0, 0), 100f);
            return false;
          default:
            break;
          }
          try {
            return method.invoke(real, args);
          } catch (final InvocationTargetException e) {
            throw e.getCause();
          }
        }));
    try {
      assertThat(engine.flush()).as("no segment without the leadership").isEqualTo(-1L);
    } finally {
      real.setWrappedDatabaseInstance(real);
    }

    assertThat(engine.hasFlushingMemtableForTest()).as("the memtable is back in its slot").isFalse();
    assertThat(engine.segmentCount()).isZero();
    assertRanked(engine);

    // The memtable put back is the live one again: the next flush seals all of it
    assertThat(engine.flush()).isNotEqualTo(-1L);
    assertRanked(engine);
  }

  /** Every posting answers, and the one written after the swap ranks with its newer weight. */
  private static void assertRanked(final PaginatedSparseVectorEngine engine) throws Exception {
    final List<RidScore> top = engine.topK(new int[] { 1 }, new float[] { 1f }, 10);
    assertThat(top).hasSize(5);
    assertThat(top.getFirst().rid()).isEqualTo(new RID(0, 0));
  }
}
