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
import com.arcadedb.query.sql.executor.ResultSet;
import com.arcadedb.schema.Schema;
import com.arcadedb.schema.Type;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Regression test for issue #7765: once a key-wide tombstone (the {@code RID(-1,-1)} sentinel written by
 * {@code IndexInternal.remove(Object[] keys)} with no RID) is COMMITTED - as opposed to still pending in the
 * writing transaction, which #6927 already covers correctly - {@code LSMTreeIndexAbstract
 * .lookupInPageAndAddInResultset()} (the reader behind {@code Index.get(keys)} / the SQL equality predicate) decoded
 * it as an ordinary per-RID tombstone via {@code getOriginalRID()}, which maps {@code (-1,-1)} back to
 * {@code (-1,-1)} - matching no real record - and only fed {@code removedKeys} (the thing that actually suppresses
 * the key) when {@code mainIndex.isUnique()}. On a NON-UNIQUE index the point lookup therefore kept returning every
 * RID written before the tombstone, while {@code range()}/{@code countEntries()} (which walk the cursor) and
 * {@code LSMTreeIndexCompactor.compactFull()} already agreed the key was gone - so the SAME index answered
 * differently depending on which read path was used, and the wrong answer even flipped to the right one the first
 * time a full compaction ran.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class Issue7765KeyWideTombstonePointLookupTest extends TestHelper {

  private static final String TYPE_NAME = "Foo";

  @Override
  protected void beginTest() {
    database.transaction(() -> {
      final var type = database.getSchema().createDocumentType(TYPE_NAME);
      type.createProperty("name", Type.STRING);
      database.getSchema().buildTypeIndex(TYPE_NAME, new String[] { "name" })
          .withType(Schema.INDEX_TYPE.LSM_TREE).withUnique(false).create();
    });
  }

  @Test
  void committedKeyWideRemoveIsHonouredByThePointLookupOnANonUniqueIndex() {
    database.transaction(() -> {
      database.newDocument(TYPE_NAME).set("name", "a").save();
      database.newDocument(TYPE_NAME).set("name", "a").save();
      database.newDocument(TYPE_NAME).set("name", "a").save();
    });

    final RangeIndex index = rangeIndex();
    assertThat(drain(index.get(new Object[] { "a" }))).hasSize(3);

    // Key-wide remove(keys), committed in its OWN transaction - separate from the inserts, so the removal really
    // reaches disk instead of being collapsed by the in-transaction ADD+REMOVE folding (#6927's territory).
    database.transaction(() -> ((IndexInternal) index).remove(new Object[] { "a" }));

    database.transaction(() -> {
      assertThat(drain(index.get(new Object[] { "a" })))
          .as("Index.get() must honour a committed key-wide tombstone on a NON-UNIQUE index")
          .isEmpty();
      assertThat(index.countEntries()).isZero();

      final List<RID> ascending = drain(index.range(true, new Object[] { "a" }, true, new Object[] { "a" }, true));
      assertThat(ascending).isEmpty();

      try (final ResultSet rs = database.query("sql", "SELECT FROM " + TYPE_NAME + " WHERE name = 'a'")) {
        assertThat(rs.hasNext()).as("the SQL equality predicate must agree with Index.get()").isFalse();
      }
    });
  }

  /**
   * The walk is newest-to-oldest, so a key-wide tombstone must suppress only what was written BEFORE it - a fresh
   * insert at the same key AFTER the tombstone has to stay visible to the point lookup.
   */
  @Test
  void anInsertAfterTheKeyWideRemoveStaysVisibleToThePointLookup() {
    database.transaction(() -> {
      database.newDocument(TYPE_NAME).set("name", "b").save();
      database.newDocument(TYPE_NAME).set("name", "b").save();
    });

    final RangeIndex index = rangeIndex();

    database.transaction(() -> ((IndexInternal) index).remove(new Object[] { "b" }));

    final RID[] resurrected = new RID[1];
    database.transaction(() -> resurrected[0] = database.newDocument(TYPE_NAME).set("name", "b").save().getIdentity());

    database.transaction(() -> {
      final var found = drain(index.get(new Object[] { "b" }));
      assertThat(found).containsExactly(resurrected[0]);
      assertThat(index.countEntries()).isEqualTo(1);
    });
  }

  private RangeIndex rangeIndex() {
    return (RangeIndex) database.getSchema().getType(TYPE_NAME).getPolymorphicIndexByProperties("name");
  }

  private List<RID> drain(final IndexCursor cursor) {
    final List<RID> rids = new ArrayList<>();
    while (cursor.hasNext())
      rids.add(cursor.next().getIdentity());
    return rids;
  }
}
