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
import com.arcadedb.database.Identifiable;
import com.arcadedb.database.RID;
import com.arcadedb.schema.Schema;
import com.arcadedb.schema.Type;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * The distinct mode of {@link MultiIndexCursor} (issue #9614): children that yield the same record return it once, a skipped
 * duplicate does not count against the limit, and the merge keeps its key order in both directions.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class MultiIndexCursorDistinctTest extends TestHelper {

  @Override
  protected void beginTest() {
    database.getSchema().createDocumentType("T").createProperty("n", Type.INTEGER);
    database.getSchema().createTypeIndex(Schema.INDEX_TYPE.LSM_TREE, false, "T", "n");
    database.transaction(() -> {
      for (int n = 1; n <= 3; n++)
        for (int copy = 0; copy < 2; copy++)
          database.newDocument("T").set("n", n).save();
    });
  }

  private List<IndexCursor> probes(final int... keys) {
    final TypeIndex index = database.getSchema().getType("T").getPolymorphicIndexByProperties("n");
    final List<IndexCursor> cursors = new ArrayList<>();
    for (final int key : keys)
      cursors.add(index.get(new Object[] { key }));
    return cursors;
  }

  private static List<Integer> drain(final MultiIndexCursor cursor, final List<RID> rids) {
    final List<Integer> keys = new ArrayList<>();
    while (cursor.hasNext()) {
      final Identifiable record = cursor.next();
      rids.add(record.getIdentity());
      assertThat(cursor.getRecord()).isSameAs(record);
      keys.add(((Number) cursor.getKeys()[0]).intValue());
    }
    return keys;
  }

  @Test
  void returnsEachRecordOnceInBothDirections() {
    for (final boolean ascending : new boolean[] { true, false }) {
      final List<RID> rids = new ArrayList<>();
      final List<Integer> keys = drain(new MultiIndexCursor(probes(1, 1, 2, 2, 3), -1, ascending, true, true), rids);
      assertThat(rids).as("ascending " + ascending).hasSize(6).doesNotHaveDuplicates();
      assertThat(keys).as("ascending " + ascending).isEqualTo(ascending ? List.of(1, 1, 2, 2, 3, 3) : List.of(3, 3, 2, 2, 1, 1));
    }
  }

  @Test
  void aSkippedDuplicateDoesNotCountAgainstTheLimit() {
    for (final boolean ascending : new boolean[] { true, false }) {
      final List<RID> rids = new ArrayList<>();
      final List<Integer> keys = drain(new MultiIndexCursor(probes(2, 2, 2, 1), 3, ascending, true, true), rids);
      assertThat(new HashSet<>(rids)).as("ascending " + ascending).hasSize(3);
      assertThat(keys).as("ascending " + ascending).isEqualTo(ascending ? List.of(1, 1, 2) : List.of(2, 2, 1));
    }
  }

  @Test
  void withoutDistinctTheDuplicatesStayAsBefore() {
    final List<RID> rids = new ArrayList<>();
    drain(new MultiIndexCursor(probes(1, 1), -1, true, true, false), rids);
    assertThat(rids).hasSize(4);
    assertThat(new HashSet<>(rids)).hasSize(2);
  }
}
