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
import com.arcadedb.database.DatabaseInternal;
import com.arcadedb.database.MutableDocument;
import com.arcadedb.database.RID;
import com.arcadedb.query.sql.executor.ResultSet;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Issue #7109: the tuple delta introduced for composite indexes with one collection modifier (issue #6934) compared tuples
 * with the shallow {@code Arrays.equals}, so a tuple element that is itself an array (a {@code BINARY} key here, the
 * {@code float[]} of a vector elsewhere) never matched its own deserialized copy. Every update of such a record - even one
 * touching no indexed property - removed and re-put every tuple, which is the very churn issue #5318 had already removed from
 * the scalar-only path of the same method.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
public class Issue7109ArrayKeyTupleDeltaChurnTest extends TestHelper {

  @Override
  public void beginTest() {
    database.transaction(() -> {
      database.command("sql", "CREATE DOCUMENT TYPE Doc");
      database.command("sql", "CREATE PROPERTY Doc.hash BINARY");
      database.command("sql", "CREATE PROPERTY Doc.tags LIST");
      database.command("sql", "CREATE PROPERTY Doc.counter INTEGER");
      database.command("sql", "CREATE INDEX ON Doc (hash, tags BY ITEM) NOTUNIQUE");
    });
  }

  @Test
  void unchangedArrayKeyDoesNotChurnTheIndex() {
    final RID rid = insert(0);

    database.begin();
    try {
      // The record is re-read from the committed buffer, so its BINARY key is a fresh byte[] that only a content-aware
      // comparison can recognise as unchanged.
      final MutableDocument doc = database.lookupByRID(rid, true).asDocument().modify();
      doc.set("counter", 1);
      doc.save();

      assertThat(((DatabaseInternal) database).getTransaction().getIndexChanges().getTotalEntries())
          .as("an update leaving both index keys untouched must not enqueue any index remove/put")
          .isZero();
    } finally {
      database.commit();
    }

    assertThat(countWhere("tags CONTAINS 'x'")).isEqualTo(1);
    assertThat(countWhere("tags CONTAINS 'y'")).isEqualTo(1);
    assertThat(indexEntries()).isEqualTo(2);
  }

  @Test
  void changedListItemStillProducesTheMinimalDelta() {
    final RID rid = insert(0);

    database.begin();
    try {
      final MutableDocument doc = database.lookupByRID(rid, true).asDocument().modify();
      doc.set("tags", List.of("x", "z"));
      doc.save();

      // One remove (hash,y) and one put (hash,z): the retained (hash,x) tuple must be left alone.
      assertThat(((DatabaseInternal) database).getTransaction().getIndexChanges().getTotalEntries()).isEqualTo(2);
    } finally {
      database.commit();
    }

    assertThat(countWhere("tags CONTAINS 'x'")).isEqualTo(1);
    assertThat(countWhere("tags CONTAINS 'z'")).isEqualTo(1);
    assertThat(countWhere("tags CONTAINS 'y'")).isZero();
    assertThat(indexEntries()).isEqualTo(2);
  }

  private RID insert(final int counter) {
    final RID[] rid = new RID[1];
    database.transaction(() -> {
      final MutableDocument doc = database.newDocument("Doc");
      doc.set("hash", new byte[] { 1, 2, 3 });
      doc.set("tags", List.of("x", "y"));
      doc.set("counter", counter);
      rid[0] = doc.save().getIdentity();
    });
    return rid[0];
  }

  private long countWhere(final String condition) {
    try (final ResultSet rs = database.query("sql", "SELECT count(*) AS c FROM Doc WHERE " + condition)) {
      return rs.next().<Number>getProperty("c").longValue();
    }
  }

  private long indexEntries() {
    long total = 0;
    for (final Index index : database.getSchema().getType("Doc").getAllIndexes(false))
      total += index.countEntries();
    return total;
  }
}
