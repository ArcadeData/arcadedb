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
import com.arcadedb.database.MutableDocument;
import com.arcadedb.database.RID;
import com.arcadedb.index.IndexInternal;
import com.arcadedb.index.TypeIndex;
import com.arcadedb.query.sql.executor.Result;
import com.arcadedb.query.sql.executor.ResultSet;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Issue #11734: {@code CHECK DATABASE} compares the entries of an LSM-Tree index with the records they point at. An entry whose
 * record does not exist, an entry whose record does not hold its key and a key of a record with no entry are reported, and
 * {@code CHECK DATABASE FIX} rebuilds the index.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class CheckDatabaseIndexContentTest extends TestHelper {
  private static final int RECORDS = 300;

  @Override
  public void beginTest() {
    database.command("sql", "CREATE DOCUMENT TYPE T BUCKETS 1");
    database.command("sql", "CREATE PROPERTY T.k INTEGER");
    database.command("sql", "CREATE PROPERTY T.v INTEGER");
    database.command("sql", "CREATE INDEX ON T (v) NOTUNIQUE");
    database.transaction(() -> {
      for (int i = 0; i < RECORDS; i++)
        database.newDocument("T").set("k", i).set("v", i % 50).save();
    });
  }

  @Test
  void aHealthyDatabaseReportsNothing() {
    final Result check = check("CHECK DATABASE");
    assertThat(check.<Long>getProperty("danglingIndexEntries")).isZero();
    assertThat(check.<Long>getProperty("staleIndexEntries")).isZero();
    assertThat(check.<Long>getProperty("missingIndexEntries")).isZero();
    assertThat(check.<Collection<?>>getProperty("corruptedIndexes")).isEmpty();
  }

  @Test
  void entriesThatDoNotMatchTheRecordsAreReportedAndFixRebuildsTheIndex() {
    final IndexInternal index = bucketIndex("T[v]");
    final RID first = rid(0);
    final RID second = rid(1);
    // an entry for a record that does not exist: the record went without its index entry
    final RID gone = deletedRid();
    database.transaction(() -> {
      // an entry whose record holds another value (k=0 holds v=0)
      index.put(new Object[] { 20 }, new RID[] { first });
      // a record whose key has no entry (k=1 holds v=1)
      index.remove(new Object[] { 1 }, second);
    });

    // the indexed lookup answers a row whose value is not 20: the symptom the check has to catch
    assertThat(values("SELECT v FROM T WHERE v = 20")).contains(0);

    final Result check = check("CHECK DATABASE");
    assertThat(check.<Long>getProperty("danglingIndexEntries")).isEqualTo(1L);
    assertThat(check.<Long>getProperty("staleIndexEntries")).isEqualTo(1L);
    assertThat(check.<Long>getProperty("missingIndexEntries")).isEqualTo(1L);
    assertThat(check.<Long>getProperty("totalErrors")).isGreaterThanOrEqualTo(3L);
    // the summary and the three examples are counted like every other warning
    assertThat(check.<Long>getProperty("totalWarnings")).isGreaterThanOrEqualTo(4L);
    assertThat(check.<Collection<String>>getProperty("corruptedIndexes")).containsExactly(index.getName());
    assertThat(String.join("\n", check.<Collection<String>>getProperty("warnings")))//
        .contains("entry [7] -> " + gone + " names a record that does not exist")//
        .contains("entry [20] -> " + first + " names a record that does not hold that key (its key: [0])")//
        .contains("record " + second + " has no entry for key [1]");

    final Result fix = check("CHECK DATABASE FIX");
    assertThat(fix.<Collection<String>>getProperty("rebuiltIndexes")).contains(index.getName());

    final Result after = check("CHECK DATABASE");
    assertThat(after.<Long>getProperty("danglingIndexEntries")).isZero();
    assertThat(after.<Long>getProperty("staleIndexEntries")).isZero();
    assertThat(after.<Long>getProperty("missingIndexEntries")).isZero();
    assertThat(after.<Collection<?>>getProperty("corruptedIndexes")).isEmpty();
    assertThat(values("SELECT v FROM T WHERE v = 20")).containsOnly(20);
    assertThat(values("SELECT v FROM T WHERE v = 1")).hasSize(RECORDS / 50);
  }

  @Test
  void theIndexesOfOtherShapesAreHealthyAfterOrdinaryWrites() {
    database.command("sql", "CREATE DOCUMENT TYPE Shapes BUCKETS 2");
    database.command("sql", "CREATE PROPERTY Shapes.tags LIST");
    database.command("sql", "CREATE PROPERTY Shapes.name STRING");
    database.command("sql", "CREATE PROPERTY Shapes.n INTEGER");
    database.command("sql", "CREATE PROPERTY Shapes.d DOUBLE");
    database.command("sql", "CREATE INDEX ON Shapes (tags BY ITEM) NOTUNIQUE");
    database.command("sql", "CREATE INDEX ON Shapes (name COLLATE ci, n) UNIQUE");
    database.command("sql", "CREATE INDEX ON Shapes (d) NOTUNIQUE NULL_STRATEGY INDEX");
    database.transaction(() -> {
      for (int i = 0; i < 100; i++) {
        final MutableDocument doc = database.newDocument("Shapes");
        // repeated items, nulls, nested lists and numbers of several types
        doc.set("tags", new ArrayList<>(Arrays.asList(i % 3, i % 3, null, "x" + i, List.of(i), 1.5)));
        doc.set("name", "Name" + i);
        doc.set("n", i % 2 == 0 ? null : i);
        if (i % 4 != 0)
          doc.set("d", i * 0.5);
        doc.save();
      }
    });
    database.transaction(() -> {
      database.command("sql", "UPDATE Shapes SET name = name.toUpperCase(), d = null WHERE n IS NULL");
      database.command("sql", "DELETE FROM Shapes WHERE name LIKE '%7'");
    });

    final Result check = check("CHECK DATABASE");
    assertThat(check.<Collection<String>>getProperty("warnings")).isEmpty();
    assertThat(check.<Collection<?>>getProperty("corruptedIndexes")).isEmpty();
  }

  private Result check(final String command) {
    try (final ResultSet rs = database.command("sql", command)) {
      return rs.next();
    }
  }

  private IndexInternal bucketIndex(final String typeIndexName) {
    final IndexInternal[] buckets = ((TypeIndex) database.getSchema().getIndexByName(typeIndexName)).getIndexesOnBuckets();
    assertThat(buckets).hasSize(1);
    return buckets[0];
  }

  private RID rid(final int k) {
    try (final ResultSet rs = database.query("sql", "SELECT @rid AS rid FROM T WHERE k = ?", k)) {
      return rs.next().getProperty("rid");
    }
  }

  private RID deletedRid() {
    final RID[] rid = new RID[1];
    database.transaction(() -> rid[0] = database.newDocument("T").set("k", -1).set("v", 7).save().getIdentity());
    // removed below the index on purpose, as a lost index removal leaves it
    database.transaction(() -> ((LocalBucket) database.getSchema().getBucketById(rid[0].getBucketId())).deleteRecord(rid[0]));
    return rid[0];
  }

  private List<Integer> values(final String query) {
    final List<Integer> values = new ArrayList<>();
    try (final ResultSet rs = database.query("sql", query)) {
      while (rs.hasNext())
        values.add(rs.next().<Integer>getProperty("v"));
    }
    return values;
  }
}
