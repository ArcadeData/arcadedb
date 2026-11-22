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

import com.arcadedb.GlobalConfiguration;
import com.arcadedb.TestHelper;
import com.arcadedb.database.Document;
import com.arcadedb.database.MutableDocument;
import com.arcadedb.database.Record;
import com.arcadedb.query.sql.executor.Result;
import com.arcadedb.query.sql.executor.ResultSet;
import com.arcadedb.schema.DocumentType;
import com.arcadedb.schema.Schema;
import com.arcadedb.schema.Type;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;

import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Random;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * The record contract of a bucket, checked against an in-memory model: random transactions of inserts, updates that grow, shrink or
 * keep the size of a record (up to several pages, which makes it a chain of chunks) and deletes, committed or rolled back, with the
 * database reopened and compressed in between. After every step each record must read back byte for byte through its RID, a SQL scan, a type
 * iteration, the bucket count and an index on a property of the record, and what the transaction reads of its own pending writes
 * must match too. {@link TestHelper} runs CHECK DATABASE at the end of every test.
 */
class BucketModelContractTest extends TestHelper {

  @ParameterizedTest(name = "page size {0}, {1} bucket(s), index={2}, seed {3}")
  @CsvSource({ "12298, 1, false, 1", "12298, 2, true, 2", "16384, 1, true, 3", "32768, 1, false, 4", "65536, 1, true, 5", "65536, 2, false, 6" })
  void everyRecordReadsBackWhatTheModelHolds(final int pageSize, final int buckets, final boolean withIndex, final long seed) {
    database.getConfiguration().setValue(GlobalConfiguration.BUCKET_DEFAULT_PAGE_SIZE, pageSize);
    final Random random = new Random(seed);
    final DocumentType type = database.getSchema().buildDocumentType().withName("B").withTotalBuckets(buckets).create();
    type.createProperty("n", Type.INTEGER);
    type.createProperty("data", Type.STRING);
    if (withIndex)
      database.getSchema().createTypeIndex(Schema.INDEX_TYPE.LSM_TREE, false, "B", "n");

    Map<String, String> model = new LinkedHashMap<>();
    int tag = 0;
    for (int step = 0; step < 60; step++) {
      final int what = random.nextInt(100);
      if (what < 88) {
        final Map<String, String> committed = new LinkedHashMap<>(model);
        final Map<String, String> work = new LinkedHashMap<>(model);
        final boolean commit = random.nextInt(6) != 0;
        final int operations = 1 + random.nextInt(8);
        database.begin();
        try {
          for (int i = 0; i < operations; i++) {
            final int op = random.nextInt(10);
            if (op < 4 || work.isEmpty()) {
              final int length = length(random, pageSize);
              final String data = data(length, tag++);
              final MutableDocument document = database.newDocument("B").set("n", length % 17).set("data", data);
              document.save();
              work.put(document.getIdentity().toString(), data);
            } else if (op < 8) {
              final String rid = pick(work, random);
              final int length = random.nextInt(3) == 0 ? work.get(rid).length() : length(random, pageSize);
              final String data = data(length, tag++);
              final MutableDocument document = database.lookupByRID(database.newRID(rid), true).asDocument().modify();
              document.set("n", length % 17).set("data", data);
              document.save();
              work.put(rid, data);
            } else {
              final String rid = pick(work, random);
              database.lookupByRID(database.newRID(rid), true).asDocument().delete();
              work.remove(rid);
            }
            if (random.nextInt(6) == 0)
              assertRecordsReadBack(work, "in the transaction of step " + step);
          }
          if (commit) {
            database.commit();
            model = work;
          } else {
            database.rollback();
            model = committed;
          }
        } finally {
          if (database.isTransactionActive())
            database.rollback();
        }
        verify(model, withIndex, "after the transaction of step " + step + (commit ? " (commit)" : " (rollback)"));
      } else if (what < 94) {
        reopenDatabase();
        verify(model, withIndex, "after a reopen at step " + step);
      } else {
        new DatabaseChecker(database).setCompress(true).check();
        verify(model, withIndex, "after a compression at step " + step);
      }
    }
  }

  private static int length(final Random random, final int pageSize) {
    final int r = random.nextInt(100);
    if (r < 55)
      return random.nextInt(120);
    if (r < 80)
      return random.nextInt(2000);
    if (r < 92)
      return random.nextInt(pageSize / 2);
    if (r < 98)
      return pageSize / 2 + random.nextInt(pageSize);
    return pageSize * (2 + random.nextInt(4)) + random.nextInt(1000);
  }

  private static String data(final int length, final int tag) {
    final StringBuilder builder = new StringBuilder(length);
    int x = tag * 31 + 7;
    for (int i = 0; i < length; i++) {
      x = x * 1103515245 + 12345;
      builder.append((char) ('a' + ((x >>> 16) & 15)));
    }
    return builder.toString();
  }

  private static String pick(final Map<String, String> records, final Random random) {
    final int target = random.nextInt(records.size());
    int i = 0;
    for (final String rid : records.keySet())
      if (i++ == target)
        return rid;
    throw new IllegalStateException();
  }

  private void assertRecordsReadBack(final Map<String, String> expected, final String when) {
    int checked = 0;
    for (final Map.Entry<String, String> entry : expected.entrySet()) {
      if (checked++ > 6)
        break;
      final Document document = database.lookupByRID(database.newRID(entry.getKey()), true).asDocument();
      assertThat(document.getString("data")).as("%s: record %s", when, entry.getKey()).isEqualTo(entry.getValue());
    }
    assertThat(database.countType("B", true)).as("%s: the count includes the changes of the open transaction", when).isEqualTo(expected.size());
  }

  private void verify(final Map<String, String> model, final boolean withIndex, final String when) {
    assertRecordsReadBack(model, when);
    for (final Map.Entry<String, String> entry : model.entrySet())
      assertThat(database.lookupByRID(database.newRID(entry.getKey()), true).asDocument().getString("data")).as("%s: record %s", when, entry.getKey())
          .isEqualTo(entry.getValue());

    final Map<String, String> scanned = new HashMap<>();
    try (final ResultSet resultSet = database.query("sql", "SELECT FROM B")) {
      while (resultSet.hasNext()) {
        final Result result = resultSet.next();
        assertThat(scanned.put(result.getIdentity().get().toString(), result.getProperty("data"))).as("%s: the scan returned %s twice", when,
            result.getIdentity().get()).isNull();
      }
    }
    assertThat(scanned).as("%s: SQL scan", when).isEqualTo(model);

    final Map<String, String> iterated = new HashMap<>();
    final Iterator<Record> iterator = database.iterateType("B", true);
    while (iterator.hasNext()) {
      final Record record = iterator.next();
      iterated.put(record.getIdentity().toString(), ((Document) record).getString("data"));
    }
    assertThat(iterated).as("%s: iterateType", when).isEqualTo(model);

    assertThat(database.countType("B", true)).as("%s: countType", when).isEqualTo(model.size());
    long total = 0;
    for (final Bucket bucket : database.getSchema().getType("B").getBuckets(false))
      total += bucket.count();
    assertThat(total).as("%s: the bucket counts", when).isEqualTo(model.size());
    try (final ResultSet resultSet = database.query("sql", "SELECT count(*) AS c FROM B")) {
      assertThat(resultSet.next().<Number>getProperty("c").longValue()).as("%s: SELECT count(*)", when).isEqualTo(model.size());
    }

    if (withIndex) {
      final Map<Integer, Integer> perValue = new HashMap<>();
      for (final String data : model.values())
        perValue.merge(data.length() % 17, 1, Integer::sum);
      for (int n = 0; n < 17; n++)
        try (final ResultSet resultSet = database.query("sql", "SELECT count(*) AS c FROM B WHERE n = ?", n)) {
          assertThat(resultSet.next().<Number>getProperty("c").longValue()).as("%s: the index entries of n=%d", when, n)
              .isEqualTo((long) perValue.getOrDefault(n, 0));
        }
    }
  }
}
