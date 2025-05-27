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

import com.arcadedb.GlobalConfiguration;
import com.arcadedb.TestHelper;
import com.arcadedb.database.Document;
import com.arcadedb.database.Identifiable;
import com.arcadedb.database.MutableDocument;
import com.arcadedb.exception.DuplicatedKeyException;
import com.arcadedb.exception.NeedRetryException;
import com.arcadedb.log.LogManager;
import com.arcadedb.query.sql.executor.Result;
import com.arcadedb.query.sql.executor.ResultSet;
import com.arcadedb.schema.DocumentType;
import com.arcadedb.schema.Schema;
import com.arcadedb.schema.Type;
import com.arcadedb.schema.VertexType;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.logging.Level;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.fail;

public class TypeLSMTreeIndexTest extends TestHelper {
  private static final int    TOT       = 100000;
  private static final String TYPE_NAME = "V";
  private static final int    PAGE_SIZE = 20000;

  @Test
  public void testGet() {
    database.transaction(() -> {

      int total = 0;

      final Collection<TypeIndex> indexes = database.getSchema().getType(TYPE_NAME).getAllIndexes(false);

      for (int i = 0; i < TOT; ++i) {
        final List<Integer> results = new ArrayList<>();
        for (final Index index : indexes) {
          final IndexCursor value = index.get(new Object[] { i });
          if (value.hasNext())
            results.add((Integer) ((Document) value.next().getRecord()).get("id"));
        }

        total++;
        assertThat(results).hasSize(1);
        assertThat((int) results.get(0)).isEqualTo(i);
      }

      assertThat(total).isEqualTo(TOT);
    });
  }

  @Test
  public void testGetAsRange() {
    database.transaction(this::execute);
  }

  @Test
  public void testRangeFromHead() {
    database.transaction(() -> {

      final Collection<TypeIndex> indexes = database.getSchema().getType(TYPE_NAME).getAllIndexes(false);
      for (int i = 0; i < TOT - 1; ++i) {
        int total = 0;

        for (final Index index : indexes) {
          assertThat(index).isNotNull();

          final IndexCursor iterator = ((RangeIndex) index).range(true, new Object[] { i }, true, new Object[] { i + 1 }, true);
          assertThat((Iterator<? extends Identifiable>) iterator).isNotNull();

          while (iterator.hasNext()) {
            final Identifiable value = iterator.next();

            assertThat(value).isNotNull();

            final int fieldValue = (int) value.asDocument().get("id");
            assertThat(fieldValue >= i && fieldValue <= i + 1).isTrue();

            assertThat(iterator.getKeys()).isNotNull();
            assertThat(iterator.getKeys().length).isEqualTo(1);

            ++total;
          }
        }

        assertThat(total).isEqualTo(2).withFailMessage("range " + i + "-" + (i + 1));
      }
    });
  }

  @Test
  public void testRangeFromTail() {
    database.transaction(() -> {

      final Collection<TypeIndex> indexes = database.getSchema().getType(TYPE_NAME).getAllIndexes(false);
      for (int i = TOT - 1; i > 0; --i) {
        int total = 0;

        for (final Index index : indexes) {
          assertThat(index).isNotNull();

          final IndexCursor iterator;
          iterator = ((RangeIndex) index).range(false, new Object[] { i }, true, new Object[] { i - 1 }, true);
          assertThat((Iterable<? extends Identifiable>) iterator).isNotNull();

          while (iterator.hasNext()) {
            final Identifiable value = iterator.next();

            assertThat(value).isNotNull();

            final int fieldValue = (int) value.asDocument().get("id");
            assertThat(fieldValue >= i - 1 && fieldValue <= i).isTrue();

            assertThat(iterator.getKeys()).isNotNull();
            assertThat(iterator.getKeys().length).isEqualTo(1);

            ++total;
          }
        }

        assertThat(total).isEqualTo(2).withFailMessage("range " + i + "-" + (i - 1));
      }
    });
  }

  @Test
  public void testRangeWithSQL() {
    database.transaction(() -> {
      for (int i = 0; i < TOT - 1; ++i) {
        int total = 0;

        try {
          ResultSet iterator = database.command("sql",
              "select from " + TYPE_NAME + " where id >= " + i + " and id <= " + (i + 1));

          assertThat((Iterator<? extends Result>) iterator).isNotNull();

          while (iterator.hasNext()) {
            Result value = iterator.next();

            assertThat(value).isNotNull();

            int id = value.<Integer>getProperty("id");

            assertThat(id)
                .isGreaterThanOrEqualTo(i)
                .isLessThanOrEqualTo(i + 1);
            total++;
          }
        } catch (final Exception e) {
          fail(e);
        }
        assertThat(total).isEqualTo(2).withFailMessage("For ids >= " + i + " and <= " + (i + 1));
      }
    });
  }

  @Test
  public void testScanIndexAscending() {
    database.transaction(() -> {

      try {
        // WAIT FOR THE INDEX TO BE COMPACTED
        Thread.sleep(1000);
      } catch (final InterruptedException e) {
        e.printStackTrace();
      }

      int total = 0;

      final Collection<TypeIndex> indexes = database.getSchema().getType(TYPE_NAME).getAllIndexes(false);
      for (final Index index : indexes) {
        assertThat(index).isNotNull();

        final IndexCursor iterator;
        try {
          iterator = ((RangeIndex) index).iterator(true);

//            LogManager.instance()
//                .log(this, Level.INFO, "*****************************************************************************\nCURSOR BEGIN%s", iterator.dumpStats());

          assertThat((Iterable<? extends Identifiable>) iterator).isNotNull();

          while (iterator.hasNext()) {
            assertThat(iterator.next()).isNotNull();

            assertThat(iterator.getKeys()).isNotNull();
            assertThat(iterator.getKeys().length).isEqualTo(1);

            total++;
          }

//            LogManager.instance().log(this, Level.INFO, "*****************************************************************************\nCURSOR END total=%d %s", total,
//                iterator.dumpStats());

        } catch (final Exception e) {
          fail(e);
        }
      }

      assertThat(total).isEqualTo(TOT);
    });
  }

  @Test
  public void testScanIndexDescending() {
    database.transaction(() -> {

      try {
        // WAIT FOR THE INDEX TO BE COMPACTED
        Thread.sleep(1000);
      } catch (final InterruptedException e) {
        e.printStackTrace();
      }

      int total = 0;

      final Collection<TypeIndex> indexes = database.getSchema().getType(TYPE_NAME).getAllIndexes(false);
      for (final Index index : indexes) {
        assertThat(index).isNotNull();

        final IndexCursor iterator;
        try {
          iterator = ((RangeIndex) index).iterator(false);
          assertThat((Iterable<? extends Identifiable>) iterator).isNotNull();

          while (iterator.hasNext()) {
            assertThat(iterator.next()).isNotNull();

            assertThat(iterator.getKeys()).isNotNull();
            assertThat(iterator.getKeys().length).isEqualTo(1);

            //LogManager.instance().log(this, Level.INFO, "Index %s Key %s", null, index, Arrays.toString(iterator.getKeys()));

            total++;
          }
        } catch (final Exception e) {
          fail(e);
        }
      }

      assertThat(total).isEqualTo(TOT);
    });
  }

  @Test
  public void testScanIndexAscendingPartialInclusive() {
    database.transaction(() -> {

      int total = 0;

      final Collection<TypeIndex> indexes = database.getSchema().getType(TYPE_NAME).getAllIndexes(false);
      for (final Index index : indexes) {
        assertThat(index).isNotNull();

        final IndexCursor iterator;
        try {
          iterator = ((RangeIndex) index).iterator(true, new Object[] { 10 }, true);

          assertThat((Iterable<? extends Identifiable>) iterator).isNotNull();

          while (iterator.hasNext()) {
            assertThat(iterator.next()).isNotNull();

            assertThat(iterator.getKeys()).isNotNull();
            assertThat(iterator.getKeys().length).isEqualTo(1);

            total++;
          }
        } catch (final Exception e) {
          fail(e);
        }
      }

      assertThat(total).isEqualTo(TOT - 10);
    });
  }

  @Test
  public void testScanIndexAscendingPartialExclusive() {
    database.transaction(() -> {

      int total = 0;

      final Collection<TypeIndex> indexes = database.getSchema().getType(TYPE_NAME).getAllIndexes(false);
      for (final Index index : indexes) {
        assertThat(index).isNotNull();

        final IndexCursor iterator;
        try {
          iterator = ((RangeIndex) index).iterator(true, new Object[] { 10 }, false);

          assertThat((Iterable<? extends Identifiable>) iterator).isNotNull();

          while (iterator.hasNext()) {
            assertThat(iterator.next()).isNotNull();

            assertThat(iterator.getKeys()).isNotNull();
            assertThat(iterator.getKeys().length).isEqualTo(1);

            total++;
          }
        } catch (final Exception e) {
          fail(e);
        }
      }

      assertThat(total).isEqualTo(TOT - 11);
    });
  }

  @Test
  public void testScanIndexDescendingPartialInclusive() {
    database.transaction(() -> {

      int total = 0;

      final Collection<TypeIndex> indexes = database.getSchema().getType(TYPE_NAME).getAllIndexes(false);
      for (final Index index : indexes) {
        assertThat(index).isNotNull();

        final IndexCursor iterator;
        try {
          iterator = ((RangeIndex) index).iterator(false, new Object[] { 9 }, true);
          assertThat((Iterable<? extends Identifiable>) iterator).isNotNull();

          while (iterator.hasNext()) {
            assertThat(iterator.next()).isNotNull();

            assertThat(iterator.getKeys()).isNotNull();
            assertThat(iterator.getKeys().length).isEqualTo(1);

            total++;
          }
        } catch (final Exception e) {
          fail(e);
        }
      }

      assertThat(total).isEqualTo(10);
    });
  }

  @Test
  public void testScanIndexDescendingPartialExclusive() {
    database.transaction(() -> {

      int total = 0;

      final Collection<TypeIndex> indexes = database.getSchema().getType(TYPE_NAME).getAllIndexes(false);
      for (final Index index : indexes) {
        assertThat(index).isNotNull();

        final IndexCursor iterator;
        try {
          iterator = ((RangeIndex) index).iterator(false, new Object[] { 9 }, false);
          assertThat((Iterable<? extends Identifiable>) iterator).isNotNull();

          while (iterator.hasNext()) {
            assertThat(iterator.next()).isNotNull();

            assertThat(iterator.getKeys()).isNotNull();
            assertThat(iterator.getKeys().length).isEqualTo(1);

            total++;
          }
        } catch (final Exception e) {
          fail(e);
        }
      }

      assertThat(total).isEqualTo(9);
    });
  }

  @Test
  public void testScanIndexRangeInclusive2Inclusive() {
    database.transaction(() -> {

      int total = 0;

      final Collection<TypeIndex> indexes = database.getSchema().getType(TYPE_NAME).getAllIndexes(false);
      for (final Index index : indexes) {
        assertThat(index).isNotNull();

        final IndexCursor iterator;
        try {
          iterator = ((RangeIndex) index).range(true, new Object[] { 10 }, true, new Object[] { 19 }, true);
          assertThat((Iterable<? extends Identifiable>) iterator).isNotNull();

          while (iterator.hasNext()) {
            final Identifiable value = iterator.next();

            assertThat(value).isNotNull();

            final int fieldValue = (int) value.asDocument().get("id");
            assertThat(fieldValue >= 10 && fieldValue <= 19).isTrue();

            assertThat(iterator.getKeys()).isNotNull();
            assertThat(iterator.getKeys().length).isEqualTo(1);

            total++;
          }
        } catch (final Exception e) {
          fail(e);
        }
      }

      assertThat(total).isEqualTo(10);
    });
  }

  @Test
  public void testScanIndexRangeInclusive2Exclusive() {
    database.transaction(() -> {

      int total = 0;

      final Collection<TypeIndex> indexes = database.getSchema().getType(TYPE_NAME).getAllIndexes(false);
      for (final Index index : indexes) {
        assertThat(index).isNotNull();

        final IndexCursor iterator;
        try {
          iterator = ((RangeIndex) index).range(true, new Object[] { 10 }, true, new Object[] { 19 }, false);
          assertThat((Iterable<? extends Identifiable>) iterator).isNotNull();

          while (iterator.hasNext()) {
            final Identifiable value = iterator.next();

            assertThat(value).isNotNull();

            final int fieldValue = (int) value.asDocument().get("id");
            assertThat(fieldValue >= 10 && fieldValue < 19).isTrue();

            assertThat(iterator.getKeys()).isNotNull();
            assertThat(iterator.getKeys().length).isEqualTo(1);

            total++;
          }
        } catch (final Exception e) {
          fail(e);
        }
      }

      assertThat(total).isEqualTo(9);
    });
  }

  @Test
  public void testScanIndexRangeExclusive2Inclusive() {
    database.transaction(() -> {

      int total = 0;

      final Collection<TypeIndex> indexes = database.getSchema().getType(TYPE_NAME).getAllIndexes(false);
      for (final Index index : indexes) {
        assertThat(index).isNotNull();

        final IndexCursor iterator;
        try {
          iterator = ((RangeIndex) index).range(true, new Object[] { 10 }, false, new Object[] { 19 }, true);
          assertThat((Iterable<? extends Identifiable>) iterator).isNotNull();

          while (iterator.hasNext()) {
            final Identifiable value = iterator.next();

            assertThat(value).isNotNull();

            final int fieldValue = (int) value.asDocument().get("id");
            assertThat(fieldValue > 10 && fieldValue <= 19).isTrue();

            assertThat(iterator.getKeys()).isNotNull();
            assertThat(iterator.getKeys().length).isEqualTo(1);

            total++;
          }
        } catch (final Exception e) {
          fail(e);
        }
      }

      assertThat(total).isEqualTo(9);
    });
  }

  @Test
  public void testScanIndexRangeExclusive2InclusiveInverse() {
    database.transaction(() -> {

      int total = 0;

      final Collection<TypeIndex> indexes = database.getSchema().getType(TYPE_NAME).getAllIndexes(false);
      for (final Index index : indexes) {
        assertThat(index).isNotNull();

        final IndexCursor iterator;
        try {
          iterator = ((RangeIndex) index).range(false, new Object[] { 19 }, false, new Object[] { 10 }, true);
          assertThat((Iterable<? extends Identifiable>) iterator).isNotNull();

          while (iterator.hasNext()) {
            final Identifiable value = iterator.next();

            assertThat(value).isNotNull();

            final int fieldValue = (int) value.asDocument().get("id");
            assertThat(fieldValue >= 10 && fieldValue < 19).isTrue();

            assertThat(iterator.getKeys()).isNotNull();
            assertThat(iterator.getKeys().length).isEqualTo(1);

            total++;
          }
        } catch (final Exception e) {
          fail(e);
        }
      }

      assertThat(total).isEqualTo(9);
    });
  }

  @Test
  public void testScanIndexRangeExclusive2Exclusive() {
    database.transaction(() -> {

      int total = 0;

      final Collection<TypeIndex> indexes = database.getSchema().getType(TYPE_NAME).getAllIndexes(false);
      for (final Index index : indexes) {
        assertThat(index).isNotNull();

        final IndexCursor iterator;
        try {
          iterator = ((RangeIndex) index).range(true, new Object[] { 10 }, false, new Object[] { 19 }, false);
          assertThat((Iterable<? extends Identifiable>) iterator).isNotNull();

          while (iterator.hasNext()) {
            final Identifiable value = iterator.next();

            assertThat(value).isNotNull();

            final int fieldValue = (int) value.asDocument().get("id");
            assertThat(fieldValue > 10 && fieldValue < 19).isTrue();

            assertThat(iterator.getKeys()).isNotNull();
            assertThat(iterator.getKeys().length).isEqualTo(1);

            total++;
          }
        } catch (final Exception e) {
          fail(e);
        }
      }

      assertThat(total).isEqualTo(8);
    });
  }

  @Test
  public void testScanIndexRangeExclusive2ExclusiveInverse() {
    database.transaction(() -> {

      int total = 0;

      final Collection<TypeIndex> indexes = database.getSchema().getType(TYPE_NAME).getAllIndexes(false);
      for (final Index index : indexes) {
        assertThat(index).isNotNull();

        final IndexCursor iterator;
        try {
          iterator = ((RangeIndex) index).range(false, new Object[] { 19 }, false, new Object[] { 10 }, false);
          assertThat((Iterable<? extends Identifiable>) iterator).isNotNull();

          while (iterator.hasNext()) {
            final Identifiable value = iterator.next();

            assertThat(value).isNotNull();

            final int fieldValue = (int) value.asDocument().get("id");
            assertThat(fieldValue > 10 && fieldValue < 19).isTrue();

            assertThat(iterator.getKeys()).isNotNull();
            assertThat(iterator.getKeys().length).isEqualTo(1);

            total++;
          }
        } catch (final Exception e) {
          fail(e);
        }
      }

      assertThat(total).isEqualTo(8);
    });
  }

  @Test
  public void testUniqueConcurrentWithIndexesCompaction() {
    GlobalConfiguration.INDEX_COMPACTION_MIN_PAGES_SCHEDULE.setValue(0);

    database.begin();
    final long startingWith = database.countType(TYPE_NAME, true);

    final long total = 2000;
    final int maxRetries = 100;

    final Thread[] threads = new Thread[16];

    final AtomicLong needRetryExceptions = new AtomicLong();
    final AtomicLong duplicatedExceptions = new AtomicLong();
    final AtomicLong crossThreadsInserted = new AtomicLong();

    LogManager.instance().log(this, Level.FINE, "%s Started with %d threads", null, getClass(), threads.length);

    for (int i = 0; i < threads.length; ++i) {
      threads[i] = new Thread(new Runnable() {
        @Override
        public void run() {
          try {
            int threadInserted = 0;
            for (int i = TOT; i < TOT + total; ++i) {
              boolean keyPresent = false;
              for (int retry = 0; retry < maxRetries && !keyPresent; ++retry) {

                try {
                  Thread.sleep(new Random().nextInt(10));
                } catch (final InterruptedException e) {
                  e.printStackTrace();
                  Thread.currentThread().interrupt();
                  return;
                }

                database.begin();
                try {
                  final MutableDocument v = database.newDocument(TYPE_NAME);
                  v.set("id", i);
                  v.set("name", "Jay");
                  v.set("surname", "Miner");
                  v.save();

                  database.commit();

                  threadInserted++;
                  crossThreadsInserted.incrementAndGet();

                  if (threadInserted % 1000 == 0)
                    LogManager.instance()
                        .log(this, Level.FINE, "%s Thread %d inserted %d records with key %d (total=%d)", null, getClass(),
                            Thread.currentThread().getId(), i, threadInserted, crossThreadsInserted.get());

                  keyPresent = true;

                } catch (final NeedRetryException e) {
                  needRetryExceptions.incrementAndGet();
                  assertThat(database.isTransactionActive()).isFalse();
                  continue;
                } catch (final DuplicatedKeyException e) {
                  duplicatedExceptions.incrementAndGet();
                  keyPresent = true;
                  assertThat(database.isTransactionActive()).isFalse();
                } catch (final Exception e) {
                  LogManager.instance()
                      .log(this, Level.SEVERE, "%s Thread %d Generic Exception", e, getClass(), Thread.currentThread().getId());
                  assertThat(database.isTransactionActive()).isFalse();
                  return;
                }
              }

              if (!keyPresent)
                LogManager.instance()
                    .log(this, Level.WARNING, "%s Thread %d Cannot create key %d after %d retries! (total=%d)", null, getClass(),
                        Thread.currentThread().getId(), i, maxRetries, crossThreadsInserted.get());

            }

            LogManager.instance()
                .log(this, Level.FINE, "%s Thread %d completed (inserted=%d)", null, getClass(), Thread.currentThread().getId(),
                    threadInserted);

          } catch (final Exception e) {
            LogManager.instance().log(this, Level.SEVERE, "%s Thread %d Error", e, getClass(), Thread.currentThread().getId());
          }
        }

      });
    }

    for (int i = 0; i < threads.length; ++i)
      threads[i].start();

    for (int i = 0; i < threads.length; ++i) {
      try {
        threads[i].join();
      } catch (final InterruptedException e) {
        e.printStackTrace();
      }
    }

    LogManager.instance()
        .log(this, Level.FINE, "%s Completed (inserted=%d needRetryExceptions=%d duplicatedExceptions=%d)", null, getClass(),
            crossThreadsInserted.get(), needRetryExceptions.get(), duplicatedExceptions.get());

    if (total != crossThreadsInserted.get()) {
      LogManager.instance().log(this, Level.FINE, "DUMP OF INSERTED RECORDS (ORDERED BY ID)");
      final ResultSet resultset = database.query("sql",
          "select id, count(*) as total from ( select from " + TYPE_NAME + " group by id ) where total > 1 order by id");
      while (resultset.hasNext())
        LogManager.instance().log(this, Level.FINE, "- %s", null, resultset.next());

      LogManager.instance().log(this, Level.FINE, "COUNT OF INSERTED RECORDS (ORDERED BY ID)");
      final Map<Integer, Integer> result = new HashMap<>();
      database.scanType(TYPE_NAME, true, record -> {
        final int id = (int) record.get("id");
        final Integer key = result.get(id);
        if (key == null)
          result.put(id, 1);
        else
          result.put(id, key + 1);
        return true;
      });

      LogManager.instance().log(this, Level.FINE, "FOUND %d ENTRIES", null, result.size());

      final Iterator<Map.Entry<Integer, Integer>> it = result.entrySet().iterator();
      while (it.hasNext()) {
        final Map.Entry<Integer, Integer> next = it.next();
        if (next.getValue() > 1)
          LogManager.instance().log(this, Level.FINE, "- %d = %d", null, next.getKey(), next.getValue());
      }
    }

    assertThat(crossThreadsInserted.get()).isEqualTo(total);
//    Assertions.assertThat(needRetryExceptions.get() > 0).isTrue();
    assertThat(duplicatedExceptions.get() > 0).isTrue();

    assertThat(database.countType(TYPE_NAME, true)).isEqualTo(startingWith + total);
  }

  @Test
  public void testRebuildIndex() {
    final Index typeIndexBefore = database.getSchema().getIndexByName(TYPE_NAME + "[id]");
    assertThat(typeIndexBefore).isNotNull();
    assertThat(typeIndexBefore.getPropertyNames().size()).isEqualTo(1);

    database.command("sql", "rebuild index * with batchSize = 1000");

    final Index typeIndexAfter = database.getSchema().getIndexByName(TYPE_NAME + "[id]");
    assertThat(typeIndexAfter).isNotNull();
    assertThat(typeIndexAfter.getPropertyNames().size()).isEqualTo(1);

    assertThat(typeIndexAfter.getName()).isEqualTo(typeIndexBefore.getName());

    assertThat(typeIndexAfter.get(new Object[] { 0 }).hasNext()).isTrue();
    assertThat(typeIndexAfter.get(new Object[] { 0 }).next().asDocument().getInteger("id")).isEqualTo(0);
  }

  @Test
  public void testIndexNameSpecialCharacters() throws InterruptedException {
    VertexType type = database.getSchema().createVertexType("This.is:special");
    type.createProperty("other.special:property", Type.STRING);

    while (true) {
      database.async().waitCompletion();
      try {
        final TypeIndex idx = type.createTypeIndex(Schema.INDEX_TYPE.LSM_TREE, true, "other.special:property");
        database.command("sql", "rebuild index `" + idx.getName() + "`");
        break;
      } catch (NeedRetryException e) {
        // RETRY
        Thread.sleep(1000);
      }
    }
  }

  @Test
  public void testIndexNameSpecialCharactersUsingSQL() throws InterruptedException {
    database.command("sql", "create vertex type `This.is:special`");
    database.command("sql", "create property `This.is:special`.`other.special:property` string");
    database.transaction(() -> {
      database.newVertex("This.is:special").set("other.special:property", "testEncoding").save();
    });

    database.async().waitCompletion();

    // THIS IS NECESSARY TO THE CI TO COMPLETE THE TEST
    Thread.sleep(1000);
    database.async().waitCompletion();

    database.command("sql", "create index on `This.is:special`(`other.special:property`) unique");
    database.command("sql", "rebuild index `This.is:special[other.special:property]`");

    database.close();

    database = factory.exists() ? factory.open() : factory.create();
    database.command("sql", "rebuild index `This.is:special[other.special:property]`");

    assertThat(database.query("sql", "select from `This.is:special` where `other.special:property` = 'testEncoding'")
        .nextIfAvailable().<String>getProperty("other.special:property")).isEqualTo("testEncoding");
  }

  @Test
  public void testSQL() {
    final Index typeIndexBefore = database.getSchema().getIndexByName(TYPE_NAME + "[id]");
    assertThat(typeIndexBefore).isNotNull();
    database.command("sql", "create index if not exists on " + TYPE_NAME + " (id) UNIQUE");
  }

  protected void beginTest() {
    database.transaction(() -> {
      assertThat(database.getSchema().existsType(TYPE_NAME)).isFalse();

      final DocumentType type = database.getSchema().buildDocumentType().withName(TYPE_NAME).withTotalBuckets(3).create();
      type.createProperty("id", Integer.class);
      final Index typeIndex = database.getSchema()
          .createTypeIndex(Schema.INDEX_TYPE.LSM_TREE, true, TYPE_NAME, new String[] { "id" }, PAGE_SIZE);

      for (int i = 0; i < TOT; ++i) {
        final MutableDocument v = database.newDocument(TYPE_NAME);
        v.set("id", i);
        v.set("name", "Jay");
        v.set("surname", "Miner");

        v.save();
      }

      database.commit();
      database.begin();

      for (final IndexInternal index : ((TypeIndex) typeIndex).getIndexesOnBuckets()) {

        assertThat(index.getStats().get("pages")).isGreaterThan(1);
      }
    });
  }

  private void execute() {

    final Collection<TypeIndex> indexes = database.getSchema().getType(TYPE_NAME).getAllIndexes(false);
    for (int i = 0; i < TOT; ++i) {
      int total = 0;

      for (final Index index : indexes) {
        assertThat(index).isNotNull();

        try {
          final IndexCursor iterator;
          iterator = ((RangeIndex) index).range(true, new Object[] { i }, true, new Object[] { i }, true);
          assertThat((Iterable<? extends Identifiable>) iterator).isNotNull();

          while (iterator.hasNext()) {
            final Identifiable value = iterator.next();

            assertThat(value).isNotNull();

            final int fieldValue = (int) value.asDocument().get("id");
            assertThat(fieldValue).isEqualTo(i);

            assertThat(iterator.getKeys()).isNotNull();
            assertThat(iterator.getKeys().length).isEqualTo(1);

            total++;
          }
        } catch (final Exception e) {
          fail(e);
        }
      }

      assertThat(total).isEqualTo(1);
    }
  }
}
