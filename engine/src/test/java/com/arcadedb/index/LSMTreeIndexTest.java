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
import com.arcadedb.database.Document;
import com.arcadedb.database.Identifiable;
import com.arcadedb.database.MutableDocument;
import com.arcadedb.database.RID;
import com.arcadedb.exception.DuplicatedKeyException;
import com.arcadedb.exception.NeedRetryException;
import com.arcadedb.log.DefaultLogger;
import com.arcadedb.log.LogManager;
import com.arcadedb.log.Logger;
import com.arcadedb.query.sql.executor.Result;
import com.arcadedb.query.sql.executor.ResultSet;
import com.arcadedb.schema.DocumentType;
import com.arcadedb.schema.Schema;
import org.assertj.core.api.Assertions;
import org.awaitility.Awaitility;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicLong;
import java.util.logging.Level;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class LSMTreeIndexTest extends TestHelper {
  private static final int    TOT       = 100000;
  private static final String TYPE_NAME = "V";
  private static final int    PAGE_SIZE = 20000;

  @Test
  void get() {
    database.transaction(() -> {
      int total = 0;

      final Index[] indexes = database.getSchema().getIndexes();

      for (int i = 0; i < TOT; ++i) {
        final List<Integer> results = new ArrayList<>();
        for (final Index index : indexes) {
          if (index instanceof TypeIndex)
            continue;

          final IndexCursor value = index.get(new Object[] { i });
          if (value.hasNext())
            results.add((Integer) ((Document) value.next().getRecord()).get("id"));
        }

        total++;
        assertThat(results).hasSize(1);
        assertThat((int) results.getFirst()).isEqualTo(i);
      }

      assertThat(total).isEqualTo(TOT);
    });
  }

  @Test
  void getAsRange() {
    database.transaction(() -> {

      final Index[] indexes = database.getSchema().getIndexes();
      for (int i = 0; i < TOT; ++i) {
        int total = 0;

        for (final Index index : indexes) {
          if (index instanceof TypeIndex)
            continue;

          assertThat(index).isNotNull();

          final IndexCursor iterator;
          try {
            iterator = ((RangeIndex) index).range(true, new Object[] { i }, true, new Object[] { i }, true);
            assertThat((Iterator<? extends Identifiable>) iterator).isNotNull();

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
            Assertions.fail(e);
          }
        }

        assertThat(total).withFailMessage("Get item with id=" + i).isEqualTo(1);
      }
    });
  }

  @Test
  void rangeFromHead() {
    database.transaction(() -> {

      final Index[] indexes = database.getSchema().getIndexes();
      for (int i = 0; i < TOT - 1; ++i) {
        int total = 0;

        for (final Index index : indexes) {
          if (index instanceof TypeIndex)
            continue;

          assertThat(index).isNotNull();

          final IndexCursor iterator;
          iterator = ((RangeIndex) index).range(true, new Object[] { i }, true, new Object[] { i + 1 }, true);
          assertThat((Iterable<? extends Identifiable>) iterator).isNotNull();

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

        assertThat(total).withFailMessage("range " + i + "-" + (i + 1)).isEqualTo(2);
      }
    });
  }

//  @Test
//  public void testRangeFromTail() {
//    database.transaction(new Database.TransactionScope() {
//      @Override
//      public void execute(Database database) {
//
//        final Index[] indexes = database.getSchema().getIndexes();
//        for (int i = TOT - 1; i > 0; --i) {
//          int total = 0;
//
//          for (Index index : indexes) {
//            if( index instanceof TypeIndex)
//              continue;
//            Assertions.assertThat(index).isNotNull();
//
//            final IndexCursor iterator;
//            iterator = ((RangeIndex) index).range(new Object[] { i }, true, new Object[] { i - 1 }, true);
//            Assertions.assertThat(iterator).isNotNull();
//
//            while (iterator.hasNext()) {
//              Identifiable value = iterator.next();
//
//              Assertions.assertThat(value).isNotNull();
//
//              int fieldValue = (int) value.asDocument().get("id");
//              Assertions.assertThat(fieldValue >= i - 1 && fieldValue <= i).isTrue();
//
//              Assertions.assertThat(iterator.getKeys()).isNotNull();
//              Assertions.assertThat(iterator.getKeys().length).isEqualTo(1);
//
//              ++total;
//            }
//          }
//
//          Assertions.assertThat(total).isEqualTo(2, within("range " + i + "-" + (i - 1)));
//        }
//      }
//    });
//  }

  @Test
  void removeKeys() {
    database.transaction(() -> {
      int total = 0;

      final Index[] indexes = database.getSchema().getIndexes();

      for (int i = 0; i < TOT; ++i) {
        int found = 0;

        final Object[] key = new Object[] { i };

        for (final Index index : indexes) {
          if (index instanceof TypeIndex)
            continue;

          final IndexCursor value = index.get(key);
          if (value.hasNext()) {
            index.remove(key);
            found++;
            total++;
          }
        }

        assertThat(found).withFailMessage("Key '" + Arrays.toString(key) + "' found " + found + " times").isEqualTo(1);
      }

      assertThat(total).isEqualTo(TOT);

      // GET EACH ITEM TO CHECK IT HAS BEEN DELETED
      for (int i = 0; i < TOT; ++i) {
        for (final Index index : indexes) {
          if (index instanceof TypeIndex)
            continue;
          assertThat(index.get(new Object[] { i }).hasNext()).withFailMessage(
              "Found item with key " + i + " inside the TX by using get()").isFalse();
        }
      }

      // CHECK ALSO WITH RANGE
// RANGE DOES NOT WORK WITH TX CHANGES YET
//        for (int i = 0; i < TOT; ++i) {
//          for (Index index : indexes) {
//            if (index instanceof TypeIndex)
//              continue;
//            final IndexCursor cursor = ((RangeIndex) index).range(new Object[] { i }, true, new Object[] { i }, true);
//
//            Assertions.assertThat(cursor.hasNext() && cursor.next().isFalse() != null, "Found item with key " + i + " inside the TX by using range()");
//          }
//        }
    }, true, 0);

    // CHECK ALSO AFTER THE TX HAS BEEN COMMITTED
    database.transaction(() -> {
      final Index[] indexes = database.getSchema().getIndexes();
      for (int i = 0; i < TOT; ++i) {
        for (final Index index : indexes) {
          if (index instanceof TypeIndex)
            continue;
          assertThat(index.get(new Object[] { i }).hasNext()).withFailMessage(
              "Found item with key " + i + " after the TX was committed").isFalse();
        }
      }

      // CHECK ALSO WITH RANGE
      for (int i = 0; i < TOT; ++i) {
        for (final Index index : indexes) {
          if (index instanceof TypeIndex)
            continue;

          final IndexCursor cursor = ((RangeIndex) index).range(true, new Object[] { i }, true, new Object[] { i }, true);

          assertThat(cursor.hasNext() && cursor.next() != null).withFailMessage(
              "Found item with key " + i + " after the TX was committed by using range()").isFalse();
        }
      }
    }, true, 0);
  }

  @Test
  void removeEntries() {
    database.transaction(() -> {
      int total = 0;

      final Index[] indexes = database.getSchema().getIndexes();

      for (int i = 0; i < TOT; ++i) {
        int found = 0;

        final Object[] key = new Object[] { i };

        for (final Index index : indexes) {
          if (index instanceof TypeIndex)
            continue;

          final IndexCursor value = index.get(key);
          if (value.hasNext()) {
            for (final Identifiable r : value)
              index.remove(key, r);
            found++;
            total++;
          }
        }

        assertThat(found).withFailMessage("Key '" + Arrays.toString(key) + "' found " + found + " times").isEqualTo(1);
      }

      assertThat(total).isEqualTo(TOT);

      // GET EACH ITEM TO CHECK IT HAS BEEN DELETED
      for (int i = 0; i < TOT; ++i) {
        for (final Index index : indexes) {
          if (index instanceof TypeIndex)
            continue;

          assertThat(index.get(new Object[] { i }).hasNext()).withFailMessage("Found item with key " + i).isFalse();
        }
      }

      // CHECK ALSO WITH RANGE
// RANGE DOES NOT WORK WITH TX CHANGES YET
//        for (int i = 0; i < TOT; ++i) {
//          for (Index index : indexes) {
//            if (index instanceof TypeIndex)
//              continue;
//
//            Assertions.assertThat(((RangeIndex) index).isFalse().range(new Object[] { i }, true, new Object[] { i }, true).hasNext(),
//                "Found item with key " + i + " inside the TX by using range()");
//          }
//        }
    });

    // CHECK ALSO AFTER THE TX HAS BEEN COMMITTED
    database.transaction(() -> {
      final Index[] indexes = database.getSchema().getIndexes();
      for (int i = 0; i < TOT; ++i) {
        for (final Index index : indexes) {
          if (index instanceof TypeIndex)
            continue;

          assertThat(index.get(new Object[] { i }).hasNext()).withFailMessage(
              "Found item with key " + i + " after the TX was committed").isFalse();
        }
      }

      // CHECK ALSO WITH RANGE
      for (int i = 0; i < TOT; ++i) {
        for (final Index index : indexes) {
          if (index instanceof TypeIndex)
            continue;

          final IndexCursor cursor = ((RangeIndex) index).range(true, new Object[] { i }, true, new Object[] { i }, true);

          assertThat(cursor.hasNext() && cursor.next() != null).withFailMessage(
              "Found item with key " + i + " after the TX was committed by using range()").isFalse();
        }
      }
    }, true, 0);
  }

  @Test
  void removeEntriesMultipleTimes() {
    database.transaction(() -> {
      int total = 0;

      final Index[] indexes = database.getSchema().getIndexes();

      for (int i = 0; i < TOT; ++i) {
        int found = 0;

        final Object[] key = new Object[] { i };

        for (final Index index : indexes) {
          if (index instanceof TypeIndex)
            continue;

          final IndexCursor value = index.get(key);
          if (value.hasNext()) {
            for (final Identifiable r : value) {
              for (int k = 0; k < 10; ++k)
                index.remove(key, r);
            }
            found++;
            total++;
          }
        }

        assertThat(found).withFailMessage("Key '" + Arrays.toString(key) + "' found " + found + " times").isEqualTo(1);
      }

      assertThat(total).isEqualTo(TOT);

      // GET EACH ITEM TO CHECK IT HAS BEEN DELETED
      for (int i = 0; i < TOT; ++i) {
        for (final Index index : indexes) {
          if (index instanceof TypeIndex)
            continue;

          assertThat(index.get(new Object[] { i }).hasNext()).withFailMessage("Found item with key " + i).isFalse();
        }
      }
    });
  }

  @Test
  void removeAndPutEntries() {
    //database.getConfiguration().setValue(GlobalConfiguration.INDEX_COMPACTION_MIN_PAGES_SCHEDULE, 0); // DISABLE COMPACTION

    database.transaction(() -> {
      int total = 0;

      final Index[] indexes = database.getSchema().getIndexes();

      for (int i = 0; i < TOT; ++i) {
        int found = 0;

        final Object[] key = new Object[] { i };

        for (final Index index : indexes) {
          if (index instanceof TypeIndex)
            continue;

          final IndexCursor value = index.get(key);
          if (value.hasNext()) {
            for (final Identifiable r : value) {
              index.remove(key, r);
              index.put(key, new RID[] { r.getIdentity() });
              index.remove(key, r);
            }
            found++;
            total++;
          }
        }

        assertThat(found).withFailMessage("Key '" + Arrays.toString(key) + "' found " + found + " times").isEqualTo(1);
      }

      assertThat(total).isEqualTo(TOT);

      // GET EACH ITEM TO CHECK IT HAS BEEN DELETED
      for (int i = 0; i < TOT; ++i) {
        for (final Index index : indexes) {
          if (index instanceof TypeIndex)
            continue;

          assertThat(index.get(new Object[] { i }).hasNext()).isFalse().withFailMessage("Found item with key " + i);
        }
      }
    });

    database.transaction(() -> {
      // GET EACH ITEM TO CHECK IT HAS BEEN DELETED
      final Collection<TypeIndex> indexes = database.getSchema().getType(TYPE_NAME).getAllIndexes(false);

      for (int i = 0; i < TOT; ++i) {
        for (final Index index : indexes) {
          if (index instanceof TypeIndex)
            continue;

          assertThat(index.get(new Object[] { i }).hasNext()).isTrue().withFailMessage("Cannot find item with key " + i);
        }
      }
    });
  }

  @Test
  void changePrimaryKeySameTx() {
    database.transaction(() -> {
      for (int i = 0; i < 1000; ++i) {
        final IndexCursor cursor = database.lookupByKey(TYPE_NAME, "id", i);
        assertThat(cursor.hasNext()).withFailMessage("Key " + i + " not found").isTrue();

        final Document doc = cursor.next().asDocument();
        doc.modify().set("id", i + TOT).save();
      }
    });
  }

  @Test
  void deleteCreateSameKeySameTx() {
    database.transaction(() -> {
      for (int i = 0; i < 1000; ++i) {
        final IndexCursor cursor = database.lookupByKey(TYPE_NAME, "id", i);
        assertThat(cursor.hasNext()).withFailMessage("Key " + i + " not found").isTrue();

        final Document doc = cursor.next().asDocument();
        doc.delete();

        final MutableDocument newDoc = database.newDocument(TYPE_NAME).fromMap(doc.toMap()).set("version", 2).save();

        assertThat(newDoc).isNotNull();
      }
    }, true, 2);

    database.transaction(() -> {
      for (int i = 0; i < 1000; ++i) {
        final IndexCursor cursor = database.lookupByKey(TYPE_NAME, "id", i);
        assertThat(cursor.hasNext()).withFailMessage("Key " + i + " not found").isTrue();
        assertThat(cursor.next().asDocument().getInteger("version")).isEqualTo(2);
      }
    });
  }

  @Test
  void updateKeys() {
    database.transaction(() -> {
      int total = 0;

      final ResultSet resultSet = database.query("sql", "select from " + TYPE_NAME);
      while (resultSet.hasNext()) {
        final Result r = resultSet.next();

        assertThat(r.getElement().get().get("id")).isNotNull();

        final MutableDocument record = r.getElement().get().modify();
        record.set("id", (Integer) record.get("id") + 1000000);
        record.save();
      }

      database.commit();
      database.begin();

      final Index[] indexes = database.getSchema().getIndexes();

      // ORIGINAL KEYS SHOULD BE REMOVED
      for (int i = 0; i < TOT; ++i) {
        int found = 0;

        final Object[] key = new Object[] { i };

        for (final Index index : indexes) {
          if (index instanceof TypeIndex)
            continue;

          final IndexCursor value = index.get(key);
          if (value.hasNext()) {
            found++;
            total++;
          }
        }

        assertThat(found).withFailMessage("Key '" + Arrays.toString(key) + "' found " + found + " times").isEqualTo(0);
      }

      assertThat(total).isEqualTo(0);

      total = 0;

      // CHECK FOR NEW KEYS
      for (int i = 1000000; i < 1000000 + TOT; ++i) {
        int found = 0;

        final Object[] key = new Object[] { i };

        for (final Index index : indexes) {
          if (index instanceof TypeIndex)
            continue;

          final IndexCursor value = index.get(key);

          if (value.hasNext()) {
            for (final Identifiable r : value) {
              index.remove(key, r);
              found++;
            }
            total++;
          }
        }

        assertThat(found).withFailMessage("Key '" + Arrays.toString(key) + "' found " + found + " times").isEqualTo(1);
      }

      assertThat(total).isEqualTo(TOT);

      // GET EACH ITEM TO CHECK IT HAS BEEN DELETED
      for (int i = 0; i < TOT; ++i) {
        for (final Index index : indexes)
          assertThat(index.get(new Object[] { i }).hasNext()).withFailMessage("Found item with key " + i).isFalse();
        ;
      }

    });
  }

  @Test
  void putDuplicates() {
    database.transaction(() -> {
      int total = 0;

      final Index[] indexes = database.getSchema().getIndexes();

      for (int i = 0; i < TOT; ++i) {
        int found = 0;

        final Object[] key = new Object[] { i };

        for (final Index index : indexes) {
          if (index instanceof TypeIndex)
            continue;

          final IndexCursor value = index.get(key);
          if (value.hasNext()) {
            assertThatThrownBy(() -> {
              index.put(key, new RID[] { new RID(database, 10, 10) });
              database.commit();
            }).isInstanceOf(DuplicatedKeyException.class);
            database.begin();
            found++;
            total++;
          }
        }

        assertThat(found).withFailMessage("Key '" + Arrays.toString(key) + "' found " + found + " times").isEqualTo(1);
      }

      assertThat(total).isEqualTo(TOT);
    });
  }

  @Test
  void scanIndexAscending() {
    database.transaction(() -> {

      // Wait for the index to be compacted using awaitility
      // Increased timeout for CI environments
      Awaitility.await("Wait for index compaction (ascending)")
          .atMost(30, TimeUnit.SECONDS)
          .pollInterval(200, TimeUnit.MILLISECONDS)
          .until(() -> {
            // Check if all indexes are ready by trying to access them
            try {
              for (final Index index : database.getSchema().getIndexes()) {
                if (!(index instanceof TypeIndex)) {
                  ((RangeIndex) index).iterator(true);
                }
              }
              return true;
            } catch (Exception e) {
              // Retry on any exception - indexes may still be compacting
              return false;
            }
          });

      int total = 0;

      final Index[] indexes = database.getSchema().getIndexes();
      for (final Index index : indexes) {
        if (index instanceof TypeIndex)
          continue;

        assertThat(index).isNotNull();

        try {
          final IndexCursor iterator = ((RangeIndex) index).iterator(true);

//            LogManager.instance()
//                .log(this, Level.INFO, "*****************************************************************************\nCURSOR BEGIN%s", iterator.dumpStats());

          assertThat((Iterator<? extends Identifiable>) iterator).isNotNull();

          while (iterator.hasNext()) {
            assertThat(iterator.next()).isNotNull();

            assertThat(iterator.getKeys()).isNotNull();
            assertThat(iterator.getKeys().length).isEqualTo(1);

            total++;
          }

//            LogManager.instance().log(this, Level.INFO, "*****************************************************************************\nCURSOR END total=%d %s", total,
//                iterator.dumpStats());

        } catch (final Exception e) {
          Assertions.fail(e);
        }
      }

      assertThat(total).isEqualTo(TOT);
    });
  }

  @Test
  void scanIndexDescending() {
    database.transaction(() -> {

      // Wait for the index to be compacted using awaitility
      // Increased timeout for CI environments
      Awaitility.await("Wait for index compaction (descending)")
          .atMost(30, TimeUnit.SECONDS)
          .pollInterval(200, TimeUnit.MILLISECONDS)
          .until(() -> {
            // Check if all indexes are ready by trying to access them
            try {
              for (final Index index : database.getSchema().getIndexes()) {
                if (!(index instanceof TypeIndex)) {
                  ((RangeIndex) index).iterator(false);
                }
              }
              return true;
            } catch (Exception e) {
              // Retry on any exception - indexes may still be compacting
              return false;
            }
          });

      int total = 0;

      final Index[] indexes = database.getSchema().getIndexes();
      for (final Index index : indexes) {
        if (index instanceof TypeIndex)
          continue;

        assertThat(index).isNotNull();

        try {
          final IndexCursor iterator = ((RangeIndex) index).iterator(false);
          assertThat((Iterator<? extends Identifiable>) iterator).isNotNull();

          Object prevKey = null;
          while (iterator.hasNext()) {
            assertThat(iterator.next()).isNotNull();

            final Object[] keys = iterator.getKeys();
            assertThat(keys).isNotNull();
            assertThat(keys.length).isEqualTo(1);

            if (prevKey != null)
              assertThat(((Comparable) keys[0]).compareTo(prevKey) < 0).withFailMessage(
                  "Key " + keys[0] + " is not minor than " + prevKey).isTrue();

            prevKey = keys[0];
            ++total;
          }

        } catch (final Exception e) {
          Assertions.fail(e);
        }
      }

      assertThat(total).isEqualTo(TOT);
    });
  }

  @Test
  void scanIndexAscendingPartialInclusive() {
    database.transaction(() -> {
      int total = 0;

      final Index[] indexes = database.getSchema().getIndexes();
      for (final Index index : indexes) {
        if (index instanceof TypeIndex)
          continue;

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
          Assertions.fail(e);
        }
      }

      assertThat(total).isEqualTo(TOT - 10);
    });
  }

  @Test
  void scanIndexAscendingPartialExclusive() {
    database.transaction(() -> {
      int total = 0;

      final Index[] indexes = database.getSchema().getIndexes();
      for (final Index index : indexes) {
        if (index instanceof TypeIndex)
          continue;

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
          Assertions.fail(e);
        }
      }

      assertThat(total).isEqualTo(TOT - 11);
    });
  }

  @Test
  void scanIndexDescendingPartialInclusive() {
    database.transaction(() -> {
      int total = 0;

      final Index[] indexes = database.getSchema().getIndexes();
      for (final Index index : indexes) {
        if (index instanceof TypeIndex)
          continue;

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
          Assertions.fail(e);
        }
      }

      assertThat(total).isEqualTo(10);
    });
  }

  @Test
  void scanIndexDescendingPartialExclusive() {
    database.transaction(() -> {
      int total = 0;

      final Index[] indexes = database.getSchema().getIndexes();
      for (final Index index : indexes) {
        if (index instanceof TypeIndex)
          continue;

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
          Assertions.fail(e);
        }
      }

      assertThat(total).isEqualTo(9);
    });
  }

  @Test
  void scanIndexRangeInclusive2Inclusive() {
    database.transaction(() -> {
      int total = 0;

      final Index[] indexes = database.getSchema().getIndexes();
      for (final Index index : indexes) {
        if (index instanceof TypeIndex)
          continue;

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
          Assertions.fail(e);
        }
      }

      assertThat(total).isEqualTo(10);
    });
  }

  @Test
  void scanIndexRangeInclusive2Exclusive() {
    database.transaction(() -> {
      int total = 0;

      final Index[] indexes = database.getSchema().getIndexes();
      for (final Index index : indexes) {
        if (index instanceof TypeIndex)
          continue;

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
          Assertions.fail(e);
        }
      }

      assertThat(total).isEqualTo(9);
    });
  }

  @Test
  void scanIndexRangeExclusive2Inclusive() {
    database.transaction(() -> {
      int total = 0;

      final Index[] indexes = database.getSchema().getIndexes();
      for (final Index index : indexes) {
        if (index instanceof TypeIndex)
          continue;

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
          Assertions.fail(e);
        }
      }

      assertThat(total).isEqualTo(9);
    });
  }

  @Test
  void scanIndexRangeExclusive2Exclusive() {
    database.transaction(() -> {
      int total = 0;

      final Index[] indexes = database.getSchema().getIndexes();
      for (final Index index : indexes) {
        if (index instanceof TypeIndex)
          continue;

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
          Assertions.fail(e);
        }
      }

      assertThat(total).isEqualTo(8);
    });
  }

  @Test
  @Tag("slow")
  void uniqueConcurrentWithIndexesCompaction() throws Exception {
    database.begin();
    final long startingWith = database.countType(TYPE_NAME, true);

    final long total = 2000;
    final int maxRetries = 100;

    final AtomicLong needRetryExceptions = new AtomicLong();
    final AtomicLong duplicatedExceptions = new AtomicLong();
    final AtomicLong crossThreadsInserted = new AtomicLong();

    final int threadCount = 16;
    final ExecutorService executorService = Executors.newFixedThreadPool(threadCount);
    final List<Future<?>> futures = new ArrayList<>();
    LogManager.instance().log(this, Level.INFO, "%s Started with %d threads", null, getClass(), threadCount);

    try {
      for (int i = 0; i < threadCount; ++i) {
        Future<?> future = executorService.submit(() -> {
          try {
            int threadInserted = 0;
            for (int i1 = TOT; i1 < TOT + total; ++i1) {
              boolean keyPresent = false;
              for (int retry = 0; retry < maxRetries && !keyPresent; ++retry) {

                // Small random delay using awaitility
                try {
                  TimeUnit.MILLISECONDS.sleep(new Random().nextInt(10));
                } catch (final InterruptedException e) {
                  e.printStackTrace();
                  Thread.currentThread().interrupt();
                  return;
                }

                database.begin();
                try {
                  final MutableDocument v = database.newDocument(TYPE_NAME);
                  v.set("id", i1);
                  v.set("name", "Jay");
                  v.set("surname", "Miner");
                  v.save();

                  database.commit();

                  threadInserted++;
                  crossThreadsInserted.incrementAndGet();

                  if (threadInserted % 1000 == 0)
                    LogManager.instance()
                        .log(this, Level.INFO, "%s Thread %d inserted record %s, total %d records with key %d (total=%d)", null,
                            getClass(), Thread.currentThread().threadId(), v.getIdentity(), i1, threadInserted,
                            crossThreadsInserted.get());

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
                      .log(this, Level.SEVERE, "%s Thread %d Generic Exception", e, getClass(), Thread.currentThread().threadId());
                  assertThat(database.isTransactionActive()).isFalse();
                  return;
                }
              }

              if (!keyPresent)
                LogManager.instance()
                    .log(this, Level.WARNING, "%s Thread %d Cannot create key %d after %d retries! (total=%d)", null, getClass(),
                        Thread.currentThread().threadId(), i1, maxRetries, crossThreadsInserted.get());

            }

            LogManager.instance()
                .log(this, Level.INFO, "%s Thread %d completed (inserted=%d)", null, getClass(), Thread.currentThread().threadId(),
                    threadInserted);

          } catch (final Exception e) {
            LogManager.instance().log(this, Level.SEVERE, "%s Thread %d Error", e, getClass(), Thread.currentThread().threadId());
          }
        });
        futures.add(future);
      }

      // Wait for all threads to complete with explicit timeout
      for (Future<?> future : futures) {
        try {
          future.get(120, TimeUnit.SECONDS);
        } catch (final InterruptedException | ExecutionException | TimeoutException e) {
          LogManager.instance().log(this, Level.WARNING, "Thread execution failed or timed out", e);
        }
      }

    } finally {
      executorService.shutdown();
      try {
        if (!executorService.awaitTermination(60, TimeUnit.SECONDS)) {
          executorService.shutdownNow();
        }
      } catch (InterruptedException e) {
        executorService.shutdownNow();
        Thread.currentThread().interrupt();
      }
    }

    LogManager.instance()
        .log(this, Level.INFO, "%s Completed (inserted=%d needRetryExceptions=%d duplicatedExceptions=%d)", null, getClass(),
            crossThreadsInserted.get(), needRetryExceptions.get(), duplicatedExceptions.get());

    if (total != crossThreadsInserted.get()) {
      LogManager.instance().log(this, Level.INFO, "DUMP OF INSERTED RECORDS (ORDERED BY ID)");
      final ResultSet resultset = database.query("sql",
          "select id, count(*) as total from ( select from " + TYPE_NAME + " group by id ) where total > 1 order by id");
      while (resultset.hasNext())
        LogManager.instance().log(this, Level.INFO, "- %s", null, resultset.next());

      LogManager.instance().log(this, Level.INFO, "COUNT OF INSERTED RECORDS (ORDERED BY ID)");
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

      LogManager.instance().log(this, Level.INFO, "FOUND %d ENTRIES", null, result.size());

      final Iterator<Map.Entry<Integer, Integer>> it = result.entrySet().iterator();
      while (it.hasNext()) {
        final Map.Entry<Integer, Integer> next = it.next();
        if (next.getValue() > 1)
          LogManager.instance().log(this, Level.INFO, "- %d = %d", null, next.getKey(), next.getValue());
      }
    }

    assertThat(crossThreadsInserted.get()).isEqualTo(total);
//    Assertions.assertThat(needRetryExceptions.get() > 0).isTrue();
    assertThat(duplicatedExceptions.get() > 0).isTrue();

    assertThat(database.countType(TYPE_NAME, true)).isEqualTo(startingWith + total);
  }

  protected void beginTest() {
    database.transaction(() -> {
      assertThat(database.getSchema().existsType(TYPE_NAME)).isFalse();

      final DocumentType type = database.getSchema().buildDocumentType().withName(TYPE_NAME).withTotalBuckets(3).create();
      type.createProperty("id", Integer.class);
      final TypeIndex typeIndex = database.getSchema().buildTypeIndex(TYPE_NAME, new String[] { "id" })
          .withType(Schema.INDEX_TYPE.LSM_TREE).withUnique(true).withPageSize(PAGE_SIZE).create();

      for (int i = 0; i < TOT; ++i) {
        final MutableDocument v = database.newDocument(TYPE_NAME);
        v.set("id", i);
        v.set("name", "Jay");
        v.set("surname", "Miner");

        v.save();
      }

      database.commit();
      database.begin();

      for (final IndexInternal index : typeIndex.getIndexesOnBuckets()) {
        assertThat(index.getStats().get("pages") > 1).isTrue();
      }
    });
  }

  @Test
  void buildWithLogging() {
    // Test that the build method logs progress messages
    final List<String> logMessages = new ArrayList<>();

    try {
      // Set custom logger to capture log messages
      LogManager.instance().setLogger(new Logger() {
        @Override
        public void log(final Object requester, final Level level, final String message, final Throwable exception, final String context,
            final Object arg1, final Object arg2, final Object arg3, final Object arg4, final Object arg5, final Object arg6,
            final Object arg7, final Object arg8, final Object arg9, final Object arg10, final Object arg11, final Object arg12,
            final Object arg13, final Object arg14, final Object arg15, final Object arg16, final Object arg17) {
          if (message != null && (message.contains("Building index") || message.contains("Completed building"))) {
            logMessages.add(String.format(message, arg1, arg2, arg3, arg4, arg5, arg6, arg7, arg8, arg9, arg10));
          }
        }

        @Override
        public void log(final Object requester, final Level level, final String message, final Throwable exception, final String context,
            final Object... args) {
          if (message != null && (message.contains("Building index") || message.contains("Completed building"))) {
            if (args != null && args.length > 0) {
              logMessages.add(String.format(message, args));
            } else {
              logMessages.add(message);
            }
          }
        }

        @Override
        public void flush() {
        }
      });

      database.transaction(() -> {
        // Create a type with data
        final DocumentType type = database.getSchema().buildDocumentType().withName("BuildTest").withTotalBuckets(1).create();
        type.createProperty("id", Integer.class);
        type.createProperty("text", String.class);

        // Insert records (enough to trigger multiple log intervals)
        for (int i = 0; i < 25000; ++i) {
          final MutableDocument doc = database.newDocument("BuildTest");
          doc.set("id", i);
          doc.set("text", "Test text " + i);
          doc.save();
        }
      });

      // Now rebuild the index which should trigger logging
      database.transaction(() -> {
        final DocumentType type = database.getSchema().getType("BuildTest");
        database.getSchema().buildTypeIndex("BuildTest", new String[] { "text" })
            .withType(Schema.INDEX_TYPE.LSM_TREE).create();
      });

      // Verify that log messages were captured
      assertThat(logMessages).isNotEmpty();

      // Check for start message
      boolean hasStartMessage = logMessages.stream()
          .anyMatch(msg -> msg.contains("Building index") && msg.contains("properties"));
      assertThat(hasStartMessage).isTrue();

      // Check for progress messages (should have at least 2 for 25000 records with 10K interval)
      long progressMessages = logMessages.stream()
          .filter(msg -> msg.contains("processed") && msg.contains("records/sec"))
          .count();
      assertThat(progressMessages).isGreaterThanOrEqualTo(2);

      // Check for completion message
      boolean hasCompletionMessage = logMessages.stream()
          .anyMatch(msg -> msg.contains("Completed building index"));
      assertThat(hasCompletionMessage).isTrue();

    } finally {
      // Restore default logger
      LogManager.instance().setLogger(new DefaultLogger());
    }
  }
}
