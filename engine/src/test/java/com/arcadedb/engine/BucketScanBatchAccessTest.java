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

import com.arcadedb.database.Database;
import com.arcadedb.database.DatabaseContext;
import com.arcadedb.database.DatabaseFactory;
import com.arcadedb.database.Document;
import com.arcadedb.database.RID;
import com.arcadedb.database.Record;
import com.arcadedb.query.sql.executor.ResultSet;
import com.arcadedb.schema.DocumentType;
import com.arcadedb.security.SecurityDatabaseUser;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Supplier;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.junit.jupiter.api.Assertions.fail;

/**
 * A scan that stays open between two reads (a cursor a client fetches from now and then) asks for the read grant of its bucket
 * again when it loads a further batch of records, so a grant that was changed on the user in the meantime applies to the rest of
 * the scan and not only to the statements that follow it.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class BucketScanBatchAccessTest {
  private static final String PATH     = "target/databases/BucketScanBatchAccessTest";
  private static final String TYPE     = "Item";
  // MORE THAN THE 1,024 RECORDS OF A BATCH, AND MORE THAN ONE PAGE
  private static final int    RECORDS  = 3_000;
  private static final int    BATCH    = 1_024;

  private DatabaseFactory factory;
  private Database        database;
  private LocalBucket     bucket;
  private GrantedUser     user;
  private final List<RID> rids = new ArrayList<>();

  @BeforeEach
  void setUp() {
    factory = new DatabaseFactory(PATH).setSecurity(db -> {
    });
    if (factory.exists())
      factory.open().drop();

    database = factory.create();

    final DocumentType type = database.getSchema().buildDocumentType().withName(TYPE).withTotalBuckets(1).create();
    bucket = (LocalBucket) type.getBuckets(false).getFirst();

    database.transaction(() -> {
      for (int i = 0; i < RECORDS; i++)
        rids.add(database.newDocument(TYPE).set("id", i).set("late", false).save().getIdentity());
    });

    user = new GrantedUser();
    DatabaseContext.INSTANCE.getContext(database.getDatabasePath()).setCurrentUser(user);
  }

  @AfterEach
  void tearDown() {
    DatabaseContext.INSTANCE.getContext(database.getDatabasePath()).setCurrentUser(null);
    database.drop();
    factory.close();
  }

  @Test
  void forwardScanStopsAtTheNextBatchOnceTheGrantIsRemoved() {
    assertRefusedAtTheNextBatch(bucket::iterator);
  }

  @Test
  void backwardScanStopsAtTheNextBatchOnceTheGrantIsRemoved() {
    assertRefusedAtTheNextBatch(bucket::inverseIterator);
  }

  @Test
  void pageRangeScanStopsAtTheNextBatchOnceTheGrantIsRemoved() {
    assertRefusedAtTheNextBatch(() -> bucket.iterator(0, -1));
  }

  @Test
  void positionScanStopsAtTheNextBatchOnceTheGrantIsRemoved() {
    final long[] positions = new long[rids.size()];
    for (int i = 0; i < positions.length; i++)
      positions[i] = rids.get(i).getPosition();

    assertRefusedAtTheNextBatch(() -> bucket.iterator(positions, 0, positions.length));
  }

  @Test
  void scanOpenedBeforeTheGrantWasRemovedIsRefusedAtItsFirstBatch() {
    final Iterator<Record> iterator = bucket.iterator();

    user.revoke(bucket.getFileId());

    assertThatThrownBy(iterator::hasNext).isInstanceOf(SecurityException.class);
  }

  @Test
  void scanKeepsGoingWhileTheGrantHolds() {
    final Iterator<Record> iterator = bucket.iterator();

    int count = 0;
    while (iterator.hasNext()) {
      iterator.next();
      ++count;
    }

    assertThat(count).isEqualTo(RECORDS);
  }

  @Test
  void queryCursorStopsAtTheNextBatchOnceTheGrantIsRemoved() {
    int delivered = 0;
    try (final ResultSet resultSet = database.query("sql", "SELECT FROM " + TYPE)) {
      assertThat(resultSet.hasNext()).isTrue();
      resultSet.next();
      ++delivered;

      user.revoke(bucket.getFileId());
      insertLateRecords();

      try {
        while (resultSet.hasNext()) {
          assertThat(resultSet.next().<Boolean>getProperty("late")).isFalse();
          ++delivered;
        }
        fail("the rest of the scan was served after the grant was removed");
      } catch (final SecurityException e) {
        assertThat(e.getMessage()).contains("not allowed");
      }
    }

    assertThat(delivered).isLessThanOrEqualTo(BATCH);
  }

  /**
   * Reads one record, removes the grant (and writes records that did not exist when it was removed), then reads on: the batch in
   * hand may be delivered, the next one has to be refused and nothing written after the removal is ever handed out.
   */
  private void assertRefusedAtTheNextBatch(final Supplier<Iterator<Record>> opener) {
    final Iterator<Record> iterator = opener.get();

    assertThat(iterator.hasNext()).isTrue();
    iterator.next();
    int delivered = 1;

    user.revoke(bucket.getFileId());
    insertLateRecords();

    try {
      while (iterator.hasNext()) {
        final Record record = iterator.next();
        if (record instanceof Document document)
          assertThat(document.getBoolean("late")).as("a record written after the grant was removed").isFalse();
        ++delivered;
      }
      fail("the rest of the scan was served after the grant was removed");
    } catch (final SecurityException e) {
      assertThat(e.getMessage()).contains("not allowed");
    }

    assertThat(delivered).as("only the batch read before the grant was removed").isLessThanOrEqualTo(BATCH);
  }

  private void insertLateRecords() {
    database.transaction(() -> {
      for (int i = 0; i < 50; i++)
        database.newDocument(TYPE).set("id", RECORDS + i).set("late", true).save();
    });
  }

  /** A user whose grants are changed in place, as a refresh of the permissions of the server does on the cached user. */
  private static final class GrantedUser implements SecurityDatabaseUser {
    private final Set<Integer> deniedFiles = ConcurrentHashMap.newKeySet();

    void revoke(final int fileId) {
      deniedFiles.add(fileId);
    }

    @Override
    public String getName() {
      return "reader";
    }

    @Override
    public boolean requestAccessOnDatabase(final DATABASE_ACCESS access) {
      return true;
    }

    @Override
    public boolean requestAccessOnFile(final int fileId, final ACCESS access) {
      return access != ACCESS.READ_RECORD || !deniedFiles.contains(fileId);
    }

    @Override
    public boolean requestAccessOnType(final String typeName, final ACCESS access) {
      return true;
    }

    @Override
    public long getResultSetLimit() {
      return -1L;
    }

    @Override
    public long getReadTimeout() {
      return -1L;
    }
  }
}
