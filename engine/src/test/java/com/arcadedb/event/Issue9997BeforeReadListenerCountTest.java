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
package com.arcadedb.event;

import com.arcadedb.TestHelper;
import com.arcadedb.database.Database;
import com.arcadedb.database.DatabaseFactory;
import com.arcadedb.database.MutableDocument;
import com.arcadedb.database.RID;
import com.arcadedb.database.Record;
import com.arcadedb.database.RecordEventsRegistry;
import com.arcadedb.query.sql.executor.ResultSet;
import com.arcadedb.schema.trigger.JavaTrigger;
import com.arcadedb.utility.FileUtils;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.File;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Issue #9997: a scan used to find the registry of the type of every record it handed out - a checked
 * {@code Database.getSchema()} and a bucket-to-type lookup per record - only to learn that nobody was listening. The
 * database now counts the BEFORE READ listeners registered on it and on every type of its schema, and a read skips the
 * lookup while that count is zero. These tests pin that the count is EXACT in both directions: a listener registered
 * after the database was already scanned is seen by the next read, and the fast path comes back once the last listener
 * is gone, however it went (unregistered, its trigger dropped, its type dropped).
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class Issue9997BeforeReadListenerCountTest extends TestHelper {
  static final AtomicInteger TRIGGER_FIRED = new AtomicInteger();

  public static class CountingBeforeReadTrigger implements JavaTrigger {
    @Override
    public boolean execute(final Database database, final Record record, final Record oldRecord) {
      return true;
    }

    @Override
    public boolean executeBeforeRead(final Database database, final RID rid) {
      TRIGGER_FIRED.incrementAndGet();
      return true;
    }
  }

  @BeforeEach
  void resetTrigger() {
    TRIGGER_FIRED.set(0);
  }

  @Test
  void aTypeListenerRegisteredAfterAScanSeesTheNextScanAndLookupAndItsRemovalRestoresTheFastPath() {
    final RID first = createRecords("Late", 100);
    assertThat(scan("Late")).isEqualTo(100);
    assertThat(anyBeforeReadListener()).isFalse();

    final AtomicInteger seen = new AtomicInteger();
    final BeforeRecordReadListener listener = rid -> {
      seen.incrementAndGet();
      return true;
    };
    database.getSchema().getType("Late").getEvents().registerListener(listener);
    assertThat(anyBeforeReadListener()).isTrue();

    assertThat(scan("Late")).isEqualTo(100);
    assertThat(seen.get()).isEqualTo(100);
    assertThat(database.lookupByRID(first, true)).isNotNull();
    assertThat(seen.get()).isEqualTo(101);

    database.getSchema().getType("Late").getEvents().unregisterListener(listener);
    assertThat(anyBeforeReadListener()).isFalse();
    assertThat(scan("Late")).isEqualTo(100);
    assertThat(seen.get()).isEqualTo(101);
  }

  @Test
  void aDatabaseListenerRegisteredAfterAScanCanHideTheRecords() {
    createRecords("Late2", 50);
    assertThat(scan("Late2")).isEqualTo(50);

    final BeforeRecordReadListener hideAll = rid -> false;
    database.getEvents().registerListener(hideAll);
    assertThat(anyBeforeReadListener()).isTrue();
    assertThat(scan("Late2")).isZero();

    database.getEvents().unregisterListener(hideAll);
    assertThat(anyBeforeReadListener()).isFalse();
    assertThat(scan("Late2")).isEqualTo(50);
  }

  @Test
  void theSameListenerRegisteredTwiceIsCountedOnceAndAStrangerUnregisteredIsNotCountedAtAll() {
    database.getSchema().createDocumentType("Twice");
    final BeforeRecordReadListener listener = rid -> true;
    final BeforeRecordReadListener stranger = rid -> true;

    database.getSchema().getType("Twice").getEvents().registerListener(listener);
    database.getSchema().getType("Twice").getEvents().registerListener(listener);
    database.getSchema().getType("Twice").getEvents().unregisterListener(stranger);
    database.getEvents().unregisterListener(stranger);
    assertThat(anyBeforeReadListener()).isTrue();

    database.getSchema().getType("Twice").getEvents().unregisterListener(listener);
    assertThat(anyBeforeReadListener()).isFalse();

    // A second unregister must not take the count below zero, where a later listener would no longer turn it on
    database.getSchema().getType("Twice").getEvents().unregisterListener(listener);
    database.getEvents().registerListener(listener);
    assertThat(anyBeforeReadListener()).isTrue();
    database.getEvents().unregisterListener(listener);
    assertThat(anyBeforeReadListener()).isFalse();
  }

  @Test
  void droppingATypeWithAListenerGivesItsCountBack() {
    database.getSchema().createDocumentType("Doomed");
    database.getSchema().getType("Doomed").getEvents().registerListener((BeforeRecordReadListener) rid -> true);
    assertThat(anyBeforeReadListener()).isTrue();

    database.getSchema().dropType("Doomed");
    assertThat(anyBeforeReadListener()).isFalse();
  }

  @Test
  void aBeforeReadTriggerTurnsTheDispatchOnUntilItIsDroppedAlsoAcrossAReopen() {
    createRecords("Triggered", 10);
    database.command("sql", "CREATE TRIGGER countRead BEFORE READ ON TYPE Triggered EXECUTE JAVA '"
        + CountingBeforeReadTrigger.class.getName() + "'");
    assertThat(anyBeforeReadListener()).isTrue();
    assertThat(scan("Triggered")).isEqualTo(10);
    assertThat(TRIGGER_FIRED.get()).isEqualTo(10);

    reopenDatabase();

    // THE TYPES ARE NEW INSTANCES WITH NEW REGISTRIES: THE TRIGGER REGISTERED ON LOAD MUST HAVE TURNED THE COUNT ON
    assertThat(anyBeforeReadListener()).isTrue();
    assertThat(scan("Triggered")).isEqualTo(10);
    assertThat(TRIGGER_FIRED.get()).isEqualTo(20);

    database.command("sql", "DROP TRIGGER countRead");
    assertThat(anyBeforeReadListener()).isFalse();
    assertThat(scan("Triggered")).isEqualTo(10);
    assertThat(TRIGGER_FIRED.get()).isEqualTo(20);
  }

  /**
   * The ordering the fast path relies on, across threads: a read another thread STARTS after {@code registerListener}
   * returned must see the listener. The registry counts the listener before it adds it, so whoever observes the
   * registration (here through a volatile flag set after the call returned) also observes a count above zero.
   */
  @Test
  void aReadStartedAfterTheRegistrationReturnedOnAnotherThreadSeesTheListener() throws Exception {
    final RID rid = createRecords("Racing", 1);
    final AtomicInteger seen = new AtomicInteger();
    final AtomicBoolean registered = new AtomicBoolean();
    final AtomicInteger readsAfterRegistration = new AtomicInteger();
    final AtomicReference<Throwable> failure = new AtomicReference<>();
    final CountDownLatch readerStarted = new CountDownLatch(1);
    final int readsToCheck = 2_000;

    final Thread reader = new Thread(() -> {
      try {
        readerStarted.countDown();
        while (readsAfterRegistration.get() < readsToCheck) {
          final boolean registeredBefore = registered.get();
          database.transaction(() -> database.lookupByRID(rid, true));
          if (registeredBefore)
            readsAfterRegistration.incrementAndGet();
        }
      } catch (final Throwable t) {
        failure.set(t);
      }
    });
    reader.start();
    assertThat(readerStarted.await(10, TimeUnit.SECONDS)).isTrue();

    final BeforeRecordReadListener listener = r -> {
      seen.incrementAndGet();
      return true;
    };
    database.getSchema().getType("Racing").getEvents().registerListener(listener);
    registered.set(true);

    reader.join(TimeUnit.MINUTES.toMillis(2));
    assertThat(reader.isAlive()).isFalse();
    assertThat(failure.get()).isNull();
    // EVERY READ THAT STARTED AFTER THE FLAG WAS SEEN MUST HAVE FIRED THE LISTENER; THE ONES IN FLIGHT AT THE
    // REGISTRATION MAY OR MAY NOT, WHICH IS WHY THIS IS A LOWER BOUND
    assertThat(seen.get()).isGreaterThanOrEqualTo(readsToCheck);

    database.getSchema().getType("Racing").getEvents().unregisterListener(listener);
    assertThat(anyBeforeReadListener()).isFalse();
  }

  @Test
  void aListenerOnAnotherDatabaseDoesNotSlowThisOneDown() {
    final String otherPath = getDatabasePath() + "-9997-other";
    FileUtils.deleteRecursively(new File(otherPath));
    final DatabaseFactory otherFactory = new DatabaseFactory(otherPath);
    final Database other = otherFactory.create();
    try {
      other.getSchema().createDocumentType("Elsewhere");
      other.getSchema().getType("Elsewhere").getEvents().registerListener((BeforeRecordReadListener) rid -> true);
      other.getEvents().registerListener((BeforeRecordReadListener) rid -> true);
      assertThat(((RecordEventsRegistry) other.getEvents()).hasBeforeReadListenersInDatabase()).isTrue();

      assertThat(anyBeforeReadListener()).isFalse();
    } finally {
      other.drop();
      otherFactory.close();
    }
  }

  private boolean anyBeforeReadListener() {
    return ((RecordEventsRegistry) database.getEvents()).hasBeforeReadListenersInDatabase();
  }

  private RID createRecords(final String typeName, final int count) {
    database.getSchema().createDocumentType(typeName);
    final RID[] first = new RID[1];
    database.transaction(() -> {
      for (int i = 0; i < count; i++) {
        final MutableDocument d = database.newDocument(typeName).set("id", i).save();
        if (i == 0)
          first[0] = d.getIdentity();
      }
    });
    return first[0];
  }

  private int scan(final String typeName) {
    int rows = 0;
    try (final ResultSet rs = database.query("sql", "SELECT id FROM " + typeName)) {
      while (rs.hasNext()) {
        rs.next();
        ++rows;
      }
    }
    return rows;
  }
}
