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
package com.arcadedb.database;

import com.arcadedb.GlobalConfiguration;
import com.arcadedb.TestHelper;
import com.arcadedb.engine.LocalBucket;
import com.arcadedb.engine.WALFile;
import com.arcadedb.exception.ConcurrentModificationException;
import com.arcadedb.exception.DuplicatedKeyException;
import com.arcadedb.exception.TransactionException;
import com.arcadedb.index.CrashingChildJvm;
import com.arcadedb.log.LogManager;
import com.arcadedb.query.sql.executor.ResultSet;
import com.arcadedb.utility.FailPoints;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
import org.junit.jupiter.params.provider.ValueSource;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Random;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.logging.Level;
import java.util.logging.Logger;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Failures injected at named points of a write ({@link FailPoints}): a transaction that fails before its WAL record is written
 * leaves nothing behind, one killed at any point is recovered to exactly what was committed, and random failures under concurrent
 * load never break the invariants the transactions keep.
 */
class FailPointCommitTest extends TestHelper {
  private static final int ACCOUNTS = 100;
  private static final int BALANCE  = 1_000;
  /** Held so the level the child gives it is not lost with a collected logger. */
  private static Logger    failPointsLogger;

  @AfterEach
  void disarm() {
    FailPoints.disarmAll();
  }

  @ParameterizedTest
  @ValueSource(strings = { FailPoints.COMMIT_BEFORE_WAL, FailPoints.BUCKET_CREATE_RECORD })
  void aTransactionFailingBeforeItsWalRecordLeavesNothingBehind(final String point) {
    createAccounts();

    FailPoints.arm(point, FailPoints.THROW);
    assertThatThrownBy(() -> database.transaction(() -> {
      for (int i = 0; i < 50; i++)
        database.newDocument("Account").set("id", ACCOUNTS + i).set("balance", 1).save();
      database.command("sql", "UPDATE Account SET balance = balance - 1 WHERE id < 10");
    })).satisfies(e -> assertThat(rootCause(e)).isInstanceOf(FailPoints.FailPointException.class));
    FailPoints.disarmAll();

    assertThat(sum()).isEqualTo((long) ACCOUNTS * BALANCE);
    assertThat(count("SELECT count(@rid) AS c FROM Account")).isEqualTo(ACCOUNTS);
    assertThat(count("SELECT count(*) AS c FROM Account WHERE id >= " + ACCOUNTS)).isZero();

    // the database takes the next transaction as if nothing happened
    database.transaction(() -> database.newDocument("Account").set("id", ACCOUNTS).set("balance", 0).save());
    assertThat(count("SELECT count(*) AS c FROM Account WHERE id = " + ACCOUNTS)).isEqualTo(1);
  }

  @ParameterizedTest
  @ValueSource(strings = { FailPoints.BUCKET_UPDATE_RECORD, FailPoints.BUCKET_DELETE_RECORD })
  void aStatementFailingInsideTheTransactionRollsItBackWhole(final String point) {
    createAccounts();

    FailPoints.arm(point, FailPoints.THROW);
    assertThatThrownBy(() -> database.transaction(() -> {
      database.newDocument("Account").set("id", ACCOUNTS).set("balance", 5).save();
      database.command("sql", "UPDATE Account SET balance = balance + 1 WHERE id = 1");
      database.command("sql", "DELETE FROM Account WHERE id = 2");
    })).satisfies(e -> assertThat(rootCause(e)).isInstanceOf(FailPoints.FailPointException.class));
    FailPoints.disarmAll();

    assertThat(count("SELECT count(@rid) AS c FROM Account")).isEqualTo(ACCOUNTS);
    assertThat(sum()).isEqualTo((long) ACCOUNTS * BALANCE);
  }

  /**
   * The engine taking back a record its unique index refused deletes it through {@code LocalBucket.retractRecord}, which reaches
   * the delete point too: a failure there marks the transaction rollback-only, so the refused record cannot be committed.
   */
  @Test
  void theRetractionOfARefusedRecordReachesTheDeletePoint() {
    createAccounts();
    final AtomicInteger deletes = new AtomicInteger();
    FailPoints.arm(FailPoints.BUCKET_DELETE_RECORD, point -> {
      deletes.incrementAndGet();
      FailPoints.THROW.run(point);
    });

    database.begin();
    try {
      // a key this transaction already holds is refused at the save, a committed one only at the commit
      database.newDocument("Account").set("id", ACCOUNTS).set("balance", 5).save();
      assertThatThrownBy(() -> database.newDocument("Account").set("id", ACCOUNTS).set("balance", 5).save())//
          .isInstanceOf(DuplicatedKeyException.class);
      FailPoints.disarmAll();
      assertThat(deletes.get()).as("deletes reaching the point while the refused record was taken back").isEqualTo(1);
      assertThatThrownBy(() -> database.commit()).isInstanceOf(TransactionException.class);
    } finally {
      if (database.isTransactionActive())
        database.rollback();
    }

    assertThat(count("SELECT count(@rid) AS c FROM Account")).isEqualTo(ACCOUNTS);
    assertThat(sum()).isEqualTo((long) ACCOUNTS * BALANCE);
  }

  /**
   * The bulk write of the graph batch ({@code LocalBucket.createRecordsBulk}) reaches the create point once per record, both for
   * the record handed to the allocator and for the ones appended after it on the page it opened.
   */
  @Test
  void aBulkWriteReachesTheCreatePointOncePerRecord() {
    createAccounts();
    final LocalBucket bucket = (LocalBucket) database.getSchema().getType("Transfer").getBuckets(false).getFirst();
    final int records = 500;
    final AtomicInteger creates = new AtomicInteger();
    FailPoints.arm(FailPoints.BUCKET_CREATE_RECORD, point -> creates.incrementAndGet());

    database.begin();
    try {
      final DatabaseInternal internal = (DatabaseInternal) database;
      final Binary[] buffers = new Binary[records];
      for (int i = 0; i < records; i++)
        buffers[i] = internal.getSerializer().serialize(internal, database.newDocument("Transfer").set("amount", i));
      final RID[] rids = new RID[records];
      bucket.createRecordsBulk(buffers, 0, records, rids);

      assertThat(rids).doesNotContainNull();
      assertThat(creates.get()).as("records reaching the create point").isEqualTo(records);
    } finally {
      FailPoints.disarmAll();
      database.rollback();
    }
  }

  /** Transfers between accounts while every write point fails now and then: the balances always add up. */
  @Test
  void randomFailuresUnderConcurrentTransfersKeepTheTotal() throws Exception {
    createAccounts();
    final Random chance = new Random(11);
    final FailPoints.Action sometimes = point -> {
      final boolean fail;
      synchronized (chance) {
        fail = chance.nextInt(20) == 0;
      }
      if (fail)
        FailPoints.THROW.run(point);
    };
    for (final String point : List.of(FailPoints.COMMIT_BEFORE_WAL, FailPoints.BUCKET_CREATE_RECORD, FailPoints.BUCKET_UPDATE_RECORD,
        FailPoints.BUCKET_DELETE_RECORD))
      FailPoints.arm(point, sometimes);

    final AtomicInteger committed = new AtomicInteger();
    final AtomicInteger injected = new AtomicInteger();
    final List<Throwable> unexpected = Collections.synchronizedList(new ArrayList<>());
    final List<Thread> writers = new ArrayList<>();
    for (int w = 0; w < 4; w++) {
      final int writer = w;
      writers.add(new Thread(() -> {
        final Random random = new Random(writer);
        for (int i = 0; i < 300; i++) {
          final int from = random.nextInt(ACCOUNTS);
          final int to = random.nextInt(ACCOUNTS);
          final int amount = 1 + random.nextInt(10);
          try {
            database.transaction(() -> {
              database.command("sql", "UPDATE Account SET balance = balance - ? WHERE id = ?", amount, from);
              database.command("sql", "UPDATE Account SET balance = balance + ? WHERE id = ?", amount, to);
              // a log record created and deleted in the same transaction, so the create and delete points are reached too
              final MutableDocument log = database.newDocument("Transfer").set("from", from).set("to", to).set("amount", amount).save();
              if (amount % 2 == 0)
                log.delete();
            });
            committed.incrementAndGet();
          } catch (final ConcurrentModificationException e) {
            // a conflict with another writer that outlasted the retries: rolled back, which keeps the invariant
          } catch (final Throwable e) {
            if (!(rootCause(e) instanceof FailPoints.FailPointException)) {
              // recorded for the test thread, which a join() alone would not tell
              unexpected.add(e);
              return;
            }
            injected.incrementAndGet();
          }
        }
      }));
    }
    writers.forEach(Thread::start);
    for (final Thread writer : writers)
      writer.join();
    FailPoints.disarmAll();

    assertThat(unexpected).as("writer failures other than the injected ones").isEmpty();
    assertThat(injected.get()).as("failures injected").isGreaterThan(10);
    assertThat(committed.get()).as("transfers committed").isGreaterThan(100);
    assertThat(sum()).as("the balances still add up after %d injected failures", injected.get()).isEqualTo((long) ACCOUNTS * BALANCE);
    assertThat(count("SELECT count(*) AS c FROM Transfer WHERE amount % 2 = 0")).as("logs deleted in their own transaction").isZero();
  }

  /**
   * A group commit only queues its WAL record where {@link FailPoints#COMMIT_AFTER_WAL} sits and writes it after the publication, so
   * the point, which promises a record already on the WAL, is not reached on that path.
   */
  @Test
  void aGroupCommitDoesNotReachTheAfterWalPoint() throws InterruptedException {
    final boolean groupCommit = GlobalConfiguration.TX_WAL_GROUP_COMMIT.getValueAsBoolean();
    try {
      GlobalConfiguration.TX_WAL_GROUP_COMMIT.setValue(true);
      createAccounts();
      database.setWALFlush(WALFile.FlushType.YES_NOMETADATA);

      final AtomicInteger beforeWal = new AtomicInteger();
      final AtomicInteger afterWal = new AtomicInteger();
      FailPoints.arm(FailPoints.COMMIT_BEFORE_WAL, point -> beforeWal.incrementAndGet());
      FailPoints.arm(FailPoints.COMMIT_AFTER_WAL, point -> {
        afterWal.incrementAndGet();
        FailPoints.THROW.run(point);
      });
      database.transaction(() -> database.newDocument("Account").set("id", ACCOUNTS).set("balance", 0).save());
      FailPoints.disarmAll();

      assertThat(beforeWal.get()).as("commits that wrote their WAL record on the group commit path").isPositive();
      assertThat(afterWal.get()).as("group commits reaching the after-WAL point").isZero();
      assertThat(count("SELECT count(@rid) AS c FROM Account")).isEqualTo(ACCOUNTS + 1);

      // control: without the group commit the same commit does reach the point. The setting is read when a thread's transaction
      // context is created, so the commit runs on a thread of its own
      GlobalConfiguration.TX_WAL_GROUP_COMMIT.setValue(false);
      ((DatabaseInternal) database).getConfiguration().setValue(GlobalConfiguration.TX_WAL_GROUP_COMMIT, false);
      FailPoints.arm(FailPoints.COMMIT_AFTER_WAL, point -> {
        afterWal.incrementAndGet();
        FailPoints.THROW.run(point);
      });
      final AtomicReference<Throwable> failure = new AtomicReference<>();
      final Thread control = new Thread(() -> {
        try {
          database.setWALFlush(WALFile.FlushType.YES_NOMETADATA);
          database.transaction(() -> database.newDocument("Account").set("id", ACCOUNTS + 1).set("balance", 0).save());
        } catch (final Throwable e) {
          failure.set(e);
        }
      });
      control.start();
      control.join();
      FailPoints.disarmAll();
      assertThat(failure.get()).as("the commit that reached the after-WAL point").isNotNull();
      assertThat(rootCause(failure.get())).isInstanceOf(FailPoints.FailPointException.class);
      assertThat(afterWal.get()).as("commits reaching the after-WAL point without the group commit").isPositive();

      // the failure past the WAL record fenced the database: reopening recovers the commit
      database.close();
      database = factory.open();
      assertThat(count("SELECT count(@rid) AS c FROM Account")).isEqualTo(ACCOUNTS + 2);
    } finally {
      GlobalConfiguration.TX_WAL_GROUP_COMMIT.setValue(groupCommit);
    }
  }

  /**
   * The child: arms {@code args[1]} to halt the process, then commits a transaction that reaches it. With a third argument
   * {@code property} the point is armed the way a launch line would arm it, from the system property {@code arcadedb.failPoints}
   * set before anything loads {@link FailPoints}.
   */
  public static void main(final String[] args) {
    final boolean fromProperty = args.length > 2 && "property".equals(args[2]);
    if (fromProperty) {
      // the test log configuration lets only SEVERE through for com.arcadedb: initialize it first (it would reset the level set
      // below), then let the WARNING of FailPoints reach the console. The class literal does not load the class
      LogManager.instance().log(FailPointCommitTest.class, Level.FINE, "Child started");
      failPointsLogger = Logger.getLogger(FailPoints.class.getName());
      failPointsLogger.setLevel(Level.WARNING);
      System.setProperty("arcadedb.failPoints", args[1] + "=halt");
    }
    final Database database = new DatabaseFactory(args[0]).open();
    if (!fromProperty)
      FailPoints.arm(args[1], FailPoints.HALT);
    database.transaction(() -> {
      for (int i = 0; i < 1_000; i++)
        database.newDocument("Account").set("id", ACCOUNTS + i).set("balance", 1).save();
      database.command("sql", "UPDATE Account SET balance = balance - 1 WHERE id < 10");
    });
    System.out.println("the transaction committed without reaching the fail point");
    Runtime.getRuntime().halt(1);
  }

  /**
   * A kill at each point of the commit: before the WAL record the transaction is lost, from the WAL record on it is recovered whole,
   * never in part.
   */
  @Tag("slow")
  @ParameterizedTest(name = "{0}")
  @CsvSource({ FailPoints.COMMIT_BEFORE_WAL + ",false", FailPoints.COMMIT_AFTER_WAL + ",true",
      FailPoints.COMMIT_AFTER_PUBLISH + ",true" })
  void aKillAtEachCommitPointRecoversAllOrNothing(final String point, final boolean recovered) throws Exception {
    createAccounts();
    final String path = database.getDatabasePath();
    database.close();

    final CrashingChildJvm.Result child = CrashingChildJvm.run(FailPointCommitTest.class, 300, path, point);
    assertThat(child.exitCode()).as("child output: %s", child.output()).isEqualTo(FailPoints.HALT_EXIT_CODE);

    database = factory.open();
    assertThat(count("SELECT count(@rid) AS c FROM Account")).isEqualTo(ACCOUNTS + (recovered ? 1_000 : 0));
    assertThat(sum()).isEqualTo((long) ACCOUNTS * BALANCE + (recovered ? 1_000 - 10 : 0));
  }

  /** A point armed from the system property works like one armed by a test, and says so in the log of the process it runs in. */
  @Tag("slow")
  @Test
  void aPointArmedFromTheSystemPropertyIsLoggedAndReached() throws Exception {
    createAccounts();
    final String path = database.getDatabasePath();
    database.close();

    final CrashingChildJvm.Result child = CrashingChildJvm.run(FailPointCommitTest.class, 300, path, FailPoints.COMMIT_AFTER_WAL,
        "property");
    assertThat(child.exitCode()).as("child output: %s", child.output()).isEqualTo(FailPoints.HALT_EXIT_CODE);
    assertThat(child.output()).contains("Fail points armed from the system property 'arcadedb.failPoints': [commit.afterWal]")
        .contains("for tests only");

    database = factory.open();
    assertThat(count("SELECT count(@rid) AS c FROM Account")).isEqualTo(ACCOUNTS + 1_000);
  }

  private void createAccounts() {
    database.command("sql", "CREATE DOCUMENT TYPE Account BUCKETS 4");
    database.command("sql", "CREATE PROPERTY Account.id INTEGER");
    database.command("sql", "CREATE INDEX ON Account (id) UNIQUE");
    database.command("sql", "CREATE DOCUMENT TYPE Transfer");
    database.transaction(() -> {
      for (int i = 0; i < ACCOUNTS; i++)
        database.newDocument("Account").set("id", i).set("balance", BALANCE).save();
    });
  }

  private long sum() {
    return count("SELECT sum(balance) AS c FROM Account");
  }

  private long count(final String sql) {
    try (final ResultSet rows = database.query("sql", sql)) {
      return rows.next().<Number>getProperty("c").longValue();
    }
  }

  private static Throwable rootCause(final Throwable e) {
    Throwable cause = e;
    while (cause.getCause() != null && cause.getCause() != cause)
      cause = cause.getCause();
    return cause;
  }
}
