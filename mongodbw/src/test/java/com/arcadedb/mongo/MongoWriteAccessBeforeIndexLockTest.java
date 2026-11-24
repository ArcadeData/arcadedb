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
package com.arcadedb.mongo;

import com.arcadedb.database.Database;
import com.arcadedb.database.DatabaseContext;
import com.arcadedb.database.DatabaseFactory;
import com.arcadedb.database.DatabaseInternal;
import com.arcadedb.query.sql.executor.ResultSet;
import com.arcadedb.security.SecurityDatabaseUser;
import com.arcadedb.security.SecurityDatabaseUser.ACCESS;
import com.arcadedb.security.SecurityDatabaseUser.DATABASE_ACCESS;
import de.bwaldvogel.mongo.bson.Document;
import de.bwaldvogel.mongo.exception.MongoServerError;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.EnumSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.fail;

/**
 * The write commands of the MongoDB wire protocol (insert, update, delete, upsert, unique createIndexes) check what the caller may
 * do on the collection before they take the lock that guards its unique indexes, not when a record is saved: a caller who may not
 * run the command is refused at once, without the lock being asked for.
 * <p>
 * The tests hold the lock of the collection on the test thread, so a command that asks for it parks, and look at whether the
 * command of a user who may not run it still answers.
 *
 * @author lvca (l.garulli@arcadedata.com)
 */
class MongoWriteAccessBeforeIndexLockTest {
  private static final String PATH          = "target/databases/MongoWriteAccessBeforeIndexLockTest";
  // a bound for a hang, never a latency: a command that answers does so at once
  private static final long   HANG_SECONDS  = 60L;
  private static final int    UNAUTHORIZED  = 13;
  private static final String INDEXED       = "Indexed";
  private static final String PLAIN         = "Plain";

  private DatabaseFactory        factory;
  private Database               database;
  private MongoDBDatabaseWrapper wrapper;
  private ExecutorService        executor;

  @BeforeEach
  void setUp() {
    factory = new DatabaseFactory(PATH).setSecurity(db -> {
    });
    if (factory.exists())
      factory.open().drop();

    database = factory.create();
    database.command("sql", "CREATE DOCUMENT TYPE " + INDEXED);
    database.command("sql", "CREATE DOCUMENT TYPE " + PLAIN);
    database.transaction(() -> {
      for (int k = 1; k <= 10; k++)
        database.newDocument(PLAIN).set("k", k).save();
    });
    wrapper = new MongoDBDatabaseWrapper(database, null, null);
    // the first insert of the plugin builds the unique index on _id: INDEXED has it, PLAIN has not
    new MongoDBCollectionWrapper(database, INDEXED).insertDocuments(List.of(new Document("_id", 1).append("k", 1)));
    executor = Executors.newCachedThreadPool();
  }

  @AfterEach
  void tearDown() throws InterruptedException {
    // the commands that were parked on a lock go on once it is released: they end before the database they run on is dropped
    executor.shutdown();
    if (!executor.awaitTermination(HANG_SECONDS, TimeUnit.SECONDS))
      executor.shutdownNow();
    DatabaseContext.INSTANCE.init((DatabaseInternal) database).setCurrentUser(null);
    while (database.isTransactionActive())
      database.rollback();
    // the locks are kept per database path: the next test starts afresh
    MongoDBCollectionWrapper.forgetIdIndexes(database);
    database.drop();
    factory.close();
  }

  @Test
  void anUpdateOfAUserWhoMayNotUpdateIsRefusedWithoutTheLock() {
    assertRefusedWhileTheLockIsHeld(reader(), () -> wrapper.handleCommand(null, "update", update(INDEXED, false), null, null));
    assertRefusedWhileTheLockIsHeld(reader(), () -> wrapper.handleCommand(null, "update", update(PLAIN, false), null, null));
    // it may read and create, not update
    assertRefusedWhileTheLockIsHeld(user(ACCESS.READ_RECORD, ACCESS.CREATE_RECORD),
        () -> wrapper.handleCommand(null, "update", update(INDEXED, false), null, null));
  }

  @Test
  void anUpdateThatMatchesNothingIsRefusedToo() {
    final Document noMatch = new Document("update", INDEXED).append("updates", List.of(
        new Document("q", new Document("k", -1)).append("u", new Document("$set", new Document("touched", true))).append("multi", true)));
    assertRefusedWhileTheLockIsHeld(reader(), () -> wrapper.handleCommand(null, "update", noMatch, null, null));
  }

  @Test
  void anUpsertOfAUserWhoMayNotCreateIsRefusedWithoutTheLock() {
    // the _id index of PLAIN does not exist: an upsert would take the exclusive lock to build it
    assertRefusedWhileTheLockIsHeld(reader(), () -> wrapper.handleCommand(null, "update", update(PLAIN, true), null, null));
    assertRefusedWhileTheLockIsHeld(user(ACCESS.READ_RECORD, ACCESS.UPDATE_RECORD),
        () -> wrapper.handleCommand(null, "update", update(PLAIN, true), null, null));
    assertRefusedWhileTheLockIsHeld(user(ACCESS.READ_RECORD, ACCESS.UPDATE_RECORD),
        () -> wrapper.handleCommand(null, "update", update(INDEXED, true), null, null));
  }

  @Test
  void aDeleteOfAUserWhoMayNotDeleteIsRefusedWithoutTheLock() {
    assertRefusedWhileTheLockIsHeld(reader(), () -> wrapper.handleCommand(null, "delete", delete(INDEXED, new Document("k", -1)), null, null));
    assertRefusedWhileTheLockIsHeld(reader(), () -> wrapper.handleCommand(null, "delete", delete(PLAIN, new Document()), null, null));
    assertRefusedWhileTheLockIsHeld(user(ACCESS.READ_RECORD, ACCESS.CREATE_RECORD, ACCESS.UPDATE_RECORD),
        () -> wrapper.handleCommand(null, "delete", delete(INDEXED, new Document("k", -1)), null, null));
  }

  @Test
  void anInsertOfAUserWhoMayNotCreateIsRefusedWithoutTheLock() {
    assertRefusedWhileTheLockIsHeld(reader(),
        () -> new MongoDBCollectionWrapper(database, INDEXED).insertDocuments(List.of(new Document("_id", 2))));
    // PLAIN has no _id index: the insert would take the exclusive lock to build it
    assertRefusedWhileTheLockIsHeld(reader(),
        () -> new MongoDBCollectionWrapper(database, PLAIN).insertDocuments(List.of(new Document("_id", 2))));
    assertRefusedWhileTheLockIsHeld(user(ACCESS.READ_RECORD, ACCESS.UPDATE_RECORD, ACCESS.DELETE_RECORD),
        () -> new MongoDBCollectionWrapper(database, PLAIN).insertDocuments(List.of(new Document("_id", 2))));
  }

  @Test
  void aUniqueIndexOfAUserWhoMayNotChangeTheSchemaIsRefusedWithoutTheLock() {
    assertRefusedWhileTheLockIsHeld(reader(), () -> wrapper.handleCommand(null, "createIndexes", uniqueIndex(INDEXED, "k"), null, null));
    assertRefusedWhileTheLockIsHeld(reader(), () -> wrapper.handleCommand(null, "createIndexes", uniqueIndex(PLAIN, "_id"), null, null));
    // it may write the records, not change the schema
    assertRefusedWhileTheLockIsHeld(user(ACCESS.READ_RECORD, ACCESS.CREATE_RECORD, ACCESS.UPDATE_RECORD, ACCESS.DELETE_RECORD),
        () -> wrapper.handleCommand(null, "createIndexes", uniqueIndex(INDEXED, "k"), null, null));

    assertThat(database.getSchema().getType(INDEXED).getAllIndexes(false)).as("no index was built").hasSize(1);
  }

  /** The exclusive lock is not asked for by a refused command: the writers that may run go on while the shared one is held. */
  @Test
  void aRefusedCommandLeavesTheLockToTheWriters() throws Exception {
    final ReentrantReadWriteLock lock = (ReentrantReadWriteLock) MongoDBCollectionWrapper.idIndexLock(database, INDEXED);
    // held by a command that is allowed to run
    lock.readLock().lock();
    try {
      final Future<Document> unique = executor.submit(
          () -> as(reader(), () -> wrapper.handleCommand(null, "createIndexes", uniqueIndex(INDEXED, "k"), null, null)));
      awaitAnsweredOrParked(unique, lock);
      assertThat(unique).as("the refused command answered without asking for the lock").isDone();
      assertThat(lock.hasQueuedThreads()).isFalse();

      final Future<Integer> insert = executor.submit(() -> as(user(ACCESS.READ_RECORD, ACCESS.CREATE_RECORD),
          () -> new MongoDBCollectionWrapper(database, INDEXED).insertDocumentsCountingStored(List.of(new Document("_id", 3)))));
      awaitAnsweredOrParked(insert, lock);
      assertThat(insert).as("the insert of a user who may create is not held back").isDone();
      assertThat(insert.get()).isEqualTo(1);
    } finally {
      lock.readLock().unlock();
    }
  }

  @Test
  void aUserWhoMayWriteIsNotRefused() {
    bind(user(ACCESS.READ_RECORD, ACCESS.UPDATE_RECORD));
    final Document updated = wrapper.handleCommand(null, "update", update(INDEXED, false), null, null);
    assertThat(updated.get("n")).isEqualTo(1);
    assertThat(updated.get("nModified")).isEqualTo(1);

    bind(user(ACCESS.READ_RECORD, ACCESS.UPDATE_RECORD, ACCESS.CREATE_RECORD));
    final Document upserted = wrapper.handleCommand(null, "update", new Document("update", INDEXED).append("updates",
        List.of(new Document("q", new Document("k", 99)).append("u", new Document("$set", new Document("touched", true))).append("upsert", true))),
        null, null);
    assertThat(upserted.get("n")).isEqualTo(1);
    assertThat(upserted.get("upserted")).isNotNull();

    bind(user(ACCESS.READ_RECORD, ACCESS.CREATE_RECORD));
    assertThat(new MongoDBCollectionWrapper(database, INDEXED).insertDocumentsCountingStored(List.of(new Document("_id", 7)))).isEqualTo(1);

    bind(user(ACCESS.READ_RECORD, ACCESS.DELETE_RECORD));
    final Document deleted = wrapper.handleCommand(null, "delete", delete(INDEXED, new Document("_id", 7)), null, null);
    assertThat(deleted.get("n")).isEqualTo(1);

    bind(schemaUser());
    final Document created = wrapper.handleCommand(null, "createIndexes", uniqueIndex(INDEXED, "k"), null, null);
    assertThat(created.get("ok")).isEqualTo(1.0);
    assertThat(database.getSchema().getType(INDEXED).getAllIndexes(false)).hasSizeGreaterThan(1);
  }

  @Test
  void aRefusedCommandChangesNothing() {
    bind(reader());
    assertThat(refusedCode(() -> wrapper.handleCommand(null, "update", update(PLAIN, false), null, null))).isEqualTo(UNAUTHORIZED);
    assertThat(refusedCode(() -> wrapper.handleCommand(null, "delete", delete(PLAIN, new Document()), null, null))).isEqualTo(UNAUTHORIZED);
    bind(null);

    assertThat(count("SELECT count(*) AS c FROM " + PLAIN)).isEqualTo(10);
    assertThat(count("SELECT count(*) AS c FROM " + PLAIN + " WHERE touched = true")).isZero();
  }

  /**
   * Runs {@code command} on another thread, as {@code denied}, while the test thread holds the exclusive lock of the collection:
   * a command that asks for the lock parks on it, one refused before it answers.
   */
  private void assertRefusedWhileTheLockIsHeld(final SecurityDatabaseUser denied, final Runnable command) {
    final ReentrantReadWriteLock lock = (ReentrantReadWriteLock) MongoDBCollectionWrapper.idIndexLock(database, INDEXED);
    final ReentrantReadWriteLock plainLock = (ReentrantReadWriteLock) MongoDBCollectionWrapper.idIndexLock(database, PLAIN);
    lock.writeLock().lock();
    plainLock.writeLock().lock();
    try {
      final Future<Object> answer = executor.submit(() -> as(denied, () -> {
        command.run();
        return null;
      }));
      awaitAnsweredOrParked(answer, lock, plainLock);
      if (!answer.isDone())
        fail("the command parked on the lock of the unique indexes instead of being refused");

      try {
        answer.get();
        fail("the command ran for a user who may not run it");
      } catch (final ExecutionException e) {
        assertRefused(e.getCause());
      } catch (final InterruptedException e) {
        Thread.currentThread().interrupt();
        fail("interrupted");
      }
    } finally {
      plainLock.writeLock().unlock();
      lock.writeLock().unlock();
    }
  }

  private static void assertRefused(final Throwable refusal) {
    if (refusal instanceof MongoServerError error)
      assertThat(error.getCode()).as(error.getMessage()).isEqualTo(UNAUTHORIZED);
    else
      assertThat(refusal).isInstanceOf(SecurityException.class);
  }

  private static int refusedCode(final Runnable command) {
    try {
      command.run();
    } catch (final MongoServerError e) {
      return e.getCode();
    }
    return 0;
  }

  /** Waits until the command answered or parked on a lock: the lock of the test is held, so a command that asks for it queues. */
  private static void awaitAnsweredOrParked(final Future<?> answer, final ReentrantReadWriteLock... locks) {
    final long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(HANG_SECONDS);
    while (!answer.isDone() && !queued(locks)) {
      if (System.nanoTime() - deadline > 0L)
        fail("the command neither answered nor queued for the lock");
      Thread.onSpinWait();
    }
    if (!answer.isDone())
      return;
    try {
      answer.get(HANG_SECONDS, TimeUnit.SECONDS);
    } catch (final ExecutionException e) {
      // a refusal is the answer
    } catch (final InterruptedException e) {
      Thread.currentThread().interrupt();
    } catch (final TimeoutException e) {
      fail("the answer of a finished command never arrived");
    }
  }

  private static boolean queued(final ReentrantReadWriteLock[] locks) {
    for (final ReentrantReadWriteLock lock : locks)
      if (lock.hasQueuedThreads())
        return true;
    return false;
  }

  private <T> T as(final SecurityDatabaseUser user, final Callable<T> work) throws Exception {
    bind(user);
    try {
      return work.call();
    } finally {
      bind(null);
    }
  }

  private void bind(final SecurityDatabaseUser user) {
    DatabaseContext.INSTANCE.init((DatabaseInternal) database).setCurrentUser(user);
  }

  private int count(final String sql) {
    try (final ResultSet rs = database.query("sql", sql)) {
      return rs.next().<Number>getProperty("c").intValue();
    }
  }

  private static Document update(final String collection, final boolean upsert) {
    return new Document("update", collection).append("updates", List.of(
        new Document("q", new Document("k", 1)).append("u", new Document("$set", new Document("touched", true))).append("upsert", upsert)));
  }

  private static Document delete(final String collection, final Document filter) {
    return new Document("delete", collection).append("deletes", List.of(new Document("q", filter).append("limit", 0)));
  }

  private static Document uniqueIndex(final String collection, final String field) {
    return new Document("createIndexes", collection).append("indexes",
        List.of(new Document("key", new Document(field, 1)).append("name", field + "_unique").append("unique", true)));
  }

  private static SecurityDatabaseUser reader() {
    return user(ACCESS.READ_RECORD);
  }

  private static SecurityDatabaseUser user(final ACCESS first, final ACCESS... more) {
    return new AccessUser(EnumSet.of(first, more), false);
  }

  private static SecurityDatabaseUser schemaUser() {
    return new AccessUser(EnumSet.allOf(ACCESS.class), true);
  }

  /** A user granted the record accesses it is given, and the schema one only when it says so. */
  private record AccessUser(Set<ACCESS> records, boolean schema) implements SecurityDatabaseUser {
    @Override
    public String getName() {
      return "limited";
    }

    @Override
    public boolean requestAccessOnDatabase(final DATABASE_ACCESS access) {
      return schema && access == DATABASE_ACCESS.UPDATE_SCHEMA;
    }

    @Override
    public boolean requestAccessOnFile(final int fileId, final ACCESS access) {
      return records.contains(access);
    }

    @Override
    public boolean requestAccessOnType(final String typeName, final ACCESS access) {
      return records.contains(access);
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
