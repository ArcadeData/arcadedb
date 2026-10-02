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

import com.arcadedb.TestHelper;
import com.arcadedb.database.async.DatabaseAsyncExecutorImpl;
import com.arcadedb.engine.DatabaseChecker;
import com.arcadedb.engine.LocalBucket;
import com.arcadedb.schema.LocalDocumentType;
import org.junit.jupiter.api.Test;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Proxy;
import java.util.concurrent.atomic.AtomicInteger;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Regression tests for issue #8292: engine code that commits on the inner {@link LocalDatabase} instead of the
 * database's current wrapper. On an HA node the wrapper is the Raft-replicated database, so a commit made on the inner
 * instance applies its pages on this node only and replicates nothing (the #5492 family).
 * <p>
 * The wrapper here is a delegating proxy that counts the {@code commit()} calls it receives, installed the same way the
 * HA plugin installs {@code RaftReplicatedDatabase} ({@link LocalDatabase#setWrappedDatabaseInstance}). Each test drives
 * one entry point that used to commit on the inner instance and asserts the commits now reach the wrapper.
 * <p>
 * The proxy counts every {@code commit()} it receives, nested ones included (a dictionary registration commits in its
 * own nested transaction). Every fixture therefore writes its property names BEFORE the wrap, so the count reflects
 * only the commits of the entry point under test.
 */
class Issue8292EngineCommitsReachTheWrapperTest extends TestHelper {

  private final AtomicInteger wrapperCommits     = new AtomicInteger();
  private final AtomicInteger asyncWorkerCommits = new AtomicInteger();

  /**
   * The async executor is created lazily and captured the wrapper as it stood at that moment. One created before the
   * HA wrap (a parallel SELECT or a scheduled index compaction while the server was starting) - or before a plugin
   * restart replaced the wrapper - kept committing every async write on the stale instance for the life of the
   * database.
   */
  @Test
  void anAsyncExecutorCreatedBeforeTheWrapCommitsThroughTheWrapper() {
    // THE FIRST RECORD REGISTERS "id" IN THE DICTIONARY BEFORE THE WRAP: that registration commits through the wrapper
    // in its own nested transaction, on whatever thread meets the name first, and would pass this test on its own
    database.transaction(() -> {
      database.getSchema().createDocumentType("AsyncDoc");
      database.newDocument("AsyncDoc").set("id", 0).save();
    });

    final LocalDatabase local = (LocalDatabase) database;
    // CREATE THE EXECUTOR BEFORE THE WRAP, AS A STARTING SERVER CAN
    local.async();

    final DatabaseInternal previous = local.getWrappedDatabaseInstance();
    local.setWrappedDatabaseInstance(countingWrapper(local));
    try {
      local.async().transaction(() -> local.newDocument("AsyncDoc").set("id", 1).save());
      local.async().waitCompletion();

      assertThat(asyncWorkerCommits.get()).as("the async worker must commit through the current wrapper").isGreaterThan(0);
    } finally {
      local.setWrappedDatabaseInstance(previous);
    }

    assertThat(database.countType("AsyncDoc", false)).isEqualTo(2);
  }

  /**
   * A plugin restart replaces wrapper A with wrapper B: the executor must follow, and A must stop receiving commits.
   */
  @Test
  void anAsyncExecutorFollowsAReplacedWrapper() {
    database.transaction(() -> {
      database.getSchema().createDocumentType("AsyncDoc");
      database.newDocument("AsyncDoc").set("id", 0).save();
    });

    final LocalDatabase local = (LocalDatabase) database;
    final DatabaseInternal previous = local.getWrappedDatabaseInstance();
    final AtomicInteger commitsOnA = new AtomicInteger();
    final AtomicInteger commitsOnB = new AtomicInteger();
    try {
      local.setWrappedDatabaseInstance(countingWrapper(local, commitsOnA, new AtomicInteger()));
      local.async().transaction(() -> local.newDocument("AsyncDoc").set("id", 1).save());
      local.async().waitCompletion();
      assertThat(commitsOnA.get()).as("the async worker must commit through wrapper A").isGreaterThan(0);

      final int commitsOnABeforeReplacement = commitsOnA.get();
      local.setWrappedDatabaseInstance(countingWrapper(local, new AtomicInteger(), commitsOnB));
      local.async().transaction(() -> local.newDocument("AsyncDoc").set("id", 2).save());
      local.async().waitCompletion();

      assertThat(commitsOnB.get()).as("the async worker must commit through the replacing wrapper B").isGreaterThan(0);
      assertThat(commitsOnA.get()).as("the replaced wrapper A must receive no further commits")
          .isEqualTo(commitsOnABeforeReplacement);
    } finally {
      local.setWrappedDatabaseInstance(previous);
    }

    assertThat(database.countType("AsyncDoc", false)).isEqualTo(3);
  }

  /**
   * A rebind while a worker holds an open batch: the batch began under wrapper A and commits through wrapper B. That
   * is still one transaction, because every wrapper delegates to the same embedded database and the transaction lives
   * in the worker's thread context - so every record of the batch must land, committed through B.
   */
  @Test
  void aBatchOpenAcrossARebindCommitsWhollyThroughTheNewWrapper() {
    database.transaction(() -> {
      database.getSchema().createDocumentType("BatchDoc");
      database.newDocument("BatchDoc").set("id", -1).save();
    });

    final LocalDatabase local = (LocalDatabase) database;
    final DatabaseInternal previous = local.getWrappedDatabaseInstance();
    final AtomicInteger asyncCommitsOnB = new AtomicInteger();
    final DatabaseInternal wrapperB = countingWrapper(local, new AtomicInteger(), asyncCommitsOnB);
    final AtomicInteger hookCalls = new AtomicInteger();
    try {
      local.async().setParallelLevel(1);
      local.async().setCommitEvery(1_000);
      local.setWrappedDatabaseInstance(countingWrapper(local, new AtomicInteger(), new AtomicInteger()));

      // REBIND RIGHT BEFORE THE BATCH COMMIT, WITH THE BATCH STILL OPEN ON THE WORKER
      DatabaseAsyncExecutorImpl.TEST_BEFORE_BATCH_COMMIT_HOOK = callNumber -> {
        if (hookCalls.getAndIncrement() == 0)
          local.setWrappedDatabaseInstance(wrapperB);
      };

      for (int i = 0; i < 20; i++)
        local.async().createRecord(local.newDocument("BatchDoc").set("id", i), null);
      local.async().waitCompletion();

      assertThat(hookCalls.get()).as("the batch commit must have been reached").isGreaterThan(0);
      assertThat(asyncCommitsOnB.get()).as("the open batch must commit through the wrapper installed meanwhile")
          .isGreaterThan(0);
    } finally {
      DatabaseAsyncExecutorImpl.TEST_BEFORE_BATCH_COMMIT_HOOK = null;
      local.setWrappedDatabaseInstance(previous);
    }

    assertThat(database.countType("BatchDoc", false)).isEqualTo(21);
  }

  /**
   * {@code copyType()} copies the records in batches of {@code transactionBatchSize}, and committed every batch on the
   * schema's own database reference, which is the inner instance.
   */
  @Test
  void copyTypeCommitsTheCopiedRecordsThroughTheWrapper() {
    database.transaction(() -> {
      database.getSchema().createDocumentType("Source");
      for (int i = 0; i < 50; i++)
        database.newDocument("Source").set("id", i).save();
    });

    final LocalDatabase local = (LocalDatabase) database;
    final DatabaseInternal previous = local.getWrappedDatabaseInstance();
    local.setWrappedDatabaseInstance(countingWrapper(local));
    try {
      local.getSchema().copyType("Source", "Target", LocalDocumentType.class, 1, 65536, 10);
      // 50 records in batches of 10: five batch commits at least, plus the final one
      assertThat(wrapperCommits.get()).as("every copied batch must commit through the current wrapper")
          .isGreaterThanOrEqualTo(5);
    } finally {
      local.setWrappedDatabaseInstance(previous);
    }

    assertThat(database.countType("Target", false)).isEqualTo(50);
  }

  /**
   * {@code CHECK DATABASE} hands the checker the wrapper, but the checker is public API: one built around the inner
   * instance committed its fixes on this node only.
   */
  @Test
  void aCheckerBuiltOnTheInnerInstanceCommitsThroughTheWrapper() {
    database.transaction(() -> {
      database.getSchema().createDocumentType("Checked");
      for (int i = 0; i < 10; i++)
        database.newDocument("Checked").set("id", i).save();
    });

    final LocalDatabase local = (LocalDatabase) database;
    final DatabaseInternal previous = local.getWrappedDatabaseInstance();
    local.setWrappedDatabaseInstance(countingWrapper(local));
    try {
      new DatabaseChecker(local).setFix(true).check();
      assertThat(wrapperCommits.get()).as("the checker's commits must go through the current wrapper").isGreaterThan(0);
    } finally {
      local.setWrappedDatabaseInstance(previous);
    }
  }

  /**
   * {@code LocalBucket.check(fix)} - reached from {@code CHECK DATABASE FIX} - opens its repair transaction on the
   * bucket's own database reference, which is the inner instance, so its repairs committed on this node only.
   */
  @Test
  void aBucketRepairPassCommitsThroughTheWrapper() {
    database.transaction(() -> {
      database.getSchema().createDocumentType("Repaired");
      for (int i = 0; i < 10; i++)
        database.newDocument("Repaired").set("id", i).save();
    });

    final LocalDatabase local = (LocalDatabase) database;
    final LocalBucket bucket = (LocalBucket) local.getSchema().getType("Repaired").getBuckets(false).getFirst();
    final DatabaseInternal previous = local.getWrappedDatabaseInstance();
    local.setWrappedDatabaseInstance(countingWrapper(local));
    try {
      bucket.check(0, true);
      assertThat(wrapperCommits.get()).as("the repair pass must commit through the current wrapper").isGreaterThan(0);
    } finally {
      local.setWrappedDatabaseInstance(previous);
    }
  }

  private DatabaseInternal countingWrapper(final DatabaseInternal delegate) {
    return countingWrapper(delegate, wrapperCommits, asyncWorkerCommits);
  }

  private static DatabaseInternal countingWrapper(final DatabaseInternal delegate, final AtomicInteger commits,
      final AtomicInteger asyncCommits) {
    return (DatabaseInternal) Proxy.newProxyInstance(DatabaseInternal.class.getClassLoader(),
        new Class<?>[] { DatabaseInternal.class }, (proxy, method, args) -> {
          if ("commit".equals(method.getName()) && (args == null || args.length == 0)) {
            commits.incrementAndGet();
            if (Thread.currentThread().getName().startsWith("AsyncExecutor-"))
              asyncCommits.incrementAndGet();
          }
          try {
            return method.invoke(delegate, args);
          } catch (final InvocationTargetException e) {
            throw e.getCause();
          }
        });
  }
}
