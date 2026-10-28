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
package com.arcadedb.database.async;

import com.arcadedb.GlobalConfiguration;
import com.arcadedb.TestHelper;
import com.arcadedb.database.DatabaseInternal;
import com.arcadedb.database.Identifiable;
import com.arcadedb.database.LocalDatabase;
import com.arcadedb.database.RID;
import com.arcadedb.database.TransactionRetryPolicy;
import com.arcadedb.engine.LocalBucket;
import com.arcadedb.exception.ConcurrentModificationException;
import com.arcadedb.exception.DuplicatedKeyException;
import com.arcadedb.exception.LockTimeoutException;
import com.arcadedb.exception.NeedRetryException;
import com.arcadedb.function.sql.SQLFunctionAbstract;
import com.arcadedb.query.sql.SQLQueryEngine;
import com.arcadedb.query.sql.executor.CommandContext;
import com.arcadedb.query.sql.executor.ResultSet;
import com.arcadedb.utility.LockManager;
import org.junit.jupiter.api.Test;

import java.io.File;
import java.lang.reflect.Modifier;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Supplier;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Issue #10759: {@code db.async().transaction(block, retries)} and the async executor's batch commit retried only a
 * {@link ConcurrentModificationException}, although both javadocs promise the whole {@link NeedRetryException} family, which
 * the synchronous loop, the remote one and {@code COMMIT RETRY} honour. A {@link LockTimeoutException} from the commit's own
 * file locks - what N async workers committing into the same buckets produce - failed to the caller on the first attempt,
 * and a batch commit abandoned every buffered command after one wait.
 * <p>
 * The family is enumerated from the source of truth, the {@link NeedRetryException} subclasses of the exception package,
 * not from a hand-written list, and each member is driven through every engine retry loop, which must make the attempts it
 * was asked for: so a new retryable exception cannot be retryable in one loop and not in another. A test throwing only a
 * {@link ConcurrentModificationException} passes against the defect, which is how it shipped.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class Issue10759RetryFamilyEveryLoopTest extends TestHelper {
  private static final String TYPE     = "Issue10759Doc";
  private static final String FUNCTION = "issue10759Throw";
  private static final int    ATTEMPTS = 4;

  @Override
  protected void beginTest() {
    database.getSchema().createDocumentType(TYPE);
    // THE BACKOFF IS NOT WHAT THIS TEST IS ABOUT: NO WAIT BETWEEN ATTEMPTS
    database.getConfiguration().setValue(GlobalConfiguration.TX_RETRY_DELAY, 0);
  }

  @Override
  protected void endTest() {
    database.getConfiguration().setValue(GlobalConfiguration.TX_RETRY_DELAY, GlobalConfiguration.TX_RETRY_DELAY.getDefValue());
  }

  @Test
  void theFamilyIsEnumeratedFromTheExceptionPackage() throws Exception {
    final List<Class<?>> family = new ArrayList<>();
    for (final Supplier<RuntimeException> member : retryableFamily()) {
      final RuntimeException instance = member.get();
      family.add(instance.getClass());
      assertThat(TransactionRetryPolicy.isRetryable(instance)).as(instance.getClass().getName()).isTrue();
    }
    assertThat(family).as("the scan must find the members the loops have to retry, or every other test proves nothing")
        .contains(NeedRetryException.class, ConcurrentModificationException.class, LockTimeoutException.class,
            DuplicatedKeyException.class);
  }

  @Test
  void synchronousTransactionRetriesEveryMember() throws Exception {
    for (final Supplier<RuntimeException> member : retryableFamily()) {
      final AtomicInteger attempts = new AtomicInteger();
      final RuntimeException thrown = member.get();
      assertThatThrownBy(() -> database.transaction(() -> {
        attempts.incrementAndGet();
        throw member.get();
      }, false, ATTEMPTS)).isInstanceOf(thrown.getClass());
      assertThat(attempts.get()).as(thrown.getClass().getSimpleName()).isEqualTo(expectedAttempts(thrown, ATTEMPTS));
    }
  }

  @Test
  void asyncTransactionRetriesEveryMember() throws Exception {
    for (final Supplier<RuntimeException> member : retryableFamily()) {
      final AtomicInteger attempts = new AtomicInteger();
      final AtomicReference<Throwable> reported = new AtomicReference<>();
      final RuntimeException thrown = member.get();
      database.async().transaction(() -> {
        attempts.incrementAndGet();
        throw member.get();
      }, ATTEMPTS, null, reported::set);
      database.async().waitCompletion();

      // THE ASYNC LOOP TAKES ITS ARGUMENT AS RETRIES: ONE ATTEMPT MORE THAN THE SYNCHRONOUS ONE
      assertThat(attempts.get()).as(thrown.getClass().getSimpleName()).isEqualTo(expectedAttempts(thrown, ATTEMPTS + 1));
      assertThat(reported.get()).as("the caller is told, once the attempts are spent").isInstanceOf(thrown.getClass());
    }
  }

  @Test
  void asyncBatchCommitRetriesEveryMember() throws Exception {
    final int maxRetries = database.getConfiguration().getValueAsInteger(GlobalConfiguration.TX_RETRIES);
    database.async().setParallelLevel(1);
    // FAR ABOVE THE TWO COMMANDS: THE ONE BATCH COMMIT IS THE FLUSH OF waitCompletion()
    database.async().setCommitEvery(1_000);
    try {
      for (final Supplier<RuntimeException> member : retryableFamily()) {
        final AtomicInteger attempts = new AtomicInteger();
        final AtomicInteger errors = new AtomicInteger();
        final RuntimeException thrown = member.get();
        DatabaseAsyncExecutorImpl.TEST_BEFORE_BATCH_COMMIT_HOOK = callNumber -> {
          attempts.incrementAndGet();
          throw member.get();
        };
        try {
          // A BATCH OF REPLAYABLE COMMANDS, THE SHAPE THE REPLAY RETRIES
          for (int i = 0; i < 2; i++)
            database.async().command("sql", "INSERT INTO " + TYPE + " SET seq = :seq", new AsyncResultsetCallback() {
              @Override
              public void onComplete(final ResultSet rs) {
              }

              @Override
              public void onError(final Exception e) {
                errors.incrementAndGet();
              }
            }, Map.of("seq", i));
          database.async().waitCompletion();
        } finally {
          DatabaseAsyncExecutorImpl.TEST_BEFORE_BATCH_COMMIT_HOOK = null;
        }

        assertThat(attempts.get()).as(thrown.getClass().getSimpleName()).isEqualTo(expectedAttempts(thrown, maxRetries + 1));
        assertThat(errors.get()).as("every buffered command hears of the abandoned batch").isEqualTo(2);
      }
    } finally {
      database.async().waitCompletion();
    }
    assertThat(database.countType(TYPE, true)).isZero();
  }

  @Test
  void commitRetryRetriesEveryMember() throws Exception {
    final AtomicInteger attempts = new AtomicInteger();
    final AtomicReference<Supplier<RuntimeException>> toThrow = new AtomicReference<>();
    final SQLQueryEngine sql = (SQLQueryEngine) database.getQueryEngine("sql");
    sql.getFunctionFactory().register(new SQLFunctionAbstract(FUNCTION) {
      @Override
      public Object execute(final Object self, final Identifiable currentRecord, final Object currentResult, final Object[] params,
          final CommandContext context) {
        attempts.incrementAndGet();
        throw toThrow.get().get();
      }

      @Override
      public String getSyntax() {
        return FUNCTION + "()";
      }
    });
    try {
      for (final Supplier<RuntimeException> member : retryableFamily()) {
        attempts.set(0);
        toThrow.set(member);
        final RuntimeException thrown = member.get();
        assertThatThrownBy(() -> database.command("sqlscript", "BEGIN; SELECT " + FUNCTION + "(); COMMIT RETRY " + ATTEMPTS + ";"))
            .isInstanceOf(thrown.getClass());
        assertThat(attempts.get()).as(thrown.getClass().getSimpleName()).isEqualTo(expectedAttempts(thrown, ATTEMPTS));
        if (database.isTransactionActive())
          database.rollback();
      }
    } finally {
      sql.getFunctionFactory().unregister(FUNCTION);
    }
  }

  /**
   * The reporter's probe on the real path, not a thrown stand-in: another holder owns the bucket's file lock when the async
   * transaction commits, so the first commit times out on the lock with a {@link LockTimeoutException}. The lock is released
   * when the second attempt starts, and the retry the caller asked for commits.
   */
  @Test
  void asyncTransactionRetriesARealCommitLockTimeout() throws Exception {
    database.getConfiguration().setValue(GlobalConfiguration.COMMIT_LOCK_TIMEOUT, 200L);
    try {
      final LocalBucket bucket = (LocalBucket) database.getSchema().getType(TYPE).getBuckets(false).getFirst();
      final Object holder = new Object();
      final LocalDatabase local = (LocalDatabase) ((DatabaseInternal) database).getEmbedded();
      assertThat(local.getTransactionManager().tryLockFile(bucket.getFileId(), 0, holder)).isEqualTo(LockManager.LOCK_STATUS.YES);

      final AtomicInteger attempts = new AtomicInteger();
      final AtomicReference<Throwable> reported = new AtomicReference<>();
      final AtomicInteger committed = new AtomicInteger();
      try {
        database.async().transaction(() -> {
          if (attempts.incrementAndGet() == 2)
            local.getTransactionManager().unlockFile(bucket.getFileId(), holder);
          database.newDocument(TYPE).set("seq", 1).save();
        }, 3, committed::incrementAndGet, reported::set);
        database.async().waitCompletion();
      } finally {
        if (attempts.get() < 2)
          local.getTransactionManager().unlockFile(bucket.getFileId(), holder);
      }

      assertThat(reported.get()).as("the lock timeout of the first attempt must be retried, not reported").isNull();
      assertThat(attempts.get()).isEqualTo(2);
      assertThat(committed.get()).isEqualTo(1);
      assertThat(database.countType(TYPE, true)).isEqualTo(1);
    } finally {
      database.getConfiguration().setValue(GlobalConfiguration.COMMIT_LOCK_TIMEOUT, GlobalConfiguration.COMMIT_LOCK_TIMEOUT.getDefValue());
    }
  }

  /** A duplicated key gets one retry in every loop (#4959); every other member of the family uses every attempt. */
  private static int expectedAttempts(final RuntimeException member, final int attempts) {
    return member instanceof DuplicatedKeyException ? 2 : attempts;
  }

  /**
   * Every concrete {@link NeedRetryException} of the exception package - the package the engine raises them from - and
   * {@link DuplicatedKeyException}, as factories of a fresh instance.
   */
  private static List<Supplier<RuntimeException>> retryableFamily() throws Exception {
    final File dir = new File(NeedRetryException.class.getResource("NeedRetryException.class").toURI()).getParentFile();
    final String pkg = NeedRetryException.class.getPackageName();
    final List<Supplier<RuntimeException>> family = new ArrayList<>();
    for (final String name : dir.list()) {
      if (!name.endsWith(".class") || name.contains("$"))
        continue;
      final Class<?> c = Class.forName(pkg + "." + name.substring(0, name.length() - ".class".length()));
      if (NeedRetryException.class.isAssignableFrom(c) && !Modifier.isAbstract(c.getModifiers()))
        family.add(() -> instance(c));
    }
    family.add(() -> new DuplicatedKeyException("Issue10759Index", "[1]", new RID(1, 0)));
    return family;
  }

  private static RuntimeException instance(final Class<?> c) {
    if (c == DuplicatedKeyException.class)
      return new DuplicatedKeyException("Issue10759Index", "[1]", new RID(1, 0));
    try {
      return (RuntimeException) c.getConstructor(String.class).newInstance("simulated " + c.getSimpleName());
    } catch (final ReflectiveOperationException e) {
      throw new AssertionError(c.getName() + " needs a (String) constructor for this census", e);
    }
  }
}
