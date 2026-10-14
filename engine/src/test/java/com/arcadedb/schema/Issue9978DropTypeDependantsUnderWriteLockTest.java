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
package com.arcadedb.schema;

import com.arcadedb.TestHelper;
import com.arcadedb.database.DatabaseInternal;
import com.arcadedb.database.LocalDatabase;
import com.arcadedb.exception.SchemaException;
import org.junit.jupiter.api.Test;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * #9978: {@code dropType} refused a type that backs or feeds a materialized view or a continuous aggregate, but decided
 * it before taking the schema write lock and never again. A view registered on the type between that verdict and the
 * lock was left naming a type the drop then removed, and neither statement reported an error.
 * <p>
 * The window is made deterministic by holding the database write lock on a helper thread: the drop is let through
 * its early check and parks on the lock, and only then does the helper - still holding the lock - register the
 * dependant. Once the lock is released the drop must refuse. The mirror is pinned too: a create that validated its
 * source type before the lock must not register a dependant on a type dropped while it waited.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class Issue9978DropTypeDependantsUnderWriteLockTest extends TestHelper {
  private static final String DOC_SOURCE = "Issue9978Source";
  private static final String TS_SOURCE  = "Issue9978Readings";

  @Override
  protected void beginTest() {
    database.getSchema().createDocumentType(DOC_SOURCE);
    database.command("sql", "CREATE TIMESERIES TYPE " + TS_SOURCE + " TIMESTAMP ts TAGS (sensor STRING) FIELDS (temperature DOUBLE)");
    database.transaction(() -> {
      database.newDocument(DOC_SOURCE).set("name", "a").save();
      database.command("sql", "INSERT INTO " + TS_SOURCE + " SET ts = 1000, sensor = 'A', temperature = 22.5");
    });
  }

  @Test
  void aMaterializedViewRegisteredWhileTheDropWaitsForTheLockStopsTheDrop() throws Exception {
    final Throwable dropOutcome = dropWhileAnotherThreadRegistersUnderTheLock(DOC_SOURCE,
        () -> database.getSchema().buildMaterializedView().withName("Issue9978View").withQuery("SELECT FROM " + DOC_SOURCE).create());

    assertThat(dropOutcome).isInstanceOf(SchemaException.class);
    assertThat(dropOutcome.getMessage()).contains("source type for materialized view 'Issue9978View'");
    assertThat(database.getSchema().existsType(DOC_SOURCE)).isTrue();
    assertThat(database.getSchema().existsMaterializedView("Issue9978View")).isTrue();
  }

  @Test
  void aContinuousAggregateRegisteredWhileTheDropWaitsForTheLockStopsTheDrop() throws Exception {
    final Throwable dropOutcome = dropWhileAnotherThreadRegistersUnderTheLock(TS_SOURCE,
        () -> database.getSchema().buildContinuousAggregate().withName("Issue9978Hourly").withQuery(
            "SELECT sensor, ts.timeBucket('1h', ts) AS hour, avg(temperature) AS avg_temp FROM " + TS_SOURCE + " GROUP BY sensor, hour")
            .create());

    assertThat(dropOutcome).isInstanceOf(SchemaException.class);
    assertThat(dropOutcome.getMessage()).contains("source type for continuous aggregate 'Issue9978Hourly'");
    assertThat(database.getSchema().existsType(TS_SOURCE)).isTrue();
    assertThat(database.getSchema().existsContinuousAggregate("Issue9978Hourly")).isTrue();
  }

  @Test
  void aMaterializedViewWhoseSourceIsDroppedWhileItWaitsForTheLockIsNotRegistered() throws Exception {
    final Throwable createOutcome = runWhileAnotherThreadHoldsTheLock(
        () -> database.getSchema().buildMaterializedView().withName("Issue9978Orphan").withQuery("SELECT FROM " + DOC_SOURCE).create(),
        () -> database.getSchema().dropType(DOC_SOURCE));

    assertThat(createOutcome).isInstanceOf(SchemaException.class);
    assertThat(createOutcome.getMessage()).contains(DOC_SOURCE).contains("does not exist");
    assertThat(database.getSchema().existsMaterializedView("Issue9978Orphan")).isFalse();
    assertThat(database.getSchema().existsType("Issue9978Orphan")).isFalse();
  }

  @Test
  void aContinuousAggregateWhoseSourceIsDroppedWhileItWaitsForTheLockIsNotRegistered() throws Exception {
    final Throwable createOutcome = runWhileAnotherThreadHoldsTheLock(
        () -> database.getSchema().buildContinuousAggregate().withName("Issue9978OrphanHourly").withQuery(
            "SELECT sensor, ts.timeBucket('1h', ts) AS hour, avg(temperature) AS avg_temp FROM " + TS_SOURCE + " GROUP BY sensor, hour")
            .create(),
        () -> database.getSchema().dropType(TS_SOURCE));

    assertThat(createOutcome).isInstanceOf(SchemaException.class);
    assertThat(createOutcome.getMessage()).contains(TS_SOURCE).contains("does not exist");
    assertThat(database.getSchema().existsContinuousAggregate("Issue9978OrphanHourly")).isFalse();
    assertThat(database.getSchema().existsType("Issue9978OrphanHourly")).isFalse();
  }

  /** The behaviour the moved guard keeps without any race: a dependant still refuses the drop, by name and by alias. */
  @Test
  void theGuardStillRefusesWithoutARace() {
    database.getSchema().buildMaterializedView().withName("Issue9978Plain").withQuery("SELECT FROM " + DOC_SOURCE).create();

    assertThat(dropOutcome(DOC_SOURCE)).isInstanceOf(SchemaException.class).hasMessageContaining("source type");
    assertThat(dropOutcome("Issue9978Plain")).isInstanceOf(SchemaException.class).hasMessageContaining("backing type");

    database.command("sql", "ALTER TYPE " + DOC_SOURCE + " ALIASES Issue9978Alias");
    assertThat(dropOutcome("Issue9978Alias")).isInstanceOf(SchemaException.class).hasMessageContaining("source type");

    database.getSchema().dropMaterializedView("Issue9978Plain");
    database.getSchema().dropType(DOC_SOURCE);
    assertThat(database.getSchema().existsType(DOC_SOURCE)).isFalse();
  }

  /** A view defined through an alias stores the alias as its source: the type is still protected under both names. */
  @Test
  void aViewDefinedThroughAnAliasStillProtectsItsSourceType() {
    database.command("sql", "ALTER TYPE " + DOC_SOURCE + " ALIASES Issue9978ViaAlias");
    database.getSchema().buildMaterializedView().withName("Issue9978AliasView").withQuery("SELECT FROM Issue9978ViaAlias").create();

    assertThat(dropOutcome(DOC_SOURCE)).isInstanceOf(SchemaException.class).hasMessageContaining("Issue9978AliasView");
    assertThat(dropOutcome("Issue9978ViaAlias")).isInstanceOf(SchemaException.class).hasMessageContaining("Issue9978AliasView");
    assertThat(database.getSchema().existsType(DOC_SOURCE)).isTrue();

    database.getSchema().dropMaterializedView("Issue9978AliasView");
    database.getSchema().dropType(DOC_SOURCE);
    assertThat(database.getSchema().existsType(DOC_SOURCE)).isFalse();
  }

  private Throwable dropOutcome(final String typeName) {
    try {
      database.getSchema().dropType(typeName);
      return null;
    } catch (final Throwable t) {
      return t;
    }
  }

  private Throwable dropWhileAnotherThreadRegistersUnderTheLock(final String typeName, final Runnable registerDependant)
      throws Exception {
    return runWhileAnotherThreadHoldsTheLock(() -> database.getSchema().dropType(typeName), registerDependant);
  }

  /**
   * Runs {@code contender} on its own thread while a helper thread holds the database write lock, waits for the
   * contender to park on that lock - that is, to have made every decision it makes before the lock - then runs
   * {@code underTheLock} on the helper thread, still holding the lock, and releases it.
   *
   * @return what the contender threw, or null
   */
  private Throwable runWhileAnotherThreadHoldsTheLock(final Runnable contender, final Runnable underTheLock) throws Exception {
    final LocalDatabase embedded = (LocalDatabase) ((DatabaseInternal) database).getEmbedded();
    final CountDownLatch lockHeld = new CountDownLatch(1);
    final CountDownLatch contenderParked = new CountDownLatch(1);
    final AtomicReference<Throwable> holderFailure = new AtomicReference<>();
    final AtomicReference<Throwable> contenderOutcome = new AtomicReference<>();

    final Thread holder = new Thread(() -> {
      try {
        embedded.executeInWriteLock(() -> {
          lockHeld.countDown();
          assertThat(contenderParked.await(30, TimeUnit.SECONDS)).isTrue();
          underTheLock.run();
          return null;
        });
      } catch (final Throwable t) {
        holderFailure.set(t);
      }
    }, "Issue9978-lock-holder");

    final Thread contenderThread = new Thread(() -> {
      try {
        contender.run();
      } catch (final Throwable t) {
        contenderOutcome.set(t);
      }
    }, "Issue9978-contender");

    holder.start();
    try {
      assertThat(lockHeld.await(30, TimeUnit.SECONDS)).as("the helper thread took the database write lock").isTrue();
      contenderThread.start();
      awaitParkedOnTheWriteLock(contenderThread);
    } finally {
      contenderParked.countDown();
      holder.join(30_000);
      contenderThread.join(30_000);
    }

    assertThat(holder.isAlive()).isFalse();
    assertThat(contenderThread.isAlive()).isFalse();
    assertThat(holderFailure.get()).as("the dependant registered under the lock").isNull();
    return contenderOutcome.get();
  }

  /** The contender is waiting inside the write lock acquisition, so whatever it checks before the lock is behind it. */
  private static void awaitParkedOnTheWriteLock(final Thread thread) throws InterruptedException {
    for (int i = 0; i < 3_000; i++) {
      final Thread.State state = thread.getState();
      if (state == Thread.State.WAITING || state == Thread.State.TIMED_WAITING)
        for (final StackTraceElement frame : thread.getStackTrace())
          if (frame.getClassName().equals(LocalDatabase.class.getName()) && frame.getMethodName().equals("executeInWriteLock"))
            return;
      assertThat(thread.isAlive()).as("the contender ended before it reached the write lock").isTrue();
      Thread.sleep(10);
    }
    throw new AssertionError("The contender never parked on the database write lock");
  }
}
