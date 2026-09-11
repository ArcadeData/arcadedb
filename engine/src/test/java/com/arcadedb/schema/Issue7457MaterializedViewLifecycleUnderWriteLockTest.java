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
import com.arcadedb.exception.SchemaException;
import org.junit.jupiter.api.Test;

import java.util.concurrent.Callable;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Companion of {@link Issue7457SchemaSaveUnderWriteLockTest} for the materialized view lifecycle: since #7457 the
 * drop and the alter of a view no longer hold the schema monitor across the database write lock, so the teardown of
 * a view's refresh resources (incremental listeners, periodic task) moved INSIDE the write-locked transition, next
 * to the removal or replacement of the view itself. What a drop tears down is therefore always what the view it
 * removed owned, including resources an alter or a create installed under the same lock just before.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class Issue7457MaterializedViewLifecycleUnderWriteLockTest extends TestHelper {

  @Override
  protected void beginTest() {
    final DocumentType source = database.getSchema().createDocumentType("Source");
    source.createProperty("value", Integer.class);
    database.transaction(() -> database.newDocument("Source").set("value", 1).save());
  }

  @Test
  void dropTearsDownTheListenersAnAlterInstalled() {
    final Schema schema = database.getSchema();
    schema.buildMaterializedView().withName("View").withQuery("SELECT value FROM Source")
        .withRefreshMode(MaterializedViewRefreshMode.MANUAL).create();

    schema.alterMaterializedView("View", MaterializedViewRefreshMode.INCREMENTAL, 0);
    final MaterializedViewImpl altered = (MaterializedViewImpl) schema.getMaterializedView("View");
    assertThat(altered.getChangeListener()).as("the alter installed the incremental listener").isNotNull();

    schema.dropMaterializedView("View");

    assertThat(altered.getChangeListener()).as("the drop tore down the listener the alter installed").isNull();
    assertThat(schema.existsMaterializedView("View")).isFalse();
    assertThat(schema.existsType("View")).isFalse();
    // A LISTENER LEFT BEHIND WOULD TRY TO REFRESH A VIEW WHOSE BACKING TYPE IS GONE
    database.transaction(() -> database.newDocument("Source").set("value", 2).save());
  }

  @Test
  void alterReplacesTheListenersUnderTheSameLockAsTheView() {
    final Schema schema = database.getSchema();
    schema.buildMaterializedView().withName("View").withQuery("SELECT value FROM Source")
        .withRefreshMode(MaterializedViewRefreshMode.INCREMENTAL).create();
    final MaterializedViewImpl created = (MaterializedViewImpl) schema.getMaterializedView("View");
    assertThat(created.getChangeListener()).isNotNull();

    schema.alterMaterializedView("View", MaterializedViewRefreshMode.MANUAL, 0);

    assertThat(created.getChangeListener()).as("the alter tore down the listener of the replaced view").isNull();
    assertThat(((MaterializedViewImpl) schema.getMaterializedView("View")).getChangeListener()).isNull();
    schema.dropMaterializedView("View");
  }

  /** The two drops of the same view: the second fails, and it fails without having torn anything down twice. */
  @Test
  void secondDropOfTheSameViewFails() {
    final Schema schema = database.getSchema();
    schema.buildMaterializedView().withName("View").withQuery("SELECT value FROM Source")
        .withRefreshMode(MaterializedViewRefreshMode.MANUAL).create();
    schema.dropMaterializedView("View");
    assertThatThrownBy(() -> schema.dropMaterializedView("View")).isInstanceOf(SchemaException.class)
        .hasMessageContaining("not found");
  }

  /** Two concurrent drops of the same view: exactly one succeeds, the other fails as not found, nothing deadlocks. */
  @Test
  void concurrentDropsOfTheSameViewProduceOneSuccessAndOneNotFound() throws Exception {
    final Schema schema = database.getSchema();
    schema.buildMaterializedView().withName("View").withQuery("SELECT value FROM Source")
        .withRefreshMode(MaterializedViewRefreshMode.INCREMENTAL).create();
    final MaterializedViewImpl view = (MaterializedViewImpl) schema.getMaterializedView("View");

    final int droppers = 4;
    final CyclicBarrier start = new CyclicBarrier(droppers);
    final AtomicInteger succeeded = new AtomicInteger();
    final AtomicInteger notFound = new AtomicInteger();
    final AtomicReference<Throwable> unexpected = new AtomicReference<>();
    final Thread[] threads = new Thread[droppers];
    for (int i = 0; i < droppers; i++) {
      threads[i] = new Thread(() -> {
        try {
          start.await(10, TimeUnit.SECONDS);
          schema.dropMaterializedView("View");
          succeeded.incrementAndGet();
        } catch (final SchemaException e) {
          if (e.getMessage().contains("not found"))
            notFound.incrementAndGet();
          else
            unexpected.set(e);
        } catch (final Throwable e) {
          unexpected.set(e);
        }
      }, "dropper-" + i);
      threads[i].start();
    }
    for (final Thread thread : threads)
      thread.join(30_000);

    assertThat(unexpected.get()).isNull();
    assertThat(succeeded.get()).isEqualTo(1);
    assertThat(notFound.get()).isEqualTo(droppers - 1);
    assertThat(view.getChangeListener()).as("the one drop that won tore the listener down").isNull();
    assertThat(schema.existsMaterializedView("View")).isFalse();
    assertThat(schema.existsType("View")).isFalse();
    database.transaction(() -> database.newDocument("Source").set("value", 3).save());
  }

  /**
   * Alters racing a drop of the same view: every alter either completes before the drop, replacing the listener under
   * the write lock, or fails as not found after it. Whatever the interleaving, the one drop tears down the resources
   * of the view it removed, and nothing is left listening on the source type.
   */
  @Test
  void concurrentAltersAndDropOfTheSameViewLeaveNothingBehind() throws Exception {
    final Schema schema = database.getSchema();
    schema.buildMaterializedView().withName("View").withQuery("SELECT value FROM Source")
        .withRefreshMode(MaterializedViewRefreshMode.INCREMENTAL).create();

    final int alterers = 3;
    final CyclicBarrier start = new CyclicBarrier(alterers + 1);
    final AtomicInteger altered = new AtomicInteger();
    final AtomicInteger alterNotFound = new AtomicInteger();
    final AtomicReference<Throwable> unexpected = new AtomicReference<>();
    final Thread[] threads = new Thread[alterers + 1];
    for (int i = 0; i < alterers; i++) {
      final MaterializedViewRefreshMode mode = i % 2 == 0 ? MaterializedViewRefreshMode.MANUAL : MaterializedViewRefreshMode.INCREMENTAL;
      threads[i] = new Thread(() -> {
        try {
          start.await(10, TimeUnit.SECONDS);
          schema.alterMaterializedView("View", mode, 0);
          altered.incrementAndGet();
        } catch (final SchemaException e) {
          if (e.getMessage().contains("not found"))
            alterNotFound.incrementAndGet();
          else
            unexpected.set(e);
        } catch (final Throwable e) {
          unexpected.set(e);
        }
      }, "alterer-" + i);
    }
    threads[alterers] = new Thread(() -> {
      try {
        start.await(10, TimeUnit.SECONDS);
        schema.dropMaterializedView("View");
      } catch (final Throwable e) {
        unexpected.set(e);
      }
    }, "dropper");
    for (final Thread thread : threads)
      thread.start();
    for (final Thread thread : threads)
      thread.join(30_000);

    assertThat(unexpected.get()).isNull();
    assertThat(altered.get() + alterNotFound.get()).isEqualTo(alterers);
    assertThat(schema.existsMaterializedView("View")).isFalse();
    assertThat(schema.existsType("View")).isFalse();
    // WHATEVER LISTENER THE LAST ALTER BEFORE THE DROP INSTALLED, THE DROP TORE IT DOWN: THE INSERT IS HARMLESS
    database.transaction(() -> database.newDocument("Source").set("value", 4).save());
  }

  /** The transition runs inside one outermost recording frame, whichever resources it installs or tears down. */
  @Test
  void lifecycleTransitionIsOneWriteLockedFrame() {
    final Schema schema = database.getSchema();
    final DatabaseInternal db = (DatabaseInternal) database;
    schema.buildMaterializedView().withName("View").withQuery("SELECT value FROM Source")
        .withRefreshMode(MaterializedViewRefreshMode.INCREMENTAL).create();

    final AtomicInteger frames = new AtomicInteger();
    final Callable<Void> observer = () -> {
      frames.incrementAndGet();
      return null;
    };
    db.registerCallback(DatabaseInternal.CALLBACK_EVENT.SCHEMA_AFTER_FILE_CHANGES, observer);
    try {
      schema.alterMaterializedView("View", MaterializedViewRefreshMode.MANUAL, 0);
      assertThat(frames.get()).isEqualTo(1);
      schema.dropMaterializedView("View");
      assertThat(frames.get()).isEqualTo(2);
    } finally {
      db.unregisterCallback(DatabaseInternal.CALLBACK_EVENT.SCHEMA_AFTER_FILE_CHANGES, observer);
    }
  }
}
