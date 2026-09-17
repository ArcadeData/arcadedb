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
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicReference;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Regression for issue #7672: {@code dropProperty} ran its hierarchy-wide index check OUTSIDE
 * {@code recordFileChanges}, i.e. outside the database write lock, while the sibling {@code renameProperty} - which
 * makes the identical call - runs it inside and documents why.
 * <p>
 * Two things follow from being outside. The check walks the {@code subTypes} list of this type and of every type
 * below it; those are plain {@code ArrayList}s that {@code linkSuperType}/{@code unlinkSuperType} structurally
 * modify under that same write lock, so an unlocked walk can land in a {@code ConcurrentModificationException}
 * surfacing as a 500 rather than a {@code SchemaException}. And a check that passes outside the lock can be
 * invalidated by a concurrent {@code CREATE INDEX}/{@code CREATE TYPE ... EXTENDS} before the drop applies, leaving
 * a subtype's index naming a property the super type no longer declares - which is exactly what the descendant walk
 * was added to prevent.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class Issue7672DropPropertyValidationUnderWriteLockTest extends TestHelper {

  private static final String PARENT = "Issue7672Parent";
  private static final String CHILD  = "Issue7672Child";

  @Override
  protected void beginTest() {
    database.getSchema().createDocumentType(PARENT).createProperty("code", Type.STRING);
    database.getSchema().createDocumentType(CHILD).addSuperType(PARENT);
    database.getSchema().createTypeIndex(Schema.INDEX_TYPE.LSM_TREE, false, CHILD, "code");
  }

  /**
   * The observable consequence of the fix, and the only one a test can pin deterministically: with the database
   * write lock held elsewhere, {@code dropProperty} cannot reach its verdict at all. Before the fix it answered
   * immediately, which is precisely what "the check does not take the lock the mutation takes" means.
   */
  @Test
  void theIndexCheckWaitsForTheWriteLockTheMutationTakes() throws Exception {
    final LocalDatabase   embedded    = (LocalDatabase) ((DatabaseInternal) database).getEmbedded();
    final CountDownLatch  lockHeld    = new CountDownLatch(1);
    final CountDownLatch  releaseLock = new CountDownLatch(1);
    final ExecutorService threads     = Executors.newFixedThreadPool(2);

    try {
      final Future<?> lockHolder = threads.submit(() -> embedded.executeInWriteLock(() -> {
        lockHeld.countDown();
        assertThat(releaseLock.await(30, TimeUnit.SECONDS)).isTrue();
        return null;
      }));

      assertThat(lockHeld.await(30, TimeUnit.SECONDS)).as("the helper thread took the database write lock").isTrue();

      final AtomicReference<Throwable> dropOutcome = new AtomicReference<>();
      final Future<?> drop = threads.submit(() -> {
        try {
          database.getSchema().getType(PARENT).dropProperty("code");
        } catch (final Throwable t) {
          dropOutcome.set(t);
        }
      });

      // A short wait EXPECTED TO TIME OUT: no elapsed-time claim is being made, so a stalled JVM can only make this
      // more true. Before the fix the drop returned its SchemaException here without ever asking for the lock.
      assertThatThrownBy(() -> drop.get(2, TimeUnit.SECONDS))
          .as("dropProperty must not reach a verdict while the write lock its mutation needs is held elsewhere")
          .isInstanceOf(TimeoutException.class);

      releaseLock.countDown();
      lockHolder.get(30, TimeUnit.SECONDS);
      drop.get(30, TimeUnit.SECONDS);

      assertThat(dropOutcome.get())
          .as("and once it does get the lock it still refuses, naming the subtype's index")
          .isInstanceOf(SchemaException.class);
      assertThat(dropOutcome.get().getMessage()).contains("code").contains(CHILD);
    } finally {
      releaseLock.countDown();
      threads.shutdownNow();
      assertThat(threads.awaitTermination(30, TimeUnit.SECONDS)).isTrue();
    }

    // Nothing was dropped: the refusal is still a refusal, not a half-applied change.
    assertThat(database.getSchema().getType(PARENT).existsProperty("code")).isTrue();
  }

  /**
   * The behaviour the moved checks must keep: a descendant's index on an inherited property still refuses the drop,
   * and a property nobody indexes still drops.
   */
  @Test
  void theMovedChecksStillRefuseAndStillAllow() {
    assertThatThrownBy(() -> database.getSchema().getType(PARENT).dropProperty("code"))
        .isInstanceOf(SchemaException.class)
        .hasMessageContaining(CHILD);

    database.getSchema().getType(PARENT).createProperty("unindexed", Type.STRING);
    assertThat(database.getSchema().getType(PARENT).dropProperty("unindexed")).isNotNull();
    assertThat(database.getSchema().getType(PARENT).existsProperty("unindexed")).isFalse();

    // A name the type does not declare is still a null answer, not an exception.
    assertThat(database.getSchema().getType(PARENT).dropProperty("neverExisted")).isNull();
  }
}
