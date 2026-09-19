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
 * Regression for issue #7918, the mirror image of #7672: {@code createProperty} ran every one of its checks OUTSIDE
 * {@code recordFileChanges} - i.e. outside the database write lock - and only {@code properties.put} inside, while
 * the sibling {@code dropProperty} in the same class had just been fixed to do the opposite and documents why.
 * <p>
 * The check that matters is {@code getPolymorphicPropertyNames().contains(propertyName)}, which recurses over
 * {@code superTypes}: a plain {@code ArrayList} that {@code linkSuperType}/{@code unlinkSuperType} structurally
 * modify under that same write lock. So an unlocked walk can land in a {@code ConcurrentModificationException} out
 * of an unrelated {@code CREATE PROPERTY}, and a check that passes outside the lock can be invalidated by a
 * concurrent {@code CREATE TYPE ... EXTENDS} before the put applies - leaving the subtype shadowing a super type's
 * property, which is exactly what the upward walk exists to prevent.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class Issue7918CreatePropertyValidationUnderWriteLockTest extends TestHelper {

  private static final String PARENT = "Issue7918Parent";
  private static final String CHILD  = "Issue7918Child";

  @Override
  protected void beginTest() {
    database.getSchema().createDocumentType(PARENT).createProperty("code", Type.STRING);
    database.getSchema().createDocumentType(CHILD).addSuperType(PARENT);
  }

  /**
   * The observable consequence of the fix, and the only one a test can pin deterministically: with the database
   * write lock held elsewhere, {@code createProperty} cannot reach its verdict at all. Before the fix it answered
   * immediately, which is precisely what "the checks do not take the lock the mutation takes" means.
   */
  @Test
  void theSuperTypeWalkWaitsForTheWriteLockTheMutationTakes() throws Exception {
    final LocalDatabase    embedded    = (LocalDatabase) ((DatabaseInternal) database).getEmbedded();
    final CountDownLatch   lockHeld    = new CountDownLatch(1);
    final CountDownLatch   releaseLock = new CountDownLatch(1);
    final ExecutorService  threads     = Executors.newFixedThreadPool(2);

    try {
      final Future<?> lockHolder = threads.submit(() -> embedded.executeInWriteLock(() -> {
        lockHeld.countDown();
        assertThat(releaseLock.await(30, TimeUnit.SECONDS)).isTrue();
        return null;
      }));

      assertThat(lockHeld.await(30, TimeUnit.SECONDS)).as("the helper thread took the database write lock").isTrue();

      final AtomicReference<Throwable> createOutcome = new AtomicReference<>();
      final Future<?> create = threads.submit(() -> {
        try {
          database.getSchema().getType(CHILD).createProperty("code", Type.STRING);
        } catch (final Throwable t) {
          createOutcome.set(t);
        }
      });

      // A short wait EXPECTED TO TIME OUT: no elapsed-time claim is being made, so a stalled JVM can only make this
      // more true. Before the fix the create returned its SchemaException here without ever asking for the lock.
      assertThatThrownBy(() -> create.get(2, TimeUnit.SECONDS))
          .as("createProperty must not reach a verdict while the write lock its mutation needs is held elsewhere")
          .isInstanceOf(TimeoutException.class);

      releaseLock.countDown();
      lockHolder.get(30, TimeUnit.SECONDS);
      create.get(30, TimeUnit.SECONDS);

      assertThat(createOutcome.get())
          .as("and once it does get the lock it still refuses, naming the super type's property")
          .isInstanceOf(SchemaException.class);
      assertThat(createOutcome.get().getMessage()).contains("code").contains("super type");
    } finally {
      releaseLock.countDown();
      threads.shutdownNow();
      assertThat(threads.awaitTermination(30, TimeUnit.SECONDS)).isTrue();
    }

    // Nothing was created: the refusal is still a refusal, not a half-applied change.
    assertThat(database.getSchema().getType(CHILD).existsProperty("code")).isFalse();
  }

  /**
   * The behaviour the moved checks must keep, one per check that moved.
   */
  @Test
  void theMovedChecksStillRefuseAndStillAllow() {
    // 1. A name already declared by a super type.
    assertThatThrownBy(() -> database.getSchema().getType(CHILD).createProperty("code", Type.STRING))
        .isInstanceOf(SchemaException.class)
        .hasMessageContaining("super type");

    // 2. A name this type already declares.
    assertThatThrownBy(() -> database.getSchema().getType(PARENT).createProperty("code", Type.STRING))
        .isInstanceOf(SchemaException.class)
        .hasMessageContaining("already exists");

    // 3. A LIGHTWEIGHT edge type has no record to hold a value.
    final LocalEdgeType light = (LocalEdgeType) database.getSchema().createEdgeType("Issue7918Light");
    light.setLightweight(true);
    assertThatThrownBy(() -> light.createProperty("weight", Type.LONG))
        .isInstanceOf(SchemaException.class)
        .hasMessageContaining("LIGHTWEIGHT");

    // And a free name still lands, with everything the non-moved part of the method used to set.
    final Property created = database.getSchema().getType(CHILD).createProperty("label", Type.LIST, "STRING");
    assertThat(created.getName()).isEqualTo("label");
    assertThat(created.getType()).isEqualTo(Type.LIST);
    assertThat(created.getOfType()).isEqualTo("STRING");
    assertThat(database.getSchema().getType(CHILD).existsProperty("label")).isTrue();
  }

  /**
   * {@code rename()}'s name bookkeeping, which the same issue touched (and which CodeRabbit's review of PR #7935
   * sharpened): the old name has to stay mapped to the type for the WHOLE rename, because the rename can still
   * roll back after the new name is reserved. Released early, a concurrent {@code CREATE TYPE} could legitimately
   * take it in that window and the rollback's restore would have evicted that type.
   * <p>
   * The window itself needs a failure injected mid-rename to observe directly; what is pinned here are the two
   * post-conditions that bracket it and that any regression in the bookkeeping breaks - a rename that succeeds
   * releases exactly the old name, and a rename that is refused changes nothing and evicts nobody.
   */
  @Test
  void renameReleasesTheOldNameOnlyOnSuccessAndEvictsNobodyOnRefusal() {
    final Schema schema = database.getSchema();
    schema.createDocumentType("Issue7918Renamed").createProperty("label", Type.STRING);

    // A rename that succeeds: the old name is gone, the new one resolves to the same type, and it kept its
    // properties (so the map entry really is the type, not a fresh one).
    schema.getType("Issue7918Renamed").rename("Issue7918RenamedNow");

    assertThat(schema.existsType("Issue7918Renamed")).isFalse();
    assertThat(schema.existsType("Issue7918RenamedNow")).isTrue();
    assertThat(schema.getType("Issue7918RenamedNow").existsProperty("label")).isTrue();

    // A rename onto a name somebody else holds is refused, and - the part that matters - the holder is still
    // there afterwards, under its own name, with its own properties.
    schema.createDocumentType("Issue7918Occupant").createProperty("occupied", Type.STRING);

    assertThatThrownBy(() -> schema.getType("Issue7918RenamedNow").rename("Issue7918Occupant"))
        .isInstanceOf(IllegalArgumentException.class);

    assertThat(schema.existsType("Issue7918Occupant")).isTrue();
    assertThat(schema.getType("Issue7918Occupant").existsProperty("occupied"))
        .as("the refused rename must not have evicted the type that legitimately holds the name").isTrue();
    assertThat(schema.existsType("Issue7918RenamedNow"))
        .as("and the type that tried to rename is still reachable under its own name").isTrue();
    assertThat(schema.getType("Issue7918RenamedNow").existsProperty("label")).isTrue();
  }

  /**
   * The property must survive a reopen, i.e. the mutation still goes through {@code recordFileChanges} and still
   * reaches {@code schema.json} - the move must not have turned the write into an in-memory-only one.
   */
  @Test
  void theCreatedPropertyIsPersisted() {
    database.getSchema().getType(CHILD).createProperty("persisted", Type.INTEGER);

    reopenDatabase();

    assertThat(database.getSchema().getType(CHILD).existsProperty("persisted")).isTrue();
    assertThat(database.getSchema().getType(CHILD).getProperty("persisted").getType()).isEqualTo(Type.INTEGER);
  }
}
