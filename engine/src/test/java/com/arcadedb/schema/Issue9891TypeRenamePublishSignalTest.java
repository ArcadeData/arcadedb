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
import com.arcadedb.exception.AlreadyExistsException;
import com.arcadedb.graph.MutableVertex;
import org.junit.jupiter.api.Test;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Proxy;
import java.util.concurrent.atomic.AtomicInteger;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Issue #9891 (item 2): which call of a type rename publishes is said by the entry point, not read off the hold count of
 * the type's rename lock. {@code rename()} always publishes, once; {@code renameDocumentPart()} is the nested step a
 * subclass uses and never publishes. A caller that took the lock and then called {@code rename()} used to look nested and
 * its rename was never published, silently; it is now refused before anything moves.
 */
class Issue9891TypeRenamePublishSignalTest extends TestHelper {

  @Test
  void aRenameCalledWhileTheThreadAlreadyHoldsTheRenameLockIsRefusedBeforeAnythingMoves() {
    database.command("sql", "CREATE DOCUMENT TYPE Held BUCKETS 2");
    database.transaction(() -> database.newDocument("Held").set("id", 1).save());
    final LocalDocumentType type = (LocalDocumentType) database.getSchema().getType("Held");
    final String bucketName = type.getBuckets(false).getFirst().getName();

    final AtomicInteger publishes = countPublishes(() -> {
      type.renameLock.lock();
      try {
        assertThatThrownBy(() -> type.rename("Held2")).isInstanceOf(IllegalStateException.class)
            .hasMessageContaining("already being renamed");
      } finally {
        type.renameLock.unlock();
      }
    });

    assertThat(publishes).hasValue(0);
    assertThat(database.getSchema().existsType("Held")).isTrue();
    assertThat(database.getSchema().existsType("Held2")).isFalse();
    assertThat(type.getBuckets(false).getFirst().getName()).as("no file moved").isEqualTo(bucketName);
  }

  @Test
  void theDocumentPartCannotBeRenamedOutsideARename() {
    database.command("sql", "CREATE DOCUMENT TYPE Outside");
    final LocalDocumentType type = (LocalDocumentType) database.getSchema().getType("Outside");

    assertThatThrownBy(() -> type.renameDocumentPart("Outside2")).isInstanceOf(IllegalStateException.class)
        .hasMessageContaining("call rename()");

    assertThat(database.getSchema().existsType("Outside")).isTrue();
    assertThat(database.getSchema().existsType("Outside2")).isFalse();
  }

  /** A vertex type renames its document part nested inside its own rename: still exactly one publication. */
  @Test
  void aVertexTypeRenamePublishesExactlyOnce() {
    database.command("sql", "CREATE VERTEX TYPE Pv BUCKETS 2");
    database.command("sql", "CREATE EDGE TYPE Pe");
    database.transaction(() -> {
      final MutableVertex a = database.newVertex("Pv").save();
      final MutableVertex b = database.newVertex("Pv").save();
      a.newEdge("Pe", b).save();
    });

    final AtomicInteger publishes = countPublishes(() -> database.getSchema().getType("Pv").rename("Pv2"));

    assertThat(publishes).hasValue(1);
    assertThat(database.getSchema().existsType("Pv2")).isTrue();
    assertThat(database.countType("Pv2", false)).isEqualTo(2);
  }

  @Test
  void aDocumentTypeRenamePublishesExactlyOnce() {
    database.command("sql", "CREATE DOCUMENT TYPE Pd");

    final AtomicInteger publishes = countPublishes(() -> database.getSchema().getType("Pd").rename("Pd2"));

    assertThat(publishes).hasValue(1);
    assertThat(database.getSchema().existsType("Pd2")).isTrue();
  }

  /** A vertex rename refused by its nested document part leaves the type where it was, and its rename lock released. */
  @Test
  void aVertexRenameRefusedByItsDocumentPartLeavesTheTypeAndReleasesTheLock() {
    database.command("sql", "CREATE VERTEX TYPE Rb");
    database.command("sql", "CREATE VERTEX TYPE Taken");
    database.transaction(() -> database.newVertex("Rb").save());

    assertThatThrownBy(() -> database.getSchema().getType("Rb").rename("Taken")).isInstanceOf(AlreadyExistsException.class);

    assertThat(database.getSchema().existsType("Rb")).isTrue();
    assertThat(database.countType("Rb", false)).isEqualTo(1);
    // The lock was released: a second rename of the same type goes through
    database.getSchema().getType("Rb").rename("Rb2");
    assertThat(database.getSchema().existsType("Rb2")).isTrue();
  }

  /**
   * Runs {@code action} with the database's wrapper replaced by one that counts the publishing sessions a rename opens,
   * through the same seam the HA server installs its wrapper on.
   */
  private AtomicInteger countPublishes(final Runnable action) {
    final LocalDatabase local = (LocalDatabase) database;
    final DatabaseInternal previous = local.getWrappedDatabaseInstance();
    final AtomicInteger publishes = new AtomicInteger();
    local.setWrappedDatabaseInstance((DatabaseInternal) Proxy.newProxyInstance(DatabaseInternal.class.getClassLoader(),
        new Class<?>[] { DatabaseInternal.class }, (proxy, method, args) -> {
          if ("recordFileChanges".equals(method.getName()))
            publishes.incrementAndGet();
          try {
            return method.invoke(local, args);
          } catch (final InvocationTargetException e) {
            throw e.getCause();
          }
        }));
    try {
      action.run();
    } finally {
      local.setWrappedDatabaseInstance(previous);
    }
    return publishes;
  }
}
