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
import com.arcadedb.engine.Bucket;
import com.arcadedb.exception.NeedRetryException;
import com.arcadedb.exception.SchemaException;
import com.arcadedb.index.IndexInternal;
import com.arcadedb.index.TypeIndex;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.junit.jupiter.api.Assumptions.assumeFalse;

/**
 * Issue #10421: a schema statement that changes several parts of the schema graph kept what it had already done when it was
 * refused part way. {@code DROP TYPE} unlinked the sub types and dropped the indexes one by one before it reached the one that
 * refused, {@code ALTER TYPE ... BUCKET -} detached the bucket before it dropped its sub-indexes, {@code ALTER TYPE ... SUPERTYPE -}
 * severed the link before it dropped the sub-indexes the link had created - and the schema was saved in that state, so the
 * statement was refused AND half done, in memory and on disk.
 * <p>
 * A refusal leaves the schema as it was now: every statement asks what can refuse - the file of an index that cannot be deleted, a
 * file lock another thread holds - before its first change, and takes the locks of every file it is going to drop up front.
 * <p>
 * The failure is injected as {@code Issue10317RenamedTypeInheritedIndexTest} does, with a file system fault: the file of a sub-index
 * is moved aside and a non-empty directory takes its path, which {@code Files.delete} refuses.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class Issue10421SchemaStatementsAreAtomicTest extends TestHelper {

  /** A directory with something in it where a file of the schema is: it cannot be deleted. */
  private record Fault(Path file, Path aside) {
    void remove() throws IOException {
      Files.deleteIfExists(file.resolve("blocker"));
      Files.deleteIfExists(file);
      Files.move(aside, file);
    }
  }

  private Fault injectFault(final IndexInternal subIndex) throws IOException {
    assumeFalse(System.getProperty("os.name", "").toLowerCase().contains("win"), "an open file cannot be moved on Windows");
    final Path file = Path.of(((DatabaseInternal) database).getFileManager().getFile(subIndex.getFileId()).getFilePath());
    final Path aside = file.resolveSibling(file.getFileName() + ".aside");
    Files.move(file, aside);
    Files.createDirectory(file);
    Files.writeString(file.resolve("blocker"), "x");
    return new Fault(file, aside);
  }

  // ---- DROP TYPE

  @Test
  void aDropTypeThatCannotDeleteAFileIsRefusedBeforeItChangesAnything() throws IOException {
    createSuperAndSubType();
    insert("T5", "a");
    insert("T10", "b");
    final Fault fault = injectFault(subIndexOn("T10"));
    try {
      assertThatThrownBy(() -> database.command("sql", "DROP TYPE T5")).isInstanceOf(SchemaException.class)
          .hasMessageContaining("Nothing was changed");

      assertSchemaAsCreated();
    } finally {
      fault.remove();
    }

    reopenDatabase();

    assertSchemaAsCreated();
    assertThat(count("T5", "a")).isEqualTo(1L);
    assertThat(count("T5", "b")).isEqualTo(1L);

    // and the statement completes once nothing refuses it
    database.command("sql", "DROP TYPE T5");

    assertThat(database.getSchema().existsType("T5")).isFalse();
    assertThat(database.getSchema().getType("T10").getSuperTypes()).isEmpty();
    assertThat(database.getSchema().getType("T10").getAllIndexes(true)).isEmpty();
    assertThat(database.countType("T10", true)).isEqualTo(1L);

    reopenDatabase();

    assertThat(database.getSchema().existsType("T5")).isFalse();
    assertThat(database.getSchema().getType("T10").getSuperTypes()).isEmpty();
    assertThat(database.countType("T10", true)).isEqualTo(1L);
  }

  @Test
  void aDropTypeOfAVertexTypeThatCannotDeleteAFileIsRefusedBeforeItChangesAnything() throws IOException {
    database.command("sql", "CREATE VERTEX TYPE V1");
    database.command("sql", "CREATE PROPERTY V1.p STRING");
    database.command("sql", "CREATE INDEX ON V1 (p) NOTUNIQUE");
    database.command("sql", "CREATE EDGE TYPE E1");
    database.transaction(() -> {
      final var a = database.newVertex("V1").set("p", "a").save();
      final var b = database.newVertex("V1").set("p", "b").save();
      a.newEdge("E1", b);
    });
    final TypeIndex index = database.getSchema().getType("V1").getAllIndexes(false).iterator().next();
    final Fault fault = injectFault(index.getIndexesOnBuckets()[0]);
    try {
      assertThatThrownBy(() -> database.command("sql", "DROP TYPE V1 UNSAFE")).isInstanceOf(SchemaException.class)
          .hasMessageContaining("Nothing was changed");

      assertThat(database.getSchema().existsType("V1")).isTrue();
      assertThat(database.getSchema().getType("V1").getAllIndexes(false)).hasSize(1);
      assertThat(database.getSchema().getType("V1").getBuckets(false)).isNotEmpty();
    } finally {
      fault.remove();
    }

    reopenDatabase();

    assertThat(database.getSchema().existsType("V1")).isTrue();
    assertThat(database.query("sql", "SELECT FROM V1 WHERE p = 'a'").stream().count()).isEqualTo(1L);
    assertThat(database.query("sql", "SELECT expand(out('E1')) FROM V1 WHERE p = 'a'").stream().count()).isEqualTo(1L);
  }

  @Test
  @Tag("slow")
  void aDropTypeThatCannotTakeTheLockOfAFileIsRefusedBeforeItChangesAnything() throws Exception {
    createSuperAndSubType();
    insert("T5", "a");
    insert("T10", "b");

    // another thread holds the lock of the file of the sub-index on the bucket of T10, which DROP TYPE T5 has to take
    final List<Integer> locked = new ArrayList<>(subIndexOn("T10").getFileIds());
    final CountDownLatch holding = new CountDownLatch(1);
    final CountDownLatch release = new CountDownLatch(1);
    final Thread holder = new Thread(() -> ((DatabaseInternal) database).executeLockingFiles(locked, () -> {
      holding.countDown();
      try {
        release.await(30, TimeUnit.SECONDS);
      } catch (final InterruptedException e) {
        Thread.currentThread().interrupt();
      }
      return null;
    }));
    holder.start();
    assertThat(holding.await(10, TimeUnit.SECONDS)).isTrue();
    try {
      assertThatThrownBy(() -> database.command("sql", "DROP TYPE T5")).isInstanceOf(NeedRetryException.class);

      assertSchemaAsCreated();
    } finally {
      release.countDown();
      holder.join(10_000);
    }

    reopenDatabase();

    assertSchemaAsCreated();
    database.command("sql", "DROP TYPE T5");
    assertThat(database.getSchema().existsType("T5")).isFalse();
    assertThat(database.getSchema().getType("T10").getSuperTypes()).isEmpty();
  }

  // ---- ALTER TYPE ... BUCKET -

  @Test
  void aBucketRemovalThatCannotDeleteAFileIsRefusedBeforeItChangesAnything() throws IOException {
    database.command("sql", "CREATE DOCUMENT TYPE T5 BUCKETS 2");
    database.command("sql", "CREATE PROPERTY T5.p STRING");
    database.command("sql", "CREATE INDEX ON T5 (p) NOTUNIQUE");
    final DocumentType type = database.getSchema().getType("T5");
    final Bucket removed = type.getBuckets(false).getLast();
    final List<Integer> bucketIdsBefore = type.getBucketIds(false);
    final IndexInternal subIndex = subIndexOn(type, removed);
    final Fault fault = injectFault(subIndex);
    try {
      assertThatThrownBy(() -> database.command("sql", "ALTER TYPE T5 BUCKET -" + removed.getName())).isInstanceOf(SchemaException.class)
          .hasMessageContaining("Nothing was changed");

      assertThat(type.hasBucket(removed.getName())).isTrue();
      assertThat(type.getBucketIds(false)).isEqualTo(bucketIdsBefore);
      assertThat(database.getSchema().getTypeByBucketId(removed.getFileId())).isSameAs(type);
      assertThat(subIndexOn(type, removed)).isSameAs(subIndex);
      assertThat(database.getSchema().existsIndex(subIndex.getName())).isTrue();
    } finally {
      fault.remove();
    }

    reopenDatabase();

    final DocumentType reopened = database.getSchema().getType("T5");
    assertThat(reopened.getBucketIds(false)).isEqualTo(bucketIdsBefore);
    assertThat(reopened.getAllIndexes(false).iterator().next().getIndexesOnBuckets()).hasSize(2);

    // and the statement completes once nothing refuses it
    database.command("sql", "ALTER TYPE T5 BUCKET -" + removed.getName());

    assertThat(reopened.getBucketIds(false)).hasSize(1);
    assertThat(reopened.getAllIndexes(false).iterator().next().getIndexesOnBuckets()).hasSize(1);
  }

  // ---- ALTER TYPE ... SUPERTYPE -

  @Test
  void aSuperTypeRemovalThatCannotDeleteAFileKeepsTheLink() throws IOException {
    createSuperAndSubType();
    insert("T5", "a");
    insert("T10", "b");
    final Fault fault = injectFault(subIndexOn("T10"));
    try {
      assertThatThrownBy(() -> database.command("sql", "ALTER TYPE T10 SUPERTYPE -T5")).isInstanceOf(SchemaException.class)
          .hasMessageContaining("Nothing was changed");

      assertSchemaAsCreated();
    } finally {
      fault.remove();
    }

    reopenDatabase();

    assertSchemaAsCreated();

    database.command("sql", "ALTER TYPE T10 SUPERTYPE -T5");

    assertThat(database.getSchema().getType("T10").getSuperTypes()).isEmpty();
    assertThat(database.getSchema().getType("T5").getSubTypes()).isEmpty();
    assertThat(count("T5", "b")).isZero();
    assertThat(count("T5", "a")).isEqualTo(1L);
  }

  // ---- ALTER TYPE ... NAME

  @Test
  void aRefusedRenameLeavesTheBucketMapsAsTheyWere() throws IOException {
    createSuperAndSubType();
    insert("T10", "x");
    final Bucket bucket = database.getSchema().getType("T10").getBuckets(false).getFirst();
    final String bucketName = bucket.getName();
    final IndexInternal inherited = subIndexOn("T10");
    final Path blocked = Path.of(((DatabaseInternal) database).getFileManager().getFile(inherited.getFileId()).getFilePath());
    final Path parked = blocked.resolveSibling(blocked.getFileName().toString().replace("T10_0_", "T11_0_"));
    Files.createDirectory(parked);
    try {
      assertThatThrownBy(() -> database.getSchema().getType("T10").rename("T11")).isInstanceOf(SchemaException.class);

      assertThat(database.getSchema().existsType("T10")).isTrue();
      assertThat(database.getSchema().existsType("T11")).isFalse();
      assertThat(database.getSchema().existsBucket(bucketName)).isTrue();
      assertThat(database.getSchema().getBucketByName(bucketName)).isSameAs(bucket);
      assertThat(database.getSchema().getTypeByBucketId(bucket.getFileId()).getName()).isEqualTo("T10");
      assertThat(database.getSchema().getTypeByBucketName(bucketName).getName()).isEqualTo("T10");
    } finally {
      Files.deleteIfExists(parked);
    }

    reopenDatabase();

    assertThat(database.getSchema().existsType("T10")).isTrue();
    assertThat(count("T5", "x")).isEqualTo(1L);
  }

  // ---- helpers

  /** T5 and T10 EXTENDS T5 with the index of T5 over the buckets of both, as {@link #createSuperAndSubType} built them. */
  private void assertSchemaAsCreated() {
    final DocumentType t5 = database.getSchema().getType("T5");
    final DocumentType t10 = database.getSchema().getType("T10");
    assertThat(t10.getSuperTypes()).containsExactly(t5);
    assertThat(t5.getSubTypes()).containsExactly(t10);
    assertThat(t5.getAllIndexes(false)).hasSize(1);
    final TypeIndex index = t5.getAllIndexes(false).iterator().next();
    assertThat(index.getIndexesOnBuckets()).hasSize(2);
    assertThat(t10.getAllIndexes(true)).containsExactly(index);
    for (final IndexInternal subIndex : index.getIndexesOnBuckets())
      assertThat(database.getSchema().existsIndex(subIndex.getName())).isTrue();
    assertThat(database.getSchema().getTypeByBucketId(t10.getBuckets(false).getFirst().getFileId())).isSameAs(t10);
    assertThat(t5.getBucketIds(true)).hasSize(2);
    assertThat(t10.getBucketIds(true)).hasSize(1);
    assertThat(t10.getPolymorphicPropertyNames()).contains("p");
    assertThat(t10.isSubTypeOf("T5")).isTrue();
  }

  private void createSuperAndSubType() {
    database.command("sql", "CREATE DOCUMENT TYPE T5");
    database.command("sql", "CREATE PROPERTY T5.p STRING");
    database.command("sql", "CREATE INDEX ON T5 (p) NOTUNIQUE");
    database.command("sql", "CREATE DOCUMENT TYPE T10 EXTENDS T5");
  }

  private void insert(final String typeName, final String value) {
    database.transaction(() -> database.newDocument(typeName).set("p", value).save());
  }

  private long count(final String typeName, final String value) {
    return database.query("sql", "SELECT FROM " + typeName + " WHERE p = ?", value).stream().count();
  }

  private IndexInternal subIndexOn(final String typeName) {
    final DocumentType type = database.getSchema().getType(typeName);
    return subIndexOn(type, type.getBuckets(false).getFirst());
  }

  private IndexInternal subIndexOn(final DocumentType type, final Bucket bucket) {
    for (final DocumentType candidate : database.getSchema().getTypes())
      for (final TypeIndex index : candidate.getAllIndexes(false))
        for (final IndexInternal subIndex : index.getIndexesOnBuckets())
          if (subIndex.getAssociatedBucketId() == bucket.getFileId())
            return subIndex;
    throw new AssertionError("no sub-index on the bucket " + bucket.getName() + " of " + type.getName());
  }
}
