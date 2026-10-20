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
import com.arcadedb.index.IndexInternal;
import com.arcadedb.index.TypeIndex;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Issue #10412: after #10348 (a rename relabels only the sub-indexes sitting on the buckets of the renamed type), an index created
 * with {@code CREATE INDEX} on a type that already had a sub type was left with the sub-index of the sub type's bucket naming the
 * type that DECLARES the index, not the type that owns the bucket. The rename of the declaring type then never reached it, and
 * {@code DROP TYPE} / {@code ALTER TYPE ... BUCKET -} of the sub type could not find the type the sub-index named ("Type with name
 * 'T22' was not found") and left the schema half changed. A schema reload binds every sub-index to the type that owns its bucket,
 * so the layout in memory differed from the one after a reopen.
 * <p>
 * The invariant pinned here: a sub-index names the type that owns its bucket, also right after {@code CREATE INDEX}, so that
 * the layout does not change at the next open and every sequence of schema statements ends the same before and after a reopen.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class Issue10412RenamedIndexedSuperTypeTest extends TestHelper {

  @Test
  void dropTypeOfASubTypeAfterItsIndexedSuperTypeWasRenamed() {
    run("CREATE DOCUMENT TYPE T22 BUCKETS 3", "CREATE DOCUMENT TYPE T25 EXTENDS T22", "CREATE PROPERTY T22.p STRING",
        "CREATE INDEX ON T22 (p) NOTUNIQUE", "ALTER TYPE T22 NAME T28");

    run("DROP TYPE T25");

    assertThat(database.getSchema().existsType("T25")).isFalse();
    final String expected = "T28{super [], buckets [T28_0, T28_1, T28_2], indexes [T28[p]]}";
    assertThat(schema()).isEqualTo(expected);
    assertEverySubIndexNamesTheTypeOwningItsBucket();

    reopenDatabase();

    assertThat(schema()).isEqualTo(expected);
    assertEverySubIndexNamesTheTypeOwningItsBucket();
  }

  @Test
  void removingTheBucketOfARenamedSubTypeAfterItsIndexedSuperTypeWasRenamed() {
    run("CREATE EDGE TYPE T1", "CREATE EDGE TYPE T2 EXTENDS T1", "ALTER TYPE T2 NAME T4", "CREATE PROPERTY T1.p STRING",
        "CREATE INDEX ON T1 (p) NOTUNIQUE", "ALTER TYPE T1 NAME T10");

    run("ALTER TYPE T4 BUCKET -T4_0");

    final String expected = "T10{super [], buckets [T10_0], indexes [T10[p]]} T4{super [T10], buckets [], indexes []}";
    assertThat(schema()).isEqualTo(expected);
    assertEverySubIndexNamesTheTypeOwningItsBucket();

    reopenDatabase();

    assertThat(schema()).isEqualTo(expected);
    assertEverySubIndexNamesTheTypeOwningItsBucket();
  }

  @Test
  void anIndexedTypeRenamedTwiceLosesItsLastBucketThenItsSubTypeIsDropped() {
    run("CREATE DOCUMENT TYPE T1", "ALTER TYPE T1 NAME T13", "CREATE DOCUMENT TYPE T15 EXTENDS T13", "CREATE PROPERTY T15.p STRING",
        "CREATE INDEX ON T15 (p) NOTUNIQUE", "CREATE DOCUMENT TYPE T64 EXTENDS T15 BUCKETS 4", "ALTER TYPE T15 NAME T65",
        "ALTER TYPE T65 NAME T66", "ALTER TYPE T66 BUCKET -T66_0");

    run("DROP TYPE T64");

    final String expected = "T13{super [], buckets [T13_0], indexes []} T66{super [T13], buckets [], indexes []}";
    assertThat(schema()).isEqualTo(expected);

    reopenDatabase();

    assertThat(schema()).isEqualTo(expected);
  }

  /**
   * The same drop without a rename: the indexed super type has lost its own buckets, so the sub-index of its last sub type is the
   * only one left under the wrapper, and dropping that sub type must take the wrapper with it instead of leaving an empty one that
   * the next schema save fails on.
   */
  @Test
  void droppingTheLastSubTypeOfAnIndexedSuperTypeWithoutBucketsTakesTheEmptyIndexWithIt() {
    run("CREATE DOCUMENT TYPE A", "CREATE DOCUMENT TYPE B EXTENDS A BUCKETS 2", "CREATE PROPERTY A.p STRING",
        "CREATE INDEX ON A (p) NOTUNIQUE", "ALTER TYPE A BUCKET -A_0");

    run("DROP TYPE B");

    final String expected = "A{super [], buckets [], indexes []}";
    assertThat(schema()).isEqualTo(expected);

    reopenDatabase();

    assertThat(schema()).isEqualTo(expected);
  }

  /** The layout the sub-indexes have right after CREATE INDEX is the one the next open gives them. */
  @Test
  void aSubIndexNamesTheTypeOwningItsBucketRightAfterCreateIndex() {
    run("CREATE DOCUMENT TYPE A", "CREATE DOCUMENT TYPE B EXTENDS A", "CREATE DOCUMENT TYPE C EXTENDS B",
        "CREATE PROPERTY A.p STRING", "CREATE INDEX ON A (p) NOTUNIQUE");
    database.transaction(() -> {
      database.newDocument("A").set("p", "a").save();
      database.newDocument("B").set("p", "b").save();
      database.newDocument("C").set("p", "c").save();
    });

    assertEverySubIndexNamesTheTypeOwningItsBucket();
    final String live = layout();

    reopenDatabase();

    assertEverySubIndexNamesTheTypeOwningItsBucket();
    assertThat(layout()).isEqualTo(live);
    assertThat(database.query("sql", "SELECT FROM A WHERE p = 'c'").stream().count()).isEqualTo(1L);
  }

  /** Renaming the indexed super type leaves the files and the names of the sub types' sub-indexes alone, and the data stays indexed. */
  @Test
  void renamingTheIndexedSuperTypeKeepsTheSubTypesIndexed() {
    run("CREATE DOCUMENT TYPE A", "CREATE DOCUMENT TYPE B EXTENDS A", "CREATE PROPERTY A.p STRING",
        "CREATE INDEX ON A (p) NOTUNIQUE");
    database.transaction(() -> {
      database.newDocument("A").set("p", "a").save();
      database.newDocument("B").set("p", "b").save();
    });

    run("ALTER TYPE A NAME A2");

    assertEverySubIndexNamesTheTypeOwningItsBucket();
    assertThat(database.query("sql", "SELECT FROM A2 WHERE p = 'b'").stream().count()).isEqualTo(1L);

    reopenDatabase();

    assertEverySubIndexNamesTheTypeOwningItsBucket();
    assertThat(database.query("sql", "SELECT FROM A2 WHERE p = 'b'").stream().count()).isEqualTo(1L);
    assertThat(database.query("sql", "SELECT FROM B WHERE p = 'b'").stream().count()).isEqualTo(1L);
  }

  private void run(final String... statements) {
    for (final String statement : statements)
      database.command("sql", statement).close();
  }

  /** Types, supertypes, buckets and indexes, in the shape of the report. */
  private String schema() {
    final StringBuilder out = new StringBuilder();
    final List<DocumentType> types = new ArrayList<>(database.getSchema().getTypes());
    types.sort(Comparator.comparing(DocumentType::getName));
    for (final DocumentType type : types) {
      final List<String> superTypes = new ArrayList<>();
      for (final DocumentType superType : type.getSuperTypes())
        superTypes.add(superType.getName());
      final List<String> buckets = new ArrayList<>();
      for (final Bucket bucket : type.getBuckets(false))
        buckets.add(bucket.getName());
      final List<String> indexes = new ArrayList<>();
      for (final TypeIndex index : type.getAllIndexes(false))
        indexes.add(index.getName());
      if (!out.isEmpty())
        out.append(' ');
      out.append(type.getName()).append("{super ").append(superTypes).append(", buckets ").append(buckets).append(", indexes ")
          .append(indexes).append('}');
    }
    return out.toString();
  }

  /** Which type each sub-index names and which file carries it, independent of the generated index names. */
  private String layout() {
    final List<String> rows = new ArrayList<>();
    for (final DocumentType type : database.getSchema().getTypes())
      for (final TypeIndex index : type.getAllIndexes(false))
        for (final IndexInternal subIndex : index.getIndexesOnBuckets())
          rows.add(index.getName() + " bucket " + subIndex.getAssociatedBucketId() + " names " + subIndex.getTypeName());
    rows.sort(Comparator.naturalOrder());
    return String.join("\n", rows);
  }

  /** A sub-index names the type that owns its bucket, and its file is named after the bucket. */
  private void assertEverySubIndexNamesTheTypeOwningItsBucket() {
    for (final DocumentType type : database.getSchema().getTypes())
      for (final TypeIndex index : type.getAllIndexes(false))
        for (final IndexInternal subIndex : index.getIndexesOnBuckets()) {
          final String owner = database.getSchema().getTypeByBucketId(subIndex.getAssociatedBucketId()).getName();
          assertThat(subIndex.getTypeName()).as("type named by the sub-index " + subIndex.getName()).isEqualTo(owner);
          assertThat(((DatabaseInternal) database).getFileManager().getFile(subIndex.getFileId()).getComponentName())
              .as("file of the sub-index " + subIndex.getName()).startsWith(owner + "_");
        }
  }
}
