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
import com.arcadedb.engine.Bucket;
import com.arcadedb.exception.SchemaException;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Issue #10317 (case A): {@code CREATE DOCUMENT TYPE B BUCKET A_0}, where {@code A_0} belongs to {@code A}, was refused, but only
 * after {@code B} had been registered, so {@code B} stayed behind with no bucket, also after a reopen. The bucket is now checked
 * before the type is created, and a refused statement leaves the schema as it was.
 */
class Issue10317CreateTypeTakenBucketTest extends TestHelper {

  @Test
  void createTypeWithAnotherTypesBucketLeavesNoTypeBehind() {
    database.command("sql", "CREATE DOCUMENT TYPE A");

    assertThatThrownBy(() -> database.command("sql", "CREATE DOCUMENT TYPE B BUCKET A_0"))
        .isInstanceOf(SchemaException.class)
        .hasMessageContaining("already associated to the type 'A'");

    assertThat(database.getSchema().existsType("B")).isFalse();
    assertThat(database.getSchema().getType("A").getBuckets(false)).extracting(b -> b.getName()).containsExactly("A_0");

    reopenDatabase();

    assertThat(database.getSchema().existsType("B")).isFalse();
    assertThat(database.getSchema().getType("A").getBuckets(false)).extracting(b -> b.getName()).containsExactly("A_0");
  }

  @Test
  void typeBuilderWithAnotherTypesBucketLeavesNoTypeBehind() {
    final DocumentType a = database.getSchema().createDocumentType("A");

    assertThatThrownBy(() -> database.getSchema().buildDocumentType().withName("B").withBuckets(List.of(a.getBuckets(false).getFirst()))
        .create())
        .isInstanceOf(SchemaException.class);

    assertThat(database.getSchema().existsType("B")).isFalse();
  }

  @Test
  void createTypeReusingABucketNamedLikeItsOwnButOwnedByAnotherLeavesNoTypeBehind() {
    // C_0 is the name CREATE TYPE C would give its first bucket, and it already belongs to A
    database.command("sql", "CREATE BUCKET C_0");
    database.command("sql", "CREATE DOCUMENT TYPE A BUCKET C_0");

    assertThatThrownBy(() -> database.command("sql", "CREATE DOCUMENT TYPE C BUCKETS 1"))
        .isInstanceOf(SchemaException.class)
        .hasMessageContaining("already associated to the type 'A'");

    assertThat(database.getSchema().existsType("C")).isFalse();
  }

  @Test
  void createTypeWithTheSameBucketTwiceLeavesNoTypeBehind() {
    // the second Free is refused by addBucket as "already associated to B", which B only is once the first one is attached
    database.command("sql", "CREATE BUCKET Free");

    assertThatThrownBy(() -> database.command("sql", "CREATE DOCUMENT TYPE B BUCKET Free, Free"))
        .isInstanceOf(SchemaException.class)
        .hasMessageContaining("'Free'");

    assertThat(database.getSchema().existsType("B")).isFalse();
    assertThat(database.getSchema().existsBucket("Free")).isTrue();

    reopenDatabase();

    assertThat(database.getSchema().existsType("B")).isFalse();
    assertThat(database.getSchema().existsBucket("Free")).isTrue();
  }

  @Test
  void typeBuilderWithTheSameBucketTwiceLeavesNoTypeBehind() {
    final Bucket free = database.getSchema().createBucket("Free");

    assertThatThrownBy(() -> database.getSchema().buildDocumentType().withName("B").withBuckets(List.of(free, free)).create())
        .isInstanceOf(SchemaException.class);

    assertThat(database.getSchema().existsType("B")).isFalse();
  }

  @Test
  void growingAnExistingTypeOntoATakenBucketLeavesItAsItWas() {
    // G_1 is free and reused, G_2 belongs to Owner: the refusal must not keep the G_1 attached on the way
    database.command("sql", "CREATE DOCUMENT TYPE G BUCKETS 1");
    database.command("sql", "CREATE BUCKET G_1");
    database.command("sql", "CREATE BUCKET G_2");
    database.command("sql", "CREATE DOCUMENT TYPE Owner BUCKET G_2");

    assertThatThrownBy(() -> database.getSchema().buildDocumentType().withName("G").withIgnoreIfExists(true).withTotalBuckets(3)
        .create())
        .isInstanceOf(SchemaException.class)
        .hasMessageContaining("already associated to the type 'Owner'");

    assertThat(database.getSchema().getType("G").getBuckets(false)).extracting(b -> b.getName()).containsExactly("G_0");
    assertThat(database.getSchema().getType("Owner").getBuckets(false)).extracting(b -> b.getName()).containsExactly("G_2");

    reopenDatabase();

    assertThat(database.getSchema().getType("G").getBuckets(false)).extracting(b -> b.getName()).containsExactly("G_0");
  }

  @Test
  void growingAnExistingTypeOntoFreeBucketsStillWorks() {
    database.command("sql", "CREATE DOCUMENT TYPE G BUCKETS 1");
    database.command("sql", "CREATE BUCKET G_1");

    database.getSchema().buildDocumentType().withName("G").withIgnoreIfExists(true).withTotalBuckets(3).create();

    assertThat(database.getSchema().getType("G").getBuckets(false)).extracting(b -> b.getName())
        .containsExactly("G_0", "G_1", "G_2");
  }

  @Test
  void createTypeWithAFreeBucketStillWorks() {
    database.command("sql", "CREATE BUCKET Free");
    database.command("sql", "CREATE DOCUMENT TYPE B BUCKET Free");

    assertThat(database.getSchema().getType("B").getBuckets(false)).extracting(b -> b.getName()).containsExactly("Free");
  }
}
