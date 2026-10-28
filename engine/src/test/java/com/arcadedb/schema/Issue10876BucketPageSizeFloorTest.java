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

import com.arcadedb.GlobalConfiguration;
import com.arcadedb.TestHelper;
import com.arcadedb.database.MutableDocument;
import com.arcadedb.database.RID;
import com.arcadedb.engine.Bucket;
import com.arcadedb.engine.LocalBucket;
import com.arcadedb.exception.SchemaException;
import com.arcadedb.query.sql.executor.ResultSet;

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Issue #10876: a bucket page smaller than its content header plus one head chunk was accepted by {@code ALTER DATABASE}
 * and by the bucket creation, and every insert into a type created afterwards failed inside the page writer with an
 * offset that named no setting.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class Issue10876BucketPageSizeFloorTest extends TestHelper {

  private static final String BUCKET_SETTING   = GlobalConfiguration.BUCKET_DEFAULT_PAGE_SIZE.getKey();
  private static final String EXTERNAL_SETTING = GlobalConfiguration.EXTERNAL_PROPERTY_BUCKET_DEFAULT_PAGE_SIZE.getKey();

  @Test
  void theMinimumIsDerivedFromTheBucketFormatAndStatedBySettings() {
    // page header (8) + record count and slot table (2 + slots * 4) + the head chunk of a record (14)
    assertThat(LocalBucket.minimumPageSize(LocalBucket.CURRENT_VERSION)).isEqualTo(8 + 2 + 2048 * 4 + 14);
    assertThat(LocalBucket.minimumPageSize(LocalBucket.EXTERNAL_BUCKET_VERSION)).isEqualTo(8 + 2 + 256 * 4 + 14);

    assertThat(GlobalConfiguration.BUCKET_DEFAULT_PAGE_SIZE.getMinimum()).isEqualTo(
        LocalBucket.minimumPageSize(LocalBucket.CURRENT_VERSION));
    assertThat(GlobalConfiguration.EXTERNAL_PROPERTY_BUCKET_DEFAULT_PAGE_SIZE.getMinimum()).isEqualTo(
        LocalBucket.minimumPageSize(LocalBucket.EXTERNAL_BUCKET_VERSION));
    // The description is what an operator reads: it must state the minimum the check enforces
    assertThat(GlobalConfiguration.BUCKET_DEFAULT_PAGE_SIZE.getDescription()).contains(
        "Minimum is " + GlobalConfiguration.BUCKET_DEFAULT_PAGE_SIZE.getMinimum() + " bytes");
    assertThat(GlobalConfiguration.EXTERNAL_PROPERTY_BUCKET_DEFAULT_PAGE_SIZE.getDescription()).contains(
        "Minimum is " + GlobalConfiguration.EXTERNAL_PROPERTY_BUCKET_DEFAULT_PAGE_SIZE.getMinimum() + " bytes");
  }

  @Test
  void alterDatabaseRefusesAPageSizeBelowTheMinimum() {
    final int minimum = LocalBucket.minimumPageSize(LocalBucket.CURRENT_VERSION);
    final Object before = database.getConfiguration().getValue(GlobalConfiguration.BUCKET_DEFAULT_PAGE_SIZE);

    // 8192 is the minimum of an index page, and still below a bucket's
    for (final int tooSmall : new int[] { 4096, 8192, minimum - 1 })
      assertThatThrownBy(() -> database.command("sql", "ALTER DATABASE `" + BUCKET_SETTING + "` " + tooSmall).close())
          .hasMessageContaining(BUCKET_SETTING).hasMessageContaining("Minimum is " + minimum);
    assertThat((Object) database.getConfiguration().getValue(GlobalConfiguration.BUCKET_DEFAULT_PAGE_SIZE)).isEqualTo(before);

    assertThatThrownBy(() -> database.command("sql", "ALTER DATABASE `" + EXTERNAL_SETTING + "` 1024").close())
        .hasMessageContaining(EXTERNAL_SETTING);
  }

  @Test
  void aTypeCreatedAtTheMinimumPageSizeStoresAndGrowsRecords() {
    final int minimum = LocalBucket.minimumPageSize(LocalBucket.CURRENT_VERSION);
    try (final ResultSet rs = database.command("sql", "ALTER DATABASE `" + BUCKET_SETTING + "` " + minimum)) {
      assertThat(rs.next().<Object>getProperty("newValue")).isEqualTo(minimum);
    }
    database.command("sql", "CREATE DOCUMENT TYPE Smallest").close();
    assertThat(database.getSchema().getType("Smallest").getBuckets(false).getFirst()).isInstanceOf(LocalBucket.class);
    assertThat(((LocalBucket) database.getSchema().getType("Smallest").getBuckets(false).getFirst()).getPageSize()).isEqualTo(minimum);

    final RID[] rids = new RID[2];
    database.transaction(() -> {
      rids[0] = database.newDocument("Smallest").set("v", "x").save().getIdentity();
      rids[1] = database.newDocument("Smallest").set("v", "y".repeat(200)).save().getIdentity();
    });
    database.transaction(() -> {
      final MutableDocument grown = database.lookupByRID(rids[0], true).asDocument().modify();
      grown.set("v", "z".repeat(300)).save();
    });

    assertThat(database.lookupByRID(rids[0], true).asDocument().getString("v")).isEqualTo("z".repeat(300));
    assertThat(database.lookupByRID(rids[1], true).asDocument().getString("v")).isEqualTo("y".repeat(200));
  }

  @Test
  void aBucketBelowTheMinimumIsRefusedWhereverItsPageSizeComesFrom() {
    // Through the Java API the setting is a plain map put, so the creation is the check that always runs
    final Object previous = database.getConfiguration().getValue(GlobalConfiguration.BUCKET_DEFAULT_PAGE_SIZE);
    database.getConfiguration().setValue(GlobalConfiguration.BUCKET_DEFAULT_PAGE_SIZE, 4096);
    try {
      assertThatThrownBy(() -> database.getSchema().createDocumentType("FromTheSetting")).isInstanceOf(SchemaException.class)
          .hasMessageContaining("4096").hasMessageContaining(BUCKET_SETTING);
      // Refused before anything was created: no half-built type, no bucket file
      assertThat(database.getSchema().existsType("FromTheSetting")).isFalse();
      assertThat(database.getSchema().existsBucket("FromTheSetting_0")).isFalse();

      assertThatThrownBy(() -> database.getSchema().createBucket("Loose")).isInstanceOf(SchemaException.class)
          .hasMessageContaining("Loose");
      assertThat(database.getSchema().existsBucket("Loose")).isFalse();
    } finally {
      database.getConfiguration().setValue(GlobalConfiguration.BUCKET_DEFAULT_PAGE_SIZE, previous);
    }

    assertThatThrownBy(() -> database.command("sql", "CREATE DOCUMENT TYPE FromTheStatement PAGESIZE 4096").close())
        .hasMessageContaining("4096");
    assertThat(database.getSchema().existsType("FromTheStatement")).isFalse();

    // A type growing its bucket count is refused before its first new bucket, and keeps the buckets it had
    database.getSchema().createDocumentType("Growing", 1);
    assertThatThrownBy(() -> database.getSchema().buildDocumentType().withName("Growing").withTotalBuckets(3).withPageSize(4096)
        .withIgnoreIfExists(true).create()).isInstanceOf(SchemaException.class);
    assertThat(database.getSchema().getType("Growing").getBuckets(false)).hasSize(1);
    assertThat(database.getSchema().existsBucket("Growing_1")).isFalse();
  }

  @Test
  void anExternalPropertyBucketBelowItsMinimumIsRefused() {
    database.command("sql", "CREATE DOCUMENT TYPE Heavy").close();
    database.command("sql", "CREATE PROPERTY Heavy.blob STRING").close();
    final Object previous = database.getConfiguration().getValue(GlobalConfiguration.EXTERNAL_PROPERTY_BUCKET_DEFAULT_PAGE_SIZE);
    database.getConfiguration().setValue(GlobalConfiguration.EXTERNAL_PROPERTY_BUCKET_DEFAULT_PAGE_SIZE, 1024);
    try {
      assertThatThrownBy(() -> database.command("sql", "ALTER PROPERTY Heavy.blob EXTERNAL true").close())
          .hasMessageContaining(EXTERNAL_SETTING);
    } finally {
      database.getConfiguration().setValue(GlobalConfiguration.EXTERNAL_PROPERTY_BUCKET_DEFAULT_PAGE_SIZE, previous);
    }

    final int minimum = LocalBucket.minimumPageSize(LocalBucket.EXTERNAL_BUCKET_VERSION);
    database.command("sql", "ALTER DATABASE `" + EXTERNAL_SETTING + "` " + minimum).close();
    database.command("sql", "ALTER PROPERTY Heavy.blob EXTERNAL true").close();
    // At the minimum a page holds one byte of a chunk past the head, so a few hundred bytes already span hundreds of chunks
    final RID[] rid = new RID[1];
    database.transaction(() -> rid[0] = database.newDocument("Heavy").set("blob", "b".repeat(300)).save().getIdentity());
    assertThat(database.lookupByRID(rid[0], true).asDocument().getString("blob")).isEqualTo("b".repeat(300));
  }

  @Test
  void aBucketAddedToATypeWithExternalPropertiesIsRefusedWhenItsPairCannotBeCreated() {
    database.command("sql", "CREATE DOCUMENT TYPE Paired BUCKETS 1").close();
    database.command("sql", "CREATE PROPERTY Paired.blob STRING").close();
    database.command("sql", "ALTER PROPERTY Paired.blob EXTERNAL true").close();
    final DocumentType type = database.getSchema().getType("Paired");
    final Bucket loose = database.getSchema().createBucket("Loose");

    final Object previous = database.getConfiguration().getValue(GlobalConfiguration.EXTERNAL_PROPERTY_BUCKET_DEFAULT_PAGE_SIZE);
    database.getConfiguration().setValue(GlobalConfiguration.EXTERNAL_PROPERTY_BUCKET_DEFAULT_PAGE_SIZE, 1024);
    try {
      // Growing the type: refused before the primary bucket is created, so no bucket is left without its pair
      assertThatThrownBy(() -> database.getSchema().buildDocumentType().withName("Paired").withTotalBuckets(2)
          .withIgnoreIfExists(true).create()).isInstanceOf(SchemaException.class).hasMessageContaining(EXTERNAL_SETTING);
      assertThat(type.getBuckets(false)).hasSize(1);
      assertThat(database.getSchema().existsBucket("Paired_1")).isFalse();

      // Attaching a bucket that exists: refused before it is attached
      assertThatThrownBy(() -> type.addBucket(loose)).isInstanceOf(SchemaException.class).hasMessageContaining(EXTERNAL_SETTING);
      assertThat(type.getBuckets(false)).hasSize(1);
      assertThat(database.getSchema().getTypeByBucketId(loose.getFileId())).isNull();
    } finally {
      database.getConfiguration().setValue(GlobalConfiguration.EXTERNAL_PROPERTY_BUCKET_DEFAULT_PAGE_SIZE, previous);
    }

    // With a page size the pair can have, both work and every primary bucket has its pair
    database.getSchema().buildDocumentType().withName("Paired").withTotalBuckets(2).withIgnoreIfExists(true).create();
    type.addBucket(loose);
    assertThat(type.getBuckets(false)).hasSize(3);
    for (final Bucket bucket : type.getBuckets(false))
      assertThat(((LocalDocumentType) type).getExternalBucketIdFor(bucket.getFileId())).isNotNull();
  }
}
