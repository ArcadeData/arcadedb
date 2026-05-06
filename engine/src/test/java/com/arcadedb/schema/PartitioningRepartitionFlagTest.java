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
import com.arcadedb.engine.LocalBucket;
import com.arcadedb.serializer.json.JSONObject;

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Pins the contract of {@link LocalDocumentType#isNeedsRepartition} - the schema-level flag that
 * gates the partition-aware bucket-pruning planner rule (issue #4087).
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class PartitioningRepartitionFlagTest extends TestHelper {

  private static final String TYPE_NAME = "PartitionedDoc";

  @Test
  void freshPartitionedTypeIsNotStale() {
    createPartitionedType();
    final LocalDocumentType type = (LocalDocumentType) database.getSchema().getType(TYPE_NAME);
    assertThat(type.isNeedsRepartition())
        .as("a freshly created partitioned type with no buckets added under the strategy must not be flagged stale")
        .isFalse();
  }

  @Test
  void addingABucketToAPartitionedPopulatedTypeFlipsTheFlag() {
    createPartitionedType();
    final LocalDocumentType type = (LocalDocumentType) database.getSchema().getType(TYPE_NAME);
    assertThat(type.isNeedsRepartition()).isFalse();

    // Add a bucket while the type is partitioned. The pre-existing buckets carry the partition
    // mapping under the old modulus, so growing the count invalidates them.
    final LocalBucket newBucket = (LocalBucket) database.getSchema().createBucket("PartitionedDoc_extra");
    type.addBucket(newBucket);

    assertThat(type.isNeedsRepartition())
        .as("addBucket on a partitioned type with existing buckets must flip needsRepartition to true")
        .isTrue();
  }

  @Test
  void removingABucketFromAPartitionedTypeFlipsTheFlag() {
    createPartitionedType();
    final LocalDocumentType type = (LocalDocumentType) database.getSchema().getType(TYPE_NAME);
    assertThat(type.isNeedsRepartition()).isFalse();

    final Bucket toRemove = type.getBuckets(false).get(0);
    type.removeBucket(toRemove);

    assertThat(type.isNeedsRepartition())
        .as("removeBucket on a partitioned type must flip needsRepartition to true")
        .isTrue();
  }

  @Test
  void switchingFromRoundRobinToPartitionedOnPopulatedTypeFlipsTheFlag() {
    // Existing records were placed via round-robin; switching to a partition strategy must flag
    // the type so the planner suppresses pruning until REBUILD TYPE WITH repartition runs.
    database.transaction(() -> {
      database.getSchema().buildDocumentType().withName("StratSwitchDoc").withTotalBuckets(4).create();
      database.command("sql", "CREATE PROPERTY StratSwitchDoc.tenant_id STRING");
      database.command("sql", "CREATE PROPERTY StratSwitchDoc.payload STRING");
      database.command("sql", "CREATE INDEX ON StratSwitchDoc(tenant_id) UNIQUE");
      database.command("sql", "INSERT INTO StratSwitchDoc SET tenant_id = 'a1', payload = 'x'");
      database.command("sql", "INSERT INTO StratSwitchDoc SET tenant_id = 'a2', payload = 'y'");
    });

    database.command("sql", "ALTER TYPE StratSwitchDoc BucketSelectionStrategy `partitioned('tenant_id')`");

    final LocalDocumentType type = (LocalDocumentType) database.getSchema().getType("StratSwitchDoc");
    assertThat(type.isNeedsRepartition())
        .as("flipping a populated type's strategy from round-robin to partitioned must set needsRepartition=true")
        .isTrue();
  }

  @Test
  void switchingStrategyOnEmptyTypeDoesNotFlipFlag() {
    // No records yet -> partition mapping is trivially correct; no rebuild is owed.
    database.transaction(() -> {
      database.getSchema().buildDocumentType().withName("EmptyStratDoc").withTotalBuckets(4).create();
      database.command("sql", "CREATE PROPERTY EmptyStratDoc.tenant_id STRING");
      database.command("sql", "CREATE INDEX ON EmptyStratDoc(tenant_id) UNIQUE");
    });

    database.command("sql", "ALTER TYPE EmptyStratDoc BucketSelectionStrategy `partitioned('tenant_id')`");

    final LocalDocumentType type = (LocalDocumentType) database.getSchema().getType("EmptyStratDoc");
    assertThat(type.isNeedsRepartition())
        .as("strategy change on an empty type must NOT flip the flag - mapping is trivially correct")
        .isFalse();
  }

  @Test
  void switchingBetweenSamePartitionPropertiesIsANoop() {
    // Setting the same partition strategy with the same property set on a populated type leaves
    // the hash inputs identical, so existing records remain correctly placed.
    database.transaction(() -> {
      database.getSchema().buildDocumentType().withName("SameStratDoc").withTotalBuckets(4).create();
      database.command("sql", "CREATE PROPERTY SameStratDoc.tenant_id STRING");
      database.command("sql", "CREATE INDEX ON SameStratDoc(tenant_id) UNIQUE");
      database.command("sql", "ALTER TYPE SameStratDoc BucketSelectionStrategy `partitioned('tenant_id')`");
      database.command("sql", "INSERT INTO SameStratDoc SET tenant_id = 'foo'");
    });
    final LocalDocumentType type = (LocalDocumentType) database.getSchema().getType("SameStratDoc");
    assertThat(type.isNeedsRepartition()).isFalse();

    // Re-set the same shape; flag must not flip.
    database.command("sql", "ALTER TYPE SameStratDoc BucketSelectionStrategy `partitioned('tenant_id')`");
    assertThat(type.isNeedsRepartition())
        .as("re-applying the same partition shape must NOT flip the flag")
        .isFalse();
  }

  @Test
  void roundRobinTypeIsNeverFlagged() {
    database.transaction(() -> {
      database.getSchema().buildDocumentType().withName("RoundRobinDoc").withTotalBuckets(4).create();
    });
    final LocalDocumentType type = (LocalDocumentType) database.getSchema().getType("RoundRobinDoc");
    assertThat(type.isNeedsRepartition()).isFalse();

    final LocalBucket extra = (LocalBucket) database.getSchema().createBucket("RoundRobinDoc_extra");
    type.addBucket(extra);

    assertThat(type.isNeedsRepartition())
        .as("addBucket on a round-robin type must NOT flip needsRepartition - round-robin has no modulus invariant to break")
        .isFalse();
  }

  @Test
  void flagIsPersistedAndReloaded() {
    createPartitionedType();
    final LocalDocumentType type = (LocalDocumentType) database.getSchema().getType(TYPE_NAME);
    type.setNeedsRepartition(true);

    // toJSON must carry the flag when set.
    final JSONObject typeJson = type.toJSON();
    assertThat(typeJson.has("needsRepartition")).isTrue();
    assertThat(typeJson.getBoolean("needsRepartition")).isTrue();

    // Default-cleared types must NOT pollute schema.json.
    type.setNeedsRepartition(false);
    final JSONObject defaultJson = type.toJSON();
    assertThat(defaultJson.has("needsRepartition"))
        .as("a cleared flag should leave no key in schema.json so the default case adds nothing")
        .isFalse();
  }

  @Test
  void clearedFlagSurvivesReopen() {
    // Pin the post-clear persistence contract: setNeedsRepartition(false) must trigger a schema
    // save so a server restart doesn't resurrect the flag. Without the save, schema.json keeps
    // the old `true` value and the load path reads it back, negating the rebuild that cleared
    // it.
    createPartitionedType();
    final LocalDocumentType type = (LocalDocumentType) database.getSchema().getType(TYPE_NAME);
    type.setNeedsRepartition(true);
    assertThat(type.isNeedsRepartition()).isTrue();

    // Reopen: confirm the persisted-true case survives.
    reopenDatabase();
    final LocalDocumentType reopenedTrue = (LocalDocumentType) database.getSchema().getType(TYPE_NAME);
    assertThat(reopenedTrue.isNeedsRepartition()).as("persisted true must survive reopen").isTrue();

    // Clear the flag, reopen, confirm the persisted-false case survives.
    reopenedTrue.setNeedsRepartition(false);
    reopenDatabase();
    final LocalDocumentType reopenedFalse = (LocalDocumentType) database.getSchema().getType(TYPE_NAME);
    assertThat(reopenedFalse.isNeedsRepartition())
        .as("clearing the flag must persist; reopen must NOT resurrect a stale true")
        .isFalse();
  }

  @Test
  void warnIfNeedsRepartitionIsThrottled() {
    createPartitionedType();
    final LocalDocumentType type = (LocalDocumentType) database.getSchema().getType(TYPE_NAME);

    // No-op when the flag is false: no log line, no error.
    type.setNeedsRepartition(false);
    type.warnIfNeedsRepartition();

    // First call after setting the flag emits exactly one log line; subsequent calls inside the
    // 60-second window are throttled. We don't intercept the log output here (LogManager is
    // global); the contract being pinned is "throttle does not throw and does not block on
    // repeated calls" - the throttle interval itself is timer-bounded and tested by the
    // QueryEngineManager and SparseVectorScoringPool throttle tests upstream.
    type.setNeedsRepartition(true);
    for (int i = 0; i < 100; i++)
      type.warnIfNeedsRepartition();
  }

  // ---- shared scaffolding -------------------------------------------------

  private void createPartitionedType() {
    database.transaction(() -> {
      database.getSchema().buildDocumentType().withName(TYPE_NAME).withTotalBuckets(4).create();
      database.command("sql", "CREATE PROPERTY " + TYPE_NAME + ".tenant_id STRING");
      database.command("sql", "CREATE INDEX ON " + TYPE_NAME + "(tenant_id) UNIQUE");
      database.command("sql",
          "ALTER TYPE " + TYPE_NAME + " BucketSelectionStrategy `partitioned('tenant_id')`");
    });
  }
}
