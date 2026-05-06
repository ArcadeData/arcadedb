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
import com.arcadedb.partitioning.PartitioningTestFixture;
import com.arcadedb.query.sql.executor.Result;
import com.arcadedb.query.sql.executor.ResultSet;
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
    populate();
    final LocalDocumentType type = (LocalDocumentType) database.getSchema().getType(TYPE_NAME);
    assertThat(type.isNeedsRepartition()).isFalse();

    // Add a bucket while the type is partitioned and populated. The pre-existing buckets carry
    // the partition mapping under the old modulus, so growing the count invalidates them.
    final LocalBucket newBucket = (LocalBucket) database.getSchema().createBucket("PartitionedDoc_extra");
    type.addBucket(newBucket);

    assertThat(type.isNeedsRepartition())
        .as("addBucket on a populated partitioned type must flip needsRepartition to true")
        .isTrue();
  }

  @Test
  void addingABucketToAPartitionedEmptyTypeDoesNotFlipTheFlag() {
    // No records means the modulus invariant has nothing to invalidate. Flagging would prompt a
    // no-op rebuild and a needless schema save.
    createPartitionedType();
    final LocalDocumentType type = (LocalDocumentType) database.getSchema().getType(TYPE_NAME);
    assertThat(type.isNeedsRepartition()).isFalse();

    final LocalBucket newBucket = (LocalBucket) database.getSchema().createBucket("PartitionedDoc_extra");
    type.addBucket(newBucket);

    assertThat(type.isNeedsRepartition())
        .as("addBucket on an empty partitioned type must NOT flip needsRepartition - nothing to repartition")
        .isFalse();
  }

  @Test
  void removingABucketFromAPartitionedPopulatedTypeFlipsTheFlag() {
    createPartitionedType();
    populate();
    final LocalDocumentType type = (LocalDocumentType) database.getSchema().getType(TYPE_NAME);
    assertThat(type.isNeedsRepartition()).isFalse();

    // Pick a bucket guaranteed to be empty so removeBucket doesn't fail validation. We added an
    // extra empty bucket above so we always have at least one removable target regardless of how
    // populate() distributed records across the original buckets.
    final LocalBucket emptyExtra = (LocalBucket) database.getSchema().createBucket("PartitionedDoc_remove");
    type.addBucket(emptyExtra);
    // Re-clear in case adding the bucket flipped the flag (the partitioned + populated guard
    // would set it). We only want to observe the removeBucket transition.
    type.setNeedsRepartition(false);

    type.removeBucket(emptyExtra);

    assertThat(type.isNeedsRepartition())
        .as("removeBucket on a populated partitioned type must flip needsRepartition to true")
        .isTrue();
  }

  @Test
  void removingABucketFromAPartitionedEmptyTypeDoesNotFlipTheFlag() {
    // Symmetric to addBucket: an empty type has nothing to repartition.
    createPartitionedType();
    final LocalDocumentType type = (LocalDocumentType) database.getSchema().getType(TYPE_NAME);
    final LocalBucket extra = (LocalBucket) database.getSchema().createBucket("PartitionedDoc_remove2");
    type.addBucket(extra);
    type.setNeedsRepartition(false);

    type.removeBucket(extra);

    assertThat(type.isNeedsRepartition())
        .as("removeBucket on an empty partitioned type must NOT flip needsRepartition")
        .isFalse();
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
  void selectFromSchemaTypesExposesNeedsRepartition() {
    // FetchFromSchemaTypesStep is the canonical SQL surface for schema metadata; remote clients
    // reload their RemoteDocumentType from this output, so the property must be on every row
    // (regardless of whether the flag is set) so a remote consumer never silently sees the
    // default-false fallback when the server actually has it true.
    createPartitionedType();
    final LocalDocumentType type = (LocalDocumentType) database.getSchema().getType(TYPE_NAME);
    type.setNeedsRepartition(true);

    boolean foundOurType = false;
    try (final ResultSet rs = database.query("sql", "SELECT FROM schema:types WHERE name = '" + TYPE_NAME + "'")) {
      while (rs.hasNext()) {
        final Result row = rs.next();
        assertThat(row.<String>getProperty("name")).isEqualTo(TYPE_NAME);
        assertThat(row.hasProperty("needsRepartition"))
            .as("FetchFromSchemaTypesStep must surface needsRepartition unconditionally so remote "
                + "schema reloads pick it up")
            .isTrue();
        assertThat(row.<Boolean>getProperty("needsRepartition")).isTrue();
        foundOurType = true;
      }
    }
    assertThat(foundOurType).isTrue();

    // Cleared flag also surfaces (false), not omitted.
    type.setNeedsRepartition(false);
    try (final ResultSet rs = database.query("sql", "SELECT FROM schema:types WHERE name = '" + TYPE_NAME + "'")) {
      assertThat(rs.hasNext()).isTrue();
      final Result row = rs.next();
      assertThat(row.<Boolean>getProperty("needsRepartition")).isFalse();
    }
  }

  @Test
  void warnIfNeedsRepartitionIsThrottled() {
    createPartitionedType();
    final LocalDocumentType type = (LocalDocumentType) database.getSchema().getType(TYPE_NAME);

    // No-op when the flag is false: timestamp must NOT advance. Reading the throttle field is the
    // only proxy we have for "did the warn fire" without intercepting the global LogManager.
    type.setNeedsRepartition(false);
    type.setLastRepartitionWarnMsForTesting(0L);
    type.warnIfNeedsRepartition();
    assertThat(type.lastRepartitionWarnMsForTesting())
        .as("warnIfNeedsRepartition with flag=false must not advance the throttle timestamp")
        .isZero();

    // First call after setting the flag advances the timestamp; subsequent calls inside the
    // 60-second window must NOT advance it again. If the throttle were removed, every iteration
    // would CAS a fresh System.currentTimeMillis() and the timestamp would change at least once
    // across 100 rapid calls.
    type.setNeedsRepartition(true);
    type.setLastRepartitionWarnMsForTesting(0L);

    type.warnIfNeedsRepartition();
    final long firstStamp = type.lastRepartitionWarnMsForTesting();
    assertThat(firstStamp)
        .as("first warn after flag=true must advance the throttle timestamp from 0")
        .isPositive();

    for (int i = 0; i < 100; i++)
      type.warnIfNeedsRepartition();
    assertThat(type.lastRepartitionWarnMsForTesting())
        .as("100 rapid warns inside the throttle window must not advance the timestamp - "
            + "removing the throttle would make at least one CAS succeed and change this value")
        .isEqualTo(firstStamp);

    // Simulate the window expiring by rewinding the throttle past the 60-second boundary. The
    // next call must advance the timestamp again, proving the throttle releases on schedule
    // without blocking the test on a real 60-second sleep.
    type.setLastRepartitionWarnMsForTesting(System.currentTimeMillis() - 120_000L);
    type.warnIfNeedsRepartition();
    assertThat(type.lastRepartitionWarnMsForTesting())
        .as("warn after the throttle window has elapsed must advance the timestamp")
        .isGreaterThan(firstStamp);
  }

  // ---- shared scaffolding -------------------------------------------------

  private void createPartitionedType() {
    PartitioningTestFixture.createPartitionedDocType(database, TYPE_NAME, 4, false);
  }

  private void populate() {
    PartitioningTestFixture.populateDocs(database, TYPE_NAME, false);
  }
}
