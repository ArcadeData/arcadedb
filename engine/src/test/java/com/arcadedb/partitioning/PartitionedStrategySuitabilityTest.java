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
package com.arcadedb.partitioning;

import com.arcadedb.TestHelper;
import com.arcadedb.database.DatabaseInternal;
import com.arcadedb.database.MutableDocument;
import com.arcadedb.database.RID;
import com.arcadedb.database.bucketselectionstrategy.PartitionedBucketSelectionStrategy;
import com.arcadedb.database.bucketselectionstrategy.RoundRobinBucketSelectionStrategy;
import com.arcadedb.exception.CommandParsingException;
import com.arcadedb.exception.DuplicatedKeyException;
import com.arcadedb.schema.LocalDocumentType;
import com.arcadedb.schema.LocalSchema;
import com.arcadedb.schema.Schema;
import com.arcadedb.serializer.json.JSONArray;
import com.arcadedb.serializer.json.JSONObject;
import com.arcadedb.utility.FileUtils;

import org.junit.jupiter.api.Test;

import java.io.File;
import java.io.FileWriter;
import java.math.BigDecimal;
import java.time.Instant;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.util.Date;
import java.util.List;

import static com.arcadedb.log.WarningCapture.captureWarnings;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Issue #5603, the follow-ups to #5589 and #5595. Both of those fixed the read side of
 * {@code PartitionedBucketSelectionStrategy}: a lookup that could not reach the bucket placement chose learned to
 * decline and fan out. What was left is that nothing told you when you configured a type into a state where that is
 * the only possible answer - the strategy attached without complaint and the cost surfaced much later, as an
 * unexplained slowdown or, worse, as duplicates in a UNIQUE index.
 * <p>
 * The duplicates are the part that had not been measured. On a partitioned type a UNIQUE index is a set of per-bucket
 * sub-indexes, so the constraint is global only while every record carrying one index key lands in one bucket. Where
 * the hash placement uses disagrees with the way the index compares keys, each bucket accepts its own copy. Three
 * declared types did exactly that, and every one of them is proven here against a round-robin control that rejects
 * the duplicate every time:
 * <ul>
 *   <li>{@code BINARY} - a {@code byte[]} hashes by identity, so every write of the same bytes drew a fresh bucket;</li>
 *   <li>{@code DECIMAL} - {@code 1.1} and {@code 1.10} are one index key but two hash codes;</li>
 *   <li>{@code DATETIME} under a zone-carrying implementation - only the instant reaches disk, so the writer's zone
 *       is hashed at placement and gone on the way back.</li>
 * </ul>
 * Such a configuration is now refused when it is assigned, and merely warned about when an existing database is read
 * back in one - a refusal at load would turn a slow database into an unopenable one.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class PartitionedStrategySuitabilityTest extends TestHelper {

  private static final int BUCKETS = 3;

  /**
   * The date/time implementation is a runtime setting, not part of the schema: switching it mid-test is how an
   * already-partitioned type is driven into - and out of - a state its partition key cannot survive.
   */
  private void useDateTimeImplementation(final Class<?> implementation) {
    try {
      ((DatabaseInternal) database).getSerializer().setDateTimeImplementation(implementation);
    } catch (final ClassNotFoundException e) {
      throw new IllegalStateException(e);
    }
  }

  private void createIndexedType(final String typeName, final String propertyType) {
    database.transaction(() -> {
      database.getSchema().buildDocumentType().withName(typeName).withTotalBuckets(BUCKETS).create();
      database.command("sql", "CREATE PROPERTY " + typeName + ".k " + propertyType);
      database.command("sql", "CREATE INDEX ON " + typeName + "(k) UNIQUE");
    });
  }

  private void partition(final String typeName) {
    database.transaction(
        () -> database.command("sql", "ALTER TYPE " + typeName + " BucketSelectionStrategy `partitioned('k')`"));
  }

  /**
   * Number of the given values the UNIQUE index let through. Anything above 1 is a broken constraint.
   * <p>
   * Only a {@link DuplicatedKeyException} counts as the constraint holding, and every other failure propagates.
   * Swallowing all of them would let an unrelated error be tallied as a rejection, which is the difference between
   * "the constraint held" and "nothing was written for some other reason" - and it would make every count in this
   * class meaningless in exactly the direction that reads as success.
   */
  private int insertAll(final String typeName, final Object... values) {
    int admitted = 0;
    for (final Object value : values) {
      try {
        database.transaction(() -> database.newDocument(typeName).set("k", value).save());
        admitted++;
      } catch (final DuplicatedKeyException e) {
        // THE CONSTRAINT HELD
      }
    }
    return admitted;
  }

  /** Six writes of the same bytes. Distinct instances, so identity hashing scatters them across the buckets. */
  private static Object[] sameBytesSixTimes() {
    final Object[] values = new Object[6];
    for (int i = 0; i < values.length; i++)
      values[i] = new byte[] { 1, 2, 3, 4 };
    return values;
  }

  /** One instant, spelled in six zones. The index sees one key; {@code ZonedDateTime.hashCode} sees six. */
  private static Object[] oneInstantInSixZones() {
    final ZonedDateTime base = ZonedDateTime.of(2020, 9, 13, 14, 26, 40, 0, ZoneId.of("Europe/Rome"));
    final String[] zones = { "Europe/Rome", "Asia/Tokyo", "America/New_York", "UTC", "Australia/Sydney", "Africa/Cairo" };
    final Object[] values = new Object[zones.length];
    for (int i = 0; i < zones.length; i++)
      values[i] = base.withZoneSameInstant(ZoneId.of(zones[i]));
    return values;
  }

  /** Four spellings of one number. {@code BigDecimal.hashCode} folds in the scale; the index compares by value. */
  private static Object[] oneNumberInFourScales() {
    return new Object[] { new BigDecimal("1.1"), new BigDecimal("1.10"), new BigDecimal("1.100"),
        new BigDecimal("1.1000") };
  }

  // ---------------------------------------------------------------------------------------------------------------
  // The configuration is refused where pruning could never happen.
  // ---------------------------------------------------------------------------------------------------------------

  @Test
  void aBinaryPartitionKeyIsRefused() {
    createIndexedType("BinPart", "BINARY");
    assertThatThrownBy(() -> partition("BinPart"))
        .as("a byte[] hashes by identity, so it can never resolve a bucket")
        .hasMessageContaining("BINARY")
        .hasMessageContaining("UNIQUE");
  }

  @Test
  void aDecimalPartitionKeyIsRefused() {
    createIndexedType("DecPart", "DECIMAL");
    assertThatThrownBy(() -> partition("DecPart"))
        .as("BigDecimal hashes the scale the index ignores")
        .hasMessageContaining("DECIMAL");
  }

  /**
   * Every datetime precision, not only the base type. {@code Type.getJavaImplementation} resolves the configured
   * implementation for {@code DATE} and {@code DATETIME} alone and answers the static {@code LocalDateTime} default
   * for the three precision subtypes, while the deserializer honours the configured class for all four - so asking it
   * for the read-back class reported every subtype zone-free and let the configuration through.
   */
  @Test
  void aZoneCarryingDateTimeImplementationIsRefusedAtEveryPrecision() {
    useDateTimeImplementation(ZonedDateTime.class);

    for (final String propertyType : new String[] { "DATETIME", "DATETIME_SECOND", "DATETIME_MICROS",
        "DATETIME_NANOS" }) {
      final String typeName = "Zoned" + propertyType;
      createIndexedType(typeName, propertyType);
      assertThatThrownBy(() -> partition(typeName))
          .as("%s: only the instant is stored, so the writer's zone cannot be recovered", propertyType)
          .hasMessageContaining(propertyType);
    }
  }

  @Test
  void aCaseInsensitivePartitionIndexIsRefused() {
    database.transaction(() -> {
      database.getSchema().buildDocumentType().withName("CiPart").withTotalBuckets(BUCKETS).create();
      database.command("sql", "CREATE PROPERTY CiPart.k STRING");
      database.command("sql", "CREATE INDEX ON CiPart (k COLLATE CI) UNIQUE");
    });
    assertThatThrownBy(() -> partition("CiPart"))
        .as("case folding is an index-level normalisation placement never applies")
        .hasMessageContaining("COLLATE CI");
  }

  /**
   * A composite partition is only as prunable as its worst component: one unsuitable property in the set is enough,
   * because placement sums the per-value hashes and the sum is no more reproducible than any term in it. The
   * suitable component must not mask it.
   */
  @Test
  void oneUnsuitableComponentRefusesTheWholeCompositePartition() {
    database.transaction(() -> {
      database.getSchema().buildDocumentType().withName("Composite").withTotalBuckets(BUCKETS).create();
      database.command("sql", "CREATE PROPERTY Composite.a STRING");
      database.command("sql", "CREATE PROPERTY Composite.b DECIMAL");
      database.command("sql", "CREATE INDEX ON Composite(a,b) UNIQUE");
    });

    assertThatThrownBy(() -> database.transaction(() -> database.command("sql",
        "ALTER TYPE Composite BucketSelectionStrategy `partitioned('a','b')`")))
        .as("the DECIMAL component alone makes the composite partition unprunable")
        .hasMessageContaining("DECIMAL")
        .hasMessageContaining("'b'");
  }

  /**
   * The composite counterpart of the fan-out advisory, and the case {@code coversPartitionProperties} exists for: an
   * index on a strict SUBSET of the partition properties hashes fewer values than placement did, so it cannot be
   * pruned to one bucket either. Accepted, and reported.
   */
  @Test
  void anIndexOnPartOfACompositePartitionIsWarnedAbout() {
    database.transaction(() -> {
      database.getSchema().buildDocumentType().withName("CompositeSubset").withTotalBuckets(BUCKETS).create();
      database.command("sql", "CREATE PROPERTY CompositeSubset.a STRING");
      database.command("sql", "CREATE PROPERTY CompositeSubset.b STRING");
      database.command("sql", "CREATE INDEX ON CompositeSubset(a,b) UNIQUE");
      database.command("sql", "CREATE INDEX ON CompositeSubset(a) NOTUNIQUE");
    });

    final List<String> warnings = captureWarnings(() -> database.transaction(() -> database.command("sql",
        "ALTER TYPE CompositeSubset BucketSelectionStrategy `partitioned('a','b')`")));

    assertThat(database.getSchema().getType("CompositeSubset").getBucketSelectionStrategy().getName())
        .as("a partial index is a warning, never a refusal")
        .isEqualTo("partitioned");
    assertThat(warnings).anyMatch(m -> m.contains("CompositeSubset") && m.contains("[a]"));

    // And the runtime guard agrees with the diagnosis: a key covering only part of the partition declines to prune.
    final LocalDocumentType type = (LocalDocumentType) database.getSchema().getType("CompositeSubset");
    final PartitionedBucketSelectionStrategy strategy =
        (PartitionedBucketSelectionStrategy) type.getBucketSelectionStrategy();
    assertThat(strategy.getBucketIdByKeys(List.of("a"), new Object[] { "acme" }, false))
        .as("a partial key hashes fewer values than placement did")
        .isEqualTo(-1);
    assertThat(strategy.getBucketIdByKeys(List.of("a", "b"), new Object[] { "acme", "eu" }, false))
        .as("but the full composite key still prunes")
        .isNotEqualTo(-1);
  }

  /**
   * A refused assignment must leave nothing behind. The strategy is stored on the type before it is validated - it
   * has to be, so it can read back the type it is being bound to - so a refusal that does not put the previous one
   * back leaves the type running the very strategy the DDL just rejected.
   */
  @Test
  void aRefusedAssignmentLeavesThePreviousStrategyInPlace() {
    createIndexedType("Rollback", "BINARY");
    // A CommandParsingException subtype, which is what the HTTP layer maps to 400 - a refusal is a client-side DDL
    // mistake, and a CommandExecutionException here would be answered 500. Pinned end to end by
    // PartitionedStrategyRefusalHttpTest in the server module.
    assertThatThrownBy(() -> partition("Rollback")).isInstanceOf(CommandParsingException.class);

    assertThat(database.getSchema().getType("Rollback").getBucketSelectionStrategy().getName())
        .isEqualTo(new RoundRobinBucketSelectionStrategy().getName());

    // And the type is still usable: placement falls back to round-robin, so the UNIQUE index is global again.
    assertThat(insertAll("Rollback", sameBytesSixTimes())).isEqualTo(1);
  }

  /**
   * A zone-free implementation round-trips unchanged, so the very same {@code DATETIME} partition key that the test
   * above refuses is accepted here. Pins that the refusal keys on the configured implementation and not on the
   * declared type, which is what makes it recoverable by changing a setting rather than the schema.
   */
  @Test
  void aZoneFreeDateTimeImplementationIsAccepted() {
    useDateTimeImplementation(Date.class);
    createIndexedType("DatePart", "DATETIME");
    partition("DatePart");

    assertThat(database.getSchema().getType("DatePart").getBucketSelectionStrategy().getName()).isEqualTo("partitioned");
  }

  // ---------------------------------------------------------------------------------------------------------------
  // Everything the refusal is meant to prevent, measured against a round-robin control.
  // ---------------------------------------------------------------------------------------------------------------

  /**
   * The end-to-end proof, on the one unsuitable state that is still reachable after the refusal: the date/time
   * implementation is a runtime setting, so a type partitioned on a {@code DATETIME} key while it was
   * {@code java.util.Date} keeps that strategy after the setting is changed to a zone-carrying class. Before the fix
   * the pruned uniqueness check looked in one bucket and admitted 3 of the 6 writes of a single instant.
   */
  @Test
  void aUniqueIndexHoldsAfterTheDateTimeImplementationTurnsZoneCarrying() {
    useDateTimeImplementation(Date.class);
    createIndexedType("ZoneDrift", "DATETIME");
    partition("ZoneDrift");

    useDateTimeImplementation(ZonedDateTime.class);

    assertThat(insertAll("ZoneDrift", oneInstantInSixZones()))
        .as("six spellings of one instant against a UNIQUE index")
        .isEqualTo(1);
    assertThat(database.countType("ZoneDrift", false)).isEqualTo(1);
  }

  /**
   * The same proof one precision down. Before the read-back class was resolved for the subtypes, a
   * {@code DATETIME_NANOS} partition key under {@code ZonedDateTime} was accepted outright - no runtime setting had
   * to change to get there - and admitted 3 of these 6 writes of a single instant.
   */
  @Test
  void aUniqueIndexHoldsOnASubPrecisionDateTimePartitionKey() {
    useDateTimeImplementation(Date.class);
    createIndexedType("NanoDrift", "DATETIME_NANOS");
    partition("NanoDrift");

    useDateTimeImplementation(ZonedDateTime.class);

    assertThat(insertAll("NanoDrift", oneInstantInSixZones()))
        .as("six spellings of one instant against a UNIQUE DATETIME_NANOS index")
        .isEqualTo(1);
  }

  /** Control for the above: the same six writes on a type that never prunes. */
  @Test
  void aRoundRobinTypeRejectsTheSameInstantInEveryZone() {
    useDateTimeImplementation(ZonedDateTime.class);
    createIndexedType("ZoneControl", "DATETIME");

    assertThat(insertAll("ZoneControl", oneInstantInSixZones())).isEqualTo(1);
  }

  /** Control: a UNIQUE index on a plain type has always caught these; only the partitioned path let them through. */
  @Test
  void aRoundRobinTypeRejectsRepeatedBytesAndRescaledDecimals() {
    createIndexedType("BinControl", "BINARY");
    assertThat(insertAll("BinControl", sameBytesSixTimes())).isEqualTo(1);

    createIndexedType("DecControl", "DECIMAL");
    assertThat(insertAll("DecControl", oneNumberInFourScales())).isEqualTo(1);
  }

  /**
   * A database created before the fix carries the refused configuration in its {@code schema.json}. It must still
   * open - refusing at load would make a slow database unopenable - and, once open, the UNIQUE index it was silently
   * violating must hold again, because the strategy declines to prune and the check fans out.
   * <p>
   * The strategy is injected straight into the persisted schema because there is no longer any way to ask for it:
   * that is the point of the refusal, and it is exactly the state an upgraded database arrives in.
   */
  @Test
  void anAlreadyPartitionedBinaryTypeStillOpensAndRegainsItsUniqueConstraint() throws Exception {
    createIndexedType("LegacyBin", "BINARY");

    final File schemaFile = ((LocalSchema) database.getSchema().getEmbedded()).getConfigurationFile();
    database.close();

    final JSONObject schemaJson = new JSONObject(FileUtils.readFileAsString(schemaFile));
    schemaJson.getJSONObject("types").getJSONObject("LegacyBin").put("bucketSelectionStrategy",
        new JSONObject().put("name", "partitioned").put("properties", new JSONArray(List.of("k"))));
    try (final FileWriter writer = new FileWriter(schemaFile)) {
      writer.write(schemaJson.toString());
    }

    final List<String> warnings = captureWarnings(() -> database = factory.open());

    assertThat(database.getSchema().getType("LegacyBin").getBucketSelectionStrategy().getName())
        .as("the persisted strategy is kept, not silently swapped out from under the records")
        .isEqualTo("partitioned");
    assertThat(warnings)
        .as("opening a database in a state that can never prune must say so")
        .anyMatch(m -> m.contains("LegacyBin") && m.contains("BINARY"));

    assertThat(insertAll("LegacyBin", sameBytesSixTimes()))
        .as("the UNIQUE index holds again because the pruned lookup gave way to a fan-out")
        .isEqualTo(1);
  }

  // ---------------------------------------------------------------------------------------------------------------
  // Warnings, which never refuse.
  // ---------------------------------------------------------------------------------------------------------------

  /**
   * A second index on properties the partition does not cover cannot be pruned (issue #5589) and fans out. That is a
   * perfectly reasonable schema, just one that gets less out of the partitioning than the wording suggests, so it is
   * reported and accepted.
   */
  @Test
  void aSecondIndexOnOtherPropertiesIsWarnedAboutAndAccepted() {
    database.transaction(() -> {
      database.getSchema().buildDocumentType().withName("TwoIdx").withTotalBuckets(BUCKETS).create();
      database.command("sql", "CREATE PROPERTY TwoIdx.k STRING");
      database.command("sql", "CREATE PROPERTY TwoIdx.code STRING");
      database.command("sql", "CREATE INDEX ON TwoIdx(k) UNIQUE");
      database.command("sql", "CREATE INDEX ON TwoIdx(code) UNIQUE");
    });

    final List<String> warnings = captureWarnings(() -> partition("TwoIdx"));

    assertThat(database.getSchema().getType("TwoIdx").getBucketSelectionStrategy().getName()).isEqualTo("partitioned");
    assertThat(warnings).anyMatch(m -> m.contains("TwoIdx") && m.contains("code"));
  }

  /** A partition whose only index is its own must not draw the fan-out warning. */
  @Test
  void aSingleIndexPartitionIsWarningFree() {
    createIndexedType("OneIdx", "STRING");

    assertThat(captureWarnings(() -> partition("OneIdx"))).noneMatch(m -> m.contains("OneIdx"));
  }

  /**
   * The fan-out warning is advice about the shape just chosen, so it is said once, at assignment, and never again on
   * reopening a database whose schema has not changed. Unconditional it would be a WARNING per startup, forever,
   * against a schema that was accepted and works as designed - the kind of line operators learn to filter out, which
   * would take the blocker warnings with it.
   */
  @Test
  void theFanOutWarningIsNotRepeatedOnEveryReopen() throws Exception {
    database.transaction(() -> {
      database.getSchema().buildDocumentType().withName("ReopenIdx").withTotalBuckets(BUCKETS).create();
      database.command("sql", "CREATE PROPERTY ReopenIdx.k STRING");
      database.command("sql", "CREATE PROPERTY ReopenIdx.code STRING");
      database.command("sql", "CREATE INDEX ON ReopenIdx(k) UNIQUE");
      database.command("sql", "CREATE INDEX ON ReopenIdx(code) UNIQUE");
    });
    partition("ReopenIdx");
    database.close();

    final List<String> warnings = captureWarnings(() -> database = factory.open());

    assertThat(warnings).as("an accepted schema must not warn again on every open").noneMatch(m -> m.contains("ReopenIdx"));
    assertThat(database.getSchema().getType("ReopenIdx").getBucketSelectionStrategy().getName()).isEqualTo("partitioned");
  }

  // ---------------------------------------------------------------------------------------------------------------
  // The DDL says what actually went wrong.
  // ---------------------------------------------------------------------------------------------------------------

  /**
   * {@code ALTER TYPE ... BucketSelectionStrategy} used to rewrite every failure as "was not found", which sent the
   * user hunting for a typo in a name that was perfectly valid.
   */
  @Test
  void aMissingUniqueIndexReportsWhyRatherThanClaimingTheStrategyDoesNotExist() {
    database.transaction(() -> {
      database.getSchema().buildDocumentType().withName("NoIdx").withTotalBuckets(BUCKETS).create();
      database.command("sql", "CREATE PROPERTY NoIdx.k STRING");
    });

    assertThatThrownBy(() -> partition("NoIdx"))
        .hasMessageContaining("unique automatic index")
        .hasMessageNotContaining("was not found");
  }

  /** ...while a genuinely unknown implementation still says it cannot be found. */
  @Test
  void anUnknownStrategyStillReportsThatItCannotBeFound() {
    database.transaction(() -> database.getSchema().buildDocumentType().withName("Unknown").create());

    assertThatThrownBy(() -> database.transaction(
        () -> database.command("sql", "ALTER TYPE Unknown BucketSelectionStrategy `no.such.Strategy`")))
        .hasMessageContaining("Cannot find bucket selection strategy class");
  }

  // ---------------------------------------------------------------------------------------------------------------
  // The suitable case keeps pruning: without this, a regression to an unconditional -1 would pass every test above.
  // ---------------------------------------------------------------------------------------------------------------

  @Test
  void aSuitablePartitionStillPrunesToTheBucketPlacementChose() {
    createIndexedType("Good", "STRING");
    partition("Good");
    database.transaction(() -> database.newDocument("Good").set("k", "acme").save());

    final LocalDocumentType type = (LocalDocumentType) database.getSchema().getType("Good");
    final PartitionedBucketSelectionStrategy strategy =
        (PartitionedBucketSelectionStrategy) type.getBucketSelectionStrategy();

    final int lookupBucket = strategy.getBucketIdByKeys(List.of("k"), new Object[] { "acme" }, false);
    assertThat(lookupBucket).as("a suitable partition must still prune").isNotEqualTo(-1);
    assertThat(type.getBucketIdByRecord(database.iterateType("Good", false).next().asDocument(), false).getFileId())
        .as("and prune to the bucket the record was actually placed in")
        .isEqualTo(type.getBuckets(false).get(lookupBucket).getFileId());
  }

  /** {@link PartitionedBucketSelectionStrategy#checkSuitability()} is the single source both reactions read. */
  @Test
  void checkSuitabilityNamesEveryBlockerAndWarning() {
    database.transaction(() -> {
      database.getSchema().buildDocumentType().withName("Diag").withTotalBuckets(BUCKETS).create();
      database.command("sql", "CREATE PROPERTY Diag.k DECIMAL");
      database.command("sql", "CREATE PROPERTY Diag.code STRING");
      database.command("sql", "CREATE INDEX ON Diag(k) UNIQUE");
      database.command("sql", "CREATE INDEX ON Diag(code) UNIQUE");
    });

    final PartitionedBucketSelectionStrategy strategy = new PartitionedBucketSelectionStrategy(List.of("k"));
    strategy.setType((LocalDocumentType) database.getSchema().getType("Diag"));

    final PartitionedBucketSelectionStrategy.Suitability suitability = strategy.checkSuitability();
    assertThat(suitability.isUsable()).isFalse();
    assertThat(suitability.blockers()).singleElement().asString().contains("DECIMAL");
    assertThat(suitability.warnings()).singleElement().asString().contains("code");

    assertThat(strategy.getBucketIdByKeys(List.of("k"), new Object[] { new BigDecimal("1.1") }, false))
        .as("a blocked partition declines to prune, whatever the key")
        .isEqualTo(-1);
  }

  // ---------------------------------------------------------------------------------------------------------------
  // Placement has to survive a round trip through disk, or a rebuild would relocate correctly placed records.
  // ---------------------------------------------------------------------------------------------------------------

  /**
   * {@code getBucketIdByRecord} runs on a freshly built {@code MutableDocument} when a record is inserted, and again
   * on one deserialized from disk when {@code REBUILD TYPE ... WITH repartition = true} decides whether to move it.
   * If those two disagree the rebuild relocates records that were correctly placed, and every subsequent lookup - which
   * hashes the in-memory form - misses them.
   * <p>
   * Issue #5603 asked specifically about a {@code DATETIME} key under a non-default {@code dateTimeImplementation},
   * on the theory that a sub-millisecond value kept its extra digits in memory and lost them on disk. It does not:
   * {@code MutableDocument.convertValueToSchemaType} truncates to the declared precision on the way in, so both sides
   * see the same truncated value - which is why {@code Instant} with nanoseconds is in the matrix below rather than
   * in a fix. The types that genuinely did drift are the ones the suitability check now refuses.
   */
  @Test
  void placementSurvivesARoundTripThroughDiskForEveryPrunableKeyType() {
    useDateTimeImplementation(Instant.class);

    assertPlacementSurvivesRoundTrip("RtString", "STRING", "acme");
    assertPlacementSurvivesRoundTrip("RtLong", "LONG", -8589934592L);
    assertPlacementSurvivesRoundTrip("RtInteger", "INTEGER", -12345);
    assertPlacementSurvivesRoundTrip("RtShort", "SHORT", (short) -12);
    assertPlacementSurvivesRoundTrip("RtByte", "BYTE", (byte) 7);
    assertPlacementSurvivesRoundTrip("RtFloat", "FLOAT", -1.5f);
    assertPlacementSurvivesRoundTrip("RtDouble", "DOUBLE", -1.5d);
    assertPlacementSurvivesRoundTrip("RtBoolean", "BOOLEAN", Boolean.TRUE);
    // The exact case the issue proposed: sub-millisecond precision on a millisecond-precision DATETIME property.
    assertPlacementSurvivesRoundTrip("RtInstant", "DATETIME", Instant.ofEpochSecond(1600000000L, 123456789));
    assertPlacementSurvivesRoundTrip("RtInstantNanos", "DATETIME_NANOS", Instant.ofEpochSecond(1600000000L, 123456789));
  }

  private void assertPlacementSurvivesRoundTrip(final String typeName, final String propertyType, final Object value) {
    createIndexedType(typeName, propertyType);
    partition(typeName);

    final RID[] rid = new RID[1];
    database.transaction(() -> {
      final MutableDocument document = database.newDocument(typeName).set("k", value);
      document.save();
      rid[0] = document.getIdentity();
    });

    // A fresh transaction, so the record is deserialized rather than served from the writing transaction's cache.
    database.transaction(() -> assertThat(database.getSchema().getType(typeName)
        .getBucketIdByRecord(database.lookupByRID(rid[0], true).asDocument(), false).getFileId())
        .as("%s placement recomputed from the record as it was read back", propertyType)
        .isEqualTo(rid[0].getBucketId()));
  }

  /**
   * Issue #5603 item 2 asked whether an UNDECLARED partition property could place {@code 5} and {@code 5L} in
   * different buckets, since neither side has a conversion target. It cannot be reached: the strategy demands a
   * unique automatic index on the partition properties, and an index cannot be created on a property that does not
   * exist. Pinned here so that loosening either of those two rules fails loudly rather than reopening the hole - the
   * strategy's own blocker for an undeclared property stays as the backstop.
   */
  @Test
  void anUndeclaredPartitionPropertyCannotBeIndexedAndThereforeCannotBePartitionedOn() {
    database.transaction(() -> database.getSchema().buildDocumentType().withName("Undeclared")
        .withTotalBuckets(BUCKETS).create());

    assertThatThrownBy(() -> database.transaction(() -> database.getSchema()
        .createTypeIndex(Schema.INDEX_TYPE.LSM_TREE, true, "Undeclared", "k")))
        .as("a unique index is the gate the partition strategy depends on")
        .hasMessageContaining("the property does not exist");

    assertThatThrownBy(() -> partition("Undeclared")).hasMessageContaining("unique automatic index");
  }

  @Override
  protected String getDatabasePath() {
    return "target/databases/PartitionedStrategySuitabilityTest";
  }

}
