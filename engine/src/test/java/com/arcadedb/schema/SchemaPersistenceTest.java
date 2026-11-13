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
import com.arcadedb.database.Database;
import com.arcadedb.database.DatabaseContext;
import com.arcadedb.database.DatabaseFactory;
import com.arcadedb.database.DatabaseInternal;
import com.arcadedb.database.MutableDocument;
import com.arcadedb.database.TransactionContext;
import com.arcadedb.engine.Bucket;
import com.arcadedb.engine.LocalBucket;
import com.arcadedb.exception.DuplicatedKeyException;
import com.arcadedb.exception.SchemaException;
import com.arcadedb.query.sql.executor.ResultSet;
import com.arcadedb.serializer.json.JSONObject;
import com.arcadedb.utility.FileUtils;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.mockito.MockedStatic;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileWriter;
import java.io.IOException;
import java.nio.channels.FileChannel;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.ZoneId;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Locale;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.Mockito.mockStatic;

/**
 * Schema regression tests: schema persistence.
 *
 * <p>One nested class per fixed issue, each keeping its own fixture.</p>
 */
class SchemaPersistenceTest {
  /**
   * Regression for #8040: statistics format version.
   *
   * Regression test for issue #8040: the record counters persisted in {@code statistics.json} by any build up to and
   * including 26.9.1 may have been double-folded on a Raft replay (#7126). The fix for that (#7162) stops NEW drift,
   * but a counter already wrong on disk is restored verbatim on the next open and never self-corrects, so the release
   * that carries the fix would have carried the wrong numbers forward with it.
   * <p>
   * The file therefore declares a format version. Counts written without one - which is every file written before this
   * change, and the pre-25.2.1 {@code cached-count.json} - are dropped on load, and the bucket recomputes on its next
   * {@code count()}. The marker is written back on the next clean close, so the recount is paid exactly once.
   *
   * @author Luca Garulli (l.garulli@arcadedata.com)
   */
  @Nested
  class StatisticsFormatVersion extends TestHelper {

    private static final String TYPE    = "Counted";
    private static final int    RECORDS = 25;

    @Override
    protected void beginTest() {
      database.getSchema().createDocumentType(TYPE, 1);
      database.transaction(() -> {
        for (int i = 0; i < RECORDS; i++)
          database.newDocument(TYPE).set("id", i).save();
      });
      // Materialize the counter so there is something for the close to persist.
      assertThat(bucket().count()).isEqualTo(RECORDS);
    }

    @Test
    void aVersionedStatisticsFileKeepsItsCachedCounts() throws IOException {
      reopenDatabase();

      // Written by this build, so the marker is there and the counter is restored without a scan.
      assertThat(statisticsFile().getInt(LocalSchema.STATISTICS_FORMAT_VERSION_KEY, 0))
          .isEqualTo(LocalSchema.STATISTICS_FORMAT_VERSION);
      assertThat(bucket().getCachedRecordCount()).isEqualTo(RECORDS);
    }

    @Test
    void anUnversionedStatisticsFileHasItsCachedCountsDropped() throws IOException {
      database.close();

      // Exactly what a 26.9.1 close leaves behind: the per-bucket entries, no marker. The count is deliberately a
      // wrong one, so a build that trusted the file would answer it and this test would see the drift #8040 reports.
      final JSONObject json = statisticsFile();
      json.remove(LocalSchema.STATISTICS_FORMAT_VERSION_KEY);
      for (final String key : json.keySet())
        json.getJSONObject(key).put("count", RECORDS + 7);
      writeStatisticsFile(json);

      database = factory.open();

      // Not trusted, so not restored ...
      assertThat(bucket().getCachedRecordCount()).isEqualTo(-1L);
      // ... and count(*) answers from the authoritative scan rather than from the wrong number on disk.
      assertThat(bucket().count()).isEqualTo(RECORDS);

      // The recount is paid once: the close below writes the marker, and the next open trusts the file again.
      reopenDatabase();
      assertThat(bucket().getCachedRecordCount()).isEqualTo(RECORDS);
    }

    @Test
    void theLegacyCachedCountFileIsNeverTrusted() throws IOException {
      final String bucketName = bucket().getName();
      database.close();

      final File dir = new File(getDatabasePath());
      Files.delete(new File(dir, LocalSchema.STATISTICS_FILE_NAME).toPath());
      // The <v25.2.1 shape: a flat bucketName -> count map, and older than every build that could have been right.
      try (final FileWriter writer = new FileWriter(new File(dir, LocalSchema.CACHED_COUNT_FILE_NAME_LEGACY))) {
        writer.write(new JSONObject().put(bucketName, RECORDS + 7).toString());
      }

      database = factory.open();

      assertThat(bucket().getCachedRecordCount()).isEqualTo(-1L);
      assertThat(bucket().count()).isEqualTo(RECORDS);
    }

    private LocalBucket bucket() {
      return (LocalBucket) database.getSchema().getType(TYPE).getBuckets(false).getFirst();
    }

    private JSONObject statisticsFile() throws IOException {
      final File file = new File(getDatabasePath(), LocalSchema.STATISTICS_FILE_NAME);
      try (final FileInputStream fis = new FileInputStream(file)) {
        return new JSONObject(FileUtils.readStreamAsString(fis, LocalSchema.DEFAULT_ENCODING));
      }
    }

    private void writeStatisticsFile(final JSONObject json) throws IOException {
      try (final FileWriter writer = new FileWriter(new File(getDatabasePath(), LocalSchema.STATISTICS_FILE_NAME))) {
        writer.write(json.toString());
      }
    }
  }

  /**
   * Regression for #8152: legacy watermark migration.
   *
   * https://github.com/ArcadeData/arcadedb/issues/8152 - the upgrade path (found by CodeRabbit).
   * <p>
   * A schema written before the watermark fix carries a {@code watermarkTs} of 0 and no {@code watermarkSet}
   * property, and because the defect pinned EVERY watermark at 0 that describes every continuous aggregate that ever
   * ran on an affected version. Two things follow from that one ambiguous number:
   * <ul>
   *   <li>its backing type already holds one full copy of the aggregate per refresh that has happened, and</li>
   *   <li>reading it back as "never refreshed" - the only reading that is safe in general - makes the next refresh
   *   re-aggregate the whole source range and append it on top, adding one more copy before the repaired watermark
   *   takes hold.</li>
   * </ul>
   * So the ambiguous legacy zero is treated as what it is, a database carrying the defect's duplicates, and the next
   * refresh rebuilds the backing type from empty exactly once. That repairs the existing duplication rather than
   * merely declining to add to it. A legacy watermark that is NOT zero did advance, so its refreshes deleted before
   * recomputing and it is healthy - it must not be rebuilt.
   *
   * @author Luca Garulli (l.garulli@arcadedata.com)
   */
  @Nested
  class LegacyWatermarkMigration extends TestHelper {

    private static final long HOUR = 3_600_000L;

    @Test
    void anAmbiguousLegacyZeroWatermarkRebuildsTheBackingTypeOnce() {
      final ContinuousAggregateImpl ca = createAggregate();

      // What the defect leaves behind: extra full copies of the aggregate, one per refresh that ran.
      // Written as the refresh that had the defect wrote them: nothing else may write a backing type (issue #11020)
      try (final BackingTypeWriteScope ignored = BackingTypeWriteScope.open(ca)) {
        duplicateBackingRows();
        duplicateBackingRows();
      }
      assertThat(countBackingRows()).isEqualTo(8);

      final ContinuousAggregateImpl legacy = reloadAsLegacySchema(ca, 0);
      assertThat(legacy.needsCleanRebuild()).isTrue();
      assertThat(legacy.isWatermarkSet()).isFalse();

      ContinuousAggregateRefresher.incrementalRefresh(database, legacy);

      // Rebuilt from empty: one row per (sensor, bucket), and the watermark is now real.
      assertThat(countBackingRows()).isEqualTo(2);
      assertThat(legacy.isWatermarkSet()).isTrue();
      assertThat(legacy.getWatermarkTs()).isEqualTo(HOUR);
      assertThat(legacy.needsCleanRebuild()).isFalse();

      // And the repair does not repeat: a second refresh is an ordinary incremental one.
      ContinuousAggregateRefresher.incrementalRefresh(database, legacy);
      assertThat(countBackingRows()).isEqualTo(2);
    }

    @Test
    void aLegacyNonZeroWatermarkIsHealthyAndIsNotRebuilt() {
      final ContinuousAggregateImpl ca = createAggregate();

      // Its watermark advanced, so its refreshes deleted before recomputing: nothing to repair.
      final ContinuousAggregateImpl legacy = reloadAsLegacySchema(ca, HOUR);
      assertThat(legacy.needsCleanRebuild()).isFalse();
      assertThat(legacy.isWatermarkSet()).isTrue();
      assertThat(legacy.getWatermarkTs()).isEqualTo(HOUR);
    }

    /**
     * A schema this version wrote carries the flag, so it is never mistaken for a legacy one - not even when its
     * watermark is a perfectly legitimate 0.
     */
    @Test
    void anAggregateAnchoredAtTheEpochIsNotMistakenForALegacyOne() {
      database.command("sql",
          "CREATE TIMESERIES TYPE SensorReading TIMESTAMP ts TAGS (sensor_id STRING) FIELDS (temperature DOUBLE)");
      database.transaction(() -> database.command("sql",
          "INSERT INTO SensorReading SET ts = 100, sensor_id = 'A', temperature = 20.0"));
      final ContinuousAggregateImpl ca = (ContinuousAggregateImpl) database.getSchema().buildContinuousAggregate()
          .withName("hourly_temps")
          .withQuery("SELECT sensor_id, ts.timeBucket('1h', ts) AS hour, avg(temperature) AS avg_temp "
              + "FROM SensorReading GROUP BY sensor_id, hour")
          .create();

      assertThat(ca.getWatermarkTs()).isEqualTo(0L);
      final JSONObject json = ca.toJSON();
      assertThat(json.getBoolean("watermarkSet", false)).isTrue();

      final ContinuousAggregateImpl reloaded = ContinuousAggregateImpl.fromJSON(database, json);
      assertThat(reloaded.needsCleanRebuild()).isFalse();
      assertThat(reloaded.isWatermarkSet()).isTrue();
    }

    private ContinuousAggregateImpl createAggregate() {
      database.command("sql",
          "CREATE TIMESERIES TYPE SensorReading TIMESTAMP ts TAGS (sensor_id STRING) FIELDS (temperature DOUBLE)");
      database.transaction(() -> {
        database.command("sql", "INSERT INTO SensorReading SET ts = 0, sensor_id = 'A', temperature = 20.0");
        database.command("sql", "INSERT INTO SensorReading SET ts = ?, sensor_id = 'A', temperature = 25.0", HOUR);
      });
      return (ContinuousAggregateImpl) database.getSchema().buildContinuousAggregate()
          .withName("hourly_temps")
          .withQuery("SELECT sensor_id, ts.timeBucket('1h', ts) AS hour, avg(temperature) AS avg_temp "
              + "FROM SensorReading GROUP BY sensor_id, hour")
          .create();
    }

    /** The schema a version carrying the defect would have written: no flag, and the watermark it was stuck at. */
    private ContinuousAggregateImpl reloadAsLegacySchema(final ContinuousAggregateImpl ca, final long watermarkTs) {
      final JSONObject legacy = ca.toJSON();
      legacy.remove("watermarkSet");
      legacy.put("watermarkTs", watermarkTs);
      return ContinuousAggregateImpl.fromJSON(database, legacy);
    }

    /** Appends another copy of every backing row, which is exactly what a refresh used to do. */
    private void duplicateBackingRows() {
      database.transaction(() -> {
        try (final ResultSet rs = database.query("sql", "SELECT FROM hourly_temps")) {
          rs.stream().toList().forEach(row -> {
            final var doc = database.newDocument("hourly_temps");
            row.getPropertyNames().stream().filter(p -> !p.startsWith("@")).forEach(p -> doc.set(p, row.getProperty(p)));
            doc.save();
          });
        }
      });
    }

    private long countBackingRows() {
      // count(@rid), not count(*): the latter answers from a cached counter rather than scanning, and this test is
      // about rows that should not be there.
      try (final ResultSet rs = database.query("sql", "SELECT count(@rid) AS c FROM hourly_temps")) {
        return rs.next().<Number>getProperty("c").longValue();
      }
    }
  }

  /**
   * Regression for #8206: schema serialization order.
   *
   * The schema file must not depend on the history of the in-memory maps it is serialised from (issue #8206).
   * <p>
   * A Raft leader builds its type map through DDL - growing it, and never shrinking it when a type is dropped - while a
   * follower rebuilds a fresh map from the schema file on every replicated schema entry. Both are hash maps, whose
   * iteration order depends on their table capacity, so the two replicas serialised the same types in different orders
   * and {@code RaftReplicationChangeSchemaIT} saw "different" schema files. A reopen walks exactly the follower's path
   * (fresh maps filled from the file), so a schema written before and after a reopen must be the same string.
   */
  @Nested
  class SchemaSerializationOrder extends TestHelper {
    private static final int CREATED = 40;
    private static final int KEPT    = 5;

    @Test
    void typesSerializeInTheSameOrderAfterDropAndReload() {
      // Grow the type map well past its initial capacity, then shrink it back: a hash map keeps the large table.
      for (int i = 0; i < CREATED; i++)
        database.getSchema().createVertexType("Type" + i);
      for (int i = KEPT; i < CREATED; i++)
        database.getSchema().dropType("Type" + i);
      database.getSchema().createVertexType("TypeAfterDrop");

      assertSameSchemaAfterReopen();
      assertThat(keysOf(schemaJson().getJSONObject("types"))).isSorted();
    }

    @Test
    void propertiesAndCustomValuesSerializeInTheSameOrderAfterDropAndReload() {
      final DocumentType type = database.getSchema().createDocumentType("Wide");
      for (int i = 0; i < CREATED; i++) {
        type.createProperty("prop" + i, String.class);
        type.setCustomValue("custom" + i, i);
      }
      for (int i = KEPT; i < CREATED; i++) {
        type.dropProperty("prop" + i);
        type.setCustomValue("custom" + i, null);
      }
      type.createProperty("propAfterDrop", String.class);

      final Property kept = type.getProperty("prop0");
      for (int i = 0; i < CREATED; i++)
        kept.setCustomValue("propertyCustom" + i, i);
      for (int i = KEPT; i < CREATED; i++)
        kept.setCustomValue("propertyCustom" + i, null);

      assertSameSchemaAfterReopen();
      final JSONObject wide = schemaJson().getJSONObject("types").getJSONObject("Wide");
      assertThat(keysOf(wide.getJSONObject("properties"))).isSorted();
      assertThat(keysOf(wide.getJSONObject("custom"))).isSorted();
      assertThat(keysOf(wide.getJSONObject("properties").getJSONObject("prop0").getJSONObject("custom"))).isSorted();
    }

    @Test
    void indexesSerializeInTheSameOrderAfterDropAndReload() {
      final DocumentType type = database.getSchema().createDocumentType("Indexed");
      final String[] indexNames = new String[CREATED];
      for (int i = 0; i < CREATED; i++) {
        type.createProperty("prop" + i, Integer.class);
        indexNames[i] = type.createTypeIndex(Schema.INDEX_TYPE.LSM_TREE, false, "prop" + i).getName();
      }
      for (int i = KEPT; i < CREATED; i++)
        database.getSchema().dropIndex(indexNames[i]);

      assertSameSchemaAfterReopen();
      assertThat(keysOf(schemaJson().getJSONObject("types").getJSONObject("Indexed").getJSONObject("indexes"))).isSorted();
    }

    @Test
    void triggersAndFunctionsSerializeInTheSameOrderAfterDropAndReload() {
      database.getSchema().createDocumentType("Triggered");
      for (int i = 0; i < CREATED; i++) {
        database.command("sql", "CREATE TRIGGER trigger" + i + " BEFORE CREATE ON TYPE Triggered EXECUTE SQL 'SELECT 1'");
        database.command("sql", "DEFINE FUNCTION lib" + i + ".f 'SELECT 1 AS result' LANGUAGE sql");
      }
      for (int i = KEPT; i < CREATED; i++)
        database.getSchema().unregisterFunctionLibrary("lib" + i);
      for (int i = KEPT; i < CREATED; i++)
        database.getSchema().dropTrigger("trigger" + i);

      assertSameSchemaAfterReopen();
      assertThat(keysOf(schemaJson().getJSONObject("triggers"))).isSorted();
      assertThat(keysOf(schemaJson().getJSONObject("functions"))).isSorted();
    }

    // Issue #8924: a type's alias set is an immutable Set.copyOf() whose iteration order is salted per JVM, so the array
    // must be written sorted or two nodes holding the same aliases render different schema.json bytes.
    @Test
    void aliasesSerializeSorted() {
      final DocumentType type = database.getSchema().createDocumentType("Aliased");
      final Set<String> aliases = new HashSet<>();
      for (int i = 0; i < CREATED; i++)
        aliases.add("Alias" + (CREATED - i));
      type.setAliases(aliases);

      final List<String> written = schemaJson().getJSONObject("types").getJSONObject("Aliased").getJSONArray("aliases")
          .toListOfStrings();
      assertThat(written).containsExactlyInAnyOrderElementsOf(aliases);
      assertThat(written).isSorted();
      assertSameSchemaAfterReopen();
    }

    private void assertSameSchemaAfterReopen() {
      final String before = normalized();
      reopenDatabase();
      assertThat(normalized()).isEqualTo(before);
    }

    private JSONObject schemaJson() {
      return database.getSchema().getEmbedded().toJSON();
    }

    // schemaVersion is a per-node save counter, not schema content; a reload legitimately changes it.
    private String normalized() {
      final JSONObject json = schemaJson();
      json.remove("schemaVersion");
      return json.toString();
    }

    private static List<String> keysOf(final JSONObject json) {
      return new ArrayList<>(json.keySet());
    }
  }

  /**
   * Regression for #8635: schema save once per ddl.
   *
   * Issue #8635: every DDL statement rewrote {@code schema.json} several times - five for a type, two for a property
   * or an index - and each rewrite is an fsync'd atomic publication, so on a real disk a type with one property and one
   * index cost about 150 ms. Wrapping the DDL in a transaction did not help as much as it should have, because the
   * internal transactions a DDL runs (the dictionary entry for a new name, the index build) committed on their own and
   * saved the schema on the way out, outer transaction or not.
   * <p>
   * The number counted here is {@link LocalSchema#getVersion()}, which moves once per write of {@code schema.json} and
   * never otherwise, so the assertions are exact and independent of the disk the test runs on.
   *
   * @author Luca Garulli (l.garulli@arcadedata.com)
   */
  @Nested
  class SchemaSaveOncePerDdl extends TestHelper {

    @Override
    protected void beginTest() {
      // aSchemaWriteForcesTheDatabaseDirectoryOnce counts directory fsyncs, which the unit-test lanes turn off
      // (arcadedb.fileSync)
      GlobalConfiguration.FILE_SYNC.setValue(true);
    }

    @Test
    void createDocumentTypeWritesTheSchemaOnce() {
      assertThat(schemaWritesOf(() -> database.getSchema().createDocumentType("Doc"))).isEqualTo(1);
      assertThat(schemaWritesOf(() -> database.getSchema().buildDocumentType().withName("Doc4").withTotalBuckets(4).create()))
          .isEqualTo(1);
    }

    @Test
    void createVertexAndEdgeTypeWriteTheSchemaOnce() {
      assertThat(schemaWritesOf(() -> database.getSchema().createVertexType("V1"))).isEqualTo(1);
      assertThat(schemaWritesOf(() -> database.getSchema().createEdgeType("E1"))).isEqualTo(1);
    }

    @Test
    void createPropertyWritesTheSchemaOnce() {
      final DocumentType type = database.getSchema().createDocumentType("Doc");
      // A NAME THE DICTIONARY HAS NEVER SEEN: ITS ENTRY IS COMMITTED BY AN INTERNAL TRANSACTION, WHICH USED TO SAVE TOO
      assertThat(schemaWritesOf(() -> type.createProperty("neverSeenBefore", Type.LONG))).isEqualTo(1);
    }

    @Test
    void createTypeIndexWritesTheSchemaOnce() {
      database.getSchema().buildDocumentType().withName("Doc").withTotalBuckets(3).create().createProperty("k", Type.LONG);
      assertThat(schemaWritesOf(
          () -> database.getSchema().createTypeIndex(Schema.INDEX_TYPE.LSM_TREE, false, "Doc", "k"))).isEqualTo(1);
    }

    @Test
    void createTypeIndexOnAPopulatedTypeWritesTheSchemaOnce() {
      database.getSchema().buildDocumentType().withName("Doc").withTotalBuckets(3).create().createProperty("k", Type.LONG);
      database.transaction(() -> {
        for (int i = 0; i < 300; i++)
          database.newDocument("Doc").set("k", i).save();
      });

      assertThat(schemaWritesOf(
          () -> database.getSchema().createTypeIndex(Schema.INDEX_TYPE.LSM_TREE, true, "Doc", "k"))).isEqualTo(1);
      assertThat(database.query("sql", "select from Doc where k = 150").stream().count()).isEqualTo(1);
    }

    @Test
    void sqlDdlStatementsWriteTheSchemaOnceEach() {
      assertThat(schemaWritesOf(() -> database.command("sql", "CREATE DOCUMENT TYPE SqlDoc"))).isEqualTo(1);
      assertThat(schemaWritesOf(() -> database.command("sql", "CREATE PROPERTY SqlDoc.k LONG"))).isEqualTo(1);
      assertThat(schemaWritesOf(() -> database.command("sql", "CREATE INDEX ON SqlDoc (k) NOTUNIQUE"))).isEqualTo(1);
      assertThat(schemaWritesOf(() -> database.command("sql", "CREATE VERTEX TYPE SqlVertex"))).isEqualTo(1);
      assertThat(schemaWritesOf(() -> database.command("sql", "CREATE EDGE TYPE SqlEdge"))).isEqualTo(1);
    }

    @Test
    void ddlScriptWritesTheSchemaOnce() {
      // A PURE-DDL SCRIPT RUNS AS ONE SCHEMA SESSION (#6990), SO THE WHOLE SCRIPT IS ONE WRITE
      assertThat(schemaWritesOf(() -> database.command("sqlscript", """
          CREATE DOCUMENT TYPE S1;
          CREATE PROPERTY S1.k LONG;
          CREATE INDEX ON S1 (k) NOTUNIQUE;
          CREATE DOCUMENT TYPE S2;
          CREATE PROPERTY S2.k LONG;
          CREATE INDEX ON S2 (k) NOTUNIQUE;
          """))).isEqualTo(1);
    }

    @Test
    void ddlInsideATransactionWritesTheSchemaOnceAtCommit() {
      final LocalSchema schema = (LocalSchema) database.getSchema().getEmbedded();

      database.begin();
      final long before = schema.getVersion();
      for (int i = 0; i < 10; i++) {
        final DocumentType type = database.getSchema().createDocumentType("T" + i);
        type.createProperty("k" + i, Type.LONG);
        database.getSchema().createTypeIndex(Schema.INDEX_TYPE.LSM_TREE, false, "T" + i, "k" + i);
      }
      // NOTHING WRITTEN WHILE THE TRANSACTION IS OPEN: NOT BY THE DDL, NOT BY THE INTERNAL TRANSACTIONS IT RAN
      assertThat(schema.getVersion()).isEqualTo(before);
      assertThat(schema.isDirty()).isTrue();

      database.commit();
      assertThat(schema.getVersion()).isEqualTo(before + 1);
      assertThat(schema.isDirty()).isFalse();

      // AND THE ONE WRITE HOLDS ALL OF IT
      reopenDatabase();
      for (int i = 0; i < 10; i++) {
        assertThat(database.getSchema().existsType("T" + i)).isTrue();
        assertThat(database.getSchema().getType("T" + i).existsProperty("k" + i)).isTrue();
        assertThat(database.getSchema().getType("T" + i).getAllIndexes(false)).hasSize(1);
      }
    }

    @Test
    void bulkChangeWritesTheSchemaOnce() {
      assertThat(schemaWritesOf(() -> database.getSchema().bulkChange(() -> {
        for (int i = 0; i < 5; i++) {
          database.getSchema().createDocumentType("B" + i).createProperty("k", Type.LONG);
          database.getSchema().createTypeIndex(Schema.INDEX_TYPE.LSM_TREE, false, "B" + i, "k");
        }
      }))).isEqualTo(1);
    }

    @Test
    void ddlInsideARolledBackTransactionIsWrittenAtTheRollback() {
      final LocalSchema schema = (LocalSchema) database.getSchema().getEmbedded();

      database.begin();
      final long before = schema.getVersion();
      database.getSchema().createDocumentType("Doc").createProperty("k", Type.LONG);
      assertThat(schema.getVersion()).isEqualTo(before);

      // A SCHEMA CHANGE IS NOT TRANSACTIONAL: THE TYPE AND ITS BUCKET FILE STAND AFTER THE ROLLBACK, SO THE FILE HAS TO
      // SAY SO NOW, NOT WHENEVER THE NEXT UNRELATED SCHEMA CHANGE OR THE CLOSE HAPPENS TO WRITE IT
      database.rollback();
      assertThat(schema.getVersion()).isEqualTo(before + 1);
      assertThat(schema.isDirty()).isFalse();

      reopenDatabase();
      assertThat(database.getSchema().getType("Doc").existsProperty("k")).isTrue();
    }

    @Test
    void aFailedDdlStillWritesWhatItsNestedStepsApplied() {
      // TWO BUCKETS, THE SAME KEY IN EACH: THE UNIQUE INDEX IS CREATED AND BUILT BUCKET BY BUCKET, EACH IN A TRANSACTION
      // OF ITS OWN, AND THE SECOND ONE FAILS. THE STEPS BEFORE IT LEFT THEIR SAVE TO THE DDL, WHICH NEVER GOT TO ITS OWN
      database.getSchema().buildDocumentType().withName("Doc").withTotalBuckets(2).create().createProperty("k", Type.LONG);
      database.transaction(() -> {
        database.newDocument("Doc").set("k", 1).save();
        database.newDocument("Doc").set("k", 1).save();
      });

      assertThatThrownBy(() -> database.getSchema().createTypeIndex(Schema.INDEX_TYPE.LSM_TREE, true, "Doc", "k"))
          .hasRootCauseInstanceOf(DuplicatedKeyException.class);

      final LocalSchema schema = (LocalSchema) database.getSchema().getEmbedded();
      assertThat(schema.isDirty()).isFalse();
      final int indexesInMemory = database.getSchema().getType("Doc").getAllIndexes(false).size();

      reopenDatabase();
      assertThat(database.getSchema().getType("Doc").getAllIndexes(false)).hasSize(indexesInMemory);
      assertThat(database.countType("Doc", false)).isEqualTo(2);
    }

    /**
     * The Raft commit paths ask for the save BEFORE the ending transaction leaves the stack, where it is already
     * inactive and {@code isTransactionActive()} therefore answers {@code false} even though an enclosing transaction is
     * open. The stack depth is what tells.
     */
    @Test
    void aTransactionEndAskedBeforeTheNestedTransactionLeftTheStackDoesNotWrite() {
      final LocalSchema schema = (LocalSchema) database.getSchema().getEmbedded();

      database.begin();
      final long before = schema.getVersion();
      database.getSchema().createDocumentType("Doc");
      database.begin();

      // WHAT THE RAFT READ-ONLY COMMIT DOES: RESET THE NESTED TRANSACTION, ASK FOR THE SAVE, THEN POP
      final TransactionContext nested = ((DatabaseInternal) database).getTransaction();
      nested.reset();
      assertThat(database.isTransactionActive()).as("the inactive nested transaction is still on top").isFalse();

      schema.saveConfigurationAtTransactionEnd();
      assertThat(schema.getVersion()).as("an enclosing transaction is open").isEqualTo(before);

      DatabaseContext.INSTANCE.getContext(database.getDatabasePath()).popIfNotLastTransaction();
      assertThat(database.isTransactionActive()).isTrue();
      database.commit();
      assertThat(schema.getVersion()).isEqualTo(before + 1);
    }

    @Test
    void rollbackAllNestedWritesThePostponedSchemaToo() {
      final LocalSchema schema = (LocalSchema) database.getSchema().getEmbedded();

      database.begin();
      final long before = schema.getVersion();
      database.getSchema().createDocumentType("Doc");
      database.begin();
      assertThat(schema.getVersion()).isEqualTo(before);

      // THE LOOP ROLLS BACK THE LAST TRANSACTION TOO, AND LEAVES IT INACTIVE: THE POSTPONED SAVE HAPPENS THERE
      ((DatabaseInternal) database).rollbackAllNested();
      assertThat(database.isTransactionActive()).isFalse();
      assertThat(schema.getVersion()).isEqualTo(before + 1);
      assertThat(schema.isDirty()).isFalse();
    }

    /**
     * A DDL that fails inside an open transaction, after a nested step created a bucket: the save it would have made on
     * the way out is postponed by the transaction, so the schema has to stay dirty for the transaction end to write it.
     * (A duplicate key while building a unique index is no way to test this: it rolls the caller's transaction back.)
     */
    @Test
    void aFailedDdlInsideATransactionLeavesItsNestedWorkToTheTransactionEnd() {
      final LocalSchema schema = (LocalSchema) database.getSchema().getEmbedded();

      database.begin();
      final long before = schema.getVersion();
      assertThatThrownBy(() -> schema.recordFileChanges(() -> {
        schema.createBucket("LeftBehind");
        throw new IllegalStateException("the DDL fails after its nested step");
      })).hasMessageContaining("the DDL fails after its nested step");

      assertThat(database.isTransactionActive()).isTrue();
      assertThat(schema.getVersion()).isEqualTo(before);
      assertThat(schema.isDirty()).as("the nested step's work is still waiting to be written").isTrue();

      database.commit();
      assertThat(schema.getVersion()).isEqualTo(before + 1);
      assertThat(schema.isDirty()).isFalse();
      assertThat(schema.existsBucket("LeftBehind")).isTrue();
    }

    /** A nested frame no longer saves, but a bucket it attached still resolves to its type before the outer frame ends. */
    @Test
    void aBucketAttachedByANestedFrameResolvesToItsTypeBeforeTheOuterFrameEnds() {
      final LocalSchema schema = (LocalSchema) database.getSchema().getEmbedded();
      final DocumentType type = database.getSchema().createDocumentType("Resolved");

      final DocumentType[] seen = new DocumentType[1];
      schema.recordFileChanges(() -> {
        final Bucket bucket = schema.createBucket("ResolvedExtra");
        ((LocalDocumentType) type).addBucket(bucket);
        seen[0] = schema.getTypeByBucketId(bucket.getFileId());
        return null;
      });
      assertThat(seen[0]).isSameAs(type);
    }

    /**
     * The schema is saved by LocalDatabase.commit(), not by the TransactionContext it commits: a caller that commits the
     * context directly leaves the schema dirty, and close() is the backstop that still writes it.
     */
    @Test
    void committingTheContextDirectlyLeavesTheSchemaToClose() {
      final LocalSchema schema = (LocalSchema) database.getSchema().getEmbedded();

      database.begin();
      final long before = schema.getVersion();
      database.getSchema().createDocumentType("Direct");
      ((DatabaseInternal) database).getTransaction().commit();
      assertThat(schema.isDirty()).isTrue();
      assertThat(schema.getVersion()).isEqualTo(before);
      database.rollback();

      // THE ROLLBACK ABOVE HAD NOTHING LEFT TO END, BUT THE SCHEMA WAS STILL DIRTY: CLOSE WRITES IT
      reopenDatabase();
      assertThat(database.getSchema().existsType("Direct")).isTrue();
    }

    /**
     * begin(); CREATE TYPE; INSERT; commit(): the schema file names the type BEFORE the records are durable, so a crash
     * between the commit and the schema write cannot leave acknowledged records in files no schema entry names.
     */
    @Test
    void theSchemaNamesTheTypeBeforeTheRecordsInsertedIntoItAreDurable() throws Exception {
      final Path schemaFile = ((LocalSchema) database.getSchema().getEmbedded()).getConfigurationFile().toPath();
      final boolean[] namedWhenDurable = new boolean[1];
      final boolean[] fired = new boolean[1];

      database.begin();
      database.getSchema().createDocumentType("Loaded");
      database.newDocument("Loaded").set("x", 1).save();
      ((DatabaseInternal) database).getTransaction().addAfterCommitCallback(() -> {
        fired[0] = true;
        try {
          namedWhenDurable[0] = Files.readString(schemaFile).contains("\"Loaded\"");
        } catch (final java.io.IOException e) {
          throw new IllegalStateException(e);
        }
      });
      database.commit();

      assertThat(fired[0]).isTrue();
      assertThat(namedWhenDurable[0]).as("schema.json named the type when the commit became durable").isTrue();
      assertThat(((LocalSchema) database.getSchema().getEmbedded()).isDirty()).isFalse();
      assertThat(database.countType("Loaded", false)).isEqualTo(1);
    }

    /** The write count is the schema version; the file itself has to agree that the write happened. */
    @Test
    void theSchemaFileNamesTheTypeAsSoonAsTheDdlReturns() throws Exception {
      final Path schemaFile = ((LocalSchema) database.getSchema().getEmbedded()).getConfigurationFile().toPath();
      database.getSchema().createDocumentType("OnDisk").createProperty("k", Type.LONG);
      final String content = Files.readString(schemaFile);
      assertThat(content).contains("\"OnDisk\"").contains("\"k\"");

      database.begin();
      database.getSchema().createDocumentType("Pending");
      assertThat(Files.readString(schemaFile)).as("postponed to the end of the transaction").doesNotContain("Pending");
      database.commit();
      assertThat(Files.readString(schemaFile)).contains("Pending");
    }

    /**
     * A commit that fails after the transaction ran DDL still writes it: the schema change stands whatever happened to
     * the records, and a caller that just propagates the failure never calls rollback().
     */
    @Test
    void aFailedCommitStillWritesTheDdlItsTransactionRan() {
      database.getSchema().createDocumentType("Unique").createProperty("k", Type.LONG);
      database.getSchema().createTypeIndex(Schema.INDEX_TYPE.LSM_TREE, true, "Unique", "k");
      database.transaction(() -> database.newDocument("Unique").set("k", 1).save());

      final LocalSchema schema = (LocalSchema) database.getSchema().getEmbedded();
      database.begin();
      final long before = schema.getVersion();
      database.getSchema().createDocumentType("CreatedInAFailedTransaction");
      database.newDocument("Unique").set("k", 1).save();
      assertThat(schema.getVersion()).isEqualTo(before);

      assertThatThrownBy(() -> database.commit()).isInstanceOf(DuplicatedKeyException.class);
      assertThat(database.isTransactionActive()).isFalse();
      assertThat(schema.getVersion()).as("written by the failed commit, no rollback() needed").isEqualTo(before + 1);
      assertThat(schema.isDirty()).isFalse();

      reopenDatabase();
      assertThat(database.getSchema().existsType("CreatedInAFailedTransaction")).isTrue();
    }

    /**
     * A transaction ending on another thread cannot write the schema while a DDL is half applied: the DDL holds the
     * database write lock, and a commit needs the read lock. What the DDL left pending is written once, by the DDL.
     */
    @Test
    void aCommitOnAnotherThreadCannotWriteTheSchemaInsideADdl() throws Exception {
      database.getSchema().createDocumentType("Busy");
      final LocalSchema schema = (LocalSchema) database.getSchema().getEmbedded();
      final CountDownLatch insideTheDdl = new CountDownLatch(1);
      final CountDownLatch release = new CountDownLatch(1);
      final CountDownLatch committed = new CountDownLatch(1);

      final Thread ddl = new Thread(() -> schema.recordFileChanges(() -> {
        schema.createBucket("HalfApplied");
        insideTheDdl.countDown();
        release.await();
        return null;
      }));
      final Thread committer = new Thread(() -> {
        database.transaction(() -> database.newDocument("Busy").set("x", 1).save());
        committed.countDown();
      });

      ddl.start();
      try {
        assertThat(insideTheDdl.await(30, TimeUnit.SECONDS)).isTrue();
        final long duringTheDdl = schema.getVersion();

        committer.start();
        // A WAIT EXPECTED TO TIME OUT: THE COMMITTER CANNOT GET PAST THE DDL'S WRITE LOCK
        assertThat(committed.await(300, TimeUnit.MILLISECONDS)).isFalse();
        assertThat(schema.getVersion()).as("nothing written while the DDL is half applied").isEqualTo(duringTheDdl);
      } finally {
        release.countDown();
        ddl.join(TimeUnit.SECONDS.toMillis(30));
      }

      assertThat(committed.await(30, TimeUnit.SECONDS)).isTrue();
      committer.join(TimeUnit.SECONDS.toMillis(30));
      assertThat(ddl.isAlive()).isFalse();
      assertThat(schema.isDirty()).isFalse();
      assertThat(database.countType("Busy", false)).isEqualTo(1);
    }

    /**
     * The deferral belongs to the thread running the DDL. A transaction ending on another thread while the DDL is in
     * flight is not the DDL's to write, and must not be left to it either.
     */
    @Test
    void aTransactionEndingOnAnotherThreadIsNotDeferredToThisThreadsDdl() throws Exception {
      final LocalSchema schema = (LocalSchema) database.getSchema().getEmbedded();
      final CountDownLatch insideTheDdl = new CountDownLatch(1);
      final CountDownLatch release = new CountDownLatch(1);

      final Thread ddl = new Thread(() -> schema.recordFileChanges(() -> {
        insideTheDdl.countDown();
        release.await();
        return null;
      }));
      ddl.start();
      try {
        assertThat(insideTheDdl.await(30, TimeUnit.SECONDS)).isTrue();
        // THE FRAME MARKED THE SCHEMA DIRTY ON ENTRY
        assertThat(schema.isDirty()).isTrue();

        final long before = schema.getVersion();
        schema.saveConfigurationAtTransactionEnd();
        assertThat(schema.getVersion()).as("written by this thread, not left to the other thread's DDL").isEqualTo(before + 1);
      } finally {
        release.countDown();
        ddl.join(TimeUnit.SECONDS.toMillis(30));
      }
      assertThat(ddl.isAlive()).isFalse();
      assertThat(schema.isDirty()).isFalse();
    }

    /**
     * A DDL inside a transaction writes the schema at the end of it, so a crash before then leaves bucket and index
     * files on disk that {@code schema.json} does not name. The database has to open anyway, and the same DDL has to
     * succeed again.
     */
    @Test
    void filesOfADdlThatNeverReachedTheSchemaFileAreToleratedOnReopen() throws Exception {
      final LocalSchema schema = (LocalSchema) database.getSchema().getEmbedded();
      final Path schemaFile = schema.getConfigurationFile().toPath();
      final Path previousFile = schemaFile.resolveSibling(LocalSchema.SCHEMA_PREV_FILE_NAME);
      final byte[] schemaBefore = Files.readAllBytes(schemaFile);
      final byte[] previousBefore = Files.exists(previousFile) ? Files.readAllBytes(previousFile) : null;

      database.begin();
      database.getSchema().createDocumentType("Doc").createProperty("k", Type.LONG);
      database.getSchema().createTypeIndex(Schema.INDEX_TYPE.LSM_TREE, true, "Doc", "k");
      ((DatabaseInternal) database).kill();
      database.close();

      // kill() STILL SAVES THE DIRTY SCHEMA ON ITS WAY OUT: PUT BACK WHAT A CRASH BEFORE THE WRITE LEAVES
      Files.write(schemaFile, schemaBefore);
      if (previousBefore != null)
        Files.write(previousFile, previousBefore);
      else
        Files.deleteIfExists(previousFile);

      database = factory.open();
      assertThat(database.getSchema().existsType("Doc")).isFalse();

      database.getSchema().createDocumentType("Doc").createProperty("k", Type.LONG);
      database.getSchema().createTypeIndex(Schema.INDEX_TYPE.LSM_TREE, true, "Doc", "k");
      database.transaction(() -> database.newDocument("Doc").set("k", 1).save());
      assertThat(database.query("sql", "select from Doc where k = 1").stream().count()).isEqualTo(1);

      reopenDatabase();
      assertThat(database.getSchema().getType("Doc").getAllIndexes(false)).hasSize(1);
      assertThat(database.query("sql", "select from Doc where k = 1").stream().count()).isEqualTo(1);
    }

    // Pins the number of directory opens on purpose: it is the fsync reduction itself, so a change to how FileUtils opens
    // the directory has to update this count deliberately.
    @Test
    void aSchemaWriteForcesTheDatabaseDirectoryOnce() {
      // schema.prev.json AND schema.json ARE PUBLISHED INTO THE SAME DIRECTORY, AND ONE FSYNC OF IT AFTER THE SECOND
      // RENAME MAKES BOTH DURABLE: THE COPY USED TO FORCE IT TOO, A SECOND FULL DEVICE FLUSH ON EVERY SCHEMA CHANGE
      final Path databaseDirectory = ((LocalSchema) database.getSchema().getEmbedded()).getConfigurationFile().toPath()
          .toAbsolutePath().getParent();
      final AtomicInteger opens = new AtomicInteger();
      try (final MockedStatic<FileChannel> ignored = mockStatic(FileChannel.class, invocation -> {
        if ("open".equals(invocation.getMethod().getName()) && invocation.getMethod().getParameterCount() == 2
            && databaseDirectory.equals(invocation.getArgument(0)))
          opens.incrementAndGet();
        return invocation.callRealMethod();
      })) {
        assertThat(schemaWritesOf(() -> database.getSchema().createDocumentType("Doc").createProperty("k", Type.LONG)))
            .isEqualTo(2);
      }
      assertThat(opens.get()).isEqualTo(System.getProperty("os.name").toLowerCase(Locale.ROOT).contains("win") ? 0 : 2);
    }

    @Test
    void nestedTransactionCommitDoesNotWriteWhileTheOuterOneIsOpen() {
      final LocalSchema schema = (LocalSchema) database.getSchema().getEmbedded();

      database.begin();
      final long before = schema.getVersion();
      database.getSchema().createDocumentType("Doc");
      assertThat(schema.isDirty()).isTrue();

      // A PROPERTY NAME THE DICTIONARY HAS NEVER SEEN: ITS ENTRY IS COMMITTED BY A NESTED TRANSACTION, WHICH USED TO
      // WRITE THE DIRTY SCHEMA UNDER THE OUTER TRANSACTION'S FEET
      final MutableDocument doc = database.newDocument("Doc");
      doc.set("aNameTheDictionaryHasNeverSeen", 1).save();
      assertThat(schema.getVersion()).isEqualTo(before);

      database.commit();
      assertThat(schema.getVersion()).isEqualTo(before + 1);
      assertThat(schema.isDirty()).isFalse();
    }

    @Test
    void everyDdlIsDurableAfterItsSingleWrite() {
      database.getSchema().createDocumentType("Doc").createProperty("k", Type.LONG);
      database.getSchema().createTypeIndex(Schema.INDEX_TYPE.LSM_TREE, true, "Doc", "k");
      database.getSchema().createVertexType("V").createProperty("name", Type.STRING);
      database.getSchema().createEdgeType("E");

      final LocalSchema schema = (LocalSchema) database.getSchema().getEmbedded();
      assertThat(schema.isDirty()).isFalse();

      reopenDatabase();
      assertThat(database.getSchema().getType("Doc").existsProperty("k")).isTrue();
      assertThat(database.getSchema().getType("Doc").getAllIndexes(false)).hasSize(1);
      assertThat(database.getSchema().getType("Doc").getBuckets(false)).isNotEmpty();
      assertThat(database.getSchema().getType("V").existsProperty("name")).isTrue();
      assertThat(database.getSchema().existsType("E")).isTrue();
    }

    private long schemaWritesOf(final Runnable ddl) {
      final LocalSchema schema = (LocalSchema) database.getSchema().getEmbedded();
      final long before = schema.getVersion();
      ddl.run();
      assertThat(schema.isDirty()).isFalse();
      return schema.getVersion() - before;
    }
  }

  /**
   * Regression for #9747: schema setters persist.
   *
   * Issue #9747, the embedded side: the schema writers now apply their change inside a schema recording session instead of
   * ending in a bare save. Off HA the session must still persist every change across a reopen, and a setter handed the
   * value it already holds must not open a session at all - no save, no schema version bump.
   */
  @Nested
  class SchemaSettersPersist extends TestHelper {

    @Test
    void everyMovedSetterStillPersistsAcrossAReopen() {
      database.command("sql", "CREATE DOCUMENT TYPE P");
      database.command("sql", "CREATE PROPERTY P.x INTEGER");
      database.command("sql", "CREATE PROPERTY P.s STRING");
      database.command("sql", "CREATE PROPERTY P.l LIST");
      database.command("sql", "CREATE EDGE TYPE E");
      database.command("sql", "CREATE TIMESERIES TYPE TS TIMESTAMP ts FIELDS (v DOUBLE)");
      database.command("sql", "CREATE DOCUMENT TYPE B BUCKETS 2");
      database.command("sql", "CREATE PROPERTY B.tenant STRING");
      database.command("sql", "CREATE INDEX ON B (tenant) UNIQUE");

      final Property x = database.getSchema().getType("P").getProperty("x");
      x.setMandatory(true);
      x.setNotNull(true);
      x.setReadonly(true);
      x.setMin("1");
      x.setMax("9");
      final Property s = database.getSchema().getType("P").getProperty("s");
      s.setRegexp("[a-z]+");
      s.setHidden(true);
      s.setCompression("max");
      s.setCustomValue("k", "v");
      database.getSchema().getType("P").getProperty("l").setOfType("STRING");

      database.getSchema().setExtension("probe", new JSONObject().put("a", 1));
      database.command("sql", "ALTER DATABASE `arcadedb.dateFormat` 'dd-MM-yyyy'");
      database.command("sql", "ALTER DATABASE `arcadedb.dateTimeFormat` 'dd-MM-yyyy HH:mm'");
      database.getSchema().getEmbedded().setZoneId(ZoneId.of("Asia/Tokyo"));
      database.command("sql", "ALTER TYPE E WITH lightweight = true");
      database.command("sql", "ALTER TIMESERIES TYPE TS ADD DOWNSAMPLING POLICY AFTER 7 DAYS GRANULARITY 1 HOURS");
      database.command("sql", "ALTER TYPE B BucketSelectionStrategy `partitioned('tenant')`");

      reopenDatabase();

      final Schema schema = database.getSchema();
      final Property rx = schema.getType("P").getProperty("x");
      assertThat(rx.isMandatory()).isTrue();
      assertThat(rx.isNotNull()).isTrue();
      assertThat(rx.isReadonly()).isTrue();
      assertThat(rx.getMin()).isEqualTo("1");
      assertThat(rx.getMax()).isEqualTo("9");
      final Property rs = schema.getType("P").getProperty("s");
      assertThat(rs.getRegexp()).isEqualTo("[a-z]+");
      assertThat(rs.isHidden()).isTrue();
      assertThat(rs.getCompression()).isEqualTo("max");
      assertThat(rs.getCustomValue("k")).isEqualTo("v");
      assertThat(schema.getType("P").getProperty("l").getOfType()).isEqualTo("STRING");

      assertThat(schema.getExtension("probe").getInt("a")).isEqualTo(1);
      assertThat(schema.getDateFormat()).isEqualTo("dd-MM-yyyy");
      assertThat(schema.getDateTimeFormat()).isEqualTo("dd-MM-yyyy HH:mm");
      assertThat(schema.getZoneId()).isEqualTo(ZoneId.of("Asia/Tokyo"));
      assertThat(((LocalEdgeType) schema.getType("E")).isLightweight()).isTrue();
      assertThat(((LocalTimeSeriesType) schema.getType("TS")).getDownsamplingTiers()).hasSize(1);
      assertThat(schema.getType("B").getBucketSelectionStrategy().getName()).isEqualTo("partitioned");
    }

    @Test
    void aSetterHandedTheValueItHoldsOpensNoSession() {
      database.command("sql", "CREATE DOCUMENT TYPE N");
      database.command("sql", "CREATE PROPERTY N.x INTEGER");
      final LocalSchema schema = database.getSchema().getEmbedded();
      final Property x = schema.getType("N").getProperty("x");
      x.setMandatory(true);
      x.setCustomValue("k", "v");
      schema.setDateFormat("dd-MM-yyyy");
      schema.setZoneId(ZoneId.of("Asia/Tokyo"));

      final long version = schema.getVersion();

      x.setMandatory(true);
      x.setCustomValue("k", "v");
      x.setCustomValue("absent", null);
      schema.setDateFormat("dd-MM-yyyy");
      schema.setZoneId(ZoneId.of("Asia/Tokyo"));
      ((LocalDocumentType) schema.getType("N")).setNeedsRepartition(false);

      assertThat(schema.getVersion()).isEqualTo(version);
      assertThat(schema.isDirty()).isFalse();
    }

    /**
     * Validation runs before the session opens: a refused value leaves the schema, its version and schema.json as they
     * were, and nothing is shipped.
     */
    @Test
    void aRefusedValueTouchesNothing() {
      database.command("sql", "CREATE DOCUMENT TYPE R");
      database.command("sql", "CREATE PROPERTY R.b BOOLEAN");
      database.command("sql", "CREATE VERTEX TYPE RV");
      database.command("sql", "CREATE EDGE TYPE RE");
      database.transaction(() -> database.newVertex("RV").save().newEdge("RE", database.newVertex("RV").save()));

      final LocalSchema schema = database.getSchema().getEmbedded();
      final long version = schema.getVersion();

      final Property b = schema.getType("R").getProperty("b");
      assertThatThrownBy(() -> b.setMin("1")).isInstanceOf(IllegalArgumentException.class);
      assertThatThrownBy(() -> ((LocalEdgeType) schema.getType("RE")).setLightweight(true)).isInstanceOf(SchemaException.class);

      assertThat(b.getMin()).isNull();
      assertThat(((LocalEdgeType) schema.getType("RE")).isLightweight()).isFalse();
      assertThat(schema.getVersion()).isEqualTo(version);
      assertThat(schema.isDirty()).isFalse();
    }
  }

  /**
   * Regression for #9764: failed import restore keeps prefix.
   *
   * Issue #9764: an imported restore runs inside a schema recording session, and a failure part way through must not
   * leave the members restored before it in memory only. There is no schema rollback, so the session closes normally -
   * saving, and under Raft HA shipping, the prefix that landed - and the failure is rethrown after it. Escaping the session
   * instead would fail it unpublished: the members would stay on this node with nothing on disk and nothing shipped.
   */
  @Nested
  class FailedImportRestoreKeepsPrefix {
    private static final String DATABASE_PATH = "target/databases/test-issue9764-failed-import-restore";

    @BeforeEach
    @AfterEach
    void clean() {
      FileUtils.deleteRecursively(new File(DATABASE_PATH));
    }

    @Test
    void theMembersRestoredBeforeAMalformedSectionArePersisted() throws Exception {
      try (final Database db = new DatabaseFactory(DATABASE_PATH).create()) {
        db.command("sql", "CREATE DOCUMENT TYPE Account");
        db.command("sql", "CREATE DOCUMENT TYPE AccountAudit");
        db.command("sql",
            "CREATE TRIGGER audit AFTER CREATE ON TYPE Account EXECUTE SQL \"INSERT INTO AccountAudit SET what = 'a'\"");

        final LocalSchema schema = db.getSchema().getEmbedded();
        final JSONObject triggers = schema.toJSON().getJSONObject("triggers");
        db.command("sql", "DROP TRIGGER audit");
        assertThat(schema.existsTrigger("audit")).isFalse();

        // Triggers are restored first; the views section that follows is not an object, which throws outside the
        // per-member try blocks
        final JSONObject imported = new JSONObject().put("triggers", triggers).put("materializedViews", "not-an-object");

        assertThatThrownBy(() -> schema.restoreSchemaMembersFromJSON(imported, LocalSchema.SchemaMemberSource.IMPORTED_FILE))
            .isInstanceOf(RuntimeException.class);

        assertThat(schema.existsTrigger("audit")).as("the trigger restored before the failure is installed").isTrue();

        final JSONObject onDisk = new JSONObject(
            Files.readString(new File(db.getDatabasePath(), "schema.json").toPath(), StandardCharsets.UTF_8));
        assertThat(onDisk.getJSONObject("triggers").has("audit"))
            .as("and saved, rather than left in memory only by a session that failed unpublished").isTrue();
      }
    }
  }
}
