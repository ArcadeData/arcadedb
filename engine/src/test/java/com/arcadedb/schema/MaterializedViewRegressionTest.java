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
import com.arcadedb.database.DatabaseFactory;
import com.arcadedb.database.DatabaseInternal;
import com.arcadedb.database.Document;
import com.arcadedb.database.LocalDatabase;
import com.arcadedb.database.MutableDocument;
import com.arcadedb.database.RID;
import com.arcadedb.engine.Component;
import com.arcadedb.engine.ComponentFile;
import com.arcadedb.engine.LocalBucket;
import com.arcadedb.engine.timeseries.TimeSeriesEngine;
import com.arcadedb.event.BeforeRecordCreateListener;
import com.arcadedb.event.BeforeRecordUpdateListener;
import com.arcadedb.exception.CommandExecutionException;
import com.arcadedb.exception.SchemaException;
import com.arcadedb.index.TypeIndex;
import com.arcadedb.index.lsm.LSMTreeIndex;
import com.arcadedb.index.lsm.LSMTreeIndexBloomFilter;
import com.arcadedb.index.lsm.LSMTreeIndexCompacted;
import com.arcadedb.query.sql.antlr.SQLAntlrParser;
import com.arcadedb.query.sql.executor.Result;
import com.arcadedb.query.sql.executor.ResultSet;
import com.arcadedb.query.sql.parser.CreateMaterializedViewStatement;
import com.arcadedb.query.sql.parser.Statement;
import com.arcadedb.serializer.json.JSONArray;
import com.arcadedb.serializer.json.JSONObject;
import com.arcadedb.utility.DateUtils;
import org.awaitility.Awaitility;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.io.File;
import java.lang.reflect.InvocationHandler;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.lang.reflect.Proxy;
import java.nio.file.Files;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Function;
import java.util.function.IntConsumer;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.assertj.core.api.Assertions.catchThrowable;

/**
 * Regression tests, one nested class per fixed issue.
 */
class MaterializedViewRegressionTest {

  /**
   * Regression for #10020: {@link LocalSchema#loadIncremental} used to answer {@code false} without saying why, so the full load a
   * replica then ran on its apply thread could not be tied to anything. {@link LocalSchema#tryLoadIncremental} reports the
   * reason; this pins one test per way the incremental refresh refuses, and that a refusal names the file that caused it.
   *
   * @author Luca Garulli (l.garulli@arcadedata.com)
   */
  @Nested
  class IncrementalLoadRefusalReason extends TestHelper {

    private static final String TYPE_NAME = "Issue10020Indexed";
    private static final int    RECORDS   = 2_000;

    @Override
    protected void beginTest() {
      // A small index page size makes the compaction write a real series with a bloom filter over it
      database.getConfiguration().setValue(GlobalConfiguration.INDEX_COMPACTION_MIN_PAGES_SCHEDULE, 0);

      database.transaction(() -> {
        final DocumentType type = database.getSchema().buildDocumentType().withName(TYPE_NAME).withTotalBuckets(1).create();
        type.createProperty("value", Integer.class);
        database.getSchema().buildTypeIndex(TYPE_NAME, new String[] { "value" }).withType(Schema.INDEX_TYPE.LSM_TREE)
            .withUnique(true).withPageSize(1024).create();
      });
      database.transaction(() -> {
        for (int i = 0; i < RECORDS; i++)
          database.newDocument(TYPE_NAME).set("value", i).save();
      });

      final LSMTreeIndex index = (LSMTreeIndex) database.getSchema().getType(TYPE_NAME).getAllIndexes(false).iterator().next()
          .getIndexesOnBuckets()[0];
      try {
        if (index.scheduleCompaction())
          index.compact();
      } catch (final Exception e) {
        throw new IllegalStateException("cannot compact the fixture index", e);
      }
    }

    @Test
    void anEntryThatCanBeAppliedIncrementallyReportsNoRefusal() throws Exception {
      assertThat(schema().tryLoadIncremental(ComponentFile.MODE.READ_WRITE, Set.of(), Set.of())).isNull();
      assertThat(schema().loadIncremental(ComponentFile.MODE.READ_WRITE, Set.of(), Set.of())).isTrue();
    }

    @Test
    void aRetiredFileIsReported() throws Exception {
      final int anyFileId = database.getSchema().getType(TYPE_NAME).getBuckets(false).get(0).getFileId();

      final IncrementalLoadRefusal refusal = schema().tryLoadIncremental(ComponentFile.MODE.READ_WRITE, Set.of(anyFileId), Set.of());

      assertThat(refusal).isNotNull();
      assertThat(refusal.reason()).isEqualTo(FullSchemaLoadReason.FILE_RETIRED);
      assertThat(refusal.detail()).contains(String.valueOf(anyFileId));
      assertThat(schema().loadIncremental(ComponentFile.MODE.READ_WRITE, Set.of(anyFileId), Set.of())).isFalse();
    }

    @Test
    void aTouchedBloomFilterIsReportedWithItsExtension() throws Exception {
      final int filterFileId = fileIdOfRegistered(LSMTreeIndexBloomFilter.class);
      assertThat(filterFileId).as("the fixture must produce a bloom filter, otherwise this test asserts nothing").isNotNegative();

      final IncrementalLoadRefusal refusal = schema().tryLoadIncremental(ComponentFile.MODE.READ_WRITE, Set.of(), Set.of(filterFileId));

      assertThat(refusal).isNotNull();
      assertThat(refusal.reason()).isEqualTo(FullSchemaLoadReason.NON_INCREMENTAL_TOUCHED_FILE);
      assertThat(refusal.detail()).contains(LSMTreeIndexBloomFilter.FILE_EXT).contains(String.valueOf(filterFileId));
    }

    @Test
    void aTouchedCompactedIndexIsReportedWithItsExtension() throws Exception {
      final int compactedFileId = fileIdOfRegistered(LSMTreeIndexCompacted.class);
      assertThat(compactedFileId).as("the fixture must produce a compacted index, otherwise this test asserts nothing")
          .isNotNegative();

      final IncrementalLoadRefusal refusal = schema().tryLoadIncremental(ComponentFile.MODE.READ_WRITE, Set.of(), Set.of(compactedFileId));

      assertThat(refusal).isNotNull();
      assertThat(refusal.reason()).isEqualTo(FullSchemaLoadReason.NON_INCREMENTAL_TOUCHED_FILE);
      assertThat(refusal.detail()).contains(String.valueOf(compactedFileId));
    }

    @Test
    void everyReasonHasADistinctMetricTag() {
      final Set<String> tags = new HashSet<>();
      for (final FullSchemaLoadReason reason : FullSchemaLoadReason.values()) {
        assertThat(reason.getTag()).as("tag of %s", reason).matches("[a-z][a-z0-9_]*");
        assertThat(tags.add(reason.getTag())).as("tag of %s must be unique", reason).isTrue();
      }
      assertThat(Arrays.stream(FullSchemaLoadReason.values()).map(FullSchemaLoadReason::getDescription))
          .allSatisfy(description -> assertThat(description).isNotBlank());
    }

    private int fileIdOfRegistered(final Class<? extends Component> componentClass) {
      for (final ComponentFile file : ((DatabaseInternal) database).getFileManager().getFiles()) {
        if (file == null)
          continue;
        final Component component = schema().getFileByIdIfExists(file.getFileId());
        if (componentClass.isInstance(component))
          return component.getFileId();
      }
      return -1;
    }

    private LocalSchema schema() {
      return ((DatabaseInternal) database).getSchema().getEmbedded();
    }
  }

  /**
   * Regression for #10205: a REFRESH INCREMENTAL view stayed VALID but stale after writes to a subtype of its source type, and after
   * writes to any type its query reads besides the first FROM identifier (a subquery in FROM, a subquery in WHERE or in the
   * projection).
   *
   * @author Luca Garulli (l.garulli@arcadedata.com)
   */
  @Nested
  class IncrementalViewSources extends TestHelper {

    private static final String COUNT    = "SELECT count(*) AS c FROM Item";
    private static final String GROUPED  = "SELECT grp, count(*) AS c FROM (SELECT grp FROM Item) GROUP BY grp";
    private static final String OWNED    = "SELECT grp FROM Item WHERE grp IN (SELECT name FROM Owner)";
    private static final String PROJECTS = "SELECT grp, (SELECT count(*) AS n FROM Owner)[0].n AS owners FROM Item";

    @BeforeEach
    void setUpTypes() {
      database.command("sql", "CREATE DOCUMENT TYPE Item");
      database.command("sql", "CREATE DOCUMENT TYPE ItemB EXTENDS Item");
      database.command("sql", "CREATE DOCUMENT TYPE Owner");
      database.command("sql", "CREATE DOCUMENT TYPE Unrelated");
      database.transaction(() -> {
        database.command("sql", "INSERT INTO Item SET grp = 'x'");
        database.command("sql", "INSERT INTO ItemB SET grp = 'y'");
        database.command("sql", "INSERT INTO Owner SET name = 'x'");
      });
    }

    @Test
    void writesToASubtypeRefreshTheView() {
      database.command("sql", "CREATE MATERIALIZED VIEW Vc AS " + COUNT + " REFRESH INCREMENTAL");
      assertSameAsQuery("Vc", COUNT);

      database.transaction(() -> database.command("sql", "INSERT INTO ItemB SET grp = 'y'"));
      assertSameAsQuery("Vc", COUNT);
      assertThat(viewRows("Vc")).containsExactly("c=3");

      database.transaction(() -> database.command("sql", "DELETE FROM ItemB WHERE grp = 'y' LIMIT 1"));
      assertThat(viewRows("Vc")).containsExactly("c=2");
      assertThat(statusOf("Vc")).isEqualTo("VALID");
    }

    @Test
    void aSubtypeCreatedAfterTheViewRefreshesItToo() {
      database.command("sql", "CREATE MATERIALIZED VIEW Vc AS " + COUNT + " REFRESH INCREMENTAL");
      database.command("sql", "CREATE DOCUMENT TYPE ItemC EXTENDS ItemB");

      database.transaction(() -> database.command("sql", "INSERT INTO ItemC SET grp = 'z'"));
      assertThat(viewRows("Vc")).containsExactly("c=3");

      database.transaction(() -> database.command("sql", "UPDATE ItemC SET grp = 'w'"));
      assertSameAsQuery("Vc", COUNT);

      database.transaction(() -> database.command("sql", "DELETE FROM ItemC"));
      assertThat(viewRows("Vc")).containsExactly("c=2");
      assertSameAsQuery("Vc", COUNT);
    }

    @Test
    void aViewGroupingASubtypeSeesItsUpdates() {
      database.command("sql", "CREATE MATERIALIZED VIEW Vg AS " + GROUPED + " REFRESH INCREMENTAL");
      database.command("sql", "CREATE DOCUMENT TYPE ItemC EXTENDS Item");
      database.transaction(() -> database.command("sql", "INSERT INTO ItemC SET grp = 'x'"));
      database.transaction(() -> database.command("sql", "UPDATE ItemC SET grp = 'q'"));
      assertSameAsQuery("Vg", GROUPED);

      reopenDatabase();
      database.transaction(() -> database.command("sql", "UPDATE ItemC SET grp = 'r'"));
      assertSameAsQuery("Vg", GROUPED);
      assertThat(viewRows("Vg")).containsExactly("c=1 grp=r", "c=1 grp=x", "c=1 grp=y");
    }

    @Test
    void aSubqueryInFromIsTracked() {
      database.command("sql", "CREATE MATERIALIZED VIEW Vg AS " + GROUPED + " REFRESH INCREMENTAL");
      assertThat(sourceTypesOf("Vg")).containsExactly("Item");

      database.transaction(() -> database.command("sql", "INSERT INTO Item SET grp = 'z'"));
      assertSameAsQuery("Vg", GROUPED);
      assertThat(viewRows("Vg")).containsExactly("c=1 grp=x", "c=1 grp=y", "c=1 grp=z");
    }

    @Test
    void aSubqueryInWhereIsTracked() {
      database.command("sql", "CREATE MATERIALIZED VIEW Vo AS " + OWNED + " REFRESH INCREMENTAL");
      assertThat(sourceTypesOf("Vo")).containsExactlyInAnyOrder("Item", "Owner");

      database.transaction(() -> database.command("sql", "INSERT INTO Owner SET name = 'y'"));
      assertSameAsQuery("Vo", OWNED);
      assertThat(viewRows("Vo")).containsExactly("grp=x", "grp=y");
    }

    @Test
    void aSubqueryInTheProjectionIsTracked() {
      database.command("sql", "CREATE MATERIALIZED VIEW Vp AS " + PROJECTS + " REFRESH INCREMENTAL");

      database.transaction(() -> database.command("sql", "INSERT INTO Owner SET name = 'q'"));
      assertSameAsQuery("Vp", PROJECTS);
      assertThat(viewRows("Vp")).containsExactly("grp=x owners=2", "grp=y owners=2");
    }

    @Test
    void aBackQuotedSourceAndParentReferencesInANestedSubquery() {
      final String query = "SELECT grp, (SELECT count(*) AS n FROM `Owner` WHERE name = $parent.$current.grp)[0].n AS owners FROM `Item`";
      database.command("sql", "CREATE MATERIALIZED VIEW Vn AS " + query + " REFRESH INCREMENTAL");
      // THE $ VARIABLES ARE NOT SOURCES, THE BACK-QUOTED NAMES ARE
      assertThat(sourceTypesOf("Vn")).containsExactlyInAnyOrder("Item", "Owner");

      database.transaction(() -> database.command("sql", "INSERT INTO Owner SET name = 'y'"));
      assertSameAsQuery("Vn", query);
      assertThat(viewRows("Vn")).containsExactly("grp=x owners=1", "grp=y owners=1");
    }

    /** The count of views on the polymorphic registries follows every lifecycle step, so it never drifts. */
    @Test
    void theListenerCountFollowsCreateAlterReopenAndDrop() {
      final LocalSchema schema = (LocalSchema) database.getSchema();
      assertThat(schema.hasPolymorphicViewListeners()).isFalse();

      database.command("sql", "CREATE MATERIALIZED VIEW Va AS " + COUNT + " REFRESH INCREMENTAL");
      database.command("sql", "CREATE MATERIALIZED VIEW Vb AS " + OWNED + " REFRESH INCREMENTAL");
      assertThat(schema.hasPolymorphicViewListeners()).isTrue();

      database.command("sql", "ALTER MATERIALIZED VIEW Va REFRESH MANUAL");
      database.command("sql", "ALTER MATERIALIZED VIEW Vb REFRESH MANUAL");
      assertThat(schema.hasPolymorphicViewListeners()).isFalse();

      database.command("sql", "ALTER MATERIALIZED VIEW Va REFRESH INCREMENTAL");
      database.command("sql", "ALTER MATERIALIZED VIEW Va REFRESH INCREMENTAL");
      assertThat(schema.hasPolymorphicViewListeners()).isTrue();

      reopenDatabase();
      final LocalSchema reopened = (LocalSchema) database.getSchema();
      assertThat(reopened.hasPolymorphicViewListeners()).isTrue();

      database.command("sql", "DROP MATERIALIZED VIEW Va");
      assertThat(reopened.hasPolymorphicViewListeners()).isFalse();
      database.command("sql", "DROP MATERIALIZED VIEW Vb");
      assertThat(reopened.hasPolymorphicViewListeners()).isFalse();

      // AND THE REMAINING VIEW STILL REFRESHES AFTER ALL OF IT
      database.command("sql", "CREATE MATERIALIZED VIEW Vc AS " + COUNT + " REFRESH INCREMENTAL");
      database.transaction(() -> database.command("sql", "INSERT INTO ItemB SET grp = 'z'"));
      assertSameAsQuery("Vc", COUNT);
      database.command("sql", "DROP MATERIALIZED VIEW Vc");
      assertThat(reopened.hasPolymorphicViewListeners()).isFalse();
    }

    /** A source named only inside a subquery is a dependency a rename cannot carry with certainty: refused, not broken. */
    @Test
    void renamingASourceReadOnlyByASubqueryIsRefused() {
      database.command("sql", "CREATE MATERIALIZED VIEW Vo AS " + OWNED + " REFRESH INCREMENTAL");

      assertThatThrownBy(() -> database.command("sql", "ALTER TYPE Owner NAME Owner2"))
          .hasMessageContaining("materialized view 'Vo'");
      assertThat(database.getSchema().existsType("Owner")).isTrue();
      database.transaction(() -> database.command("sql", "INSERT INTO Owner SET name = 'y'"));
      assertSameAsQuery("Vo", OWNED);
    }

    @Test
    void writesToATypeTheQueryDoesNotReadDoNotRefresh() {
      database.command("sql", "CREATE MATERIALIZED VIEW Vo AS " + OWNED + " REFRESH INCREMENTAL");
      final MaterializedViewImpl view = (MaterializedViewImpl) database.getSchema().getMaterializedView("Vo");
      final long refreshes = view.getRefreshCount();

      database.transaction(() -> database.command("sql", "INSERT INTO Unrelated SET name = 'x'"));
      assertThat(view.getRefreshCount()).isEqualTo(refreshes);
    }

    @Test
    void aTypeReadOnlyByASubqueryCannotBeDroppedUnderTheView() {
      database.command("sql", "CREATE MATERIALIZED VIEW Vo AS " + OWNED + " REFRESH INCREMENTAL");

      assertThatThrownBy(() -> database.command("sql", "DROP TYPE Owner"))
          .hasMessageContaining("source type for materialized view 'Vo'");
    }

    @Test
    void sourcesThatNoTypeEventReportsAreRefusedForIncremental() {
      for (final String query : new String[] {
          "SELECT FROM (TRAVERSE out() FROM Item)",
          "SELECT FROM bucket:Item",
          "SELECT FROM (MATCH {type: Item, as: i} RETURN i)" }) {
        assertThatThrownBy(() -> database.command("sql", "CREATE MATERIALIZED VIEW Vx AS " + query + " REFRESH INCREMENTAL"))
            .as(query)
            .hasMessageContaining("REFRESH INCREMENTAL");
        assertThat(database.getSchema().existsMaterializedView("Vx")).as(query).isFalse();
        assertThat(database.getSchema().existsType("Vx")).as(query).isFalse();
      }

      // THE SAME QUERY IS FINE FOR A VIEW REFRESHED ON DEMAND, BUT CANNOT BE ALTERED TO INCREMENTAL
      database.command("sql", "CREATE MATERIALIZED VIEW Vt AS SELECT FROM (TRAVERSE out() FROM Item) REFRESH MANUAL");
      assertThatThrownBy(() -> database.command("sql", "ALTER MATERIALIZED VIEW Vt REFRESH INCREMENTAL"))
          .isInstanceOfAny(CommandExecutionException.class, SchemaException.class)
          .hasMessageContaining("REFRESH INCREMENTAL");
      assertThat(database.getSchema().getMaterializedView("Vt").getRefreshMode()).isEqualTo(MaterializedViewRefreshMode.MANUAL);
    }

    @Test
    void theListenersSurviveAReopen() {
      database.command("sql", "CREATE MATERIALIZED VIEW Vc AS " + COUNT + " REFRESH INCREMENTAL");
      database.command("sql", "CREATE MATERIALIZED VIEW Vo AS " + OWNED + " REFRESH INCREMENTAL");
      reopenDatabase();

      database.transaction(() -> {
        database.command("sql", "INSERT INTO ItemB SET grp = 'z'");
        database.command("sql", "INSERT INTO Owner SET name = 'z'");
      });
      assertSameAsQuery("Vc", COUNT);
      assertSameAsQuery("Vo", OWNED);
    }

    /** A view stored before every source was tracked lists only its first FROM identifier: the load completes the list. */
    @Test
    void aViewStoredWithOnlyItsFirstSourceTracksAllAfterLoad() throws Exception {
      database.command("sql", "CREATE MATERIALIZED VIEW Vo AS " + OWNED + " REFRESH INCREMENTAL");
      database.close();

      final File schemaFile = new File(getDatabasePath(), LocalSchema.SCHEMA_FILE_NAME);
      final JSONObject schema = new JSONObject(Files.readString(schemaFile.toPath()));
      schema.getJSONObject("materializedViews").getJSONObject("Vo").put("sourceTypes", new JSONArray().put("Item"));
      Files.writeString(schemaFile.toPath(), schema.toString());

      database = factory.open();
      assertThat(sourceTypesOf("Vo")).containsExactlyInAnyOrder("Item", "Owner");
      database.transaction(() -> database.command("sql", "INSERT INTO Owner SET name = 'y'"));
      assertSameAsQuery("Vo", OWNED);
    }

    @Test
    void anAlterToIncrementalTracksEverySource() {
      database.command("sql", "CREATE MATERIALIZED VIEW Vo AS " + OWNED + " REFRESH MANUAL");
      database.command("sql", "ALTER MATERIALIZED VIEW Vo REFRESH INCREMENTAL");

      database.transaction(() -> database.command("sql", "INSERT INTO Owner SET name = 'y'"));
      assertSameAsQuery("Vo", OWNED);
    }

    private void assertSameAsQuery(final String view, final String definingQuery) {
      assertThat(viewRows(view)).as(view).isEqualTo(rows(definingQuery));
      assertThat(statusOf(view)).as(view).isEqualTo("VALID");
    }

    private List<String> viewRows(final String view) {
      return rows("SELECT FROM " + view);
    }

    private List<String> rows(final String sql) {
      final List<String> rows = new ArrayList<>();
      try (final ResultSet rs = database.query("sql", sql)) {
        while (rs.hasNext()) {
          final Result r = rs.next();
          final List<String> values = new ArrayList<>();
          for (final String p : r.getPropertyNames())
            if (!p.startsWith("@"))
              values.add(p + "=" + r.getProperty(p));
          Collections.sort(values);
          rows.add(String.join(" ", values));
        }
      }
      Collections.sort(rows);
      return rows;
    }

    private String statusOf(final String view) {
      return database.getSchema().getMaterializedView(view).getStatus();
    }

    private List<String> sourceTypesOf(final String view) {
      return database.getSchema().getMaterializedView(view).getSourceTypeNames();
    }
  }

  /**
   * Regression for #10502: the start of the bucket a timestamp is in, on the grid of fixed-width buckets a watermark is a start of.
   */
  @Nested
  class BucketStart {
    private static final long HOUR = 3_600_000L;

    @Test
    void aTimestampOnTheEpochGridFallsInTheBucketStartingAtOrBeforeIt() {
      assertThat(ContinuousAggregateImpl.bucketStart(10 * HOUR + 5, 20 * HOUR, HOUR)).isEqualTo(10 * HOUR);
      assertThat(ContinuousAggregateImpl.bucketStart(10 * HOUR, 20 * HOUR, HOUR)).isEqualTo(10 * HOUR);
      assertThat(ContinuousAggregateImpl.bucketStart(11 * HOUR - 1, 20 * HOUR, HOUR)).isEqualTo(10 * HOUR);
    }

    @Test
    void theGridIsTheOneTheAnchorIsOn() {
      // buckets of a day starting at 08:00: the anchor is one of their starts
      final long day = 24 * HOUR;
      final long anchor = 100 * day + 8 * HOUR;
      assertThat(ContinuousAggregateImpl.bucketStart(50 * day + 6 * HOUR, anchor, day)).isEqualTo(49 * day + 8 * HOUR);
      assertThat(ContinuousAggregateImpl.bucketStart(50 * day + 8 * HOUR, anchor, day)).isEqualTo(50 * day + 8 * HOUR);
      // a timestamp after the anchor is on the same grid
      assertThat(ContinuousAggregateImpl.bucketStart(anchor + day + 1, anchor, day)).isEqualTo(anchor + day);
    }

    @Test
    void negativeTimestampsAndAnchorsAreFloored() {
      assertThat(ContinuousAggregateImpl.bucketStart(-1, 0, HOUR)).isEqualTo(-HOUR);
      assertThat(ContinuousAggregateImpl.bucketStart(-HOUR, 0, HOUR)).isEqualTo(-HOUR);
      assertThat(ContinuousAggregateImpl.bucketStart(-HOUR - 1, -30 * 60_000L, HOUR)).isEqualTo(-HOUR - 30 * 60_000L);
    }

    @Test
    void anIntervalOfOneIsTheTimestampItself() {
      assertThat(ContinuousAggregateImpl.bucketStart(12_345, 7, 1)).isEqualTo(12_345);
      assertThat(ContinuousAggregateImpl.bucketStart(-12_345, 7, 1)).isEqualTo(-12_345);
    }

    @Test
    void aStartBeforeTheFirstRepresentableInstantIsClampedToIt() {
      // Long.MIN_VALUE is not on the grid anchored at 0 with this interval: its bucket starts before it
      assertThat(Long.MIN_VALUE % HOUR).isNotZero();
      assertThat(ContinuousAggregateImpl.bucketStart(Long.MIN_VALUE, 0, HOUR)).isEqualTo(Long.MIN_VALUE);
      assertThat(ContinuousAggregateImpl.bucketStart(Long.MIN_VALUE + 1, 0, HOUR)).isEqualTo(Long.MIN_VALUE);
    }
  }

  /**
   * Regression for #10634: the watermark of a continuous aggregate is saved with a plain schema save after each refresh, so a node can hold
   * the rows of the backing type (replicated with the refresh transaction, or committed before a schema save that failed) beside a
   * schema whose watermark was never set: a follower promoted to leader before any schema session carried the watermark to it, an
   * aggregate created over a source that was empty at the time. A refresh from an unset watermark used to run the whole defining
   * query and delete nothing, so every bucket the backing type held was added a second time.
   * <p>
   * A refresh that finds the watermark unset and the backing type not empty now takes the newest bucket of the backing type as the
   * watermark first: that is the watermark the refresh that wrote those rows installed.
   */
  @Nested
  class UnsetWatermarkWithRows extends TestHelper {
    private static final long HOUR = 3_600_000L;
    private static final long H0   = 1_700_000_000_000L - (1_700_000_000_000L % HOUR);

    @Test
    void aRefreshFromAnUnsetWatermarkDoesNotDuplicateTheBucketsTheBackingTypeHolds() {
      createAggregateOverAnEmptySource();
      assertThat(aggregate().isWatermarkSet()).as("created over an empty source").isFalse();

      for (int h = 0; h < 3; h++)
        insert(H0 + h * HOUR, 1.0);
      refresh();
      assertThat(buckets()).isEqualTo(Map.of(H0, 1.0, H0 + HOUR, 1.0, H0 + 2 * HOUR, 1.0));

      // the node holds the rows but not the watermark the refresh that wrote them installed
      aggregate().restoreWatermark(0L, false);

      insert(H0 + 2 * HOUR + 1, 2.0);
      refresh();

      assertThat(rowCount()).as("every bucket once").isEqualTo(3L);
      assertThat(buckets()).isEqualTo(Map.of(H0, 1.0, H0 + HOUR, 1.0, H0 + 2 * HOUR, 3.0));
      assertThat(aggregate().isWatermarkSet()).isTrue();
      assertThat(aggregate().getWatermarkTs()).isEqualTo(H0 + 2 * HOUR);
    }

    /** The same state read from the schema file, as a promoted follower or a restart after a failed schema save finds it. */
    @Test
    void aRefreshAfterAReopenWithAnUnsetWatermarkDoesNotDuplicateTheBuckets() {
      createAggregateOverAnEmptySource();
      for (int h = 0; h < 3; h++)
        insert(H0 + h * HOUR, 1.0);
      refresh();

      aggregate().restoreWatermark(0L, false);
      ((LocalSchema) database.getSchema()).saveConfiguration();
      reopenDatabase();
      assertThat(aggregate().isWatermarkSet()).as("the schema file holds no watermark").isFalse();

      refresh();

      assertThat(rowCount()).as("every bucket once").isEqualTo(3L);
      assertThat(buckets()).isEqualTo(Map.of(H0, 1.0, H0 + HOUR, 1.0, H0 + 2 * HOUR, 1.0));
      assertThat(aggregate().getWatermarkTs()).isEqualTo(H0 + 2 * HOUR);

      // ... and the watermark it took is saved
      reopenDatabase();
      assertThat(aggregate().isWatermarkSet()).isTrue();
      assertThat(aggregate().getWatermarkTs()).isEqualTo(H0 + 2 * HOUR);
    }

    /** An aggregate whose backing type is empty keeps reading every sample on its first refresh, as it always did. */
    @Test
    void anUnsetWatermarkWithAnEmptyBackingTypeStillReadsEverySample() {
      createAggregateOverAnEmptySource();
      refresh();
      assertThat(aggregate().isWatermarkSet()).isFalse();
      assertThat(rowCount()).isZero();

      insert(H0, 1.0);
      insert(H0 + HOUR, 2.0);
      refresh();

      assertThat(buckets()).isEqualTo(Map.of(H0, 1.0, H0 + HOUR, 2.0));
      assertThat(aggregate().getWatermarkTs()).isEqualTo(H0 + HOUR);
    }

    private void createAggregateOverAnEmptySource() {
      database.command("sql", "CREATE TIMESERIES TYPE S TIMESTAMP ts TAGS (h STRING) FIELDS (v DOUBLE)");
      database.command("sql", "CREATE CONTINUOUS AGGREGATE A AS SELECT ts.timeBucket('1h', ts) AS hr, sum(v) AS total FROM S GROUP BY hr");
    }

    private void insert(final long ts, final double v) {
      database.command("sql", "INSERT INTO S SET ts = ?, h = 'a', v = ?", ts, v).close();
    }

    private void refresh() {
      database.command("sql", "REFRESH CONTINUOUS AGGREGATE A").close();
    }

    private ContinuousAggregateImpl aggregate() {
      return (ContinuousAggregateImpl) database.getSchema().getContinuousAggregate("A");
    }

    private long rowCount() {
      try (final ResultSet rs = database.query("sql", "SELECT count(*) AS c FROM A")) {
        return rs.next().<Number>getProperty("c").longValue();
      }
    }

    private Map<Long, Double> buckets() {
      final Map<Long, Double> buckets = new HashMap<>();
      try (final ResultSet rs = database.query("sql", "SELECT hr, total FROM A")) {
        while (rs.hasNext()) {
          final Result row = rs.next();
          buckets.merge(DateUtils.toEpochMillis(row.getProperty("hr")), row.<Number>getProperty("total").doubleValue(), Double::sum);
        }
      }
      return buckets;
    }
  }

  /**
   * Regression for #10673: under HA the record of the samples an append adds behind the watermark of a continuous aggregate ships to the
   * followers in a schema session BEFORE the samples commit, so a leader lost right after the commit has shipped it already. The check
   * made again after the commit ships a second session only when the record changed since, or the first session failed.
   * <p>
   * The database here is served through a wrapper that reports itself replicated, as the Raft wrapper does, and that counts the schema
   * sessions the append opens and the samples the type holds at each: a session that sees 3 samples ran before the commit of the 4th.
   */
  @Nested
  class LateSampleRecordBeforeCommit extends TestHelper {
    private static final long HOUR = 3_600_000L;
    private static final long H0   = 1_700_000_000_000L - (1_700_000_000_000L % HOUR);

    /** The samples the type held when each schema session opened, in order. */
    private final List<Long> samplesAtEachSession = new ArrayList<>();

    @BeforeEach
    void createAggregateWithAWatermark() {
      database.command("sql", "CREATE TIMESERIES TYPE sensor TIMESTAMP ts TAGS (host STRING) FIELDS (v DOUBLE) SHARDS 1").close();
      database.command("sql",
          "CREATE CONTINUOUS AGGREGATE hourly AS SELECT ts.timeBucket('1h', ts) AS hr, sum(v) AS total FROM sensor GROUP BY hr").close();
      for (int h = 0; h < 3; h++)
        database.command("sql", "INSERT INTO sensor SET ts = ?, host = 'h0', v = 1", H0 + h * HOUR).close();
      database.command("sql", "REFRESH CONTINUOUS AGGREGATE hourly").close();
      assertThat(aggregate().getWatermarkTs()).isEqualTo(H0 + 2 * HOUR);
      assertThat(aggregate().getInvalidation()).isNull();
    }

    @Test
    void aLateSampleShipsOneSessionBeforeItsCommitAndNoneAfter() throws Exception {
      servedAsReplicated(session -> { }, () -> {
        appendAt(H0 + 60_000L);
        assertThat(samplesAtEachSession).as("one session, before the commit of the 4th sample").containsExactly(3L);
        assertThat(aggregate().getInvalidation()).isNotNull();
        assertThat(aggregate().getInvalidation().fromTs()).isEqualTo(H0);

        // a stream of late samples into a bucket already recorded ships nothing more, before or after the commit
        appendAt(H0 + 120_000L);
        // and nor does a sample at or after the watermark
        appendAt(H0 + 2 * HOUR + 1);
        assertThat(samplesAtEachSession).containsExactly(3L);
      });
    }

    @Test
    void aSessionThatFailsBeforeTheCommitIsShippedAfterIt() throws Exception {
      servedAsReplicated(session -> {
        if (session == 0)
          throw new IllegalStateException("simulated failure of the replication session");
      }, () -> {
        // the append is not reported as failed
        appendAt(H0 + 60_000L);
        assertThat(samplesAtEachSession).as("the failed session before the commit, then the one after it").containsExactly(3L, 4L);
        assertThat(aggregate().getInvalidation()).isNotNull();
      });
    }

    @Test
    void aRecordClearedBeforeTheCommitIsRecordedAndShippedAgainAfterIt() throws Exception {
      servedAsReplicated(session -> {
        if (session == 0) {
          // what a refresh that read the source before the commit does once it has recomputed the buckets: it clears the record
          final ContinuousAggregateImpl.Invalidation read = aggregate().getInvalidation();
          assertThat(read).isNotNull();
          aggregate().clearInvalidation(read);
        }
      }, () -> {
        appendAt(H0 + 60_000L);
        assertThat(samplesAtEachSession).as("shipped before the commit, then again after it").containsExactly(3L, 4L);
        assertThat(aggregate().getInvalidation()).as("recorded again once the sample committed").isNotNull();
        assertThat(aggregate().getInvalidation().fromTs()).isEqualTo(H0);
      });
    }

    private interface Body {
      void run() throws Exception;
    }

    /**
     * Runs {@code body} with the database served through a wrapper that reports itself replicated. Every schema session opens through
     * the real database, after {@code afterSession} is told its ordinal; a failure it throws is the session's.
     */
    private void servedAsReplicated(final IntConsumer afterSession, final Body body) throws Exception {
      final LocalDatabase local = (LocalDatabase) ((DatabaseInternal) database).getEmbedded();
      final DatabaseInternal[] self = new DatabaseInternal[1];
      final InvocationHandler handler = (proxy, method, args) -> {
        switch (method.getName()) {
        case "isReplicated":
          return true;
        case "getWrappedDatabaseInstance":
          return self[0];
        case "recordFileChanges":
          final int session = samplesAtEachSession.size();
          samplesAtEachSession.add(samples());
          final Object result = invoke(local, method, args);
          afterSession.accept(session);
          return result;
        default:
          return invoke(local, method, args);
        }
      };
      self[0] = (DatabaseInternal) Proxy.newProxyInstance(DatabaseInternal.class.getClassLoader(),
          new Class<?>[] { DatabaseInternal.class }, handler);
      local.setWrappedDatabaseInstance(self[0]);
      try {
        body.run();
      } finally {
        local.setWrappedDatabaseInstance(local);
      }
    }

    private static Object invoke(final Object target, final Method method, final Object[] args) throws Throwable {
      try {
        return method.invoke(target, args);
      } catch (final InvocationTargetException e) {
        throw e.getCause();
      }
    }

    private void appendAt(final long ts) throws Exception {
      // Without the refresh every ingest path runs once the samples commit (#10686): it recomputes the buckets and clears the record
      // this test asserts on, which is not what ships the record to the followers
      engine().appendSamplesWithoutRefresh(new long[] { ts }, new Object[] { "h0" }, new Object[] { 1.0 });
    }

    private long samples() {
      try (final ResultSet rs = database.query("sql", "SELECT count(*) AS c FROM sensor")) {
        return rs.next().<Number>getProperty("c").longValue();
      }
    }

    private TimeSeriesEngine engine() {
      return ((LocalTimeSeriesType) database.getSchema().getType("sensor")).getEngine();
    }

    private ContinuousAggregateImpl aggregate() {
      return (ContinuousAggregateImpl) database.getSchema().getContinuousAggregate("hourly");
    }
  }

  /**
   * Regression for #10804: every ingest request refreshes the continuous aggregates over its type, and a refresh deleted the buckets at or
   * after the watermark before it recomputed them. The backing type had no index, so that delete read every row the aggregate ever
   * wrote: measured with {@code IngestRefreshCostBenchmark}, an autocommit INSERT into the open bucket of an aggregate holding
   * 100k rows cost 9 ms, 400 times the INSERT alone, and the cost grew with the history of the aggregate, not with the bucket. The open
   * bucket itself, the cost the issue was filed for, is 10 to 30 times the INSERT and bounded by the bucket.
   * <p>
   * The backing type is indexed on its bucket column now and both reads of the rows a refresh replaces are served by it. The rows are
   * rewritten in place rather than deleted and inserted again: with the index, the delete and insert of the open bucket's rows under one
   * key on every request left a tombstone and an entry per request that every lookup of the bucket walked (35 ms a request after 20k).
   * An aggregate created before is named at open with the statements that index it.
   *
   * @author Luca Garulli (l.garulli@arcadedata.com)
   */
  @Nested
  class ContinuousAggregateBucketIndex extends TestHelper {
    private static final long HOUR = 3_600_000L;
    private static final long H0   = 1_700_000_000_000L - (1_700_000_000_000L % HOUR);

    private void createSourceAndAggregate() {
      database.command("sql", "CREATE TIMESERIES TYPE S TIMESTAMP ts TAGS (h STRING) FIELDS (v DOUBLE) SHARDS 1");
      database.command("sql",
          "CREATE CONTINUOUS AGGREGATE A AS SELECT ts.timeBucket('1h', ts) AS hr, h, sum(v) AS total FROM S GROUP BY hr, h");
    }

    private ContinuousAggregateImpl aggregate() {
      return (ContinuousAggregateImpl) database.getSchema().getContinuousAggregate("A");
    }

    private void insert(final long ts, final String tag, final double v) {
      database.command("sql", "INSERT INTO S SET ts = ?, h = ?, v = ?", ts, tag, v);
    }

    private record Bucket(long start, String tag, double total) {
    }

    private List<Bucket> buckets() {
      final List<Bucket> result = new ArrayList<>();
      try (final ResultSet rs = database.query("sql", "SELECT FROM A ORDER BY hr, h")) {
        while (rs.hasNext()) {
          final Result row = rs.next();
          result.add(new Bucket(DateUtils.toEpochMillis(row.getProperty("hr")), row.getProperty("h"),
              ((Number) row.getProperty("total")).doubleValue()));
        }
      }
      return result;
    }

    @Test
    void theBackingTypeIsIndexedOnItsBucketColumn() {
      createSourceAndAggregate();

      final DocumentType backing = aggregate().getBackingType();
      assertThat(backing.getProperty("hr").getType()).isEqualTo(Type.DATETIME);
      final TypeIndex index = backing.getPolymorphicIndexByProperties("hr");
      assertThat(index).isNotNull();
      assertThat(index.getType()).isEqualTo(Schema.INDEX_TYPE.LSM_TREE);
      assertThat(index.isUnique()).isFalse();
      assertThat(aggregate().getUpgradeWarning()).isNull();
    }

    /** Both reads of the rows a refresh replaces are the index's, so neither reads the buckets the refresh has no reason to look at. */
    @Test
    void bothReadsOfTheReplacedRowsAreServedByTheIndex() {
      createSourceAndAggregate();

      assertThat(plan(ContinuousAggregateRefresher.bucketsFromSelect(aggregate()), new Date(H0))).contains("FETCH FROM INDEX");
      assertThat(plan(ContinuousAggregateRefresher.bucketRangeSelect(aggregate()), new Date(H0), new Date(H0 + HOUR)))
          .contains("FETCH FROM INDEX");
    }

    /**
     * The row of the open bucket is the same record however many requests recompute it, and so is the row of a bucket a late sample
     * recomputes: its bucket key never changes, so the index on it is not written, and no request leaves a hole in the backing type.
     */
    @Test
    void aRecomputedBucketRewritesItsRowInPlace() {
      createSourceAndAggregate();
      insert(H0 + 1_000, "a", 1);
      final RID openBucketRow = rowOf(H0);
      final long deletedBefore = statistic("deleteRecord");
      final long createdBefore = statistic("createRecord");
      for (int i = 0; i < 50; i++)
        insert(H0 + 2_000 + i, "a", 1);
      // a delete and an insert of the row would reuse its freed slot as well: the RID alone does not tell the two apart
      assertThat(statistic("deleteRecord") - deletedBefore).as("rows deleted by 50 refreshes of the open bucket").isZero();
      assertThat(statistic("createRecord") - createdBefore).as("rows created by 50 refreshes of the open bucket").isZero();
      assertThat(rowOf(H0)).isEqualTo(openBucketRow);
      assertThat(buckets()).containsExactly(new Bucket(H0, "a", 51));

      // a new bucket moves the watermark; a late sample then recomputes the closed bucket, through the range of the late samples
      insert(H0 + 2 * HOUR, "a", 7);
      final long deletedBeforeLate = statistic("deleteRecord");
      insert(H0 + 3_000_000, "a", 9);
      assertThat(statistic("deleteRecord") - deletedBeforeLate).as("rows deleted by the recompute of a late sample's bucket").isZero();
      assertThat(rowOf(H0)).isEqualTo(openBucketRow);
      assertThat(buckets()).containsExactly(new Bucket(H0, "a", 60), new Bucket(H0 + 2 * HOUR, "a", 7));
      assertThat(database.countType("A", false)).isEqualTo(2L);
    }

    /**
     * A recomputed range whose buckets gain rows and lose rows: a late sample of a tag new to a closed bucket adds a row to it (no
     * replaced row left to take over, so it is inserted), a delete of every sample of a tag in another closed bucket removes one (a
     * replaced row nothing took over, so it is deleted), and the open bucket does both in turn.
     */
    @Test
    void aRecomputedBucketThatGainsOrLosesRowsInsertsOrDeletesOnlyThose() {
      createSourceAndAggregate();
      for (int hour = 0; hour < 3; hour++) {
        insert(H0 + hour * HOUR + 1_000, "a", 1);
        insert(H0 + hour * HOUR + 2_000, "b", 10);
      }
      insert(H0 + 4 * HOUR + 1_000, "a", 100);

      // grows: tag c is new to hour 0, behind the watermark
      insert(H0 + 5_000, "c", 1000);
      // shrinks: every sample of tag b in hour 1 is deleted
      database.command("sql", "DELETE FROM S WHERE ts >= ? AND ts < ? AND h = 'b'", H0 + HOUR, H0 + 2 * HOUR);
      assertThat(buckets()).containsExactly(new Bucket(H0, "a", 1), new Bucket(H0, "b", 10), new Bucket(H0, "c", 1000),
          new Bucket(H0 + HOUR, "a", 1), new Bucket(H0 + 2 * HOUR, "a", 1), new Bucket(H0 + 2 * HOUR, "b", 10),
          new Bucket(H0 + 4 * HOUR, "a", 100));

      // the open bucket gains a tag, then loses it
      insert(H0 + 4 * HOUR + 2_000, "d", 5);
      assertThat(buckets().subList(6, 8)).containsExactly(new Bucket(H0 + 4 * HOUR, "a", 100), new Bucket(H0 + 4 * HOUR, "d", 5));
      database.command("sql", "DELETE FROM S WHERE ts >= ? AND h = 'd'", H0 + 4 * HOUR);
      assertThat(buckets()).hasSize(7).last().isEqualTo(new Bucket(H0 + 4 * HOUR, "a", 100));

      // and a full refresh agrees with what the incremental ones left
      final List<Bucket> incremental = buckets();
      database.command("sql", "DROP CONTINUOUS AGGREGATE A");
      database.command("sql",
          "CREATE CONTINUOUS AGGREGATE A AS SELECT ts.timeBucket('1h', ts) AS hr, h, sum(v) AS total FROM S GROUP BY hr, h");
      assertThat(buckets()).isEqualTo(incremental);
    }

    /** A creation that fails after its backing type was made, here in the first refresh, leaves neither the aggregate nor the type. */
    @Test
    void aFailedCreationLeavesNoBackingTypeBehind() {
      database.command("sql", "CREATE TIMESERIES TYPE S TIMESTAMP ts TAGS (h STRING) FIELDS (v DOUBLE) SHARDS 1");
      insert(H0 + 1_000, "not a number", 1);

      final Throwable failure = catchThrowable(() -> database.command("sql",
          "CREATE CONTINUOUS AGGREGATE A AS SELECT ts.timeBucket('1h', ts) AS hr, h.asLong() AS n, sum(v) AS total FROM S GROUP BY hr, n"));

      assertThat(failure).as("the first refresh has to fail, or this test proves nothing").isNotNull();
      assertThat(database.getSchema().existsContinuousAggregate("A")).isFalse();
      assertThat(database.getSchema().existsType("A")).isFalse();
      assertThat(database.getSchema().getIndexes()).noneMatch(index -> index.getName().startsWith("A["));
    }

    private long statistic(final String name) {
      return ((Number) ((DatabaseInternal) database).getStats().get(name)).longValue();
    }

    private RID rowOf(final long bucketStart) {
      try (final ResultSet rs = database.query("sql", "SELECT @rid AS rid FROM A WHERE hr = ?", new Date(bucketStart))) {
        return rs.next().getProperty("rid");
      }
    }

    /**
     * What the aggregate holds is the same as before: the open bucket is recomputed by every INSERT, a new bucket is added, a late
     * sample recomputes its own bucket (the range replacement), a delete of the source too, and all of it survives a reopen.
     */
    @Test
    void theRefreshKeepsEveryBucketItHeld() {
      createSourceAndAggregate();
      for (int hour = 0; hour < 5; hour++)
        insert(H0 + hour * HOUR + 1_000, "a", hour + 1);
      insert(H0 + 4 * HOUR + 2_000, "a", 10);
      insert(H0 + 4 * HOUR + 3_000, "b", 100);
      // late, behind the watermark
      insert(H0 + HOUR + 5_000, "a", 20);
      database.command("sql", "DELETE FROM S WHERE ts >= ? AND ts < ?", H0 + 2 * HOUR, H0 + 3 * HOUR);

      final List<Bucket> expected = List.of(new Bucket(H0, "a", 1), new Bucket(H0 + HOUR, "a", 22), new Bucket(H0 + 3 * HOUR, "a", 4),
          new Bucket(H0 + 4 * HOUR, "a", 15), new Bucket(H0 + 4 * HOUR, "b", 100));
      assertThat(buckets()).isEqualTo(expected);

      reopenDatabase();
      assertThat(buckets()).isEqualTo(expected);
      assertThat(aggregate().getBackingType().getPolymorphicIndexByProperties("hr")).isNotNull();
      insert(H0 + 4 * HOUR + 4_000, "b", 1);
      assertThat(buckets().getLast()).isEqualTo(new Bucket(H0 + 4 * HOUR, "b", 101));
    }

    /**
     * An aggregate created before #10804 has neither the property nor the index. It is not altered at open (under HA every node would
     * make that schema change on its own), it is named, with the statements that index it, and they work as written.
     */
    @Test
    void anAggregateWithoutTheIndexIsNamedWithTheStatementsThatIndexIt() {
      createSourceAndAggregate();
      insert(H0 + 1_000, "a", 1);
      // the shape of an aggregate created before #10804
      database.getSchema().dropIndex(aggregate().getBackingType().getPolymorphicIndexByProperties("hr").getName());
      database.getSchema().getType("A").dropProperty("hr");

      // as an older version left it on disk: the open names it, and the refresh still works without the index
      reopenDatabase();
      final String warning = aggregate().getUpgradeWarning();
      assertThat(warning).contains("'A'").contains("'hr'")
          .endsWith("Run: CREATE PROPERTY `A`.`hr` DATETIME; CREATE INDEX ON `A` (`hr`) NOTUNIQUE");
      insert(H0 + 2_000, "a", 2);
      assertThat(buckets()).containsExactly(new Bucket(H0, "a", 3));

      for (final String statement : warning.substring(warning.indexOf("Run: ") + 5).split("; "))
        database.command("sql", statement);

      assertThat(aggregate().getUpgradeWarning()).isNull();
      assertThat(plan(ContinuousAggregateRefresher.bucketsFromSelect(aggregate()), new Date(H0))).contains("FETCH FROM INDEX");
      // the rows written before the property was declared are found through the index
      insert(H0 + 3_000, "a", 4);
      assertThat(buckets()).containsExactly(new Bucket(H0, "a", 7));
      assertThat(database.query("sql", "SELECT count(*) AS n FROM A WHERE hr >= ?", new Date(H0)).next().<Long>getProperty("n"))
          .isEqualTo(1L);
    }

    /** An index other than the one the creation builds serves the deletes as well, when the bucket column leads it. */
    @Test
    void anIndexLedByTheBucketColumnIsEnough() {
      createSourceAndAggregate();
      database.getSchema().dropIndex(aggregate().getBackingType().getPolymorphicIndexByProperties("hr").getName());
      assertThat(aggregate().getUpgradeWarning()).endsWith("Run: CREATE INDEX ON `A` (`hr`) NOTUNIQUE");

      database.command("sql", "CREATE PROPERTY A.h STRING");
      database.command("sql", "CREATE INDEX ON A (hr, h) NOTUNIQUE");
      assertThat(aggregate().getUpgradeWarning()).isNull();

      database.command("sql", "DROP INDEX `A[hr,h]`");
      database.command("sql", "CREATE INDEX ON A (h, hr) NOTUNIQUE");
      assertThat(aggregate().getUpgradeWarning()).as("an index the bucket column does not lead cannot serve a range on it").isNotNull();
    }

    /**
     * The index is built inside the schema frame of the creation, and an index build waits for the async workers to drain, which need
     * the write lock of that frame to commit their batch. A hang detector, not a latency bound.
     */
    @Test
    @Timeout(value = 30, unit = TimeUnit.SECONDS)
    void theCreationDoesNotStallWhileTheAsyncExecutorHoldsABatch() {
      database.getSchema().createDocumentType("Batch");
      for (int i = 0; i < 100; i++)
        database.async().createRecord(database.newDocument("Batch").set("i", i), null);

      createSourceAndAggregate();

      database.async().waitCompletion();
      assertThat(database.countType("Batch", false)).isEqualTo(100L);
      assertThat(aggregate().getBackingType().getPolymorphicIndexByProperties("hr")).isNotNull();
    }

    private String plan(final String statement, final Object... params) {
      try (final ResultSet rs = database.command("sql", "EXPLAIN " + statement, params)) {
        return rs.next().getProperty("executionPlanAsString");
      }
    }
  }

  /**
   * Regression for #10857: under HA an append checks whether its samples are behind the watermark of a continuous aggregate BEFORE it commits,
   * to ship their record to the followers ahead of them (#10673). A refresh that began after that check and ended before the commit
   * moved the watermark past the samples (from newer samples already committed): they became late only once committed, and their
   * record shipped in a session that followed the commit, the shape of #10673. The refresh now keeps its watermark at or before the
   * bucket of the oldest sample of an append in flight, so the samples are never late and no session follows the commit.
   * <p>
   * The database is served through a wrapper that reports itself replicated, as the Raft wrapper does. It counts the schema sessions the
   * appending thread opens and the samples the type holds at each, and runs a refresh in another thread when the append commits, after
   * its check for late samples and before its samples are visible.
   */
  @Nested
  class RefreshDuringAppendCommit extends TestHelper {
    private static final long HOUR = 3_600_000L;
    private static final long H0   = 1_700_000_000_000L - (1_700_000_000_000L % HOUR);

    private record Bucket(long start, double total) {
    }

    /** The samples the type held when each schema session of the appending thread opened, in order. */
    private final List<Long> samplesAtEachSession = new ArrayList<>();

    @BeforeEach
    void createAggregateWithAWatermarkAndANewerSample() throws Exception {
      database.command("sql", "CREATE TIMESERIES TYPE sensor TIMESTAMP ts TAGS (host STRING) FIELDS (v DOUBLE) SHARDS 1").close();
      database.command("sql",
          "CREATE CONTINUOUS AGGREGATE hourly AS SELECT ts.timeBucket('1h', ts) AS hr, sum(v) AS total FROM sensor GROUP BY hr").close();
      for (int h = 0; h < 3; h++)
        engine().appendSamplesWithoutRefresh(new long[] { H0 + h * HOUR }, new Object[] { "h0" }, new Object[] { 1.0 });
      refresh();
      assertThat(aggregate().getWatermarkTs()).isEqualTo(H0 + 2 * HOUR);
      // committed and not refreshed yet: the next refresh reads it and would move the watermark to its bucket
      engine().appendSamplesWithoutRefresh(new long[] { H0 + 3 * HOUR + 1_000 }, new Object[] { "h0" }, new Object[] { 1.0 });
    }

    /** The repro of the issue: the sample is at the watermark when the append checks it, the refresh would move it one bucket past. */
    @Test
    void aRefreshDuringTheCommitDoesNotMoveTheWatermarkPastTheSamples() throws Exception {
      final AtomicLong watermarkAfterRefresh = new AtomicLong();
      servedAsReplicated(() -> watermarkAfterRefresh.set(aggregate().getWatermarkTs()),
          () -> engine().appendSamplesWithoutRefresh(new long[] { H0 + 2 * HOUR + 60_000 }, new Object[] { "h0" },
              new Object[] { 1.0 }));

      assertThat(watermarkAfterRefresh.get()).as("kept at the bucket of the sample in flight").isEqualTo(H0 + 2 * HOUR);
      assertThat(samplesAtEachSession).as("the sample is not late, nothing ships after its commit").isEmpty();
      assertThat(aggregate().getInvalidation()).isNull();
      assertThat(engine().oldestInFlightAppendTs()).as("the append is in flight no more").isEqualTo(Long.MAX_VALUE);

      refresh();
      assertThat(buckets()).containsExactly(new Bucket(H0, 1), new Bucket(H0 + HOUR, 1), new Bucket(H0 + 2 * HOUR, 2),
          new Bucket(H0 + 3 * HOUR, 1));
      assertThat(aggregate().getWatermarkTs()).isEqualTo(H0 + 3 * HOUR);
    }

    /** The watermark a refresh adopts from the rows of the backing type (#10634) is kept at that bucket too. */
    @Test
    void aWatermarkAdoptedDuringTheCommitIsNotPastTheSamples() throws Exception {
      refresh();
      assertThat(aggregate().getWatermarkTs()).isEqualTo(H0 + 3 * HOUR);
      // the rows of the backing type beside a watermark never set: what a promoted follower holds
      aggregate().restoreWatermark(0, false);

      final AtomicLong watermarkAfterRefresh = new AtomicLong();
      servedAsReplicated(() -> watermarkAfterRefresh.set(aggregate().getWatermarkTs()),
          () -> engine().appendSamplesWithoutRefresh(new long[] { H0 + 2 * HOUR + 60_000 }, new Object[] { "h0" },
              new Object[] { 1.0 }));

      assertThat(watermarkAfterRefresh.get()).as("adopted at the bucket of the sample in flight").isEqualTo(H0 + 2 * HOUR);
      assertThat(samplesAtEachSession).as("the sample is not late, nothing ships after its commit").isEmpty();

      refresh();
      assertThat(buckets()).containsExactly(new Bucket(H0, 1), new Bucket(H0 + HOUR, 1), new Bucket(H0 + 2 * HOUR, 2),
          new Bucket(H0 + 3 * HOUR, 1));
    }

    /**
     * Found by the adversarial pass of the fix: the append registers while a refresh is already in progress, after the refresh read the
     * appends in flight. It finds the refresh in progress and records its sample (#10633), and the refresh, which read the source
     * before the commit, must not clear that record: cleared, the check made after the commit finds the sample behind the new watermark
     * and ships its record after the commit.
     */
    @Test
    void aRefreshInProgressDoesNotClearTheRecordOfAnAppendRegisteredAfterIt() throws Exception {
      final AtomicLong watermarkAfterRefresh = new AtomicLong();
      refreshWithAnAppendRegisteredMidway(() -> watermarkAfterRefresh.set(aggregate().getWatermarkTs()), H0 + 2 * HOUR + 60_000);

      assertThat(watermarkAfterRefresh.get()).as("the refresh did not find the append, nothing clips it").isEqualTo(H0 + 3 * HOUR);
      assertThat(samplesAtEachSession).as("one session, before the commit, and none after it").containsExactly(4L);
      assertThat(aggregate().getInvalidation()).as("the record of the sample survives the refresh").isNotNull();
      assertThat(engine().oldestInFlightAppendTs()).isEqualTo(Long.MAX_VALUE);

      refresh();
      assertThat(buckets()).containsExactly(new Bucket(H0, 1), new Bucket(H0 + HOUR, 1), new Bucket(H0 + 2 * HOUR, 2),
          new Bucket(H0 + 3 * HOUR, 1));
      assertThat(aggregate().getInvalidation()).isNull();
    }

    /**
     * A sample already late when the append checks it: its record ships before the commit. A refresh that begins after the check and
     * reads the source before the commit finds the append in flight, so it keeps the record instead of clearing it: cleared, the check
     * made after the commit would record the sample again and ship that record after the commit.
     */
    @Test
    void aRefreshDuringTheCommitKeepsTheRecordOfALateSample() throws Exception {
      servedAsReplicated(() -> assertThat(aggregate().getInvalidation()).as("kept by the refresh").isNotNull(),
          () -> engine().appendSamplesWithoutRefresh(new long[] { H0 + HOUR + 60_000 }, new Object[] { "h0" },
              new Object[] { 1.0 }));

      assertThat(samplesAtEachSession).as("one session, before the commit, and none after it").containsExactly(4L);

      refresh();
      assertThat(buckets()).containsExactly(new Bucket(H0, 1), new Bucket(H0 + HOUR, 2), new Bucket(H0 + 2 * HOUR, 1),
          new Bucket(H0 + 3 * HOUR, 1));
      assertThat(aggregate().getInvalidation()).isNull();
    }

    @Test
    void aRecordIsKeptWhenAnAppendInFlightMayHaveAddedToIt() {
      final ContinuousAggregateImpl.Invalidation range = new ContinuousAggregateImpl.Invalidation(H0, H0 + HOUR - 1);
      assertThat(ContinuousAggregateRefresher.mayHoldInFlightSamples(range, Long.MAX_VALUE)).isFalse();
      assertThat(ContinuousAggregateRefresher.mayHoldInFlightSamples(range, H0 + HOUR)).isFalse();
      assertThat(ContinuousAggregateRefresher.mayHoldInFlightSamples(range, H0 + HOUR - 1)).isTrue();
      assertThat(ContinuousAggregateRefresher.mayHoldInFlightSamples(range, H0 - HOUR)).isTrue();
    }

    @Test
    void theWatermarkIsKeptAtTheBucketOfTheOldestSampleInFlight() {
      assertThat(ContinuousAggregateRefresher.clipToInFlightAppends(H0 + 3 * HOUR, Long.MAX_VALUE, HOUR)).isEqualTo(H0 + 3 * HOUR);
      assertThat(ContinuousAggregateRefresher.clipToInFlightAppends(H0 + 3 * HOUR, H0 + 3 * HOUR, HOUR)).isEqualTo(H0 + 3 * HOUR);
      assertThat(ContinuousAggregateRefresher.clipToInFlightAppends(H0 + 3 * HOUR, H0 + HOUR + 1, HOUR)).isEqualTo(H0 + HOUR);
      // on the grid of the watermark, not the epoch's
      assertThat(ContinuousAggregateRefresher.clipToInFlightAppends(H0 + 3 * HOUR + 60_000, H0 + HOUR + 1, HOUR)).isEqualTo(
          H0 + 60_000);
      // no grid: the sample itself
      assertThat(ContinuousAggregateRefresher.clipToInFlightAppends(H0 + 3 * HOUR, H0 + HOUR + 1, 0)).isEqualTo(H0 + HOUR + 1);
    }

    private interface Body {
      void run() throws Exception;
    }

    /**
     * Runs {@code body} with the database served through a wrapper that reports itself replicated. The first commit of the thread
     * running {@code body} first runs a REFRESH in another thread, waits for it to end and runs {@code afterRefresh}.
     */
    private void servedAsReplicated(final Runnable afterRefresh, final Body body) throws Exception {
      final LocalDatabase local = (LocalDatabase) ((DatabaseInternal) database).getEmbedded();
      final DatabaseInternal[] self = new DatabaseInternal[1];
      final Thread appending = Thread.currentThread();
      final AtomicBoolean refreshed = new AtomicBoolean();
      final AtomicReference<Throwable> refreshFailure = new AtomicReference<>();
      final InvocationHandler handler = (proxy, method, args) -> {
        final boolean ownThread = Thread.currentThread() == appending;
        switch (method.getName()) {
        case "isReplicated":
          return true;
        case "getWrappedDatabaseInstance":
          return self[0];
        case "commit":
          if (ownThread && refreshed.compareAndSet(false, true)) {
            final Thread refresher = new Thread(() -> {
              try {
                refresh();
              } catch (final Throwable t) {
                refreshFailure.set(t);
              }
            });
            refresher.start();
            refresher.join(30_000);
            assertThat(refresher.isAlive()).isFalse();
            afterRefresh.run();
          }
          return invoke(local, method, args);
        case "recordFileChanges":
          if (ownThread)
            samplesAtEachSession.add(samples());
          return invoke(local, method, args);
        default:
          return invoke(local, method, args);
        }
      };
      self[0] = (DatabaseInternal) Proxy.newProxyInstance(DatabaseInternal.class.getClassLoader(),
          new Class<?>[] { DatabaseInternal.class }, handler);
      local.setWrappedDatabaseInstance(self[0]);
      try {
        body.run();
      } finally {
        local.setWrappedDatabaseInstance(local);
      }
      assertThat(refreshed.get()).as("the refresh ran during the commit").isTrue();
      assertThat(refreshFailure.get()).isNull();
    }

    /**
     * Runs a REFRESH in this thread, served through a wrapper that reports itself replicated. Right after the refresh read the appends
     * in flight, another thread appends a sample at {@code ts}: it checks for late samples, then waits in its commit until the refresh
     * has ended and {@code afterRefresh} has run.
     */
    private void refreshWithAnAppendRegisteredMidway(final Runnable afterRefresh, final long ts) throws Exception {
      final LocalDatabase local = (LocalDatabase) ((DatabaseInternal) database).getEmbedded();
      final DatabaseInternal[] self = new DatabaseInternal[1];
      final AtomicReference<Thread> appending = new AtomicReference<>();
      final CountDownLatch inCommit = new CountDownLatch(1);
      final CountDownLatch refreshEnded = new CountDownLatch(1);
      final AtomicReference<Throwable> appendFailure = new AtomicReference<>();
      final InvocationHandler handler = (proxy, method, args) -> {
        final boolean appendingThread = Thread.currentThread() == appending.get();
        switch (method.getName()) {
        case "isReplicated":
          return true;
        case "getWrappedDatabaseInstance":
          return self[0];
        case "commit":
          if (appendingThread) {
            inCommit.countDown();
            assertThat(refreshEnded.await(30, TimeUnit.SECONDS)).isTrue();
          }
          return invoke(local, method, args);
        case "recordFileChanges":
          if (appendingThread)
            samplesAtEachSession.add(samples());
          return invoke(local, method, args);
        default:
          return invoke(local, method, args);
        }
      };
      self[0] = (DatabaseInternal) Proxy.newProxyInstance(DatabaseInternal.class.getClassLoader(),
          new Class<?>[] { DatabaseInternal.class }, handler);
      final Thread appender = new Thread(() -> {
        try {
          engine().appendSamplesWithoutRefresh(new long[] { ts }, new Object[] { "h0" }, new Object[] { 1.0 });
        } catch (final Throwable t) {
          appendFailure.set(t);
          inCommit.countDown();
        }
      });
      appending.set(appender);
      final AtomicBoolean started = new AtomicBoolean();
      ContinuousAggregateRefresher.TEST_AFTER_IN_FLIGHT_READ_HOOK = () -> {
        if (!started.compareAndSet(false, true))
          return;
        appender.start();
        try {
          assertThat(inCommit.await(30, TimeUnit.SECONDS)).isTrue();
        } catch (final InterruptedException e) {
          Thread.currentThread().interrupt();
          throw new IllegalStateException(e);
        }
      };
      local.setWrappedDatabaseInstance(self[0]);
      try {
        refresh();
        afterRefresh.run();
      } finally {
        ContinuousAggregateRefresher.TEST_AFTER_IN_FLIGHT_READ_HOOK = null;
        refreshEnded.countDown();
        appender.join(30_000);
        local.setWrappedDatabaseInstance(local);
      }
      assertThat(started.get()).as("the append ran during the refresh").isTrue();
      assertThat(appender.isAlive()).isFalse();
      assertThat(appendFailure.get()).isNull();
    }

    private static Object invoke(final Object target, final Method method, final Object[] args) throws Throwable {
      try {
        return method.invoke(target, args);
      } catch (final InvocationTargetException e) {
        throw e.getCause();
      }
    }

    private void refresh() {
      database.command("sql", "REFRESH CONTINUOUS AGGREGATE hourly").close();
    }

    private long samples() {
      try (final ResultSet rs = database.query("sql", "SELECT count(*) AS c FROM sensor")) {
        return rs.next().<Number>getProperty("c").longValue();
      }
    }

    private List<Bucket> buckets() {
      final List<Bucket> result = new ArrayList<>();
      try (final ResultSet rs = database.query("sql", "SELECT FROM hourly ORDER BY hr")) {
        while (rs.hasNext()) {
          final Result row = rs.next();
          result.add(new Bucket(DateUtils.toEpochMillis(row.getProperty("hr")), ((Number) row.getProperty("total")).doubleValue()));
        }
      }
      return result;
    }

    private TimeSeriesEngine engine() {
      return ((LocalTimeSeriesType) database.getSchema().getType("sensor")).getEngine();
    }

    private ContinuousAggregateImpl aggregate() {
      return (ContinuousAggregateImpl) database.getSchema().getContinuousAggregate("hourly");
    }
  }

  /**
   * Regression for #10940, the residue of #10857: an HA append registered itself in flight only when its pre-commit check found a continuous
   * aggregate over the type. A {@code CREATE CONTINUOUS AGGREGATE} run after that check and before the commit refreshed without the
   * uncommitted samples and installed a watermark past them (from newer samples already committed); the check made after the commit
   * then found them late and shipped their record in a session that followed the commit, the shape of #10673. Every HA append is now
   * registered before it reads the aggregates, and the first refresh of the new aggregate keeps its watermark at the bucket of the
   * oldest sample in flight, which it scans itself since the append, finding no aggregate, did not.
   * <p>
   * The database is served through a wrapper that reports itself replicated, as the Raft wrapper does. It counts the schema sessions the
   * appending thread opens, and creates the aggregate in another thread when the append commits, after its check for late samples and
   * before its samples are visible.
   *
   * @author Luca Garulli (l.garulli@arcadedata.com)
   */
  @Nested
  class AggregateCreatedDuringAppendCommit extends TestHelper {
    private static final long HOUR = 3_600_000L;
    private static final long H0   = 1_700_000_000_000L - (1_700_000_000_000L % HOUR);

    private record Bucket(long start, double total) {
    }

    /** The samples the type held when each schema session of the appending thread opened, in order. */
    private final List<Long> samplesAtEachSession = new ArrayList<>();

    @BeforeEach
    void createTypeWithoutAggregateAndANewerSample() throws Exception {
      database.command("sql", "CREATE TIMESERIES TYPE sensor TIMESTAMP ts TAGS (host STRING) FIELDS (v DOUBLE) SHARDS 1").close();
      for (int h = 0; h < 3; h++)
        engine().appendSamplesWithoutRefresh(new long[] { H0 + h * HOUR }, new Object[] { "h0" }, new Object[] { 1.0 });
      // committed: the creation refresh reads it and would move the watermark to its bucket
      engine().appendSamplesWithoutRefresh(new long[] { H0 + 3 * HOUR + 1_000 }, new Object[] { "h0" }, new Object[] { 1.0 });
    }

    @Test
    void anAggregateCreatedDuringTheCommitDoesNotPutItsWatermarkPastTheSamples() throws Exception {
      final AtomicLong inFlightWhileCommitting = new AtomicLong();
      final AtomicLong watermarkAfterCreation = new AtomicLong();
      servedAsReplicated(() -> {
        inFlightWhileCommitting.set(engine().oldestInFlightAppendTs());
        database.command("sql",
            "CREATE CONTINUOUS AGGREGATE hourly AS SELECT ts.timeBucket('1h', ts) AS hr, sum(v) AS total FROM sensor GROUP BY hr")
            .close();
        watermarkAfterCreation.set(aggregate().getWatermarkTs());
      }, () -> engine().appendSamplesWithoutRefresh(new long[] { H0 + 2 * HOUR + 60_000 }, new Object[] { "h0" },
          new Object[] { 1.0 }));

      assertThat(watermarkAfterCreation.get()).as("kept at the bucket of the sample in flight").isEqualTo(H0 + 2 * HOUR);
      assertThat(samplesAtEachSession).as("the sample is not late, nothing ships after its commit").isEmpty();
      assertThat(inFlightWhileCommitting.get()).as("in flight although no aggregate read the type")
          .isEqualTo(H0 + 2 * HOUR + 60_000);
      assertThat(aggregate().getInvalidation()).isNull();
      assertThat(engine().oldestInFlightAppendTs()).as("the append is in flight no more").isEqualTo(Long.MAX_VALUE);

      refresh();
      assertThat(buckets()).containsExactly(new Bucket(H0, 1), new Bucket(H0 + HOUR, 1), new Bucket(H0 + 2 * HOUR, 2),
          new Bucket(H0 + 3 * HOUR, 1));
      assertThat(aggregate().getWatermarkTs()).isEqualTo(H0 + 3 * HOUR);
    }

    private interface Body {
      void run() throws Exception;
    }

    /**
     * Runs {@code body} with the database served through a wrapper that reports itself replicated. The first commit of the thread
     * running {@code body} first runs {@code inCommit} in another thread and waits for it to end.
     */
    private void servedAsReplicated(final Runnable inCommit, final Body body) throws Exception {
      final LocalDatabase local = (LocalDatabase) ((DatabaseInternal) database).getEmbedded();
      final DatabaseInternal[] self = new DatabaseInternal[1];
      final Thread appending = Thread.currentThread();
      final AtomicBoolean ran = new AtomicBoolean();
      final AtomicReference<Throwable> failure = new AtomicReference<>();
      final InvocationHandler handler = (proxy, method, args) -> {
        final boolean ownThread = Thread.currentThread() == appending;
        switch (method.getName()) {
        case "isReplicated":
          return true;
        case "getWrappedDatabaseInstance":
          return self[0];
        case "commit":
          if (ownThread && ran.compareAndSet(false, true)) {
            final Thread other = new Thread(() -> {
              try {
                inCommit.run();
              } catch (final Throwable t) {
                failure.set(t);
              }
            });
            other.start();
            other.join(30_000);
            assertThat(other.isAlive()).isFalse();
          }
          return invoke(local, method, args);
        case "recordFileChanges":
          if (ownThread)
            samplesAtEachSession.add(samples());
          return invoke(local, method, args);
        default:
          return invoke(local, method, args);
        }
      };
      self[0] = (DatabaseInternal) Proxy.newProxyInstance(DatabaseInternal.class.getClassLoader(),
          new Class<?>[] { DatabaseInternal.class }, handler);
      local.setWrappedDatabaseInstance(self[0]);
      try {
        body.run();
      } finally {
        local.setWrappedDatabaseInstance(local);
      }
      assertThat(ran.get()).as("the callback ran during the commit").isTrue();
      assertThat(failure.get()).isNull();
    }

    private static Object invoke(final Object target, final Method method, final Object[] args) throws Throwable {
      try {
        return method.invoke(target, args);
      } catch (final InvocationTargetException e) {
        throw e.getCause();
      }
    }

    private void refresh() {
      database.command("sql", "REFRESH CONTINUOUS AGGREGATE hourly").close();
    }

    private long samples() {
      // count(), not count(*): the rows, not the cached counter
      try (final ResultSet rs = database.query("sql", "SELECT count() AS c FROM sensor")) {
        return rs.next().<Number>getProperty("c").longValue();
      }
    }

    private List<Bucket> buckets() {
      final List<Bucket> result = new ArrayList<>();
      try (final ResultSet rs = database.query("sql", "SELECT FROM hourly ORDER BY hr")) {
        while (rs.hasNext()) {
          final Result row = rs.next();
          result.add(new Bucket(DateUtils.toEpochMillis(row.getProperty("hr")), ((Number) row.getProperty("total")).doubleValue()));
        }
      }
      return result;
    }

    private TimeSeriesEngine engine() {
      return ((LocalTimeSeriesType) database.getSchema().getType("sensor")).getEngine();
    }

    private ContinuousAggregateImpl aggregate() {
      return (ContinuousAggregateImpl) database.getSchema().getContinuousAggregate("hourly");
    }
  }

  /**
   * Regression for #11019: a refresh that failed set the in-memory status to ERROR (or STALE) and nothing wrote it to the schema, so a clean
   * close and reopen republished VALID over the rows the failed refresh left behind. The status an operator reads is the status of
   * the last refresh that ran, whether or not the process that ran it is still alive.
   */
  @Nested
  class RefreshStatusPersisted extends TestHelper {

    private void createViewWithUniqueRegion(final String refreshMode) {
      database.command("sql", "CREATE DOCUMENT TYPE Sale");
      for (final String region : new String[] { "EU", "US", "APAC" })
        insertSale(region);
      database.command("sql", "CREATE MATERIALIZED VIEW mv AS SELECT region, amount FROM Sale REFRESH " + refreshMode);
      database.command("sql", "CREATE PROPERTY mv.region STRING");
      database.command("sql", "CREATE INDEX ON mv (region) UNIQUE");
    }

    private void insertSale(final String region) {
      database.transaction(() -> database.command("sql", "INSERT INTO Sale SET region = ?, amount = 10", region));
    }

    private void insertSample(final long ts) {
      database.transaction(() -> database.command("sql", "INSERT INTO S SET ts = ?, h = 's1', v = 1.0", ts));
    }

    private String viewStatus() {
      try (final ResultSet rs = database.query("sql", "SELECT status FROM schema:materializedViews WHERE name = 'mv'")) {
        final Result row = rs.next();
        return row.getProperty("status");
      }
    }

    private String aggregateStatus() {
      try (final ResultSet rs = database.query("sql", "SELECT status FROM schema:continuousAggregates WHERE name = 'ca'")) {
        final Result row = rs.next();
        return row.getProperty("status");
      }
    }

    @Test
    void aFailedManualRefreshStaysErrorAfterReopen() {
      createViewWithUniqueRegion("MANUAL");
      insertSale("EU");

      assertThatThrownBy(() -> database.command("sql", "REFRESH MATERIALIZED VIEW mv")).hasMessageContaining("Duplicated key [EU]");
      assertThat(viewStatus()).isEqualTo("ERROR");

      reopenDatabase();

      assertThat(viewStatus()).isEqualTo("ERROR");
    }

    @Test
    void aRefreshThatRecoversFromErrorIsValidAfterReopen() {
      createViewWithUniqueRegion("MANUAL");
      insertSale("EU");
      assertThatThrownBy(() -> database.command("sql", "REFRESH MATERIALIZED VIEW mv")).isNotNull();
      reopenDatabase();
      assertThat(viewStatus()).isEqualTo("ERROR");

      database.transaction(() -> database.command("sql", "DELETE FROM Sale WHERE region = 'EU' LIMIT 1"));
      database.command("sql", "REFRESH MATERIALIZED VIEW mv");
      assertThat(viewStatus()).isEqualTo("VALID");

      reopenDatabase();

      assertThat(viewStatus()).isEqualTo("VALID");
    }

    @Test
    void anIncrementalViewMarkedStaleStaysStaleAfterReopen() {
      createViewWithUniqueRegion("INCREMENTAL");

      // the commit of this insert triggers the incremental refresh, which hits the duplicate and marks the view STALE
      insertSale("EU");
      assertThat(viewStatus()).isEqualTo("STALE");

      reopenDatabase();

      // The open hands the per-record view its recovery full pass (issue #11550), which meets the same duplicate: it is still STALE
      // once that pass has failed, never VALID
      final MaterializedViewImpl view = (MaterializedViewImpl) database.getSchema().getMaterializedView("mv");
      Awaitility.await().atMost(30, TimeUnit.SECONDS).pollInterval(20, TimeUnit.MILLISECONDS)
          .until(() -> view.getErrorCount() > 0 && "STALE".equals(viewStatus()));
      assertThat(viewStatus()).isEqualTo("STALE");
    }

    @Test
    void aFailedContinuousAggregateRefreshStaysErrorAfterReopen() {
      database.command("sql", "CREATE TIMESERIES TYPE S TIMESTAMP ts TAGS (h STRING) FIELDS (v DOUBLE) SHARDS 1");
      insertSample(1700000000000L);
      database.command("sql",
          "CREATE CONTINUOUS AGGREGATE ca AS SELECT ts.timeBucket('1h', ts) AS hr, h, sum(v) AS total FROM S GROUP BY hr, h");
      database.command("sql", "CREATE PROPERTY ca.h STRING");
      database.command("sql", "CREATE INDEX ON ca (h) UNIQUE");
      assertThat(aggregateStatus()).isEqualTo("VALID");

      // a second bucket for the same tag: the unique index on h refuses it
      insertSample(1700000000000L + 7200000L);
      assertThatThrownBy(() -> database.command("sql", "REFRESH CONTINUOUS AGGREGATE ca")).isNotNull();
      assertThat(aggregateStatus()).isEqualTo("ERROR");

      reopenDatabase();

      assertThat(aggregateStatus()).isEqualTo("ERROR");
    }
  }

  /**
   * Regression for #11020: {@code TRUNCATE TYPE} on the backing type of a continuous aggregate emptied it and left its watermark where it was,
   * so the next refresh recomputed only the buckets at or after the watermark: every bucket behind it was lost for good, with the
   * aggregate reporting VALID. A materialized view truncated the same way answered no rows while reporting VALID, and a row
   * INSERTed into a view was answered beside the rows of its query. {@code DELETE FROM} the aggregate lost the same buckets as the
   * truncate.
   * <p>
   * A backing type is now written by its owner's refresh and by nothing else, as PostgreSQL refuses to change a materialized view:
   * every record insert, update and delete, {@code TRUNCATE TYPE} (of the type, or reaching it POLYMORPHIC), {@code TRUNCATE BUCKET}
   * and the removal of one of its buckets is refused, naming the statement that does the job.
   * <p>
   * Issue #11204: a row can be in the backing type's answer without being written to it, by a type linked below the backing type
   * ({@code CREATE ... EXTENDS}, {@code ALTER TYPE ... SUPERTYPE +}): that link is refused.
   *
   * @author Luca Garulli (l.garulli@arcadedata.com)
   */
  @Nested
  class BackingTypeWrites extends TestHelper {
    private static final long HOUR = 3_600_000L;
    /** 2023-11-14T22:13:20Z, the first sample of the issue. */
    private static final long T0   = 1_700_000_000_000L;

    // ---- continuous aggregate: the issue's reproduction

    @Test
    void truncateOfAnAggregateBackingTypeIsRefusedAndNoBucketIsLost() {
      createAggregate();
      assertThat(aggregateRows()).containsExactly(1.0, 2.0, 3.0);

      assertThatThrownBy(() -> database.command("sql", "TRUNCATE TYPE ca")).isInstanceOf(SchemaException.class)
          .hasMessageContaining("backing type for continuous aggregate 'ca'").hasMessageContaining("REFRESH CONTINUOUS AGGREGATE ca")
          .hasMessageContaining("DROP CONTINUOUS AGGREGATE ca");
      assertThatThrownBy(() -> database.command("sql", "TRUNCATE TYPE ca UNSAFE")).isInstanceOf(SchemaException.class);

      assertThat(aggregateRows()).containsExactly(1.0, 2.0, 3.0);
      assertThat(status("continuousAggregates", "ca")).isEqualTo("VALID");

      // and the refreshes the operator runs next keep every bucket
      database.command("sql", "REFRESH CONTINUOUS AGGREGATE ca");
      database.command("sql", "REFRESH CONTINUOUS AGGREGATE ca");
      assertThat(aggregateRows()).containsExactly(1.0, 2.0, 3.0);
    }

    @Test
    void deleteFromAnAggregateBackingTypeIsRefusedAndNoBucketIsLost() {
      createAggregate();

      assertThatThrownBy(() -> database.command("sql", "DELETE FROM ca")).isInstanceOf(SchemaException.class)
          .hasMessageContaining("Cannot delete from type 'ca'");
      assertThatThrownBy(() -> database.command("sql", "DELETE FROM ca WHERE av < 3")).isInstanceOf(SchemaException.class);
      assertThatThrownBy(() -> database.command("sql", "UPDATE ca SET av = 42")).isInstanceOf(SchemaException.class)
          .hasMessageContaining("Cannot update a record of type 'ca'");
      assertThatThrownBy(() -> database.command("sql", "INSERT INTO ca SET sid = 'x', av = 9")).isInstanceOf(SchemaException.class)
          .hasMessageContaining("Cannot insert into type 'ca'");

      database.command("sql", "REFRESH CONTINUOUS AGGREGATE ca");
      assertThat(aggregateRows()).containsExactly(1.0, 2.0, 3.0);
    }

    @Test
    void anAggregateKeepsRefreshingFromItsSource() {
      createAggregate();

      database.command("sql", "INSERT INTO SR SET ts = ?, sid = 's1', v = 4.0", T0 + 3 * 2 * HOUR);
      database.command("sql", "REFRESH CONTINUOUS AGGREGATE ca");

      assertThat(aggregateRows()).containsExactly(1.0, 2.0, 3.0, 4.0);
      assertThat(status("continuousAggregates", "ca")).isEqualTo("VALID");
    }

    // ---- materialized view

    @Test
    void everyWriteOfAViewBackingTypeIsRefused() {
      createView("");
      final long sourceRows = count("Sale");
      final List<Integer> buckets = database.getSchema().getType("mv").getBucketIds(false);
      // indexing a backing type writes no record and stays allowed; UPSERT needs the unique index
      database.command("sql", "CREATE PROPERTY mv.region STRING");
      database.command("sql", "CREATE INDEX ON mv (region) UNIQUE");

      for (final String statement : List.of("TRUNCATE TYPE mv", "TRUNCATE TYPE mv UNSAFE", "INSERT INTO mv SET region = 'NEVER IN THE QUERY'",
          "UPDATE mv SET total = 0", "UPDATE mv SET total = 1 UPSERT WHERE region = 'NOWHERE'",
          "UPDATE mv SET total = 1 UPSERT WHERE region = 'EU'", "DELETE FROM mv", "DELETE FROM mv WHERE region = 'EU'",
          "INSERT INTO BUCKET:" + database.getSchema().getBucketById(buckets.getFirst()).getName() + " SET region = 'X'",
          "TRUNCATE BUCKET " + database.getSchema().getBucketById(buckets.getFirst()).getName(),
          "ALTER TYPE mv BUCKET -" + database.getSchema().getBucketById(buckets.getFirst()).getName()))
        assertThatThrownBy(() -> database.command("sql", statement)).as(statement).isInstanceOf(SchemaException.class)
            .hasMessageContaining("backing type for materialized view 'mv'").hasMessageContaining("REFRESH MATERIALIZED VIEW mv");

      // inside a transaction too, and the transaction is left usable
      database.transaction(() -> {
        assertThatThrownBy(() -> database.command("sql", "INSERT INTO mv SET region = 'NEVER IN THE QUERY'"))
            .isInstanceOf(SchemaException.class);
        database.newDocument("Sale").set("region", "APAC").set("amount", 5).save();
      });

      // and through the API
      assertThatThrownBy(() -> database.transaction(() -> database.newDocument("mv").set("region", "X").save())).isInstanceOf(
          SchemaException.class);
      assertThatThrownBy(() -> database.transaction(() -> {
        final Document row = database.iterateType("mv", false).next().asDocument();
        row.delete();
      })).isInstanceOf(SchemaException.class);
      assertThatThrownBy(() -> database.transaction(() -> {
        final MutableDocument row = database.iterateType("mv", false).next().asDocument().modify();
        row.set("total", -1).save();
      })).isInstanceOf(SchemaException.class);

      assertThat(database.getSchema().getType("mv").getBucketIds(false)).isEqualTo(buckets);
      assertThat(count("Sale")).isEqualTo(sourceRows + 1);
      database.command("sql", "REFRESH MATERIALIZED VIEW mv");
      assertThat(viewRows()).containsExactly("APAC=5", "EU=30", "US=5");
      assertThat(status("materializedViews", "mv")).isEqualTo("VALID");
    }

    @Test
    void anIncrementalViewIsStillMaintainedByItsRefresh() {
      createView(" REFRESH INCREMENTAL");
      database.transaction(() -> database.newDocument("Sale").set("region", "US").set("amount", 7).save());
      assertThat(viewRows()).containsExactly("EU=30", "US=12");
    }

    @Test
    void aViewOverAnAggregateIsRefreshedThroughTheAggregateRefresh() {
      createAggregate();
      // the aggregate's refresh writes its own backing type and, on the same thread, the view's refresh writes the view's
      database.command("sql", "CREATE MATERIALIZED VIEW over_ca AS SELECT hour, av FROM ca REFRESH INCREMENTAL");
      database.command("sql", "INSERT INTO SR SET ts = ?, sid = 's1', v = 4.0", T0 + 3 * 2 * HOUR);
      database.command("sql", "REFRESH CONTINUOUS AGGREGATE ca");

      assertThat(count("over_ca")).isEqualTo(4L);
      assertThatThrownBy(() -> database.command("sql", "DELETE FROM over_ca")).isInstanceOf(SchemaException.class);
    }

    @Test
    void aPolymorphicTruncateReachingABackingTypeIsRefusedWhole() {
      createView("");
      database.command("sql", "CREATE DOCUMENT TYPE Base");
      database.command("sql", "CREATE DOCUMENT TYPE Plain EXTENDS Base");
      database.command("sql", "ALTER TYPE mv SUPERTYPE +Base");
      database.transaction(() -> database.command("sql", "INSERT INTO Plain SET k = 1"));

      assertThatThrownBy(() -> database.command("sql", "TRUNCATE TYPE Base POLYMORPHIC")).isInstanceOf(SchemaException.class)
          .hasMessageContaining("backing type for materialized view 'mv'");
      // refused before anything was deleted, the type that is not a backing type included
      assertThat(count("Plain")).isEqualTo(1L);
      assertThat(viewRows()).containsExactly("EU=30", "US=5");

      // and non-polymorphic, the backing type is out of scope
      database.command("sql", "TRUNCATE TYPE Plain");
      assertThat(count("Plain")).isZero();
    }

    @Test
    void theGuardSurvivesAReopenAndGoesWithTheView() {
      createView("");
      reopenDatabase();

      assertThatThrownBy(() -> database.command("sql", "TRUNCATE TYPE mv")).isInstanceOf(SchemaException.class);
      assertThat(viewRows()).containsExactly("EU=30", "US=5");

      database.command("sql", "DROP MATERIALIZED VIEW mv");
      // a type that takes the name afterwards is an ordinary one
      database.command("sql", "CREATE DOCUMENT TYPE mv");
      database.transaction(() -> database.command("sql", "INSERT INTO mv SET region = 'free'"));
      database.command("sql", "TRUNCATE TYPE mv");
      assertThat(count("mv")).isZero();
    }

    /** No mutator of the registries can leave the write guard behind: each refreshes it, and the views refuse a removal. */
    @Test
    void everyMutationOfTheRegistriesKeepsTheGuardInStep() {
      createView("");
      final LocalSchema schema = database.getSchema().getEmbedded();
      final MaterializedViewImpl view = schema.materializedViews.get("mv");

      assertThatThrownBy(() -> schema.materializedViews.keySet().remove("mv")).isInstanceOf(UnsupportedOperationException.class);
      assertThatThrownBy(() -> schema.materializedViews.values().clear()).isInstanceOf(UnsupportedOperationException.class);
      assertThatThrownBy(() -> schema.materializedViews.entrySet().clear()).isInstanceOf(UnsupportedOperationException.class);

      // the map's own mutators each refresh the guard: off with remove(key, value), back on with putIfAbsent
      assertThat(schema.materializedViews.remove("mv", view)).isTrue();
      schema.checkBackingTypeWrite("mv", "insert into");
      schema.materializedViews.putIfAbsent("mv", view);
      assertThatThrownBy(() -> schema.checkBackingTypeWrite("mv", "insert into")).isInstanceOf(SchemaException.class);
      schema.materializedViews.computeIfPresent("mv", (k, v) -> null);
      schema.checkBackingTypeWrite("mv", "insert into");
      schema.materializedViews.merge("mv", view, (a, b) -> b);
      assertThatThrownBy(() -> schema.checkBackingTypeWrite("mv", "insert into")).isInstanceOf(SchemaException.class);

      // and the SequencedMap mutators and views of LinkedHashMap
      final LinkedHashMap<String, MaterializedViewImpl> sequenced = (LinkedHashMap<String, MaterializedViewImpl>) schema.materializedViews;
      assertThat(sequenced.pollFirstEntry().getValue()).isSameAs(view);
      schema.checkBackingTypeWrite("mv", "insert into");
      sequenced.putLast("mv", view);
      assertThatThrownBy(() -> schema.checkBackingTypeWrite("mv", "insert into")).isInstanceOf(SchemaException.class);
      assertThatThrownBy(() -> sequenced.sequencedKeySet().removeFirst()).isInstanceOf(UnsupportedOperationException.class);
      assertThatThrownBy(() -> sequenced.reversed().pollFirstEntry()).isInstanceOf(UnsupportedOperationException.class);

      // issue #11203: so are the entries the views hand out, whose setValue replaced an owner behind the guard
      final MaterializedViewImpl other = new MaterializedViewImpl(database, "other", "SELECT FROM Sale", "other", List.of("Sale"),
          MaterializedViewRefreshMode.MANUAL, true, 0);
      final List<Runnable> entryWrites = List.of(//
          () -> schema.materializedViews.entrySet().iterator().next().setValue(other),
          () -> schema.materializedViews.entrySet().forEach(entry -> entry.setValue(other)),
          () -> schema.materializedViews.entrySet().stream().findFirst().orElseThrow().setValue(other),
          () -> schema.materializedViews.entrySet().removeIf(entry -> true),
          () -> sequenced.sequencedEntrySet().getFirst().setValue(other),
          () -> sequenced.sequencedEntrySet().reversed().iterator().next().setValue(other),
          () -> sequenced.sequencedEntrySet().removeFirst(),
          () -> sequenced.reversed().entrySet().iterator().next().setValue(other),
          () -> sequenced.firstEntry().setValue(other));
      for (final Runnable write : entryWrites)
        assertThatThrownBy(write::run).isInstanceOf(UnsupportedOperationException.class);
      assertThat(schema.materializedViews.entrySet()).extracting(Map.Entry::getKey).containsExactly("mv");
      assertThat(schema.materializedViews.get("mv")).isSameAs(view);
      assertThatThrownBy(() -> schema.checkBackingTypeWrite("mv", "insert into")).isInstanceOf(SchemaException.class);
    }

    /** A document of another type written into a bucket of the backing type by name is refused too (found by CodeRabbit). */
    @Test
    void aDocumentOfAnotherTypeIsNotWrittenIntoABackingBucket() {
      createView("");
      final String bucket = database.getSchema().getBucketById(database.getSchema().getType("mv").getBucketIds(false).getFirst())
          .getName();

      assertThatThrownBy(() -> database.transaction(() -> {
        database.newDocument("Sale").set("region", "X").set("amount", 1).save(bucket);
      })).isInstanceOf(SchemaException.class).hasMessageContaining("backing type for materialized view 'mv'");
      assertThat(viewRows()).containsExactly("EU=30", "US=5");
    }

    /** The refresh scope of one database authorizes no write in another one holding a view of the same name (found by CodeRabbit). */
    @Test
    void aRefreshScopeAuthorizesWritesInItsOwnDatabaseOnly() {
      createView("");
      final MaterializedViewImpl view = database.getSchema().getEmbedded().materializedViews.get("mv");

      final DatabaseFactory factory = new DatabaseFactory(getDatabasePath() + "_other");
      if (factory.exists())
        factory.open().drop();
      final Database other = factory.create();
      try {
        other.command("sql", "CREATE DOCUMENT TYPE Sale");
        other.command("sql", "CREATE MATERIALIZED VIEW mv AS SELECT region, sum(amount) AS total FROM Sale GROUP BY region");

        try (final BackingTypeWriteScope ignored = BackingTypeWriteScope.open(view)) {
          // the scope lets this database's refresh write its own backing type...
          database.transaction(() -> database.newDocument("mv").set("region", "scoped").save());
          // ...and nothing in the other database
          assertThatThrownBy(() -> other.transaction(() -> other.newDocument("mv").set("region", "X").save())).isInstanceOf(
              SchemaException.class);
        }
        assertThat(count("mv")).isEqualTo(3L);
      } finally {
        other.drop();
      }
    }

    // ---- issue #11204: a subtype of a backing type has every row answered by the view, so it is refused

    /** {@code CREATE ... TYPE Sub EXTENDS <backingType>} is refused, whatever kind of type, and nothing is created. */
    @Test
    void aNewTypeCannotExtendABackingType() {
      createView("");
      createAggregate();

      for (final String statement : List.of("CREATE DOCUMENT TYPE Sub EXTENDS mv", "CREATE DOCUMENT TYPE Sub IF NOT EXISTS EXTENDS mv",
          "CREATE DOCUMENT TYPE Sub EXTENDS Sale, mv", "CREATE VERTEX TYPE Sub EXTENDS mv", "CREATE DOCUMENT TYPE Sub EXTENDS ca"))
        assertThatThrownBy(() -> database.command("sql", statement)).as(statement).isInstanceOf(SchemaException.class)
            .hasMessageContaining("Cannot make type 'Sub' a subtype of").hasMessageContaining("only its refresh writes");
      assertThat(database.getSchema().existsType("Sub")).isFalse();

      // through the API too
      assertThatThrownBy(() -> database.getSchema().buildDocumentType().withName("Sub").withSuperType("mv").create()).isInstanceOf(
          SchemaException.class).hasMessageContaining("backing type for materialized view 'mv'");
      assertThat(database.getSchema().existsType("Sub")).isFalse();

      assertThat(viewRows()).containsExactly("EU=30", "US=5");
      assertThat(aggregateRows()).containsExactly(1.0, 2.0, 3.0);
    }

    /** {@code ALTER TYPE Legacy SUPERTYPE +<backingType>} would put a populated type's whole population into the view at once. */
    @Test
    void anExistingTypeCannotBeMadeASubtypeOfABackingType() {
      createView("");
      createAggregate();
      database.command("sql", "CREATE DOCUMENT TYPE Legacy");
      database.transaction(() -> {
        for (int i = 0; i < 3; i++)
          database.newDocument("Legacy").set("region", "JUNK" + i).set("total", 100 + i).save();
      });

      for (final String target : List.of("mv", "ca"))
        assertThatThrownBy(() -> database.command("sql", "ALTER TYPE Legacy SUPERTYPE +" + target)).as(target)
            .isInstanceOf(SchemaException.class).hasMessageContaining("Cannot make type 'Legacy' a subtype of '" + target + "'");
      assertThatThrownBy(() -> database.getSchema().getType("Legacy").addSuperType("mv")).isInstanceOf(SchemaException.class);
      assertThatThrownBy(() -> database.getSchema().getType("Legacy").setSuperTypes(List.of(database.getSchema().getType("ca"))))
          .isInstanceOf(SchemaException.class);

      assertThat(database.getSchema().getType("Legacy").getSuperTypes()).isEmpty();
      assertThat(viewRows()).containsExactly("EU=30", "US=5");
      assertThat(count("mv")).isEqualTo(2L);
      assertThat(aggregateRows()).containsExactly(1.0, 2.0, 3.0);

      // a backing type can still be given a super type of its own: its rows join the super type's answer, the view's stay its own
      database.command("sql", "CREATE DOCUMENT TYPE Base");
      database.command("sql", "ALTER TYPE mv SUPERTYPE +Base");
      database.command("sql", "CREATE DOCUMENT TYPE Sibling EXTENDS Base");
      assertThat(viewRows()).containsExactly("EU=30", "US=5");
    }

    /**
     * A database written before issue #11204 can already hold a subtype of a backing type: it still opens, the link can be taken out,
     * and nothing new can be hung under that subtype either, since its rows are the view's rows too.
     */
    @Test
    void aSubtypeLinkedBeforeTheCheckStillOpensAndCanBeUnlinked() {
      createView("");
      final LocalDocumentType mv = (LocalDocumentType) database.getSchema().getType("mv");
      final LocalDocumentType legacy = (LocalDocumentType) database.getSchema().createDocumentType("Legacy");
      // the re-establishing linkage of the schema load, which a database written before the check went through
      legacy.addSuperType(mv, false);
      database.transaction(() -> database.newDocument("Legacy").set("region", "JUNK").set("total", 1).save());
      assertThat(count("mv")).isEqualTo(3L);

      reopenDatabase();
      assertThat(database.getSchema().getType("Legacy").getSuperTypes()).extracting(DocumentType::getName).containsExactly("mv");

      assertThatThrownBy(() -> database.command("sql", "CREATE DOCUMENT TYPE Deeper EXTENDS Legacy")).isInstanceOf(SchemaException.class)
          .hasMessageContaining("'Legacy' extends 'mv', the backing type for materialized view 'mv'");

      database.command("sql", "ALTER TYPE Legacy SUPERTYPE -mv");
      assertThat(viewRows()).containsExactly("EU=30", "US=5");
      database.command("sql", "CREATE DOCUMENT TYPE Deeper EXTENDS Legacy");
    }

    /**
     * Issue #11272: {@code DROP TYPE} of an intermediate type attaches its subtypes to its super types, and that reattachment skipped the
     * #11204 check, so dropping {@code Mid} out of a pre-#11204 {@code Deep -> Mid -> mv} hung {@code Deep} directly under the backing
     * type. The drop is refused before anything changes, naming the link to take out, after which it goes through.
     */
    @Test
    void dropTypeDoesNotReattachASubtypeUnderABackingType() {
      createView("");
      final LocalDocumentType mv = (LocalDocumentType) database.getSchema().getType("mv");
      final LocalDocumentType mid = (LocalDocumentType) database.getSchema().createDocumentType("Mid");
      final LocalDocumentType deep = (LocalDocumentType) database.getSchema().createDocumentType("Deep");
      // the re-establishing linkage of the schema load, which a database written before the check went through
      mid.addSuperType(mv, false);
      deep.addSuperType(mid, false);
      database.transaction(() -> database.newDocument("Deep").set("region", "JUNK").set("total", 1).save());
      assertThat(count("mv")).isEqualTo(3L);

      for (final String statement : List.of("DROP TYPE Mid", "DROP TYPE Mid IF EXISTS", "DROP TYPE Mid UNSAFE"))
        assertThatThrownBy(() -> database.command("sql", statement)).as(statement).isInstanceOf(SchemaException.class)
            .hasMessageContaining("Cannot drop the type 'Mid'").hasMessageContaining("'Deep'")
            .hasMessageContaining("ALTER TYPE `Deep` SUPERTYPE -`Mid`");
      assertThatThrownBy(() -> database.getSchema().dropType("Mid")).isInstanceOf(SchemaException.class);

      // nothing changed
      assertThat(database.getSchema().existsType("Mid")).isTrue();
      assertThat(database.getSchema().getType("Deep").getSuperTypes()).extracting(DocumentType::getName).containsExactly("Mid");
      assertThat(database.getSchema().getType("mv").getSubTypes()).extracting(DocumentType::getName).containsExactly("Mid");

      // the repair the message names, then the drop: no subtype of the backing type is left
      database.command("sql", "ALTER TYPE `Deep` SUPERTYPE -`Mid`");
      database.command("sql", "DROP TYPE Mid");
      assertThat(database.getSchema().getType("mv").getSubTypes()).isEmpty();
      assertThat(database.getSchema().getType("Deep").getSuperTypes()).isEmpty();
      assertThat(viewRows()).containsExactly("EU=30", "US=5");

      // a drop whose subtypes land under ordinary types is untouched
      database.command("sql", "CREATE DOCUMENT TYPE Top");
      database.command("sql", "CREATE DOCUMENT TYPE Middle EXTENDS Top");
      database.command("sql", "CREATE DOCUMENT TYPE Bottom EXTENDS Middle");
      database.command("sql", "DROP TYPE Middle");
      assertThat(database.getSchema().getType("Bottom").getSuperTypes()).extracting(DocumentType::getName).containsExactly("Top");
    }

    // ---- helpers

    private void createAggregate() {
      database.command("sql", "CREATE TIMESERIES TYPE SR TIMESTAMP ts TAGS (sid STRING) FIELDS (v DOUBLE)");
      // three samples two hours apart, one per bucket
      for (int i = 0; i < 3; i++)
        database.command("sql", "INSERT INTO SR SET ts = ?, sid = 's1', v = ?", T0 + i * 2 * HOUR, i + 1.0);
      database.command("sql",
          "CREATE CONTINUOUS AGGREGATE ca AS SELECT sid, ts.timeBucket('1h', ts) AS hour, avg(v) AS av FROM SR GROUP BY sid, hour");
    }

    private void createView(final String refresh) {
      database.command("sql", "CREATE DOCUMENT TYPE Sale");
      database.transaction(() -> {
        database.newDocument("Sale").set("region", "EU").set("amount", 10).save();
        database.newDocument("Sale").set("region", "EU").set("amount", 20).save();
        database.newDocument("Sale").set("region", "US").set("amount", 5).save();
      });
      database.command("sql", "CREATE MATERIALIZED VIEW mv AS SELECT region, sum(amount) AS total FROM Sale GROUP BY region" + refresh);
      assertThat(viewRows()).containsExactly("EU=30", "US=5");
    }

    private List<Double> aggregateRows() {
      final List<Double> rows = new ArrayList<>();
      try (final ResultSet rs = database.query("sql", "SELECT hour, av FROM ca ORDER BY hour")) {
        while (rs.hasNext())
          rows.add(((Number) rs.next().getProperty("av")).doubleValue());
      }
      return rows;
    }

    private List<String> viewRows() {
      final List<String> rows = new ArrayList<>();
      try (final ResultSet rs = database.query("sql", "SELECT region, total FROM mv ORDER BY region")) {
        while (rs.hasNext()) {
          final Result row = rs.next();
          rows.add(row.getProperty("region") + "=" + row.getProperty("total"));
        }
      }
      return rows;
    }

    private long count(final String type) {
      try (final ResultSet rs = database.query("sql", "SELECT count(@rid) AS n FROM " + type)) {
        return ((Number) rs.next().getProperty("n")).longValue();
      }
    }

    private String status(final String registry, final String name) {
      try (final ResultSet rs = database.query("sql", "SELECT status FROM schema:" + registry + " WHERE name = ?", name)) {
        return rs.next().getProperty("status");
      }
    }
  }

  /**
   * Regression for #11031: the periodic refresh of a materialized view rewrites its backing type, and under HA the leader replicates its
   * own rewrite of the same records, so a follower must not recompute the view on its own timer. The scheduler is handed the raw
   * {@code LocalDatabase} the schema was loaded with, built before the server wraps the database for HA, so the guard has to
   * resolve the wrapper the node is currently served through: asked of the scheduled reference itself it could never fire.
   * <p>
   * The wrapper is installed the way the HA server installs {@code RaftReplicatedDatabase}, through
   * {@code LocalDatabase.setWrappedDatabaseInstance()}, answering itself for {@code getWrappedDatabaseInstance()}: the two
   * replication flags are the only behaviour it invents.
   *
   * @author Luca Garulli (l.garulli@arcadedata.com)
   */
  @Nested
  class MaterializedViewRefreshLeaderOnly extends TestHelper {

    /** Runs before {@code TestHelper.afterTest()}, whose integrity check would otherwise meet a database that says it is replicated. */
    @AfterEach
    void removeWrapper() {
      if (database != null && database.isOpen())
        ((LocalDatabase) database).setWrappedDatabaseInstance((LocalDatabase) database);
    }

    @Test
    void aFollowerSkipsThePeriodicRefreshAndTheLeaderRunsIt() {
      final MaterializedViewImpl view = createPeriodicViewOverTwoRecords();
      database.transaction(() -> database.newDocument("Source11031").set("id", 3).save());

      final MaterializedViewScheduler scheduler = new MaterializedViewScheduler(database.getName());
      try {
        final long refreshesBefore = view.getRefreshCount();

        installWrapper(false);
        // Handed the raw instance, as LocalSchema hands it
        scheduler.runOneRefresh(database, view);
        assertThat(view.getRefreshCount()).as("a follower does not refresh: the leader replicates its own rewrite")
            .isEqualTo(refreshesBefore);
        assertThat(database.countType(view.getBackingTypeName(), false)).isEqualTo(2);

        installWrapper(true);
        scheduler.runOneRefresh(database, view);
        assertThat(view.getRefreshCount()).as("the leader refreshes").isEqualTo(refreshesBefore + 1);
        assertThat(database.countType(view.getBackingTypeName(), false)).isEqualTo(3);
      } finally {
        scheduler.shutdown();
      }
    }

    @Test
    void aStandaloneDatabaseStillRefreshes() {
      final MaterializedViewImpl view = createPeriodicViewOverTwoRecords();
      database.transaction(() -> database.newDocument("Source11031").set("id", 3).save());

      final MaterializedViewScheduler scheduler = new MaterializedViewScheduler(database.getName());
      try {
        scheduler.runOneRefresh(database, view);
        assertThat(database.countType(view.getBackingTypeName(), false)).isEqualTo(3);
      } finally {
        scheduler.shutdown();
      }
    }

    @Test
    void theFollowerTestResolvesTheCurrentWrapperOnEveryCall() {
      final DatabaseInternal raw = (DatabaseInternal) database;
      assertThat(raw.isFollower()).as("standalone").isFalse();
      installWrapper(false);
      assertThat(raw.isReplicated()).as("the raw instance never sees the wrapper by itself").isFalse();
      assertThat(raw.isFollower()).as("a follower, through the wrapper").isTrue();
      installWrapper(true);
      assertThat(raw.isFollower()).as("promoted: the next call sees it").isFalse();
    }

    private MaterializedViewImpl createPeriodicViewOverTwoRecords() {
      database.transaction(() -> {
        database.getSchema().createDocumentType("Source11031");
        database.newDocument("Source11031").set("id", 1).save();
        database.newDocument("Source11031").set("id", 2).save();
      });
      database.transaction(() -> database.getSchema().buildMaterializedView()
          .withName("View11031")
          .withQuery("SELECT id FROM Source11031")
          .withRefreshMode(MaterializedViewRefreshMode.PERIODIC)
          // Long enough that the scheduler's own tick never fires during the test: the passes are driven by hand
          .withRefreshInterval(TimeUnit.HOURS.toMillis(1))
          .create());
      final MaterializedViewImpl view = (MaterializedViewImpl) database.getSchema().getMaterializedView("View11031");
      assertThat(database.countType(view.getBackingTypeName(), false)).isEqualTo(2);
      return view;
    }

    private void installWrapper(final boolean leader) {
      final LocalDatabase real = (LocalDatabase) database;
      final DatabaseInternal wrapper = (DatabaseInternal) Proxy.newProxyInstance(DatabaseInternal.class.getClassLoader(),
          new Class<?>[] { DatabaseInternal.class }, (proxy, method, args) -> {
            if (method.getParameterCount() == 0)
              switch (method.getName()) {
              // Mirrors RaftReplicatedDatabase: the wrapper is its own wrapped instance
              case "getWrappedDatabaseInstance" -> {
                return proxy;
              }
              case "isReplicated" -> {
                return Boolean.TRUE;
              }
              case "isLeader" -> {
                return leader;
              }
              case "isFollower" -> {
                return InvocationHandler.invokeDefault(proxy, method);
              }
              default -> {
              }
              }
            try {
              return method.invoke(real, args);
            } catch (final InvocationTargetException e) {
              throw e.getCause();
            }
          });
      real.setWrappedDatabaseInstance(wrapper);
    }
  }

  /**
   * Regression for #7688: {@link MaterializedViewBuilder} and {@link ContinuousAggregateBuilder} render their accumulated state as
   * DDL, so an implementation that can only reach the schema through {@code command("sql", ...)} - {@code RemoteSchema} -
   * can create the same view or aggregate the embedded builder creates.
   * <p>
   * These are the engine-side half of the acceptance criteria: that executing the rendered SQL yields the same
   * declaration as calling {@code create()} on the very same builder state - the local-to-local parity the remote path
   * rides on - and that a state the grammar cannot express fails at build time rather than as invalid DDL on the server.
   * The end-to-end embedded-vs-remote comparison lives in {@code Issue7688RemoteViewBuildersIT}.
   */
  @Nested
  class ViewBuilderSQL extends TestHelper {

    private static final String AGGREGATE_QUERY =
        "SELECT sensor_id, ts.timeBucket('1h', ts) AS hour, avg(temperature) AS avg_temp FROM SensorReading GROUP BY sensor_id, hour";

    @BeforeEach
    void setupTypes() {
      if (!database.getSchema().existsType("Account"))
        database.transaction(() -> {
          database.getSchema().createDocumentType("Account");
          database.newDocument("Account").set("name", "Alice").set("active", true).save();
          database.newDocument("Account").set("name", "Bob").set("active", false).save();
          database.newDocument("Account").set("name", "Carol").set("active", true).save();
        });
      if (!database.getSchema().existsType("SensorReading"))
        database.command("sql",
            "CREATE TIMESERIES TYPE SensorReading TIMESTAMP ts TAGS (sensor_id STRING) FIELDS (temperature DOUBLE)");
    }

    private static Statement parse(final String sql) {
      return new SQLAntlrParser(null).parse(sql);
    }

    /**
     * Builds the view twice from the same recipe: once through {@code create()}, once by executing {@code toSQL()} of an
     * identical builder, and asserts the two declarations agree attribute by attribute.
     */
    private void assertParity(final Function<MaterializedViewBuilder, MaterializedViewBuilder> recipe) {
      final MaterializedView viaCreate = recipe.apply(database.getSchema().buildMaterializedView().withName("ViaCreate")).create();
      final String sql = recipe.apply(database.getSchema().buildMaterializedView().withName("ViaSQL")).toSQL();
      database.command("sql", sql);
      final MaterializedView viaSQL = database.getSchema().getMaterializedView("ViaSQL");

      assertThat(viaSQL.getRefreshMode()).as(sql).isEqualTo(viaCreate.getRefreshMode());
      assertThat(viaSQL.getRefreshInterval()).as(sql).isEqualTo(viaCreate.getRefreshInterval());
      assertThat(viaSQL.getSourceTypeNames()).as(sql).isEqualTo(viaCreate.getSourceTypeNames());
      assertThat(viaSQL.isSimpleQuery()).as(sql).isEqualTo(viaCreate.isSimpleQuery());
      assertThat(viaSQL.getStatus()).as(sql).isEqualTo(viaCreate.getStatus());

      final DocumentType createdBacking = database.getSchema().getType("ViaCreate");
      final DocumentType sqlBacking = database.getSchema().getType("ViaSQL");
      assertThat(sqlBacking.getBuckets(false)).as(sql).hasSameSizeAs(createdBacking.getBuckets(false));
      assertThat(((LocalBucket) sqlBacking.getBuckets(false).getFirst()).getPageSize()).as(sql)
          .isEqualTo(((LocalBucket) createdBacking.getBuckets(false).getFirst()).getPageSize());
      assertThat(database.countType("ViaSQL", false)).as(sql).isEqualTo(database.countType("ViaCreate", false));

      database.getSchema().dropMaterializedView("ViaCreate");
      database.getSchema().dropMaterializedView("ViaSQL");
    }

    @Test
    void aManualViewWithBucketsAndPageSizeIsTheSameThroughSQL() {
      assertParity(b -> b.withQuery("SELECT name FROM Account WHERE active = true").withTotalBuckets(2).withPageSize(131072));
    }

    @Test
    void anIncrementalViewIsTheSameThroughSQL() {
      assertParity(b -> b.withQuery("SELECT name FROM Account WHERE active = true")
          .withRefreshMode(MaterializedViewRefreshMode.INCREMENTAL));
    }

    @Test
    void aPeriodicViewKeepsItsIntervalThroughSQL() {
      assertParity(b -> b.withQuery("SELECT name FROM Account").withRefreshMode(MaterializedViewRefreshMode.PERIODIC)
          .withRefreshInterval(90L * 60_000L));
    }

    @Test
    void aPeriodicViewWithNoIntervalIsTheSameThroughSQL() {
      assertParity(b -> b.withQuery("SELECT name FROM Account").withRefreshMode(MaterializedViewRefreshMode.PERIODIC));
    }

    @Test
    void rendersEveryClauseTheBuilderCarries() {
      final String sql = database.getSchema().buildMaterializedView().withName("V").withQuery("SELECT name FROM Account")
          .withRefreshMode(MaterializedViewRefreshMode.PERIODIC).withRefreshInterval(2L * 3_600_000L)
          .withTotalBuckets(4).withPageSize(65536).withIgnoreIfExists(true).toSQL();

      assertThat(sql).isEqualTo(
          "CREATE MATERIALIZED VIEW IF NOT EXISTS `V` AS SELECT name FROM Account REFRESH EVERY 2 HOUR BUCKETS 4 PAGESIZE 65536");
    }

    @Test
    void theIntervalUsesTheLargestUnitThatDividesIt() {
      final MaterializedViewBuilder builder = database.getSchema().buildMaterializedView().withName("V")
          .withQuery("SELECT FROM Account").withRefreshMode(MaterializedViewRefreshMode.PERIODIC);

      assertThat(builder.withRefreshInterval(45_000L).toSQL()).endsWith("REFRESH EVERY 45 SECOND");
      assertThat(builder.withRefreshInterval(5L * 60_000L).toSQL()).endsWith("REFRESH EVERY 5 MINUTE");
      assertThat(builder.withRefreshInterval(0L).toSQL()).endsWith("REFRESH EVERY 0 SECOND");
    }

    @Test
    void aNameCollidingWithAKeywordOrCarryingABackslashIsQuoted() {
      for (final String name : new String[] { "select", "a\\b" }) {
        final String sql = database.getSchema().buildMaterializedView().withName(name).withQuery("SELECT FROM Account").toSQL();
        final Statement parsed = parse(sql);
        assertThat(parsed).isInstanceOf(CreateMaterializedViewStatement.class);
        assertThat(((CreateMaterializedViewStatement) parsed).name.getStringValue()).isEqualTo(name);
      }
    }

    /**
     * Code review on PR #8727: the query is emitted verbatim and the refresh/bucket/page-size clauses follow it, so a
     * trailing {@code ;} ended the statement early and a trailing {@code --} comment swallowed every clause after it -
     * silently, creating a MANUAL view with default buckets.
     */
    @Test
    void aTrailingSemicolonOrLineCommentInTheQueryDoesNotSwallowTheClauses() {
      for (final String query : new String[] { "SELECT name FROM Account ; ", "SELECT name FROM Account -- the accounts" }) {
        final String sql = database.getSchema().buildMaterializedView().withName("Swallow").withQuery(query)
            .withRefreshMode(MaterializedViewRefreshMode.INCREMENTAL).withTotalBuckets(3).toSQL();

        final CreateMaterializedViewStatement parsed = (CreateMaterializedViewStatement) parse(sql);
        assertThat(parsed.refreshMode).as(sql).isEqualTo("INCREMENTAL");
        assertThat(parsed.buckets).as(sql).isEqualTo(3);

        database.command("sql", sql);
        assertThat(database.getSchema().getMaterializedView("Swallow").getRefreshMode()).as(sql)
            .isEqualTo(MaterializedViewRefreshMode.INCREMENTAL);
        assertThat(database.getSchema().getType("Swallow").getBuckets(false)).as(sql).hasSize(3);
        database.getSchema().dropMaterializedView("Swallow");
      }
    }

    @Test
    void aSubSecondIntervalHasNoSQLExpression() {
      assertThatThrownBy(() -> database.getSchema().buildMaterializedView().withName("V").withQuery("SELECT FROM Account")
          .withRefreshMode(MaterializedViewRefreshMode.PERIODIC).withRefreshInterval(1_500L).toSQL())
          .isInstanceOf(SchemaException.class).hasMessageContaining("whole number of seconds");
    }

    @Test
    void anIntervalBeyondTheIntegerLiteralHasNoSQLExpression() {
      assertThatThrownBy(() -> database.getSchema().buildMaterializedView().withName("V").withQuery("SELECT FROM Account")
          .withRefreshMode(MaterializedViewRefreshMode.PERIODIC).withRefreshInterval((Integer.MAX_VALUE + 1L) * 3_600_000L + 1_000L).toSQL())
          .isInstanceOf(SchemaException.class).hasMessageContaining("integer literal");
    }

    @Test
    void anIntervalOnANonPeriodicViewHasNoSQLExpression() {
      assertThatThrownBy(() -> database.getSchema().buildMaterializedView().withName("V").withQuery("SELECT FROM Account")
          .withRefreshMode(MaterializedViewRefreshMode.INCREMENTAL).withRefreshInterval(60_000L).toSQL())
          .isInstanceOf(SchemaException.class).hasMessageContaining("only REFRESH EVERY");
    }

    /**
     * Issue #9380: an embedded {@code create()} stored an interval on a MANUAL or INCREMENTAL view - an interval nothing
     * schedules - while the remote builder refused the same state through {@code toSQL()}. Both paths now refuse it, and
     * the embedded refusal leaves neither the view nor its backing type behind.
     */
    @Test
    void anIntervalOnANonPeriodicViewIsRefusedOnBothPaths() {
      for (final MaterializedViewRefreshMode mode : new MaterializedViewRefreshMode[] { MaterializedViewRefreshMode.MANUAL,
          MaterializedViewRefreshMode.INCREMENTAL }) {
        final String name = "NonPeriodic" + mode;
        final MaterializedViewBuilder builder = database.getSchema().buildMaterializedView().withName(name)
            .withQuery("SELECT FROM Account").withRefreshMode(mode).withRefreshInterval(5_000L);

        assertThatThrownBy(builder::toSQL).isInstanceOf(SchemaException.class).hasMessageContaining("only REFRESH EVERY");
        assertThatThrownBy(builder::create).isInstanceOf(SchemaException.class).hasMessageContaining("only REFRESH EVERY");

        assertThat(database.getSchema().existsMaterializedView(name)).isFalse();
        assertThat(database.getSchema().existsType(name)).isFalse();

        // The same mode with no interval is still accepted embedded, so the refusal is about the interval alone
        final MaterializedView view = builder.withRefreshInterval(0L).create();
        assertThat(view.getRefreshMode()).isEqualTo(mode);
        assertThat(view.getRefreshInterval()).isZero();
      }
    }

    @Test
    void aNegativeIntervalAndANullModeAreRefusedOnBothPaths() {
      final Function<String, MaterializedViewBuilder> negative = n -> database.getSchema().buildMaterializedView().withName(n)
          .withQuery("SELECT FROM Account").withRefreshMode(MaterializedViewRefreshMode.PERIODIC).withRefreshInterval(-1_000L);
      assertThatThrownBy(() -> negative.apply("N1").toSQL()).isInstanceOf(IllegalArgumentException.class)
          .hasMessageContaining("cannot be negative");
      assertThatThrownBy(() -> negative.apply("N2").create()).isInstanceOf(IllegalArgumentException.class)
          .hasMessageContaining("cannot be negative");

      final Function<String, MaterializedViewBuilder> nullMode = n -> database.getSchema().buildMaterializedView().withName(n)
          .withQuery("SELECT FROM Account").withRefreshMode(null);
      assertThatThrownBy(() -> nullMode.apply("M1").toSQL()).isInstanceOf(IllegalArgumentException.class)
          .hasMessageContaining("refresh mode is required");
      assertThatThrownBy(() -> nullMode.apply("M2").create()).isInstanceOf(IllegalArgumentException.class)
          .hasMessageContaining("refresh mode is required");

      assertThat(database.getSchema().existsMaterializedView("N2")).isFalse();
      assertThat(database.getSchema().existsMaterializedView("M2")).isFalse();
    }

    @Test
    void aNameWithABacktickIsRefusedBeforeRendering() {
      assertThatThrownBy(() -> database.getSchema().buildMaterializedView().withName("a`b").withQuery("SELECT FROM Account").toSQL())
          .isInstanceOf(IllegalArgumentException.class).hasMessageContaining("backtick");
      assertThatThrownBy(() -> database.getSchema().buildContinuousAggregate().withName("a`b").withQuery(AGGREGATE_QUERY).toSQL())
          .isInstanceOf(IllegalArgumentException.class).hasMessageContaining("backtick");
    }

    @Test
    void thePageSizeClauseParsesAndRendersBack() {
      final String sql = "CREATE MATERIALIZED VIEW V AS SELECT FROM Account BUCKETS 2 PAGESIZE 65536";
      final CreateMaterializedViewStatement parsed = (CreateMaterializedViewStatement) parse(sql);
      assertThat(parsed.buckets).isEqualTo(2);
      assertThat(parsed.pageSize).isEqualTo(65536);
      assertThat(parsed.toString()).contains("BUCKETS 2 PAGESIZE 65536");
      assertThat(parsed.copy()).isEqualTo(parsed);

      final CreateMaterializedViewStatement pageSizeOnly = (CreateMaterializedViewStatement) parse(
          "CREATE MATERIALIZED VIEW V AS SELECT FROM Account PAGESIZE 65536");
      assertThat(pageSizeOnly.buckets).isZero();
      assertThat(pageSizeOnly.pageSize).isEqualTo(65536);
    }

    @Test
    void theContinuousAggregateIsTheSameThroughSQL() {
      database.transaction(() -> {
        database.command("sql", "INSERT INTO SensorReading SET ts = 1000, sensor_id = 'a', temperature = 10.0");
        database.command("sql", "INSERT INTO SensorReading SET ts = 2000, sensor_id = 'a', temperature = 20.0");
        database.command("sql", "INSERT INTO SensorReading SET ts = 3601000, sensor_id = 'b', temperature = 5.0");
      });

      final ContinuousAggregate viaCreate = database.getSchema().buildContinuousAggregate().withName("AggViaCreate")
          .withQuery(AGGREGATE_QUERY).create();
      final String sql = database.getSchema().buildContinuousAggregate().withName("AggViaSQL").withQuery(AGGREGATE_QUERY).toSQL();
      assertThat(sql).isEqualTo("CREATE CONTINUOUS AGGREGATE `AggViaSQL` AS " + AGGREGATE_QUERY);
      database.command("sql", sql);
      final ContinuousAggregate viaSQL = database.getSchema().getContinuousAggregate("AggViaSQL");

      assertThat(viaSQL.getSourceTypeName()).isEqualTo(viaCreate.getSourceTypeName());
      assertThat(viaSQL.getBucketIntervalMs()).isEqualTo(viaCreate.getBucketIntervalMs());
      assertThat(viaSQL.getBucketColumn()).isEqualTo(viaCreate.getBucketColumn());
      assertThat(viaSQL.getTimestampColumn()).isEqualTo(viaCreate.getTimestampColumn());
      assertThat(viaSQL.getStatus()).isEqualTo(viaCreate.getStatus());
      assertThat(database.countType("AggViaSQL", false)).isEqualTo(database.countType("AggViaCreate", false)).isPositive();
    }

    @Test
    void theContinuousAggregateRendersIfNotExists() {
      assertThat(database.getSchema().buildContinuousAggregate().withName("A").withQuery(AGGREGATE_QUERY)
          .withIgnoreIfExists(true).toSQL()).startsWith("CREATE CONTINUOUS AGGREGATE IF NOT EXISTS `A` AS ");
    }
  }

  /**
   * Regression for #7457: the drop and the alter of a materialized view no longer hold the schema monitor across the database write lock.
   * <p>
   * Companion of {@link SchemaWriteLockTest.SchemaSaveUnderWriteLock} for the materialized view lifecycle: since #7457 the
   * drop and the alter of a view no longer hold the schema monitor across the database write lock, so the teardown of
   * a view's refresh resources (incremental listeners, periodic task) moved INSIDE the write-locked transition, next
   * to the removal or replacement of the view itself. What a drop tears down is therefore always what the view it
   * removed owned, including resources an alter or a create installed under the same lock just before.
   *
   * @author Luca Garulli (l.garulli@arcadedata.com)
   */
  @Nested
  class MaterializedViewLifecycleUnderWriteLock extends TestHelper {

    @Override
    protected void beginTest() {
      final DocumentType source = database.getSchema().createDocumentType("Source");
      source.createProperty("value", Integer.class);
      database.transaction(() -> database.newDocument("Source").set("value", 1).save());
    }

    @Test
    void dropTearsDownTheListenersAnAlterInstalled() {
      final Schema schema = database.getSchema();
      schema.buildMaterializedView().withName("View").withQuery("SELECT value FROM Source")
          .withRefreshMode(MaterializedViewRefreshMode.MANUAL).create();

      schema.alterMaterializedView("View", MaterializedViewRefreshMode.INCREMENTAL, 0);
      final MaterializedViewImpl altered = (MaterializedViewImpl) schema.getMaterializedView("View");
      assertThat(altered.getChangeListener()).as("the alter installed the incremental listener").isNotNull();

      schema.dropMaterializedView("View");

      assertThat(altered.getChangeListener()).as("the drop tore down the listener the alter installed").isNull();
      assertThat(schema.existsMaterializedView("View")).isFalse();
      assertThat(schema.existsType("View")).isFalse();
      // A LISTENER LEFT BEHIND WOULD TRY TO REFRESH A VIEW WHOSE BACKING TYPE IS GONE
      database.transaction(() -> database.newDocument("Source").set("value", 2).save());
    }

    @Test
    void alterReplacesTheListenersUnderTheSameLockAsTheView() {
      final Schema schema = database.getSchema();
      schema.buildMaterializedView().withName("View").withQuery("SELECT value FROM Source")
          .withRefreshMode(MaterializedViewRefreshMode.INCREMENTAL).create();
      final MaterializedViewImpl created = (MaterializedViewImpl) schema.getMaterializedView("View");
      assertThat(created.getChangeListener()).isNotNull();

      schema.alterMaterializedView("View", MaterializedViewRefreshMode.MANUAL, 0);

      assertThat(created.getChangeListener()).as("the alter tore down the listener of the replaced view").isNull();
      assertThat(((MaterializedViewImpl) schema.getMaterializedView("View")).getChangeListener()).isNull();
      schema.dropMaterializedView("View");
    }

    /** The two drops of the same view: the second fails, and it fails without having torn anything down twice. */
    @Test
    void secondDropOfTheSameViewFails() {
      final Schema schema = database.getSchema();
      schema.buildMaterializedView().withName("View").withQuery("SELECT value FROM Source")
          .withRefreshMode(MaterializedViewRefreshMode.MANUAL).create();
      schema.dropMaterializedView("View");
      assertThatThrownBy(() -> schema.dropMaterializedView("View")).isInstanceOf(SchemaException.class)
          .hasMessageContaining("not found");
    }

    /** Two concurrent drops of the same view: exactly one succeeds, the other fails as not found, nothing deadlocks. */
    @Test
    void concurrentDropsOfTheSameViewProduceOneSuccessAndOneNotFound() throws Exception {
      final Schema schema = database.getSchema();
      schema.buildMaterializedView().withName("View").withQuery("SELECT value FROM Source")
          .withRefreshMode(MaterializedViewRefreshMode.INCREMENTAL).create();
      final MaterializedViewImpl view = (MaterializedViewImpl) schema.getMaterializedView("View");

      final int droppers = 4;
      final CyclicBarrier start = new CyclicBarrier(droppers);
      final AtomicInteger succeeded = new AtomicInteger();
      final AtomicInteger notFound = new AtomicInteger();
      final AtomicReference<Throwable> unexpected = new AtomicReference<>();
      final Thread[] threads = new Thread[droppers];
      for (int i = 0; i < droppers; i++) {
        threads[i] = new Thread(() -> {
          try {
            start.await(10, TimeUnit.SECONDS);
            schema.dropMaterializedView("View");
            succeeded.incrementAndGet();
          } catch (final SchemaException e) {
            if (e.getMessage().contains("not found"))
              notFound.incrementAndGet();
            else
              unexpected.set(e);
          } catch (final Throwable e) {
            unexpected.set(e);
          }
        }, "dropper-" + i);
        threads[i].start();
      }
      for (final Thread thread : threads)
        thread.join(30_000);

      assertThat(unexpected.get()).isNull();
      assertThat(succeeded.get()).isEqualTo(1);
      assertThat(notFound.get()).isEqualTo(droppers - 1);
      assertThat(view.getChangeListener()).as("the one drop that won tore the listener down").isNull();
      assertThat(schema.existsMaterializedView("View")).isFalse();
      assertThat(schema.existsType("View")).isFalse();
      database.transaction(() -> database.newDocument("Source").set("value", 3).save());
    }

    /**
     * Alters racing a drop of the same view: every alter either completes before the drop, replacing the listener under
     * the write lock, or fails as not found after it. Whatever the interleaving, the one drop tears down the resources
     * of the view it removed, and nothing is left listening on the source type.
     */
    @Test
    void concurrentAltersAndDropOfTheSameViewLeaveNothingBehind() throws Exception {
      final Schema schema = database.getSchema();
      schema.buildMaterializedView().withName("View").withQuery("SELECT value FROM Source")
          .withRefreshMode(MaterializedViewRefreshMode.INCREMENTAL).create();

      final int alterers = 3;
      final CyclicBarrier start = new CyclicBarrier(alterers + 1);
      final AtomicInteger altered = new AtomicInteger();
      final AtomicInteger alterNotFound = new AtomicInteger();
      final AtomicReference<Throwable> unexpected = new AtomicReference<>();
      final Thread[] threads = new Thread[alterers + 1];
      for (int i = 0; i < alterers; i++) {
        final MaterializedViewRefreshMode mode = i % 2 == 0 ? MaterializedViewRefreshMode.MANUAL : MaterializedViewRefreshMode.INCREMENTAL;
        threads[i] = new Thread(() -> {
          try {
            start.await(10, TimeUnit.SECONDS);
            schema.alterMaterializedView("View", mode, 0);
            altered.incrementAndGet();
          } catch (final SchemaException e) {
            if (e.getMessage().contains("not found"))
              alterNotFound.incrementAndGet();
            else
              unexpected.set(e);
          } catch (final Throwable e) {
            unexpected.set(e);
          }
        }, "alterer-" + i);
      }
      threads[alterers] = new Thread(() -> {
        try {
          start.await(10, TimeUnit.SECONDS);
          schema.dropMaterializedView("View");
        } catch (final Throwable e) {
          unexpected.set(e);
        }
      }, "dropper");
      for (final Thread thread : threads)
        thread.start();
      for (final Thread thread : threads)
        thread.join(30_000);

      assertThat(unexpected.get()).isNull();
      assertThat(altered.get() + alterNotFound.get()).isEqualTo(alterers);
      assertThat(schema.existsMaterializedView("View")).isFalse();
      assertThat(schema.existsType("View")).isFalse();
      // WHATEVER LISTENER THE LAST ALTER BEFORE THE DROP INSTALLED, THE DROP TORE IT DOWN: THE INSERT IS HARMLESS
      database.transaction(() -> database.newDocument("Source").set("value", 4).save());
    }

    /** The transition runs inside one outermost recording frame, whichever resources it installs or tears down. */
    @Test
    void lifecycleTransitionIsOneWriteLockedFrame() {
      final Schema schema = database.getSchema();
      final DatabaseInternal db = (DatabaseInternal) database;
      schema.buildMaterializedView().withName("View").withQuery("SELECT value FROM Source")
          .withRefreshMode(MaterializedViewRefreshMode.INCREMENTAL).create();

      final AtomicInteger frames = new AtomicInteger();
      final Callable<Void> observer = () -> {
        frames.incrementAndGet();
        return null;
      };
      db.registerCallback(DatabaseInternal.CALLBACK_EVENT.SCHEMA_AFTER_FILE_CHANGES, observer);
      try {
        schema.alterMaterializedView("View", MaterializedViewRefreshMode.MANUAL, 0);
        assertThat(frames.get()).isEqualTo(1);
        schema.dropMaterializedView("View");
        assertThat(frames.get()).isEqualTo(2);
      } finally {
        db.unregisterCallback(DatabaseInternal.CALLBACK_EVENT.SCHEMA_AFTER_FILE_CHANGES, observer);
      }
    }
  }

  /**
   * Regression for #7457: the drop of a continuous aggregate checks and removes under the lock, concurrent drops must not fail.
   * <p>
   * Companion of {@link MaterializedViewLifecycleUnderWriteLock} for continuous aggregates, whose drop has
   * the same check-then-remove-under-the-lock shape since #7457: two concurrent drops of the same aggregate produce
   * exactly one success and one "not found", and nothing deadlocks against the schema saves that take the schema
   * monitor under the write lock.
   *
   * @author Luca Garulli (l.garulli@arcadedata.com)
   */
  @Nested
  class ContinuousAggregateConcurrentDrop extends TestHelper {

    @Override
    protected void beginTest() {
      database.command("sql",
          "CREATE TIMESERIES TYPE SensorReading TIMESTAMP ts TAGS (sensor_id STRING) FIELDS (temperature DOUBLE)");
      database.transaction(() -> {
        database.command("sql", "INSERT INTO SensorReading SET ts = 1000, sensor_id = 'A', temperature = 22.5");
        database.command("sql", "INSERT INTO SensorReading SET ts = 2000, sensor_id = 'B', temperature = 23.1");
      });
    }

    @Test
    void concurrentDropsOfTheSameAggregateProduceOneSuccessAndOneNotFound() throws Exception {
      final Schema schema = database.getSchema();
      schema.buildContinuousAggregate().withName("hourly_temps").withQuery(
          "SELECT sensor_id, ts.timeBucket('1h', ts) AS hour, avg(temperature) AS avg_temp FROM SensorReading GROUP BY sensor_id, hour")
          .create();
      assertThat(schema.existsContinuousAggregate("hourly_temps")).isTrue();
      assertThat(schema.existsType("hourly_temps")).isTrue();

      final int droppers = 4;
      final CyclicBarrier start = new CyclicBarrier(droppers);
      final AtomicInteger succeeded = new AtomicInteger();
      final AtomicInteger notFound = new AtomicInteger();
      final AtomicReference<Throwable> unexpected = new AtomicReference<>();
      final Thread[] threads = new Thread[droppers];
      for (int i = 0; i < droppers; i++) {
        threads[i] = new Thread(() -> {
          try {
            start.await(10, TimeUnit.SECONDS);
            schema.dropContinuousAggregate("hourly_temps");
            succeeded.incrementAndGet();
          } catch (final SchemaException e) {
            if (e.getMessage().contains("not found"))
              notFound.incrementAndGet();
            else
              unexpected.set(e);
          } catch (final Throwable e) {
            unexpected.set(e);
          }
        }, "dropper-" + i);
        threads[i].start();
      }
      for (final Thread thread : threads)
        thread.join(30_000);

      assertThat(unexpected.get()).isNull();
      assertThat(succeeded.get()).isEqualTo(1);
      assertThat(notFound.get()).isEqualTo(droppers - 1);
      assertThat(schema.existsContinuousAggregate("hourly_temps")).isFalse();
      assertThat(schema.existsType("hourly_temps")).as("the backing type went with the aggregate, once").isFalse();
      assertThat(schema.existsType("SensorReading")).isTrue();
    }

    @Test
    void secondDropOfTheSameAggregateFails() {
      final Schema schema = database.getSchema();
      schema.buildContinuousAggregate().withName("hourly_temps").withQuery(
          "SELECT sensor_id, ts.timeBucket('1h', ts) AS hour, avg(temperature) AS avg_temp FROM SensorReading GROUP BY sensor_id, hour")
          .create();
      schema.dropContinuousAggregate("hourly_temps");
      assertThatThrownBy(() -> schema.dropContinuousAggregate("hourly_temps")).isInstanceOf(SchemaException.class)
          .hasMessageContaining("not found");
    }
  }

  /**
   * Regression for #11464: an INCREMENTAL view refreshes on the thread that committed, and the owner of the refresh used to keep making
   * passes for as long as other writers kept requesting one. Under a steady write load from several clients one of them never
   * got its commit back (a 30 s stall in the three-node load test, until its HTTP read timed out). The committing thread now
   * makes the one pass it owes and hands any further pass to the schema's scheduler thread.
   */
  @Nested
  class IncrementalRefreshBoundedOnCommitter extends TestHelper {
    /**
     * ORDER BY keeps the view off the per-record path (#11475): these tests count the writes of whole-view passes, each of which
     * rewrites every row.
     */
    private static final String LEGACY_QUERY = "SELECT value FROM Src11464 ORDER BY value";

    @Test
    void theCommitterMakesOnePassAndALaterPassRunsOffItsThread() throws Exception {
      final List<String> writers = runTwoOverlappingCommits(false);
      assertThat(writers.stream().filter("writer-11464"::equals).count())
          .as("the committer wrote one pass to the view, not the follow-up pass the other writer asked for (writers: %s)", writers)
          .isEqualTo(1);
    }

    /** A scheduler that cannot take the hand-off leaves the passes with the committer, as before: the request is still serviced. */
    @Test
    void aSchedulerThatCannotTakeTheHandOffLeavesThePassesWithTheCommitter() throws Exception {
      final List<String> writers = runTwoOverlappingCommits(true);
      assertThat(writers.stream().filter("writer-11464"::equals).count())
          .as("the committer made the follow-up pass itself (writers: %s)", writers)
          .isEqualTo(3);
    }

    /** A hand-off the scheduler accepted and a shutdown dropped before it ran must not leave the view looking VALID. */
    @Test
    void aHandOffDroppedByShutdownLeavesTheViewStale() throws Exception {
      database.transaction(() -> database.getSchema().createDocumentType("Src11464"));
      database.transaction(() -> database.getSchema().buildMaterializedView()
          .withName("Src11464View")
          .withQuery(LEGACY_QUERY)
          .withRefreshMode(MaterializedViewRefreshMode.INCREMENTAL)
          .create());
      final MaterializedViewImpl view = (MaterializedViewImpl) database.getSchema().getMaterializedView("Src11464View");
      final MaterializedViewScheduler scheduler = ((LocalSchema) database.getSchema()).getMaterializedViewScheduler();

      // occupy the scheduler thread so the hand-off stays queued
      final CountDownLatch busy = new CountDownLatch(1);
      final CountDownLatch release = new CountDownLatch(1);
      scheduler.handOff(database, view, () -> {
        busy.countDown();
        try {
          release.await(60, TimeUnit.SECONDS);
        } catch (final InterruptedException e) {
          Thread.currentThread().interrupt();
        }
      });
      assertThat(busy.await(30, TimeUnit.SECONDS)).isTrue();

      assertThat(view.tryBeginRefresh()).isTrue();
      final AtomicBoolean ran = new AtomicBoolean();
      assertThat(scheduler.handOff(database, view, () -> ran.set(true))).isTrue();

      final List<MaterializedViewImpl> dropped = scheduler.shutdown();
      release.countDown();

      assertThat(dropped).containsExactly(view);
      assertThat(ran.get()).as("the queued hand-off was dropped, not run").isFalse();
    }

    private List<String> runTwoOverlappingCommits(final boolean schedulerShutDown) throws Exception {
      database.transaction(() -> database.getSchema().createDocumentType("Src11464"));
      database.transaction(() -> database.getSchema().buildMaterializedView()
          .withName("Src11464View")
          .withQuery(LEGACY_QUERY)
          .withRefreshMode(MaterializedViewRefreshMode.INCREMENTAL)
          .create());

      // every write the refresh makes to the backing type is recorded with the thread that made it; the first one parks
      final List<String> writers = new CopyOnWriteArrayList<>();
      if (schedulerShutDown)
        ((LocalSchema) database.getSchema()).getMaterializedViewScheduler().shutdown();
      final CountDownLatch firstPassParked = new CountDownLatch(1);
      final CountDownLatch releaseFirstPass = new CountDownLatch(1);
      final DocumentTypeEvents events = new DocumentTypeEvents(database.getSchema().getType("Src11464View"));
      events.onWrite(() -> {
        writers.add(Thread.currentThread().getName());
        if (writers.size() == 1) {
          firstPassParked.countDown();
          try {
            releaseFirstPass.await(60, TimeUnit.SECONDS);
          } catch (final InterruptedException e) {
            Thread.currentThread().interrupt();
          }
        }
      });

      final Thread committer = new Thread(
          () -> database.transaction(() -> database.newDocument("Src11464").set("value", 1).save()), "writer-11464");
      committer.start();
      assertThat(firstPassParked.await(30, TimeUnit.SECONDS)).as("the committer is inside its refresh pass").isTrue();

      // a second writer commits while that pass runs: it hands its request to the running refresh and returns
      database.transaction(() -> database.newDocument("Src11464").set("value", 2).save());

      releaseFirstPass.countDown();
      committer.join(TimeUnit.SECONDS.toMillis(30));
      assertThat(committer.isAlive()).as("the committer got its commit back").isFalse();

      // the request it was handed is still serviced, by a pass that is not on the committer's thread
      final long deadline = System.currentTimeMillis() + 30_000;
      while (viewCount() != 2 && System.currentTimeMillis() < deadline)
        Thread.sleep(50);
      assertThat(viewCount()).as("the view reflects both commits").isEqualTo(2);

      return writers;
    }

    private long viewCount() {
      try (final ResultSet rs = database.query("sql", "SELECT count(*) AS cnt FROM Src11464View")) {
        return rs.next().<Number>getProperty("cnt").longValue();
      }
    }

    /** Registers one callback for the creates and updates of a type. */
    private static final class DocumentTypeEvents {
      private final DocumentType type;

      private DocumentTypeEvents(final DocumentType type) {
        this.type = type;
      }

      private void onWrite(final Runnable callback) {
        type.getEvents().registerListener((BeforeRecordCreateListener) record -> {
          callback.run();
          return true;
        });
        type.getEvents().registerListener((BeforeRecordUpdateListener) record -> {
          callback.run();
          return true;
        });
      }
    }
  }

  /**
   * Regression for #11470: the passes of an INCREMENTAL view handed to the schema's scheduler thread must not run as one unbounded loop on
   * it, because that single thread also runs every periodic refresh of every view of the database. A pass that leaves more owed
   * queues the next one behind whatever is already due.
   */
  @Nested
  class HandedOffRefreshYields extends TestHelper {

    @Test
    void aHandedOffPassQueuesTheNextOneBehindWorkAlreadyWaiting() throws Exception {
      database.transaction(() -> database.getSchema().createDocumentType("Src11470"));
      // ORDER BY keeps the view off the per-record path (#11475): the test counts the writes of whole-view passes of two rows each
      database.transaction(() -> database.getSchema().buildMaterializedView()
          .withName("Src11470View")
          .withQuery("SELECT value FROM Src11470 ORDER BY value")
          .withRefreshMode(MaterializedViewRefreshMode.INCREMENTAL)
          .create());
      final MaterializedViewImpl view = (MaterializedViewImpl) database.getSchema().getMaterializedView("Src11470View");
      final MaterializedViewScheduler scheduler = ((LocalSchema) database.getSchema()).getMaterializedViewScheduler();

      final List<String> events = new CopyOnWriteArrayList<>();
      final CountDownLatch committerParked = new CountDownLatch(1);
      final CountDownLatch releaseCommitter = new CountDownLatch(1);
      final AtomicBoolean probeQueued = new AtomicBoolean();
      final Runnable onWrite = () -> {
        final String thread = Thread.currentThread().getName();
        if (thread.startsWith("ArcadeDB-MV-Scheduler")) {
          events.add("scheduler-write");
          // sustained writers: during the first handed-off pass another pass is requested, and other work is waiting for the thread
          if (probeQueued.compareAndSet(false, true)) {
            scheduler.handOff(database, view, () -> events.add("other-work"));
            view.markRefreshPendingIfRunning();
          }
        } else {
          events.add("committer-write");
          if (committerParked.getCount() > 0) {
            committerParked.countDown();
            try {
              releaseCommitter.await(60, TimeUnit.SECONDS);
            } catch (final InterruptedException e) {
              Thread.currentThread().interrupt();
            }
          }
        }
      };
      final DocumentType backing = database.getSchema().getType("Src11470View");
      backing.getEvents().registerListener((BeforeRecordCreateListener) record -> {
        onWrite.run();
        return true;
      });
      backing.getEvents().registerListener((BeforeRecordUpdateListener) record -> {
        onWrite.run();
        return true;
      });

      final Thread committer = new Thread(
          () -> database.transaction(() -> database.newDocument("Src11470").set("value", 1).save()), "writer-11470");
      committer.start();
      assertThat(committerParked.await(30, TimeUnit.SECONDS)).isTrue();
      database.transaction(() -> database.newDocument("Src11470").set("value", 2).save());
      releaseCommitter.countDown();
      committer.join(TimeUnit.SECONDS.toMillis(30));

      // two handed-off passes of two rows each, and the other work
      final long deadline = System.currentTimeMillis() + 30_000;
      while (events.stream().filter("scheduler-write"::equals).count() < 4 && System.currentTimeMillis() < deadline)
        Thread.sleep(50);

      assertThat(events).as("the other work ran between the two handed-off passes, not after both").containsSubsequence(
          "scheduler-write", "scheduler-write", "other-work", "scheduler-write", "scheduler-write");
    }

    /** A node that stopped being the leader gives the handed-off refresh up: it releases the ownership and runs nothing. */
    @Test
    void aFollowerGivesTheHandedOffRefreshUpAndReleasesTheView() throws Exception {
      database.transaction(() -> database.getSchema().createDocumentType("Follower11470"));
      database.transaction(() -> database.getSchema().buildMaterializedView()
          .withName("Follower11470View")
          .withQuery("SELECT value FROM Follower11470")
          .withRefreshMode(MaterializedViewRefreshMode.INCREMENTAL)
          .create());
      final MaterializedViewImpl view = (MaterializedViewImpl) database.getSchema().getMaterializedView("Follower11470View");
      final MaterializedViewScheduler scheduler = ((LocalSchema) database.getSchema()).getMaterializedViewScheduler();
      final String statusBefore = view.getStatus();

      final DatabaseInternal follower = (DatabaseInternal) Proxy.newProxyInstance(DatabaseInternal.class.getClassLoader(),
          new Class<?>[] { DatabaseInternal.class }, (proxy, method, args) -> {
            if (method.getName().equals("isFollower"))
              return Boolean.TRUE;
            throw new UnsupportedOperationException(method.getName());
          });

      assertThat(view.tryBeginRefresh()).isTrue();
      assertThat(view.markRefreshPendingIfRunning()).isTrue();
      final AtomicBoolean ran = new AtomicBoolean();
      assertThat(scheduler.handOff(follower, view, () -> ran.set(true))).isTrue();

      final long deadline = System.currentTimeMillis() + 30_000;
      boolean released = false;
      while (!released && System.currentTimeMillis() < deadline) {
        released = view.tryBeginRefresh();
        if (!released)
          Thread.sleep(20);
      }
      assertThat(released).as("the ownership was released, so a later refresh can run").isTrue();
      assertThat(ran.get()).as("a follower does not refresh").isFalse();
      assertThat(view.getStatus()).as("the status stays as the last pass set it").isEqualTo(statusBefore);
      view.endRefresh();
    }
  }

  /**
   * Regression for #11550: a per-record materialized view heals itself after a crash.
   * <p>
   * A per-record materialized view heals itself after a crash (issue #11550): its pending delta and recovery mark live in memory
   * only, so a commit whose pass had not run when the process died is missing from a view persisted VALID. The open that replays
   * the WAL hands every per-record view the recovery full pass, on the scheduler thread, without a manual REFRESH.
   */
  @Nested
  class PerRecordViewSelfHeal extends TestHelper {
    private static final String QUERY = "SELECT name, age FROM Person WHERE age >= 18";

    @Test
    void aCommitWhosePassDiedWithTheProcessReachesTheViewAfterTheReopen() {
      createAdultsView();
      final MaterializedViewImpl view = view();
      assertThat(view.isPerRecord()).isTrue();

      // Own the refresh, as a pass still running would: the commit below only appends its RIDs to the pending delta and asks the
      // owner for a further pass, which the crash then takes down with the delta
      assertThat(view.tryBeginRefresh()).isTrue();
      database.transaction(() -> {
        database.command("sql", "UPDATE Person SET age = 99 WHERE name = 'a3'");
        database.newDocument("Person").set("name", "late").set("age", 40).save();
        database.command("sql", "DELETE FROM Person WHERE name = 'a5'");
      });
      assertThat(viewRows()).as("the pass of the commit has not run").doesNotContain("a3/99", "late/40").contains("a5/23");
      assertThat(view.getStatus()).isEqualTo(MaterializedViewStatus.VALID.name());

      ((DatabaseInternal) database).kill();
      database.close();
      database = factory.open();

      // No REFRESH: the open handed the view its recovery full pass
      Awaitility.await().atMost(30, TimeUnit.SECONDS).pollInterval(50, TimeUnit.MILLISECONDS)
          .until(() -> MaterializedViewStatus.VALID.name().equals(view().getStatus()) && viewRows().containsAll(List.of("a3/99", "late/40")));
      assertThat(viewRows()).containsExactlyInAnyOrderElementsOf(queryRows(QUERY));
      assertThat(viewRows()).doesNotContain("a5/23");

      // And the view keeps its per-record maintenance afterwards
      database.transaction(() -> database.command("sql", "UPDATE Person SET age = 77 WHERE name = 'a7'"));
      assertThat(viewRows()).contains("a7/77");
      assertThat(viewRows()).containsExactlyInAnyOrderElementsOf(queryRows(QUERY));
    }

    @Test
    void aViewPersistedNonValidConvergesAtTheNextOpenWithoutATrigger() {
      createAdultsView();
      final MaterializedViewImpl view = view();

      // A failed pass leaves the view non-VALID, persisted, and missing the records it drained: simulated by a commit whose pass
      // never ran, followed by the status the failure persists
      assertThat(view.tryBeginRefresh()).isTrue();
      database.transaction(() -> database.command("sql", "UPDATE Person SET age = 98 WHERE name = 'a2'"));
      view.drainPendingDelta();
      view.setStatus(MaterializedViewStatus.ERROR);
      MaterializedViewRefresher.persistStatus(database, view.getName(), view.getStatus());
      view.endRefresh();
      assertThat(viewRows()).doesNotContain("a2/98");

      reopenDatabase();

      Awaitility.await().atMost(30, TimeUnit.SECONDS).pollInterval(50, TimeUnit.MILLISECONDS)
          .until(() -> MaterializedViewStatus.VALID.name().equals(view().getStatus()) && viewRows().contains("a2/98"));
      assertThat(viewRows()).containsExactlyInAnyOrderElementsOf(queryRows(QUERY));

      // The VALID the recovery pass ended in was persisted
      reopenDatabase();
      assertThat(view().getStatus()).isEqualTo(MaterializedViewStatus.VALID.name());
    }

    @Test
    void aCleanReopenOfAValidViewRunsNoPass() {
      createAdultsView();

      reopenDatabase();

      // A recovery asked at the open says STALE before the open returns: a clean close leaves nothing owed, so nothing is asked
      assertThat(view().getStatus()).isEqualTo(MaterializedViewStatus.VALID.name());
      assertThat(view().owesRecoveryPass()).isFalse();
      assertThat(view().getRefreshCount()).isZero();
    }

    @Test
    void theLeaderRecoveryAsksEveryPerRecordView() {
      createAdultsView();
      database.transaction(() -> database.getSchema().buildMaterializedView().withName("Manual").withQuery(QUERY)
          .withRefreshMode(MaterializedViewRefreshMode.MANUAL).create());

      // What a node that just took the leadership asks: the per-record view only, the manual one is not maintained on commit
      assertThat(((LocalSchema) database.getSchema()).recoverPerRecordMaterializedViews()).isEqualTo(1);

      Awaitility.await().atMost(30, TimeUnit.SECONDS).pollInterval(50, TimeUnit.MILLISECONDS)
          .until(() -> MaterializedViewStatus.VALID.name().equals(view().getStatus()) && !view().owesRecoveryPass());
      assertThat(viewRows()).containsExactlyInAnyOrderElementsOf(queryRows(QUERY));
    }

    private void createAdultsView() {
      database.transaction(() -> {
        final DocumentType type = database.getSchema().createDocumentType("Person");
        type.createProperty("name", Type.STRING);
        type.createProperty("age", Type.INTEGER);
      });
      database.transaction(() -> {
        for (int i = 0; i < 50; i++)
          database.newDocument("Person").set("name", "a" + i).set("age", 18 + (i % 40)).save();
      });
      database.transaction(() -> database.getSchema().buildMaterializedView()
          .withName("Adults")
          .withQuery(QUERY)
          .withRefreshMode(MaterializedViewRefreshMode.INCREMENTAL)
          .create());
    }

    private MaterializedViewImpl view() {
      return (MaterializedViewImpl) database.getSchema().getMaterializedView("Adults");
    }

    private List<String> viewRows() {
      return queryRows("SELECT name, age FROM Adults");
    }

    private List<String> queryRows(final String sql) {
      final List<String> rows = new ArrayList<>();
      try (final ResultSet rs = database.query("sql", sql)) {
        while (rs.hasNext()) {
          final Result row = rs.next();
          rows.add(row.getProperty("name") + "/" + row.getProperty("age"));
        }
      }
      return rows;
    }
  }

  /**
   * Regression for #11552: {@code _sourceRID} is a reserved column on a per-record materialized view. A schemaless source record can carry
   * an undeclared field of that name, which a whole-record projection ({@code SELECT *}, {@code SELECT FROM}) copies into the row;
   * the link the refresh adds always wins over it, on the full pass and on the delta pass alike, so every row stays on its own
   * source record. A field holding the RID of ANOTHER source record is the case that matters: had it won, two rows would claim the
   * same source, and the next pass would write one over the other.
   */
  @Nested
  class PerRecordSourceRidReserved extends TestHelper {
    private static final String SOURCE_RID = MaterializedViewPerRecordRefresher.SOURCE_RID;

    @ParameterizedTest
    @ValueSource(strings = { "SELECT * FROM P", "SELECT FROM P", "SELECT *, name AS n FROM P" })
    void fullPassLinkWinsOverAnUndeclaredSourceRidField(final String query) {
      database.getSchema().createDocumentType("P");
      final RID first = insert("a", null);
      insert("b", "junk");
      insert("c", first);

      final MaterializedViewImpl view = createView(query);
      assertThat(view.isPerRecord()).as("per-record: " + query).isTrue();
      assertEveryRowOnItsOwnSource(3);

      // the explicit full pass, over rows that already exist: each one is matched by its link and written in place
      final Map<RID, RID> before = rowBySource();
      database.command("sql", "REFRESH MATERIALIZED VIEW V");
      assertEveryRowOnItsOwnSource(3);
      assertThat(rowBySource()).isEqualTo(before);
    }

    @ParameterizedTest
    @ValueSource(strings = { "SELECT * FROM P", "SELECT FROM P" })
    void deltaPassLinkWinsOverAnUndeclaredSourceRidField(final String query) {
      database.getSchema().createDocumentType("P");
      final RID first = insert("a", null);
      final RID second = insert("b", null);
      createView(query);
      assertEveryRowOnItsOwnSource(2);

      // created with the field: a string, and the RID of another source record that already has its row
      insert("c", "junk");
      insert("d", first);
      assertEveryRowOnItsOwnSource(4);

      // an existing source gains the field on update
      database.transaction(() -> ((Document) database.lookupByRID(second, true)).modify().set(SOURCE_RID, first).save());
      assertEveryRowOnItsOwnSource(4);

      // and the full pass that follows agrees with the delta passes
      database.command("sql", "REFRESH MATERIALIZED VIEW V");
      assertEveryRowOnItsOwnSource(4);
    }

    /**
     * The classifier reads the schema at DDL time only, so a property declared after the view was created is the same case as an
     * undeclared field: the view stays per-record and the link wins.
     */
    @Test
    void propertyDeclaredAfterTheViewIsOverriddenByTheLink() {
      database.getSchema().createDocumentType("P");
      final RID first = insert("a", null);
      createView("SELECT * FROM P");
      assertThat(((MaterializedViewImpl) database.getSchema().getMaterializedView("V")).isPerRecord()).isTrue();

      database.command("sql", "CREATE PROPERTY P." + SOURCE_RID + " STRING");
      insert("b", "declared");
      insert("c", first.toString());
      assertEveryRowOnItsOwnSource(3);

      database.command("sql", "REFRESH MATERIALIZED VIEW V");
      assertEveryRowOnItsOwnSource(3);
    }

    private RID insert(final String name, final Object sourceRidField) {
      final RID[] rid = new RID[1];
      database.transaction(() -> {
        final MutableDocument doc = database.newDocument("P").set("name", name);
        if (sourceRidField != null)
          doc.set(SOURCE_RID, sourceRidField);
        rid[0] = doc.save().getIdentity();
      });
      return rid[0];
    }

    private MaterializedViewImpl createView(final String query) {
      database.transaction(() -> database.getSchema().buildMaterializedView()
          .withName("V")
          .withQuery(query)
          .withRefreshMode(MaterializedViewRefreshMode.INCREMENTAL)
          .create());
      return (MaterializedViewImpl) database.getSchema().getMaterializedView("V");
    }

    /** Every source record has exactly one row, linked to it, carrying its name; and the view is VALID. */
    private void assertEveryRowOnItsOwnSource(final int expectedRows) {
      assertThat(database.getSchema().getMaterializedView("V").getStatus()).isEqualTo(MaterializedViewStatus.VALID.name());
      assertThat(database.countType("P", true)).isEqualTo(expectedRows);

      final Map<RID, RID> rows = rowBySource();
      assertThat(rows).hasSize(expectedRows);
      try (final ResultSet rs = database.query("sql", "SELECT FROM V")) {
        while (rs.hasNext()) {
          final Result row = rs.next();
          final Object link = row.getProperty(SOURCE_RID);
          assertThat(link).as("the link column holds a RID, not the source's own field: " + row.toJSON()).isInstanceOf(RID.class);
          final Document source = (Document) database.lookupByRID((RID) link, true);
          assertThat(source.getTypeName()).isEqualTo("P");
          assertThat(row.<String>getProperty("name")).as("row " + row.toJSON() + " sits on its own source").isEqualTo(source.getString("name"));
        }
      }
    }

    private Map<RID, RID> rowBySource() {
      final Map<RID, RID> map = new HashMap<>();
      try (final ResultSet rs = database.query("sql", "SELECT FROM V")) {
        while (rs.hasNext()) {
          final Result row = rs.next();
          final RID source = row.getProperty(SOURCE_RID);
          assertThat(map.put(source, row.getIdentity().get())).as("one row per source record").isNull();
        }
      }
      return map;
    }
  }
}
