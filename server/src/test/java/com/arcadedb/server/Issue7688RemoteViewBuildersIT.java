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
package com.arcadedb.server;

import com.arcadedb.database.Database;
import com.arcadedb.engine.LocalBucket;
import com.arcadedb.exception.SchemaException;
import com.arcadedb.remote.RemoteContinuousAggregate;
import com.arcadedb.remote.RemoteDatabase;
import com.arcadedb.schema.ContinuousAggregate;
import com.arcadedb.schema.DocumentType;
import com.arcadedb.schema.MaterializedView;
import com.arcadedb.schema.MaterializedViewRefreshMode;
import com.arcadedb.schema.Schema;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.Arrays;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Issue #7688: {@code Schema.buildMaterializedView()} and {@code Schema.buildContinuousAggregate()} used to be
 * implementable only by the embedded schema - both builders demanded a {@code DatabaseInternal} - so
 * {@code RemoteSchema} could do nothing but throw. The same shape as #7399 for {@code buildTimeSeriesType()}.
 * <p>
 * The acceptance tests run ONE body of builder code, written against the {@link Schema} interface, twice - once
 * against the embedded database, once against a {@link RemoteDatabase} - and compare the two results attribute by
 * attribute. A test that only exercised the remote path would prove the code runs, not that the two agree.
 */
class Issue7688RemoteViewBuildersIT extends BaseGraphServerTest {

  private static final String AGGREGATE_QUERY =
      "SELECT sensor_id, ts.timeBucket('1h', ts) AS hour, avg(temperature) AS avg_temp FROM SensorReading GROUP BY sensor_id, hour";

  /** The view recipe under test. It names no implementation: the {@link Schema} it is handed decides how it is built. */
  private static MaterializedView applyTheViewRecipe(final Schema schema, final String name) {
    return schema.buildMaterializedView()
        .withName(name)
        .withQuery("SELECT name FROM Account WHERE active = true")
        .withRefreshMode(MaterializedViewRefreshMode.INCREMENTAL)
        .withTotalBuckets(2)
        .withPageSize(131072)
        .create();
  }

  private static MaterializedView applyThePeriodicRecipe(final Schema schema, final String name) {
    return schema.buildMaterializedView()
        .withName(name)
        .withQuery("SELECT name FROM Account")
        .withRefreshMode(MaterializedViewRefreshMode.PERIODIC)
        .withRefreshInterval(90L * 60_000L)
        .create();
  }

  private static ContinuousAggregate applyTheAggregateRecipe(final Schema schema, final String name) {
    return schema.buildContinuousAggregate().withName(name).withQuery(AGGREGATE_QUERY).create();
  }

  /**
   * The server's OWN database instance, not {@code getDatabase(0)}: that one is the fixture's separate handle, which
   * {@link BaseGraphServerTest} closes before starting the servers.
   */
  private Database embedded() {
    return getServerDatabase(0, getDatabaseName());
  }

  /** The port the test server actually bound, never a hand-picked one. */
  private RemoteDatabase remote() {
    return new RemoteDatabase("127.0.0.1", getServerHttpPort(0), getDatabaseName(), "root", DEFAULT_PASSWORD_FOR_TESTS);
  }

  @BeforeEach
  void createSourceTypes() {
    final Database database = embedded();
    if (!database.getSchema().existsType("Account"))
      database.transaction(() -> {
        database.getSchema().createDocumentType("Account");
        database.newDocument("Account").set("name", "Alice").set("active", true).save();
        database.newDocument("Account").set("name", "Bob").set("active", false).save();
        database.newDocument("Account").set("name", "Carol").set("active", true).save();
      });
    if (!database.getSchema().existsType("SensorReading")) {
      database.command("sql",
          "CREATE TIMESERIES TYPE SensorReading TIMESTAMP ts TAGS (sensor_id STRING) FIELDS (temperature DOUBLE)");
      database.transaction(() -> {
        database.command("sql", "INSERT INTO SensorReading SET ts = 1000, sensor_id = 'a', temperature = 10.0");
        database.command("sql", "INSERT INTO SensorReading SET ts = 2000, sensor_id = 'a', temperature = 20.0");
        database.command("sql", "INSERT INTO SensorReading SET ts = 3601000, sensor_id = 'b', temperature = 5.0");
      });
    }
  }

  private void assertSameBackingType(final String expectedName, final String actualName) {
    final DocumentType expected = embedded().getSchema().getType(expectedName);
    final DocumentType actual = embedded().getSchema().getType(actualName);
    assertThat(actual.getBuckets(false)).hasSameSizeAs(expected.getBuckets(false));
    assertThat(((LocalBucket) actual.getBuckets(false).getFirst()).getPageSize())
        .isEqualTo(((LocalBucket) expected.getBuckets(false).getFirst()).getPageSize());
    assertThat(embedded().countType(actualName, false)).isEqualTo(embedded().countType(expectedName, false));
  }

  private static void assertSameView(final MaterializedView expected, final MaterializedView actual) {
    assertThat(actual.getRefreshMode()).isEqualTo(expected.getRefreshMode());
    assertThat(actual.getRefreshInterval()).isEqualTo(expected.getRefreshInterval());
    assertThat(actual.getSourceTypeNames()).isEqualTo(expected.getSourceTypeNames());
    assertThat(actual.isSimpleQuery()).isEqualTo(expected.isSimpleQuery());
    assertThat(actual.getStatus()).isEqualTo(expected.getStatus());
  }

  @Test
  void theSameViewBuilderCodeProducesTheSameViewEmbeddedAndRemotely() {
    final MaterializedView viaEmbedded = applyTheViewRecipe(embedded().getSchema(), "ViewEmbedded");

    try (final RemoteDatabase database = remote()) {
      final MaterializedView viaRemote = applyTheViewRecipe(database.getSchema(), "ViewRemote");

      assertThat(viaRemote.getName()).isEqualTo("ViewRemote");
      assertSameView(viaEmbedded, viaRemote);
      assertSameBackingType("ViewEmbedded", "ViewRemote");

      // The backing type exists for THIS schema instance straight away: the builder invalidated the type cache.
      assertThat(database.getSchema().existsType("ViewRemote")).isTrue();
    }

    // INCREMENTAL means the remotely built view keeps following its source, like the embedded one.
    embedded().transaction(() -> embedded().newDocument("Account").set("name", "Dave").set("active", true).save());
    assertThat(embedded().countType("ViewRemote", false)).isEqualTo(embedded().countType("ViewEmbedded", false)).isEqualTo(3);
  }

  @Test
  void aPeriodicViewKeepsItsIntervalRemotely() {
    final MaterializedView viaEmbedded = applyThePeriodicRecipe(embedded().getSchema(), "PeriodicEmbedded");

    try (final RemoteDatabase database = remote()) {
      final MaterializedView viaRemote = applyThePeriodicRecipe(database.getSchema(), "PeriodicRemote");
      assertThat(viaRemote.getRefreshInterval()).isEqualTo(90L * 60_000L);
      assertSameView(viaEmbedded, viaRemote);
    }
  }

  @Test
  void theSameAggregateBuilderCodeProducesTheSameAggregateEmbeddedAndRemotely() {
    final ContinuousAggregate viaEmbedded = applyTheAggregateRecipe(embedded().getSchema(), "AggEmbedded");

    try (final RemoteDatabase database = remote()) {
      final ContinuousAggregate viaRemote = applyTheAggregateRecipe(database.getSchema(), "AggRemote");

      assertThat(viaRemote).isInstanceOf(RemoteContinuousAggregate.class);
      assertThat(viaRemote.getName()).isEqualTo("AggRemote");
      assertThat(((RemoteContinuousAggregate) viaRemote).getBackingTypeName()).isEqualTo("AggRemote");
      assertThat(viaRemote.getSourceTypeName()).isEqualTo(viaEmbedded.getSourceTypeName());
      assertThat(viaRemote.getBucketIntervalMs()).isEqualTo(viaEmbedded.getBucketIntervalMs()).isEqualTo(3_600_000L);
      assertThat(viaRemote.getBucketColumn()).isEqualTo(viaEmbedded.getBucketColumn());
      assertThat(viaRemote.getTimestampColumn()).isEqualTo(viaEmbedded.getTimestampColumn());
      assertThat(viaRemote.getStatus()).isEqualTo(viaEmbedded.getStatus());
      assertThat(viaRemote.isWatermarkSet()).isEqualTo(viaEmbedded.isWatermarkSet());
      assertThat(viaRemote.getWatermarkTs()).isEqualTo(viaEmbedded.getWatermarkTs());
      assertThat(embedded().countType("AggRemote", false)).isEqualTo(embedded().countType("AggEmbedded", false)).isPositive();

      assertThat(database.getSchema().existsType("AggRemote")).isTrue();
      assertThat(Arrays.stream(database.getSchema().getContinuousAggregates()).map(ContinuousAggregate::getName))
          .contains("AggEmbedded", "AggRemote");
      assertThat(database.getSchema().getContinuousAggregate("AggEmbedded").getBucketColumn())
          .isEqualTo(viaEmbedded.getBucketColumn());
    }
  }

  @Test
  void ignoreIfExistsReturnsTheExistingObjectOnBothPaths() {
    applyTheViewRecipe(embedded().getSchema(), "Existing");
    applyTheAggregateRecipe(embedded().getSchema(), "ExistingAgg");

    try (final RemoteDatabase database = remote()) {
      // A different query: with IF NOT EXISTS the existing view is returned untouched, as embedded does.
      final MaterializedView view = database.getSchema().buildMaterializedView().withName("Existing")
          .withQuery("SELECT FROM Account").withIgnoreIfExists(true).create();
      assertThat(view.getRefreshMode()).isEqualTo(MaterializedViewRefreshMode.INCREMENTAL);

      final ContinuousAggregate aggregate = database.getSchema().buildContinuousAggregate().withName("ExistingAgg")
          .withQuery(AGGREGATE_QUERY.replace("'1h'", "'1d'")).withIgnoreIfExists(true).create();
      assertThat(aggregate.getBucketIntervalMs()).isEqualTo(3_600_000L);
    }
  }

  /**
   * Unlike the TimeSeries builder's duplicate check, which runs client-side and so surfaces differently, these
   * refusals come from the server running the same embedded builder, and the remote client maps the server's
   * {@code SchemaException} back to a {@code SchemaException}: the caller catches the same type on both paths.
   */
  @Test
  void aDuplicateNameIsRefusedOnBothPathsWithTheSameExceptionType() {
    applyTheViewRecipe(embedded().getSchema(), "DuplicateView");
    applyTheAggregateRecipe(embedded().getSchema(), "DuplicateAgg");

    assertThatThrownBy(() -> applyTheViewRecipe(embedded().getSchema(), "DuplicateView"))
        .isInstanceOf(SchemaException.class)
        .hasMessageContaining("Materialized view 'DuplicateView' already exists");
    assertThatThrownBy(() -> applyTheAggregateRecipe(embedded().getSchema(), "DuplicateAgg"))
        .isInstanceOf(SchemaException.class)
        .hasMessageContaining("Continuous aggregate 'DuplicateAgg' already exists");

    try (final RemoteDatabase database = remote()) {
      assertThatThrownBy(() -> applyTheViewRecipe(database.getSchema(), "DuplicateView"))
          .isInstanceOf(SchemaException.class)
          .hasMessageContaining("Materialized view 'DuplicateView' already exists");
      assertThatThrownBy(() -> applyTheAggregateRecipe(database.getSchema(), "DuplicateAgg"))
          .isInstanceOf(SchemaException.class)
          .hasMessageContaining("Continuous aggregate 'DuplicateAgg' already exists");
    }
  }

  @Test
  void anAggregateOnANonTimeSeriesTypeIsRefusedByTheServerWithTheEmbeddedMessage() {
    final String query = "SELECT ts.timeBucket('1h', ts) AS hour, count(*) AS c FROM Account GROUP BY hour";
    assertThatThrownBy(() -> embedded().getSchema().buildContinuousAggregate().withName("NotTs").withQuery(query).create())
        .isInstanceOf(SchemaException.class)
        .hasMessageContaining("is not a TimeSeries type");

    try (final RemoteDatabase database = remote()) {
      assertThatThrownBy(() -> database.getSchema().buildContinuousAggregate().withName("NotTs").withQuery(query).create())
          .isInstanceOf(SchemaException.class)
          .hasMessageContaining("is not a TimeSeries type");
      assertThat(database.getSchema().existsType("NotTs")).isFalse();
    }
  }

  @Test
  void anIncompleteOrUnrenderableBuilderFailsBeforeAnythingReachesTheServer() {
    try (final RemoteDatabase database = remote()) {
      assertThatThrownBy(() -> database.getSchema().buildMaterializedView().withName("NoQuery").create())
          .isInstanceOf(IllegalArgumentException.class).hasMessageContaining("query is required");
      assertThatThrownBy(() -> embedded().getSchema().buildMaterializedView().withName("NoQuery").create())
          .isInstanceOf(IllegalArgumentException.class).hasMessageContaining("query is required");

      assertThatThrownBy(() -> database.getSchema().buildContinuousAggregate().withQuery(AGGREGATE_QUERY).create())
          .isInstanceOf(IllegalArgumentException.class).hasMessageContaining("name is required");
      assertThatThrownBy(() -> embedded().getSchema().buildContinuousAggregate().withQuery(AGGREGATE_QUERY).create())
          .isInstanceOf(IllegalArgumentException.class).hasMessageContaining("name is required");

      assertThatThrownBy(() -> database.getSchema().buildMaterializedView().withName("SubSecond")
          .withQuery("SELECT FROM Account").withRefreshMode(MaterializedViewRefreshMode.PERIODIC)
          .withRefreshInterval(1_500L).create())
          .hasMessageContaining("whole number of seconds");

      assertThat(database.getSchema().existsMaterializedView("SubSecond")).isFalse();
      assertThat(database.getSchema().existsType("SubSecond")).isFalse();
    }
  }
}
