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
import com.arcadedb.engine.LocalBucket;
import com.arcadedb.exception.SchemaException;
import com.arcadedb.query.sql.antlr.SQLAntlrParser;
import com.arcadedb.query.sql.parser.CreateMaterializedViewStatement;
import com.arcadedb.query.sql.parser.Statement;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.function.Function;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Issue #7688: {@link MaterializedViewBuilder} and {@link ContinuousAggregateBuilder} render their accumulated state as
 * DDL, so an implementation that can only reach the schema through {@code command("sql", ...)} - {@code RemoteSchema} -
 * can create the same view or aggregate the embedded builder creates.
 * <p>
 * These are the engine-side half of the acceptance criteria: that executing the rendered SQL yields the same
 * declaration as calling {@code create()} on the very same builder state - the local-to-local parity the remote path
 * rides on - and that a state the grammar cannot express fails at build time rather than as invalid DDL on the server.
 * The end-to-end embedded-vs-remote comparison lives in {@code Issue7688RemoteViewBuildersIT}.
 */
class Issue7688ViewBuilderSQLTest extends TestHelper {

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
