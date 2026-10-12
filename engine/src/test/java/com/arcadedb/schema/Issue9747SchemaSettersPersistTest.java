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
import com.arcadedb.exception.SchemaException;
import com.arcadedb.serializer.json.JSONObject;
import org.junit.jupiter.api.Test;

import java.time.ZoneId;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Issue #9747, the embedded side: the schema writers now apply their change inside a schema recording session instead of
 * ending in a bare save. Off HA the session must still persist every change across a reopen, and a setter handed the
 * value it already holds must not open a session at all - no save, no schema version bump.
 */
class Issue9747SchemaSettersPersistTest extends TestHelper {

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
