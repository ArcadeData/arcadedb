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

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Set;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * https://github.com/ArcadeData/arcadedb/issues/8169
 * <p>
 * {@code LocalSchema.dropType} removed exactly one key from the schema's shared type map - the type's own name - and
 * nothing anywhere removed the alias keys, which kept pointing at the dropped, bucket-less {@code LocalDocumentType}
 * instance. Three consequences, in increasing order of damage:
 * <ol>
 *   <li>{@code existsType(alias)} kept answering {@code true} and {@code getType(alias)} kept handing out the
 *   dropped type.</li>
 *   <li>A later {@code ALTER TYPE Other ALIASES <alias>} was refused naming a type that no longer existed - #8064's
 *   atomic {@code putIfAbsent} turned the stale entry from a silent overwrite into a hard refusal.</li>
 *   <li>{@code toJSON()} - reached from {@code saveConfiguration()} - iterates {@code typeMap().values()} and
 *   keys each entry by {@code t.getName()}, so the dropped instance, still reachable through its surviving alias
 *   key, was written back into {@code schema.json} under its own name with zero buckets and came back alive at
 *   the next open.</li>
 * </ol>
 * The drop also resolves its argument through {@code getType()}, which resolves aliases, so {@code DROP TYPE <alias>}
 * tore down the real type's buckets and indexes and then removed only the alias key, leaving the type itself
 * registered under its own name and unusable.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
public class Issue8169DropTypeLeavesAliasesTest extends TestHelper {

  @Test
  void dropTypeDeregistersTheTypeAliases() {
    final DocumentType order = database.getSchema().createDocumentType("Order");
    database.getSchema().createDocumentType("Invoice");
    order.setAliases(Set.of("PO", "PurchaseOrder"));

    assertThat(database.getSchema().existsType("PO")).isTrue();
    assertThat(database.getSchema().existsType("PurchaseOrder")).isTrue();

    database.getSchema().dropType("Order");

    assertThat(database.getSchema().existsType("Order")).isFalse();
    assertThat(database.getSchema().existsType("PO")).isFalse();
    assertThat(database.getSchema().existsType("PurchaseOrder")).isFalse();
    assertThat(database.getSchema().getTypeOrNull("PO")).isNull();
    assertThatThrownBy(() -> database.getSchema().getType("PO")).isInstanceOf(SchemaException.class);

    // THE DROPPED INSTANCE IS NO LONGER LISTED: getTypes() deduplicates typeMap().values(), but a surviving alias
    // key kept the instance IN those values
    assertThat(database.getSchema().getTypes()).extracting(DocumentType::getName).containsExactly("Invoice");
  }

  @Test
  void anAliasFreedByDropTypeCanBeClaimedByAnotherType() {
    final DocumentType order = database.getSchema().createDocumentType("Order");
    final DocumentType invoice = database.getSchema().createDocumentType("Invoice");
    order.setAliases(Set.of("PO"));

    database.getSchema().dropType("Order");

    // BEFORE THE FIX THIS WAS REFUSED WITH "already used by type 'Order'", NAMING A TYPE THAT NO LONGER EXISTED
    invoice.setAliases(Set.of("PO"));

    assertThat(database.getSchema().existsType("PO")).isTrue();
    assertThat(database.getSchema().getType("PO")).isSameAs(invoice);
  }

  @Test
  void sqlDropTypeDeregistersTheTypeAliasesAndFreesThemForAlterType() {
    database.command("sql", "CREATE DOCUMENT TYPE Order");
    database.command("sql", "CREATE DOCUMENT TYPE Invoice");
    database.command("sql", "ALTER TYPE Order ALIASES PO");

    assertThat(database.getSchema().existsType("PO")).isTrue();

    database.command("sql", "DROP TYPE Order");

    assertThat(database.getSchema().existsType("Order")).isFalse();
    assertThat(database.getSchema().existsType("PO")).isFalse();

    database.command("sql", "ALTER TYPE Invoice ALIASES PO");

    assertThat(database.getSchema().getType("PO").getName()).isEqualTo("Invoice");
  }

  @Test
  void aDroppedTypeDoesNotComeBackAfterAReopen() throws IOException {
    final DocumentType order = database.getSchema().createDocumentType("Order");
    database.getSchema().createDocumentType("Invoice");
    order.setAliases(Set.of("PO"));

    database.getSchema().dropType("Order");

    // THE FILE ITSELF, not only what the reopen makes of it: toJSON() keys typeMap().values() by t.getName(), and
    // the dropped instance was still in those values through its surviving alias key, so it was written back under
    // its own name with an empty bucket list
    final JSONObject onDisk = new JSONObject(
        Files.readString(Path.of(getDatabasePath(), "schema.json"), StandardCharsets.UTF_8));
    assertThat(onDisk.getJSONObject("types").keySet()).containsExactly("Invoice");

    reopenDatabase();

    // THE ONE THAT MATTERS MOST: the dropped instance used to be re-serialised into schema.json under its own name
    // with zero buckets, so it was back - permanently - at the next open, and unusable
    assertThat(database.getSchema().existsType("Order")).isFalse();
    assertThat(database.getSchema().existsType("PO")).isFalse();
    assertThat(database.getSchema().getTypes()).extracting(DocumentType::getName).containsExactly("Invoice");
  }

  @Test
  void dropTypeThroughAnAliasDropsTheTypeItself() {
    final DocumentType order = database.getSchema().createDocumentType("Order");
    database.getSchema().createDocumentType("Invoice");
    order.setAliases(Set.of("PO"));

    // getType() resolves aliases, so the drop tears down the real type. It has to deregister it under every name it
    // answers to, not only under the alias the caller typed
    database.getSchema().dropType("PO");

    assertThat(database.getSchema().existsType("PO")).isFalse();
    assertThat(database.getSchema().existsType("Order")).isFalse();

    reopenDatabase();

    assertThat(database.getSchema().existsType("Order")).isFalse();
    assertThat(database.getSchema().existsType("PO")).isFalse();
  }

  @Test
  void dropTypeThroughAnAliasIsStillRefusedForAMaterializedViewSourceType() {
    database.transaction(() -> database.getSchema().createDocumentType("Employee"));
    database.transaction(() -> database.getSchema().getType("Employee").setAliases(Set.of("Emp")));
    database.transaction(() -> database.newDocument("Employee").set("name", "Alice").save());
    database.transaction(() -> database.getSchema().buildMaterializedView()
        .withName("EmployeeView")
        .withQuery("SELECT name FROM Employee")
        .withRefreshMode(MaterializedViewRefreshMode.INCREMENTAL)
        .create());

    // The guard used to compare the caller's string against the view's source type names, so naming the type by one
    // of its aliases walked straight past it and dropped the view's source type out from under it
    assertThatThrownBy(() -> database.getSchema().dropType("Emp"))
        .isInstanceOf(SchemaException.class)
        .hasMessageContaining("EmployeeView");

    assertThat(database.getSchema().existsType("Employee")).isTrue();
    assertThat(database.getSchema().existsType("Emp")).isTrue();
  }

  @Test
  void dropTypeThroughAnAliasIsStillRefusedForAContinuousAggregateSourceType() {
    database.command("sql",
        "CREATE TIMESERIES TYPE SensorReading TIMESTAMP ts TAGS (sensor_id STRING) FIELDS (temperature DOUBLE)");
    database.getSchema().getType("SensorReading").setAliases(Set.of("Sensor"));
    database.transaction(() -> database.command("sql",
        "INSERT INTO SensorReading SET ts = 1000, sensor_id = 'A', temperature = 22.5"));
    database.getSchema().buildContinuousAggregate()
        .withName("hourly_temps")
        .withQuery("SELECT sensor_id, ts.timeBucket('1h', ts) AS hour, avg(temperature) AS avg_temp "
            + "FROM SensorReading GROUP BY sensor_id, hour")
        .create();

    // The continuous-aggregate guard is the twin of the materialized-view one above and was bypassed the same way
    assertThatThrownBy(() -> database.getSchema().dropType("Sensor"))
        .isInstanceOf(SchemaException.class)
        .hasMessageContaining("hourly_temps");

    assertThat(database.getSchema().existsType("SensorReading")).isTrue();
    assertThat(database.getSchema().existsType("Sensor")).isTrue();
  }

  @Test
  void dropTypeThroughAnAliasIsStillRefusedForAContinuousAggregateBackingType() {
    database.command("sql",
        "CREATE TIMESERIES TYPE SensorReading TIMESTAMP ts TAGS (sensor_id STRING) FIELDS (temperature DOUBLE)");
    database.transaction(() -> database.command("sql",
        "INSERT INTO SensorReading SET ts = 1000, sensor_id = 'A', temperature = 22.5"));
    final String backingTypeName = database.getSchema().buildContinuousAggregate()
        .withName("hourly_temps")
        .withQuery("SELECT sensor_id, ts.timeBucket('1h', ts) AS hour, avg(temperature) AS avg_temp "
            + "FROM SensorReading GROUP BY sensor_id, hour")
        .create()
        .getBackingType()
        .getName();

    database.getSchema().getType(backingTypeName).setAliases(Set.of("HourlyTemps"));

    assertThatThrownBy(() -> database.getSchema().dropType("HourlyTemps"))
        .isInstanceOf(SchemaException.class)
        .hasMessageContaining("hourly_temps");

    assertThat(database.getSchema().existsType(backingTypeName)).isTrue();
    assertThat(database.getSchema().existsType("HourlyTemps")).isTrue();
  }
}
