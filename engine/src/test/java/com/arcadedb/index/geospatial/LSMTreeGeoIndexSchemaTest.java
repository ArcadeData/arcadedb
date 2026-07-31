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
package com.arcadedb.index.geospatial;

import com.arcadedb.TestHelper;
import com.arcadedb.exception.CommandSQLParsingException;
import com.arcadedb.index.Index;
import com.arcadedb.index.TypeIndex;
import com.arcadedb.query.sql.executor.ResultSet;
import com.arcadedb.schema.GeoIndexMetadata;
import com.arcadedb.schema.Schema;
import com.arcadedb.schema.TypeIndexBuilder;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class LSMTreeGeoIndexSchemaTest extends TestHelper {

  @Test
  void createGeospatialIndexViaSql() {
    database.command("sql", "CREATE DOCUMENT TYPE Location");
    database.command("sql", "CREATE PROPERTY Location.coords STRING");
    database.command("sql", "CREATE INDEX ON Location (coords) GEOSPATIAL");

    database.transaction(() -> database.command("sql", "INSERT INTO Location SET coords = 'POINT (12.5 41.9)'"));

    final Index index = database.getSchema().getIndexByName("Location[coords]");
    assertThat(index).isNotNull();
    assertThat(index.getType()).isEqualTo(Schema.INDEX_TYPE.GEOSPATIAL);
  }

  @Test
  void geospatialIndexSurvivesReopen() {
    database.command("sql", "CREATE DOCUMENT TYPE Location");
    database.command("sql", "CREATE PROPERTY Location.coords STRING");
    database.command("sql", "CREATE INDEX ON Location (coords) GEOSPATIAL");

    reopenDatabase();

    final Index index = database.getSchema().getIndexByName("Location[coords]");
    assertThat(index).isNotNull();
    assertThat(index.getType()).isEqualTo(Schema.INDEX_TYPE.GEOSPATIAL);
    final LSMTreeGeoIndex geoIndex = (LSMTreeGeoIndex) ((TypeIndex) index).getSubIndexes().getFirst();
    assertThat(geoIndex.getPrecision()).isEqualTo(GeoIndexMetadata.DEFAULT_PRECISION);
  }

  @Test
  void geospatialIndexNonDefaultPrecisionSurvivesReopen() {
    database.command("sql", "CREATE DOCUMENT TYPE Location2");
    database.command("sql", "CREATE PROPERTY Location2.coords STRING");

    final TypeIndexBuilder builder = database.getSchema().buildTypeIndex("Location2", new String[] { "coords" });
    builder.withType(Schema.INDEX_TYPE.GEOSPATIAL);
    final GeoIndexMetadata geoMeta = new GeoIndexMetadata("Location2", new String[] { "coords" }, -1);
    geoMeta.setPrecision(7);
    builder.withMetadata(geoMeta);
    builder.create();

    reopenDatabase();

    final Index index = database.getSchema().getIndexByName("Location2[coords]");
    assertThat(index).isNotNull();
    assertThat(index.getType()).isEqualTo(Schema.INDEX_TYPE.GEOSPATIAL);
    final LSMTreeGeoIndex geoIndex = (LSMTreeGeoIndex) ((TypeIndex) index).getSubIndexes().getFirst();
    assertThat(geoIndex.getPrecision()).isEqualTo(7);
  }

  /**
   * Issue #5600 (2): the METADATA clause used to fall through to a bare create() for GEOSPATIAL, so the precision was
   * silently ignored and only the Java API could set it.
   */
  @Test
  void precisionFromSqlMetadata() {
    database.command("sql", "CREATE DOCUMENT TYPE Location3");
    database.command("sql", "CREATE PROPERTY Location3.coords STRING");
    database.command("sql", "CREATE INDEX ON Location3 (coords) GEOSPATIAL METADATA {\"precision\": 6}");

    assertThat(geoIndex("Location3").getPrecision()).isEqualTo(6);

    reopenDatabase();

    assertThat(geoIndex("Location3").getPrecision()).isEqualTo(6);
  }

  @Test
  void tokenizationFromSqlMetadata() {
    database.command("sql", "CREATE DOCUMENT TYPE Location4");
    database.command("sql", "CREATE PROPERTY Location4.coords STRING");
    database.command("sql", "CREATE INDEX ON Location4 (coords) GEOSPATIAL METADATA {\"tokenization\": \"FULL\"}");

    assertThat(geoIndex("Location4").getTokenization()).isEqualTo(GeoIndexMetadata.TOKENIZATION.FULL);
    assertThat(geoIndex("Location4").getPrecision()).isEqualTo(GeoIndexMetadata.DEFAULT_PRECISION);
  }

  /**
   * A METADATA clause that does not mention the tokenization must leave the CREATION default in place, not the
   * backward-compatible one used when reading a persisted definition that predates the field.
   */
  @Test
  void metadataWithoutTokenizationKeepsTheCreationDefault() {
    database.command("sql", "CREATE DOCUMENT TYPE Location5");
    database.command("sql", "CREATE PROPERTY Location5.coords STRING");
    database.command("sql", "CREATE INDEX ON Location5 (coords) GEOSPATIAL METADATA {\"precision\": 8}");

    assertThat(geoIndex("Location5").getTokenization()).isEqualTo(GeoIndexMetadata.DEFAULT_TOKENIZATION);
  }

  @Test
  void indexedQueryHonoursTheCoarsePrecisionFromSql() {
    database.command("sql", "CREATE DOCUMENT TYPE City");
    database.command("sql", "CREATE PROPERTY City.name STRING");
    database.command("sql", "CREATE PROPERTY City.coords STRING");
    database.command("sql", "CREATE INDEX ON City (coords) GEOSPATIAL METADATA {\"precision\": 6}");

    database.transaction(() -> {
      database.command("sql", "INSERT INTO City SET name = 'Rome', coords = 'POINT (12.5 41.9)'");
      database.command("sql", "INSERT INTO City SET name = 'Milan', coords = 'POINT (9.2 45.5)'");
    });

    final ResultSet rs = database.query("sql",
        "SELECT name FROM City WHERE geo.within(coords, geo.geomFromText('POLYGON ((10 38, 16 38, 16 44, 10 44, 10 38))')) = true");
    assertThat(rs.next().<String>getProperty("name")).isEqualTo("Rome");
    assertThat(rs.hasNext()).isFalse();
  }

  @Test
  void unknownMetadataKeyIsRejected() {
    database.command("sql", "CREATE DOCUMENT TYPE Location6");
    database.command("sql", "CREATE PROPERTY Location6.coords STRING");

    assertThatThrownBy(() -> database.command("sql",
        "CREATE INDEX ON Location6 (coords) GEOSPATIAL METADATA {\"precisin\": 6}"))
        .isInstanceOf(CommandSQLParsingException.class)
        .hasMessageContaining("precisin");

    assertThat(database.getSchema().existsIndex("Location6[coords]")).isFalse();
  }

  @Test
  void outOfRangePrecisionIsRejected() {
    database.command("sql", "CREATE DOCUMENT TYPE Location7");
    database.command("sql", "CREATE PROPERTY Location7.coords STRING");

    assertThatThrownBy(() -> database.command("sql",
        "CREATE INDEX ON Location7 (coords) GEOSPATIAL METADATA {\"precision\": 13}"))
        .isInstanceOf(CommandSQLParsingException.class)
        .hasMessageContaining("precision");
  }

  @Test
  void nonNumericPrecisionIsRejected() {
    database.command("sql", "CREATE DOCUMENT TYPE Location9");
    database.command("sql", "CREATE PROPERTY Location9.coords STRING");

    assertThatThrownBy(() -> database.command("sql",
        "CREATE INDEX ON Location9 (coords) GEOSPATIAL METADATA {\"precision\": \"six\"}"))
        .isInstanceOf(CommandSQLParsingException.class)
        .hasMessageContaining("precision");
  }

  @Test
  void fractionalPrecisionIsRejected() {
    database.command("sql", "CREATE DOCUMENT TYPE Location10");
    database.command("sql", "CREATE PROPERTY Location10.coords STRING");

    assertThatThrownBy(() -> database.command("sql",
        "CREATE INDEX ON Location10 (coords) GEOSPATIAL METADATA {\"precision\": 6.9}"))
        .isInstanceOf(CommandSQLParsingException.class)
        .hasMessageContaining("whole number");

    assertThat(database.getSchema().existsIndex("Location10[coords]")).isFalse();
  }

  @Test
  void invalidTokenizationIsRejected() {
    database.command("sql", "CREATE DOCUMENT TYPE Location8");
    database.command("sql", "CREATE PROPERTY Location8.coords STRING");

    assertThatThrownBy(() -> database.command("sql",
        "CREATE INDEX ON Location8 (coords) GEOSPATIAL METADATA {\"tokenization\": \"SPARSE\"}"))
        .isInstanceOf(CommandSQLParsingException.class)
        .hasMessageContaining("tokenization");
  }

  /**
   * An index type that has no use for METADATA must say so instead of dropping the clause on the floor: silently
   * ignoring it is what kept the geospatial gap invisible.
   */
  @Test
  void metadataOnAnIndexTypeThatDoesNotSupportItIsRejected() {
    database.command("sql", "CREATE DOCUMENT TYPE Plain");
    database.command("sql", "CREATE PROPERTY Plain.name STRING");

    assertThatThrownBy(() -> database.command("sql",
        "CREATE INDEX ON Plain (name) UNIQUE METADATA {\"precision\": 6}"))
        .isInstanceOf(CommandSQLParsingException.class)
        .hasMessageContaining("METADATA");
  }

  private LSMTreeGeoIndex geoIndex(final String typeName) {
    final Index index = database.getSchema().getIndexByName(typeName + "[coords]");
    assertThat(index).isNotNull();
    assertThat(index.getType()).isEqualTo(Schema.INDEX_TYPE.GEOSPATIAL);
    return (LSMTreeGeoIndex) ((TypeIndex) index).getSubIndexes().getFirst();
  }
}
