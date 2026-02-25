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
import com.arcadedb.index.Index;
import com.arcadedb.index.TypeIndex;
import com.arcadedb.schema.GeoIndexMetadata;
import com.arcadedb.schema.Schema;
import com.arcadedb.schema.TypeIndexBuilder;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

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
    builder.metadata = geoMeta;
    builder.create();

    reopenDatabase();

    final Index index = database.getSchema().getIndexByName("Location2[coords]");
    assertThat(index).isNotNull();
    assertThat(index.getType()).isEqualTo(Schema.INDEX_TYPE.GEOSPATIAL);
    final LSMTreeGeoIndex geoIndex = (LSMTreeGeoIndex) ((TypeIndex) index).getSubIndexes().getFirst();
    assertThat(geoIndex.getPrecision()).isEqualTo(7);
  }
}
