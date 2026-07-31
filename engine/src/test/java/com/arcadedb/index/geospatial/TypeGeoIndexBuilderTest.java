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
import com.arcadedb.index.TypeIndex;
import com.arcadedb.schema.FullTextIndexMetadata;
import com.arcadedb.schema.GeoIndexMetadata;
import com.arcadedb.schema.IndexMetadata;
import com.arcadedb.schema.Schema;
import com.arcadedb.schema.TypeGeoIndexBuilder;
import com.arcadedb.serializer.json.JSONObject;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * The Java-API side of issue #5600 (2): {@code withType(GEOSPATIAL)} now yields a builder that owns a
 * {@link GeoIndexMetadata}, so the settings a geospatial index has are reachable without going through SQL.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class TypeGeoIndexBuilderTest extends TestHelper {

  @Test
  void withTypeYieldsAGeoBuilderCarryingGeoMetadata() {
    final TypeGeoIndexBuilder builder = geoBuilder("A");

    assertThat(builder.getMetadata()).isInstanceOf(GeoIndexMetadata.class);
    final GeoIndexMetadata metadata = (GeoIndexMetadata) builder.getMetadata();
    assertThat(metadata.getPrecision()).isEqualTo(GeoIndexMetadata.DEFAULT_PRECISION);
    assertThat(metadata.getTokenization()).isEqualTo(GeoIndexMetadata.DEFAULT_TOKENIZATION);
    assertThat(metadata.typeName).isEqualTo("A");
    assertThat(metadata.propertyNames).containsExactly("coords");
  }

  @Test
  void fluentSettersReachTheCreatedIndex() {
    final TypeIndex index = geoBuilder("B") //
        .withPrecision(5) //
        .withTokenization(GeoIndexMetadata.TOKENIZATION.FULL) //
        .create();

    final LSMTreeGeoIndex geoIndex = (LSMTreeGeoIndex) index.getSubIndexes().getFirst();
    assertThat(geoIndex.getPrecision()).isEqualTo(5);
    assertThat(geoIndex.getTokenization()).isEqualTo(GeoIndexMetadata.TOKENIZATION.FULL);
  }

  @Test
  void jsonMetadataIsChainable() {
    final TypeIndex index = geoBuilder("C") //
        .withMetadata(new JSONObject().put("precision", 4)) //
        .create();

    assertThat(((LSMTreeGeoIndex) index.getSubIndexes().getFirst()).getPrecision()).isEqualTo(4);
  }

  @Test
  void aNullJsonMetadataLeavesTheDefaultsAlone() {
    final TypeGeoIndexBuilder builder = geoBuilder("D");

    assertThat(builder.withMetadata((JSONObject) null)).isSameAs(builder);
    assertThat(((GeoIndexMetadata) builder.getMetadata()).getPrecision()).isEqualTo(GeoIndexMetadata.DEFAULT_PRECISION);
  }

  /**
   * Handing the geospatial builder someone else's metadata subtype has to be reported: silently accepting it would put
   * the builder back in the state that made the SQL METADATA vanish in the first place.
   */
  @Test
  void metadataOfAnotherIndexTypeIsRejected() {
    final TypeGeoIndexBuilder builder = geoBuilder("E");

    assertThatThrownBy(() -> builder.withMetadata(new FullTextIndexMetadata("E", new String[] { "coords" }, -1)))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("GeoIndexMetadata");
  }

  /**
   * And once the metadata is not geospatial, every geospatial setter says so rather than dying on a ClassCastException.
   */
  @Test
  void aSetterOnNonGeoMetadataReportsTheState() {
    final TypeGeoIndexBuilder builder = geoBuilder("F");
    builder.withMetadata((IndexMetadata) null);

    assertThatThrownBy(() -> builder.withPrecision(6)).isInstanceOf(IllegalStateException.class)
        .hasMessageContaining("null");
    assertThatThrownBy(() -> builder.withTokenization(GeoIndexMetadata.TOKENIZATION.FULL))
        .isInstanceOf(IllegalStateException.class);
  }

  @Test
  void withGeoTypeRefusesANonGeospatialBuilder() {
    database.command("sql", "CREATE DOCUMENT TYPE G");
    database.command("sql", "CREATE PROPERTY G.coords STRING");

    assertThatThrownBy(() -> database.getSchema().buildTypeIndex("G", new String[] { "coords" })
        .withType(Schema.INDEX_TYPE.LSM_TREE).withGeoType()).isInstanceOf(IllegalStateException.class)
        .hasMessageContaining("withGeoType()");
  }

  /**
   * Asking a builder that is ALREADY geospatial for its geospatial view hands back the same instance, so settings
   * applied before the call are not silently left on an abandoned object.
   */
  @Test
  void withGeoTypeOnAGeoBuilderIsIdentity() {
    final TypeGeoIndexBuilder builder = geoBuilder("H").withPrecision(7);

    assertThat(builder.withGeoType()).isSameAs(builder);
    assertThat(((GeoIndexMetadata) builder.getMetadata()).getPrecision()).isEqualTo(7);
  }

  private TypeGeoIndexBuilder geoBuilder(final String typeName) {
    database.command("sql", "CREATE DOCUMENT TYPE " + typeName);
    database.command("sql", "CREATE PROPERTY " + typeName + ".coords STRING");
    return database.getSchema().buildTypeIndex(typeName, new String[] { "coords" })
        .withType(Schema.INDEX_TYPE.GEOSPATIAL).withGeoType();
  }
}
