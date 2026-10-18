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
package com.arcadedb.function.sql.geo;

import com.arcadedb.TestHelper;
import com.arcadedb.index.IndexException;
import com.arcadedb.database.RID;
import com.arcadedb.index.IndexInternal;
import com.arcadedb.index.TypeIndex;
import com.arcadedb.index.geospatial.LSMTreeGeoIndex;
import com.arcadedb.schema.LocalSchema;
import com.arcadedb.serializer.json.JSONObject;
import com.arcadedb.query.sql.executor.ResultSet;
import org.junit.jupiter.api.Test;

import java.io.File;
import java.nio.file.Files;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicReference;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.assertj.core.api.Assertions.within;

/**
 * Issue #10210: a polygon wider than 180 degrees of longitude, and a polar cap, were read as their complement;
 * {@code geo.dWithin} and {@code geo.distance} measured between centres; an invalid coordinate was stored, skipped by the
 * index and failed every later scan.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class Issue10210GeoWidePolygonsDistanceTest extends TestHelper {
  private static final String   CAP  = "POLYGON ((-180 80, 180 80, 180 90, -180 90, -180 80))";
  private static final String   HUGE = "POLYGON ((-179 -85, 179 -85, 179 85, -179 85, -179 -85))";
  private static final String   BOX  = "POLYGON ((0 0, 20 0, 20 20, 0 20, 0 0))";
  private static final String[] PTS  = { "POINT (0 0)", "POINT (0 89)", "POINT (-179.5 0)", "POINT (100 50)", "POINT (0 -89)" };

  @Override
  protected void beginTest() {
    database.command("sql", "CREATE DOCUMENT TYPE Pt");
    database.command("sql", "CREATE PROPERTY Pt.coords STRING");
    database.command("sql", "CREATE INDEX ON Pt (coords) GEOSPATIAL");
    database.transaction(() -> {
      for (final String p : PTS)
        database.newDocument("Pt").set("coords", p).save();
    });
  }

  @Test
  void aPolygonWiderThan180DegreesIsReadAsWritten() {
    for (final int w : new int[] { 90, 100, 150, 170, 179, 180 }) {
      final String poly = String.format("POLYGON ((-%d -10, %d -10, %d 10, -%d 10, -%d -10))", w, w, w, w, w);
      assertThat(value("SELECT geo.within(geo.geomFromText('POINT (0 0)'), geo.geomFromText(?)) AS r", poly)).as(poly).isEqualTo(true);
    }
    assertThat(value("SELECT geo.within(geo.geomFromText('POINT (0 0)'), geo.geomFromText(?)) AS r", HUGE)).isEqualTo(true);
    assertThat(value("SELECT geo.within(geo.geomFromText('POINT (-179.5 0)'), geo.geomFromText(?)) AS r", HUGE)).isEqualTo(false);

    // a wide polygon that is not a rectangle used to fail to parse
    assertThat(value("SELECT geo.within(geo.geomFromText('POINT (0 0)'), geo.geomFromText(?)) AS r",
        "POLYGON ((-100 -10, 0 -20, 100 -10, 100 10, -100 10, -100 -10))")).isEqualTo(true);

    // the index and a scan agree, and both are right
    final List<String> expected = List.of("POINT (0 0)", "POINT (100 50)");
    assertThat(coords("SELECT coords FROM Pt WHERE geo.within(coords, geo.geomFromText(?)) = true", HUGE))
        .containsExactlyInAnyOrderElementsOf(expected);
    assertThat(coords("SELECT coords FROM Pt WHERE geo.within(coords.trim(), geo.geomFromText(?)) = true", HUGE))
        .containsExactlyInAnyOrderElementsOf(expected);
    assertThat(coords("SELECT coords FROM Pt WHERE geo.disjoint(coords.trim(), geo.geomFromText(?)) = true", HUGE))
        .containsExactlyInAnyOrder("POINT (0 89)", "POINT (-179.5 0)", "POINT (0 -89)");
  }

  @Test
  void aPolarCapContainsThePole() {
    assertThat(value("SELECT geo.within(geo.geomFromText('POINT (0 89)'), geo.geomFromText(?)) AS r", CAP)).isEqualTo(true);
    assertThat(value("SELECT geo.intersects(geo.geomFromText('POINT (0 89)'), geo.geomFromText(?)) AS r", CAP)).isEqualTo(true);
    assertThat(((Number) value("SELECT geo.area(geo.geomFromText(?)) AS r", CAP)).doubleValue()).isGreaterThan(0.0);
    assertThat(coords("SELECT coords FROM Pt WHERE geo.within(coords, geo.geomFromText(?)) = true", CAP)).containsExactly(
        "POINT (0 89)");
    assertThat(coords("SELECT coords FROM Pt WHERE geo.within(coords.trim(), geo.geomFromText(?)) = true", CAP)).containsExactly(
        "POINT (0 89)");
  }

  @Test
  void aShapeAcrossTheAntimeridianIsWrittenPastIt() {
    final String across = "POLYGON ((170 1, 190 1, 190 10, 170 10, 170 1))";
    assertThat(value("SELECT geo.within(geo.geomFromText('POINT (-175 5)'), geo.geomFromText(?)) AS r", across)).isEqualTo(true);
    assertThat(value("SELECT geo.within(geo.geomFromText('POINT (175 5)'), geo.geomFromText(?)) AS r", across)).isEqualTo(true);
    assertThat(value("SELECT geo.within(geo.geomFromText('POINT (0 5)'), geo.geomFromText(?)) AS r", across)).isEqualTo(false);
    // the split at 180, written by hand, reads the same
    final String split = "MULTIPOLYGON (((170 1, 180 1, 180 10, 170 10, 170 1)), ((-180 1, -170 1, -170 10, -180 10, -180 1)))";
    assertThat(value("SELECT geo.within(geo.geomFromText('POINT (-175 5)'), geo.geomFromText(?)) AS r", split)).isEqualTo(true);

    database.transaction(() -> database.newDocument("Pt").set("coords", "POINT (185 5)").save());
    assertThat(coords("SELECT coords FROM Pt WHERE geo.within(coords, geo.geomFromText(?)) = true", split)).containsExactly(
        "POINT (185 5)");
    assertThat(coords("SELECT coords FROM Pt WHERE geo.within(coords.trim(), geo.geomFromText(?)) = true", split)).containsExactly(
        "POINT (185 5)");
  }

  @Test
  void dWithinMeasuresBetweenTheShapes() {
    assertThat(value("SELECT geo.dWithin(geo.geomFromText('POINT (1 1)'), geo.geomFromText(?), 0.5) AS r", BOX)).isEqualTo(true);
    assertThat(value("SELECT geo.dWithin(geo.geomFromText('POINT (10 10)'), geo.geomFromText(?), 0.5) AS r", BOX)).isEqualTo(true);
    assertThat(value("SELECT geo.dWithin(geo.geomFromText('POINT (21 10)'), geo.geomFromText(?), 2) AS r", BOX)).isEqualTo(true);
    assertThat(value("SELECT geo.dWithin(geo.geomFromText('POINT (21 10)'), geo.geomFromText(?), 0.9) AS r", BOX)).isEqualTo(false);
    assertThat(value("SELECT geo.dWithin(geo.geomFromText('POINT (25 10)'), geo.geomFromText(?), 2) AS r", BOX)).isEqualTo(false);
    assertThat(value("SELECT geo.dWithin(geo.geomFromText('POINT (179.5 0)'), geo.geomFromText('POINT (-179.5 0)'), 2) AS r"))
        .isEqualTo(true);
    // a polygon facing a point across the antimeridian is measured the short way round
    assertThat(value("SELECT geo.dWithin(geo.geomFromText('POINT (-179.5 5)'), geo.geomFromText(?), 1) AS r",
        "POLYGON ((170 0, 179.9 0, 179.9 10, 170 10, 170 0))")).isEqualTo(true);
  }

  @Test
  void distanceMeasuresBetweenTheShapes() {
    final double oneDegreeMetres = Math.toRadians(1) * GeoUtils.EARTH_RADIUS_KM * 1000;
    assertThat(number("SELECT geo.distance(geo.geomFromText(?), geo.geomFromText('POINT (10 10)')) AS r", BOX)).isEqualTo(0.0);
    assertThat(number("SELECT geo.distance(geo.geomFromText(?), geo.geomFromText('POINT (21 10)')) AS r", BOX)).isCloseTo(
        oneDegreeMetres * Math.cos(Math.toRadians(10)), within(500.0));
    assertThat(number("SELECT geo.distance(geo.geomFromText('POINT (179.5 0)'), geo.geomFromText('POINT (-179.5 0)')) AS r"))
        .isCloseTo(111194.92664455905, within(1e-6));
  }

  @Test
  void anInvalidCoordinateIsRefusedByTheIndex() {
    assertThatThrownBy(() -> database.transaction(() -> database.newDocument("Pt").set("coords", "POINT (10 95)").save()))//
        .hasStackTraceContaining("POINT (10 95)");
    // nothing was stored, so the scan still works and agrees with the index
    assertThat(coords("SELECT coords FROM Pt WHERE geo.within(coords, geo.geomFromText(?)) = true", BOX)).containsExactly(
        "POINT (0 0)");
    assertThat(coords("SELECT coords FROM Pt WHERE geo.within(coords.trim(), geo.geomFromText(?)) = true", BOX)).containsExactly(
        "POINT (0 0)");
  }

  @Test
  void anInvalidCoordinateWrittenBeforeTheIndexIsSkippedByTheBuild() {
    database.command("sql", "CREATE DOCUMENT TYPE Legacy");
    database.command("sql", "CREATE PROPERTY Legacy.coords STRING");
    database.transaction(() -> {
      database.newDocument("Legacy").set("coords", "POINT (10 95)").save();
      database.newDocument("Legacy").set("coords", "POINT (1 1)").save();
    });
    database.command("sql", "CREATE INDEX ON Legacy (coords) GEOSPATIAL");
    assertThat(coords("SELECT coords FROM Legacy WHERE geo.within(coords, geo.geomFromText(?)) = true", BOX)).containsExactly(
        "POINT (1 1)");
  }

  @Test
  void aSelfIntersectingShapeAcrossTheAntimeridianIsNotAGeometry() {
    // refused like any unreadable geometry, where the overlay used to leak its own exception
    assertThatThrownBy(() -> database.transaction(
        () -> database.newDocument("Pt").set("coords", "POLYGON ((170 0, 190 10, 190 0, 170 10, 170 0))").save()))//
        .hasStackTraceContaining("IndexException");
    assertThatThrownBy(() -> value("SELECT geo.within(geo.geomFromText('POINT (0 0)'), "
        + "geo.geomFromText('POLYGON ((170 0, 190 10, 190 0, 170 10, 170 0))')) AS r"))//
        .isInstanceOf(IllegalArgumentException.class);
  }

  /**
   * An index built before shapes were read as written holds the complement of a polygon wider than 180 degrees, which no
   * check after the search brings back: queries scan the type until REBUILD INDEX, and the index says so.
   */
  @Test
  void anIndexBuiltBeforeIsBypassedUntilItIsRebuilt() throws Exception {
    final String query = "SELECT coords FROM Pt WHERE geo.within(coords, geo.geomFromText('" + HUGE + "')) = true";
    final String index = "Pt[coords]";

    assertThat(planUsesIndex(query)).isTrue();

    database.close();
    final File schemaFile = new File(getDatabasePath() + "/" + LocalSchema.SCHEMA_FILE_NAME);
    final JSONObject schema = new JSONObject(Files.readString(schemaFile.toPath()));
    final JSONObject indexes = schema.getJSONObject("types").getJSONObject("Pt").getJSONObject("indexes");
    for (final String indexName : indexes.keySet())
      indexes.getJSONObject(indexName).remove("planar");
    Files.writeString(schemaFile.toPath(), schema.toString());
    database = factory.open();

    final TypeIndex typeIndex = (TypeIndex) database.getSchema().getIndexByName(index);
    for (final IndexInternal bucketIndex : typeIndex.getIndexesOnBuckets()) {
      assertThat(((LSMTreeGeoIndex) bucketIndex).isPlanar()).isFalse();
      assertThat(bucketIndex.getUpgradeWarning()).contains("#10210");
    }
    assertThat(planUsesIndex(query)).isFalse();
    assertThat(coords(query)).containsExactlyInAnyOrder("POINT (0 0)", "POINT (100 50)");

    database.command("sql", "REBUILD INDEX `" + index + "`");

    for (final IndexInternal bucketIndex : ((TypeIndex) database.getSchema().getIndexByName(index)).getIndexesOnBuckets()) {
      assertThat(((LSMTreeGeoIndex) bucketIndex).isPlanar()).isTrue();
      assertThat(bucketIndex.getUpgradeWarning()).isNull();
    }
    assertThat(planUsesIndex(query)).isTrue();
    assertThat(coords(query)).containsExactlyInAnyOrder("POINT (0 0)", "POINT (100 50)");
  }

  /**
   * An invalid geometry is skipped for the thread that is building the index and refused for every other one: a write that
   * lands while the build runs must fail, not vanish from the index.
   */
  @Test
  void anInvalidGeometryIsRefusedOnAnotherThreadWhileTheIndexIsBeingBuilt() throws Exception {
    final TypeIndex typeIndex = (TypeIndex) database.getSchema().getIndexByName("Pt[coords]");
    final LSMTreeGeoIndex geoIndex = (LSMTreeGeoIndex) typeIndex.getIndexesOnBuckets()[0];
    final AtomicReference<Throwable> refused = new AtomicReference<>();

    database.begin();
    geoIndex.build(1000, true, (document, totalIndexed) -> {
      final Thread writer = new Thread(() -> {
        try {
          geoIndex.put(new Object[] { "POINT (10 95)" }, new RID[] { document.getIdentity() });
        } catch (final Throwable e) {
          refused.set(e);
        }
      });
      writer.start();
      try {
        writer.join();
      } catch (final InterruptedException e) {
        Thread.currentThread().interrupt();
      }
    });

    database.commit();

    assertThat(refused.get()).isInstanceOf(IndexException.class);
  }

  /** A value the index used to skip is not re-put by a write that does not touch it. */
  @Test
  void updatingAnotherPropertyOfARecordWithAnInvalidStoredValueIsNotRefused() {
    database.command("sql", "CREATE DOCUMENT TYPE Old");
    database.command("sql", "CREATE PROPERTY Old.coords STRING");
    database.command("sql", "CREATE PROPERTY Old.note STRING");
    database.transaction(() -> database.newDocument("Old").set("coords", "POINT (10 95)").set("note", "a").save());
    database.command("sql", "CREATE INDEX ON Old (coords) GEOSPATIAL");

    database.transaction(() -> database.command("sql", "UPDATE Old SET note = 'b'"));

    try (final ResultSet rs = database.query("sql", "SELECT note FROM Old")) {
      assertThat(rs.next().<String>getProperty("note")).isEqualTo("b");
    }
  }

  private boolean planUsesIndex(final String sql) {
    try (final ResultSet rs = database.query("sql", sql)) {
      while (rs.hasNext())
        rs.next();
      return rs.getExecutionPlan().orElseThrow().prettyPrint(0, 2).contains("FETCH FROM INDEXED FUNCTION");
    }
  }

  private Object value(final String sql, final Object... args) {
    try (final ResultSet rs = database.query("sql", sql, args)) {
      return rs.next().getProperty("r");
    }
  }

  private double number(final String sql, final Object... args) {
    return ((Number) value(sql, args)).doubleValue();
  }

  private List<String> coords(final String sql, final Object... args) {
    final List<String> result = new ArrayList<>();
    try (final ResultSet rs = database.query("sql", sql, args)) {
      while (rs.hasNext())
        result.add(rs.next().getProperty("coords"));
    }
    return result;
  }
}
