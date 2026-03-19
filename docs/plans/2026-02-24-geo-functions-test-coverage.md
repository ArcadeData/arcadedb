# Geo Functions Test Coverage Implementation Plan

> **For Claude:** REQUIRED SUB-SKILL: Use superpowers:executing-plans to implement this plan task-by-task.

**Goal:** Split `SQLGeoFunctionsTest.java` into 4 focused test classes (one per function group), each using `@Nested` per-function structure with SQL tests, direct Java `execute()` tests, and error-path coverage.

**Architecture:** Rename `SQLGeoFunctionsTest.java` → `GeoHashIndexTest.java` (index tests only). Create four new test classes in the same package. Each new class contains one `@Nested` inner class per function. All tests are in the same package as production code (`com.arcadedb.function.sql.geo`), granting access to package-private helpers like `GeoUtils`. Java `execute()` tests call function constructors directly — no database required since all functions only use `iParams`.

**Tech Stack:** JUnit 5 (`@Test`, `@Nested`), AssertJ (`assertThat`, `assertThatThrownBy`), `TestHelper.executeInNewDatabase` for SQL tests, Spatial4j `Shape`.

---

### Task 1: Create `GeoHashIndexTest.java`

Strip `SQLGeoFunctionsTest.java` to only its two geohash index tests and save as a new file. The old file will be deleted in Task 6.

**Files:**
- Create: `engine/src/test/java/com/arcadedb/function/sql/geo/GeoHashIndexTest.java`

**Step 1: Create the file**

```java
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
import com.arcadedb.database.Document;
import com.arcadedb.database.MutableDocument;
import com.arcadedb.index.Index;
import com.arcadedb.query.sql.executor.ResultSet;
import com.arcadedb.schema.DocumentType;
import com.arcadedb.schema.Schema;
import com.arcadedb.schema.Type;

import org.junit.jupiter.api.Test;
import org.locationtech.spatial4j.io.GeohashUtils;

import static org.assertj.core.api.Assertions.assertThat;

class GeoHashIndexTest {

  @Test
  void geoManualIndexPoints() throws Exception {
    final int TOTAL = 1_000;

    TestHelper.executeInNewDatabase("GeoDatabase", (db) -> {

      db.transaction(() -> {
        final DocumentType type = db.getSchema().createDocumentType("Restaurant");
        type.createProperty("coords", Type.STRING).createIndex(Schema.INDEX_TYPE.LSM_TREE, false);

        for (int i = 0; i < TOTAL; i++) {
          final MutableDocument doc = db.newDocument("Restaurant");
          doc.set("lat", 10 + (0.01D * i));
          doc.set("long", 10 + (0.01D * i));
          doc.set("coords", GeohashUtils.encodeLatLon(doc.getDouble("lat"), doc.getDouble("long")));
          doc.save();
        }

        final String[] area = new String[] { GeohashUtils.encodeLatLon(10.5, 10.5), GeohashUtils.encodeLatLon(10.55, 10.55) };

        ResultSet result = db.query("sql", "select from Restaurant where coords >= ? and coords <= ?", area[0], area[1]);

        assertThat(result.hasNext()).isTrue();
        int returned = 0;
        while (result.hasNext()) {
          final Document record = result.next().toElement();
          assertThat(record.getDouble("lat")).isGreaterThanOrEqualTo(10.5);
          assertThat(record.getDouble("long")).isLessThanOrEqualTo(10.55);
          ++returned;
        }

        assertThat(returned).isEqualTo(6);
      });
    });
  }

  @Test
  void geoManualIndexBoundingBoxes() throws Exception {
    final int TOTAL = 1_000;

    TestHelper.executeInNewDatabase("GeoDatabase", (db) -> {

      db.transaction(() -> {
        final DocumentType type = db.getSchema().createDocumentType("Restaurant");
        type.createProperty("bboxTL", Type.STRING).createIndex(Schema.INDEX_TYPE.LSM_TREE, false);
        type.createProperty("bboxBR", Type.STRING).createIndex(Schema.INDEX_TYPE.LSM_TREE, false);

        for (int i = 0; i < TOTAL; i++) {
          final MutableDocument doc = db.newDocument("Restaurant");
          doc.set("x1", 10D + (0.0001D * i));
          doc.set("y1", 10D + (0.0001D * i));
          doc.set("x2", 10D + (0.001D * i));
          doc.set("y2", 10D + (0.001D * i));
          doc.set("bboxTL", GeohashUtils.encodeLatLon(doc.getDouble("x1"), doc.getDouble("y1")));
          doc.set("bboxBR", GeohashUtils.encodeLatLon(doc.getDouble("x2"), doc.getDouble("y2")));
          doc.save();
        }

        for (Index idx : type.getAllIndexes(false))
          assertThat(idx.countEntries()).isEqualTo(TOTAL);

        final String[] area = new String[] {
            GeohashUtils.encodeLatLon(10.0001D, 10.0001D),
            GeohashUtils.encodeLatLon(10.020D, 10.020D) };

        ResultSet result = db.query("sql", "select from Restaurant where bboxTL >= ? and bboxBR <= ?", area[0], area[1]);

        assertThat(result.hasNext()).isTrue();
        int returned = 0;
        while (result.hasNext()) {
          final Document record = result.next().toElement();
          assertThat(record.getDouble("x1")).isGreaterThanOrEqualTo(10.0001D).withFailMessage("x1: " + record.getDouble("x1"));
          assertThat(record.getDouble("y1")).isGreaterThanOrEqualTo(10.0001D).withFailMessage("y1: " + record.getDouble("y1"));
          assertThat(record.getDouble("x2")).isLessThanOrEqualTo(10.020D).withFailMessage("x2: " + record.getDouble("x2"));
          assertThat(record.getDouble("y2")).isLessThanOrEqualTo(10.020D).withFailMessage("y2: " + record.getDouble("y2"));
          ++returned;
        }

        assertThat(returned).isEqualTo(20);
      });
    });
  }
}
```

**Step 2: Compile**

```
mvn test-compile -pl engine -q
```
Expected: BUILD SUCCESS

**Step 3: Run**

```
mvn test -pl engine -Dtest=GeoHashIndexTest -q
```
Expected: 2 tests pass

**Step 4: Commit**

```bash
git add engine/src/test/java/com/arcadedb/function/sql/geo/GeoHashIndexTest.java
git commit -m "test(geo): add GeoHashIndexTest with geohash index tests"
```

---

### Task 2: Create `GeoConstructionFunctionsTest.java`

Covers: `geo.geomFromText`, `geo.point`, `geo.lineString`, `geo.polygon`.

**Files:**
- Create: `engine/src/test/java/com/arcadedb/function/sql/geo/GeoConstructionFunctionsTest.java`

**Step 1: Create the file**

```java
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
import com.arcadedb.query.sql.executor.ResultSet;

import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.locationtech.spatial4j.shape.Shape;

import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class GeoConstructionFunctionsTest {

  // ─── geo.geomFromText ─────────────────────────────────────────────────────────

  @Nested
  class GeomFromText {

    @Test
    void sqlHappyPath() throws Exception {
      TestHelper.executeInNewDatabase("GeoDatabase", (db) -> {
        final ResultSet result = db.query("sql", "select geo.geomFromText('POINT (10 20)') as geom");
        assertThat(result.hasNext()).isTrue();
        final Object geom = result.next().getProperty("geom");
        assertThat(geom).isNotNull();
        assertThat(geom).isInstanceOf(Shape.class);
      });
    }

    @Test
    void javaExecuteHappyPath() {
      final Object result = new SQLFunctionGeoGeomFromText()
          .execute(null, null, null, new Object[] { "POINT (10 20)" }, null);
      assertThat(result).isNotNull();
      assertThat(result).isInstanceOf(Shape.class);
    }

    @Test
    void nullInput_sql_returnsNull() throws Exception {
      TestHelper.executeInNewDatabase("GeoDatabase", (db) -> {
        final ResultSet result = db.query("sql", "select geo.geomFromText(null) as geom");
        assertThat(result.hasNext()).isTrue();
        assertThat(result.next().getProperty("geom")).isNull();
      });
    }

    @Test
    void nullInput_execute_returnsNull() {
      assertThat(new SQLFunctionGeoGeomFromText()
          .execute(null, null, null, new Object[] { null }, null)).isNull();
    }

    @Test
    void noArgs_execute_returnsNull() {
      assertThat(new SQLFunctionGeoGeomFromText()
          .execute(null, null, null, new Object[0], null)).isNull();
    }

    @Test
    void invalidWkt_execute_throwsException() {
      assertThatThrownBy(() -> new SQLFunctionGeoGeomFromText()
          .execute(null, null, null, new Object[] { "NOT VALID WKT ###" }, null))
          .isInstanceOf(Exception.class);
    }

    @Test
    void emptyString_execute_throwsException() {
      assertThatThrownBy(() -> new SQLFunctionGeoGeomFromText()
          .execute(null, null, null, new Object[] { "" }, null))
          .isInstanceOf(Exception.class);
    }
  }

  // ─── geo.point ────────────────────────────────────────────────────────────────

  @Nested
  class Point {

    @Test
    void sqlHappyPath() throws Exception {
      TestHelper.executeInNewDatabase("GeoDatabase", (db) -> {
        final ResultSet result = db.query("sql", "select geo.point(10, 20) as wkt");
        assertThat(result.hasNext()).isTrue();
        final String wkt = result.next().getProperty("wkt");
        assertThat(wkt).isNotNull();
        assertThat(wkt).startsWith("POINT");
        assertThat(wkt).contains("10");
        assertThat(wkt).contains("20");
      });
    }

    @Test
    void javaExecuteHappyPath() {
      final Object result = new SQLFunctionGeoPoint()
          .execute(null, null, null, new Object[] { 10.0, 20.0 }, null);
      assertThat(result).isNotNull();
      assertThat(result).isInstanceOf(String.class);
      assertThat((String) result).startsWith("POINT");
      assertThat((String) result).contains("10");
      assertThat((String) result).contains("20");
    }

    @Test
    void nullFirstArg_sql_returnsNull() throws Exception {
      TestHelper.executeInNewDatabase("GeoDatabase", (db) -> {
        final ResultSet result = db.query("sql", "select geo.point(null, 20) as wkt");
        assertThat(result.hasNext()).isTrue();
        assertThat(result.next().getProperty("wkt")).isNull();
      });
    }

    @Test
    void nullFirstArg_execute_returnsNull() {
      assertThat(new SQLFunctionGeoPoint()
          .execute(null, null, null, new Object[] { null, 20.0 }, null)).isNull();
    }

    @Test
    void nullSecondArg_execute_returnsNull() {
      assertThat(new SQLFunctionGeoPoint()
          .execute(null, null, null, new Object[] { 10.0, null }, null)).isNull();
    }

    @Test
    void tooFewArgs_execute_returnsNull() {
      assertThat(new SQLFunctionGeoPoint()
          .execute(null, null, null, new Object[] { 10.0 }, null)).isNull();
    }

    @Test
    void noArgs_execute_returnsNull() {
      assertThat(new SQLFunctionGeoPoint()
          .execute(null, null, null, new Object[0], null)).isNull();
    }
  }

  // ─── geo.lineString ───────────────────────────────────────────────────────────

  @Nested
  class LineString {

    @Test
    void sqlHappyPath() throws Exception {
      TestHelper.executeInNewDatabase("GeoDatabase", (db) -> {
        final ResultSet result = db.query("sql", "select geo.lineString([[0,0],[10,10],[20,0]]) as wkt");
        assertThat(result.hasNext()).isTrue();
        final String wkt = result.next().getProperty("wkt");
        assertThat(wkt).isNotNull();
        assertThat(wkt).startsWith("LINESTRING");
        assertThat(wkt).contains("0 0");
        assertThat(wkt).contains("10 10");
        assertThat(wkt).contains("20 0");
      });
    }

    @Test
    void javaExecuteHappyPath() {
      final List<List<Object>> coords = List.of(
          List.of(0.0, 0.0),
          List.of(10.0, 10.0),
          List.of(20.0, 0.0));
      final Object result = new SQLFunctionGeoLineString()
          .execute(null, null, null, new Object[] { coords }, null);
      assertThat(result).isNotNull();
      assertThat((String) result).startsWith("LINESTRING");
      assertThat((String) result).contains("0 0");
      assertThat((String) result).contains("10 10");
      assertThat((String) result).contains("20 0");
    }

    @Test
    void nullInput_sql_returnsNull() throws Exception {
      TestHelper.executeInNewDatabase("GeoDatabase", (db) -> {
        final ResultSet result = db.query("sql", "select geo.lineString(null) as wkt");
        assertThat(result.hasNext()).isTrue();
        assertThat(result.next().getProperty("wkt")).isNull();
      });
    }

    @Test
    void nullInput_execute_returnsNull() {
      assertThat(new SQLFunctionGeoLineString()
          .execute(null, null, null, new Object[] { null }, null)).isNull();
    }

    @Test
    void emptyList_execute_returnsNull() {
      assertThat(new SQLFunctionGeoLineString()
          .execute(null, null, null, new Object[] { List.of() }, null)).isNull();
    }

    @Test
    void invalidListElement_execute_throwsException() {
      final List<Object> badCoords = List.of("not_a_coordinate");
      assertThatThrownBy(() -> new SQLFunctionGeoLineString()
          .execute(null, null, null, new Object[] { badCoords }, null))
          .isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    void singlePoint_execute_returnsLineString() {
      // Degenerate but valid from the function's perspective — just formats the coord
      final List<List<Object>> single = List.of(List.of(0.0, 0.0));
      final Object result = new SQLFunctionGeoLineString()
          .execute(null, null, null, new Object[] { single }, null);
      assertThat(result).isNotNull();
      assertThat((String) result).startsWith("LINESTRING");
    }
  }

  // ─── geo.polygon ──────────────────────────────────────────────────────────────

  @Nested
  class Polygon {

    @Test
    void sqlHappyPath() throws Exception {
      TestHelper.executeInNewDatabase("GeoDatabase", (db) -> {
        final ResultSet result = db.query("sql", "select geo.polygon([[0,0],[10,0],[10,10],[0,10],[0,0]]) as wkt");
        assertThat(result.hasNext()).isTrue();
        final String wkt = result.next().getProperty("wkt");
        assertThat(wkt).isNotNull();
        assertThat(wkt).startsWith("POLYGON");
        assertThat(wkt).contains("0 0");
        assertThat(wkt).contains("10 0");
        assertThat(wkt).contains("10 10");
      });
    }

    @Test
    void javaExecuteHappyPath() {
      final List<List<Object>> ring = List.of(
          List.of(0.0, 0.0), List.of(10.0, 0.0),
          List.of(10.0, 10.0), List.of(0.0, 10.0),
          List.of(0.0, 0.0));
      final Object result = new SQLFunctionGeoPolygon()
          .execute(null, null, null, new Object[] { ring }, null);
      assertThat(result).isNotNull();
      assertThat((String) result).startsWith("POLYGON");
    }

    @Test
    void nullInput_sql_returnsNull() throws Exception {
      TestHelper.executeInNewDatabase("GeoDatabase", (db) -> {
        final ResultSet result = db.query("sql", "select geo.polygon(null) as wkt");
        assertThat(result.hasNext()).isTrue();
        assertThat(result.next().getProperty("wkt")).isNull();
      });
    }

    @Test
    void nullInput_execute_returnsNull() {
      assertThat(new SQLFunctionGeoPolygon()
          .execute(null, null, null, new Object[] { null }, null)).isNull();
    }

    @Test
    void openRing_sql_autoClose() throws Exception {
      TestHelper.executeInNewDatabase("GeoDatabase", (db) -> {
        final ResultSet result = db.query("sql", "select geo.polygon([[0,0],[10,0],[10,10],[0,10]]) as wkt");
        assertThat(result.hasNext()).isTrue();
        final String wkt = result.next().getProperty("wkt");
        assertThat(wkt).isNotNull();
        assertThat(wkt).startsWith("POLYGON");
        final String inner = wkt.substring(wkt.indexOf("((") + 2, wkt.lastIndexOf("))"));
        final String[] coords = inner.split(",");
        assertThat(coords[0].trim()).isEqualTo(coords[coords.length - 1].trim());
      });
    }

    @Test
    void openRing_execute_autoClose() {
      // Ring without closing coord — function should auto-close it
      final List<List<Object>> openRing = List.of(
          List.of(0.0, 0.0), List.of(10.0, 0.0),
          List.of(10.0, 10.0), List.of(0.0, 10.0));
      final String wkt = (String) new SQLFunctionGeoPolygon()
          .execute(null, null, null, new Object[] { openRing }, null);
      assertThat(wkt).isNotNull();
      assertThat(wkt).startsWith("POLYGON");
      final String inner = wkt.substring(wkt.indexOf("((") + 2, wkt.lastIndexOf("))"));
      final String[] coords = inner.split(",");
      assertThat(coords[0].trim()).isEqualTo(coords[coords.length - 1].trim());
    }

    @Test
    void invalidListElement_execute_throwsException() {
      final List<Object> badRing = List.of("not_a_coordinate");
      assertThatThrownBy(() -> new SQLFunctionGeoPolygon()
          .execute(null, null, null, new Object[] { badRing }, null))
          .isInstanceOf(IllegalArgumentException.class);
    }
  }
}
```

**Step 2: Compile**

```
mvn test-compile -pl engine -q
```
Expected: BUILD SUCCESS

**Step 3: Run**

```
mvn test -pl engine -Dtest=GeoConstructionFunctionsTest -q
```
Expected: all tests pass

**Step 4: Commit**

```bash
git add engine/src/test/java/com/arcadedb/function/sql/geo/GeoConstructionFunctionsTest.java
git commit -m "test(geo): add GeoConstructionFunctionsTest with SQL, execute(), and error paths"
```

---

### Task 3: Create `GeoMeasurementFunctionsTest.java`

Covers: `geo.buffer`, `geo.distance`, `geo.area`, `geo.envelope`.

**Files:**
- Create: `engine/src/test/java/com/arcadedb/function/sql/geo/GeoMeasurementFunctionsTest.java`

**Step 1: Create the file**

```java
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
import com.arcadedb.query.sql.executor.ResultSet;

import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class GeoMeasurementFunctionsTest {

  // ─── geo.buffer ───────────────────────────────────────────────────────────────

  @Nested
  class Buffer {

    @Test
    void sqlHappyPath() throws Exception {
      TestHelper.executeInNewDatabase("GeoDatabase", (db) -> {
        final ResultSet result = db.query("sql", "select geo.buffer('POINT (10 20)', 1.0) as wkt");
        assertThat(result.hasNext()).isTrue();
        final String wkt = result.next().getProperty("wkt");
        assertThat(wkt).isNotNull();
        assertThat(wkt).startsWith("POLYGON");
      });
    }

    @Test
    void javaExecuteHappyPath() {
      final Object result = new SQLFunctionGeoBuffer()
          .execute(null, null, null, new Object[] { "POINT (10 20)", 1.0 }, null);
      assertThat(result).isNotNull();
      assertThat((String) result).startsWith("POLYGON");
    }

    @Test
    void nullGeometry_sql_returnsNull() throws Exception {
      TestHelper.executeInNewDatabase("GeoDatabase", (db) -> {
        final ResultSet result = db.query("sql", "select geo.buffer(null, 1.0) as wkt");
        assertThat(result.hasNext()).isTrue();
        assertThat(result.next().getProperty("wkt")).isNull();
      });
    }

    @Test
    void nullGeometry_execute_returnsNull() {
      assertThat(new SQLFunctionGeoBuffer()
          .execute(null, null, null, new Object[] { null, 1.0 }, null)).isNull();
    }

    @Test
    void nullDistance_execute_returnsNull() {
      assertThat(new SQLFunctionGeoBuffer()
          .execute(null, null, null, new Object[] { "POINT (10 20)", null }, null)).isNull();
    }

    @Test
    void invalidGeometry_execute_throwsException() {
      assertThatThrownBy(() -> new SQLFunctionGeoBuffer()
          .execute(null, null, null, new Object[] { "NOT VALID WKT", 1.0 }, null))
          .isInstanceOf(Exception.class);
    }
  }

  // ─── geo.distance ─────────────────────────────────────────────────────────────

  @Nested
  class Distance {

    @Test
    void sqlHappyPath_meters() throws Exception {
      TestHelper.executeInNewDatabase("GeoDatabase", (db) -> {
        final ResultSet result = db.query("sql", "select geo.distance('POINT (0 0)', 'POINT (1 0)') as dist");
        assertThat(result.hasNext()).isTrue();
        final Double dist = result.next().getProperty("dist");
        assertThat(dist).isNotNull();
        assertThat(dist).isGreaterThan(0.0);
      });
    }

    @Test
    void sqlHappyPath_km_lessThanMeters() throws Exception {
      TestHelper.executeInNewDatabase("GeoDatabase", (db) -> {
        ResultSet r = db.query("sql", "select geo.distance('POINT (0 0)', 'POINT (1 0)') as dist");
        final Double distM = r.next().getProperty("dist");

        r = db.query("sql", "select geo.distance('POINT (0 0)', 'POINT (1 0)', 'km') as dist");
        final Double distKm = r.next().getProperty("dist");

        assertThat(distKm).isGreaterThan(0.0);
        assertThat(distKm).isLessThan(distM);
      });
    }

    @Test
    void javaExecuteHappyPath_meters() {
      final Double dist = (Double) new SQLFunctionGeoDistance()
          .execute(null, null, null, new Object[] { "POINT (0 0)", "POINT (1 0)" }, null);
      assertThat(dist).isNotNull();
      assertThat(dist).isGreaterThan(0.0);
    }

    @Test
    void javaExecuteHappyPath_miles() {
      final Double distMi = (Double) new SQLFunctionGeoDistance()
          .execute(null, null, null, new Object[] { "POINT (0 0)", "POINT (1 0)", "mi" }, null);
      assertThat(distMi).isNotNull();
      assertThat(distMi).isGreaterThan(0.0);
    }

    @Test
    void nullFirstArg_sql_returnsNull() throws Exception {
      TestHelper.executeInNewDatabase("GeoDatabase", (db) -> {
        final ResultSet result = db.query("sql", "select geo.distance(null, 'POINT (1 0)') as dist");
        assertThat(result.hasNext()).isTrue();
        assertThat(result.next().getProperty("dist")).isNull();
      });
    }

    @Test
    void nullFirstArg_execute_returnsNull() {
      assertThat(new SQLFunctionGeoDistance()
          .execute(null, null, null, new Object[] { null, "POINT (1 0)" }, null)).isNull();
    }

    @Test
    void nullSecondArg_execute_returnsNull() {
      assertThat(new SQLFunctionGeoDistance()
          .execute(null, null, null, new Object[] { "POINT (0 0)", null }, null)).isNull();
    }

    @Test
    void invalidUnit_execute_throwsIllegalArgument() {
      assertThatThrownBy(() -> new SQLFunctionGeoDistance()
          .execute(null, null, null, new Object[] { "POINT (0 0)", "POINT (1 0)", "lightyear" }, null))
          .isInstanceOf(IllegalArgumentException.class)
          .hasMessageContaining("lightyear");
    }

    @Test
    void allSupportedUnits_execute_returnPositive() {
      for (final String unit : new String[] { "m", "km", "mi", "nmi" }) {
        final Double d = (Double) new SQLFunctionGeoDistance()
            .execute(null, null, null, new Object[] { "POINT (0 0)", "POINT (1 0)", unit }, null);
        assertThat(d).as("unit=%s", unit).isGreaterThan(0.0);
      }
    }
  }

  // ─── geo.area ─────────────────────────────────────────────────────────────────

  @Nested
  class Area {

    @Test
    void sqlHappyPath() throws Exception {
      TestHelper.executeInNewDatabase("GeoDatabase", (db) -> {
        final ResultSet result = db.query("sql",
            "select geo.area('POLYGON ((0 0, 10 0, 10 10, 0 10, 0 0))') as area");
        assertThat(result.hasNext()).isTrue();
        final Double area = result.next().getProperty("area");
        assertThat(area).isNotNull();
        assertThat(area).isGreaterThan(0.0);
      });
    }

    @Test
    void javaExecuteHappyPath() {
      final Double area = (Double) new SQLFunctionGeoArea()
          .execute(null, null, null, new Object[] { "POLYGON ((0 0, 10 0, 10 10, 0 10, 0 0))" }, null);
      assertThat(area).isNotNull();
      assertThat(area).isGreaterThan(0.0);
    }

    @Test
    void nullInput_sql_returnsNull() throws Exception {
      TestHelper.executeInNewDatabase("GeoDatabase", (db) -> {
        final ResultSet result = db.query("sql", "select geo.area(null) as area");
        assertThat(result.hasNext()).isTrue();
        assertThat(result.next().getProperty("area")).isNull();
      });
    }

    @Test
    void nullInput_execute_returnsNull() {
      assertThat(new SQLFunctionGeoArea()
          .execute(null, null, null, new Object[] { null }, null)).isNull();
    }

    @Test
    void pointGeometry_execute_returnsZero() {
      // A point has no area
      final Double area = (Double) new SQLFunctionGeoArea()
          .execute(null, null, null, new Object[] { "POINT (5 5)" }, null);
      assertThat(area).isNotNull();
      assertThat(area).isEqualTo(0.0);
    }
  }

  // ─── geo.envelope ─────────────────────────────────────────────────────────────

  @Nested
  class Envelope {

    @Test
    void sqlHappyPath() throws Exception {
      TestHelper.executeInNewDatabase("GeoDatabase", (db) -> {
        final ResultSet result = db.query("sql",
            "select geo.envelope('POLYGON ((0 0, 10 0, 10 10, 0 10, 0 0))') as wkt");
        assertThat(result.hasNext()).isTrue();
        final String wkt = result.next().getProperty("wkt");
        assertThat(wkt).isNotNull();
        assertThat(wkt).startsWith("POLYGON");
        assertThat(wkt).contains("0 0");
        assertThat(wkt).contains("10 10");
      });
    }

    @Test
    void javaExecuteHappyPath() {
      final Object result = new SQLFunctionGeoEnvelope()
          .execute(null, null, null, new Object[] { "POLYGON ((0 0, 10 0, 10 10, 0 10, 0 0))" }, null);
      assertThat(result).isNotNull();
      assertThat((String) result).startsWith("POLYGON");
    }

    @Test
    void nullInput_sql_returnsNull() throws Exception {
      TestHelper.executeInNewDatabase("GeoDatabase", (db) -> {
        final ResultSet result = db.query("sql", "select geo.envelope(null) as wkt");
        assertThat(result.hasNext()).isTrue();
        assertThat(result.next().getProperty("wkt")).isNull();
      });
    }

    @Test
    void nullInput_execute_returnsNull() {
      assertThat(new SQLFunctionGeoEnvelope()
          .execute(null, null, null, new Object[] { null }, null)).isNull();
    }

    @Test
    void invalidWkt_execute_throwsException() {
      assertThatThrownBy(() -> new SQLFunctionGeoEnvelope()
          .execute(null, null, null, new Object[] { "NOT VALID WKT" }, null))
          .isInstanceOf(Exception.class);
    }

    @Test
    void pointEnvelope_execute_returnsResult() {
      // Envelope of a point is degenerate but should not crash
      final Object result = new SQLFunctionGeoEnvelope()
          .execute(null, null, null, new Object[] { "POINT (5 5)" }, null);
      assertThat(result).isNotNull();
    }
  }
}
```

**Step 2: Compile**

```
mvn test-compile -pl engine -q
```
Expected: BUILD SUCCESS

**Step 3: Run**

```
mvn test -pl engine -Dtest=GeoMeasurementFunctionsTest -q
```
Expected: all tests pass

**Step 4: Commit**

```bash
git add engine/src/test/java/com/arcadedb/function/sql/geo/GeoMeasurementFunctionsTest.java
git commit -m "test(geo): add GeoMeasurementFunctionsTest with SQL, execute(), and error paths"
```

---

### Task 4: Create `GeoConversionFunctionsTest.java`

Covers: `geo.asText`, `geo.asGeoJson`, `geo.x`, `geo.y`.

**Files:**
- Create: `engine/src/test/java/com/arcadedb/function/sql/geo/GeoConversionFunctionsTest.java`

**Step 1: Create the file**

```java
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
import com.arcadedb.query.sql.executor.ResultSet;

import org.assertj.core.data.Offset;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.locationtech.spatial4j.shape.Shape;

import static org.assertj.core.api.Assertions.assertThat;

class GeoConversionFunctionsTest {

  // ─── geo.asText ───────────────────────────────────────────────────────────────

  @Nested
  class AsText {

    @Test
    void sqlStringInput_returnedAsIs() throws Exception {
      TestHelper.executeInNewDatabase("GeoDatabase", (db) -> {
        final ResultSet result = db.query("sql", "select geo.asText('POINT (10 20)') as wkt");
        assertThat(result.hasNext()).isTrue();
        assertThat(result.next().<String>getProperty("wkt")).isEqualTo("POINT (10 20)");
      });
    }

    @Test
    void sqlShapeInput_returnsWkt() throws Exception {
      TestHelper.executeInNewDatabase("GeoDatabase", (db) -> {
        final ResultSet result = db.query("sql",
            "select geo.asText(geo.geomFromText('POINT (10 20)')) as wkt");
        assertThat(result.hasNext()).isTrue();
        final String wkt = result.next().getProperty("wkt");
        assertThat(wkt).isNotNull();
        assertThat(wkt).startsWith("POINT");
        assertThat(wkt).contains("10");
        assertThat(wkt).contains("20");
      });
    }

    @Test
    void javaExecute_stringInput_returnedAsIs() {
      final Object result = new SQLFunctionGeoAsText()
          .execute(null, null, null, new Object[] { "POINT (10 20)" }, null);
      assertThat(result).isEqualTo("POINT (10 20)");
    }

    @Test
    void javaExecute_shapeInput_returnsWkt() {
      final Shape shape = (Shape) new SQLFunctionGeoGeomFromText()
          .execute(null, null, null, new Object[] { "POINT (10 20)" }, null);
      final Object result = new SQLFunctionGeoAsText()
          .execute(null, null, null, new Object[] { shape }, null);
      assertThat(result).isNotNull();
      assertThat((String) result).startsWith("POINT");
    }

    @Test
    void nullInput_sql_returnsNull() throws Exception {
      TestHelper.executeInNewDatabase("GeoDatabase", (db) -> {
        final ResultSet result = db.query("sql", "select geo.asText(null) as wkt");
        assertThat(result.hasNext()).isTrue();
        assertThat(result.next().getProperty("wkt")).isNull();
      });
    }

    @Test
    void nullInput_execute_returnsNull() {
      assertThat(new SQLFunctionGeoAsText()
          .execute(null, null, null, new Object[] { null }, null)).isNull();
    }
  }

  // ─── geo.asGeoJson ────────────────────────────────────────────────────────────

  @Nested
  class AsGeoJson {

    @Test
    void sqlPoint_containsPointType() throws Exception {
      TestHelper.executeInNewDatabase("GeoDatabase", (db) -> {
        final ResultSet result = db.query("sql", "select geo.asGeoJson('POINT (10 20)') as json");
        assertThat(result.hasNext()).isTrue();
        final String json = result.next().getProperty("json");
        assertThat(json).isNotNull();
        assertThat(json).contains("Point");
        assertThat(json).contains("coordinates");
        assertThat(json).contains("10");
        assertThat(json).contains("20");
      });
    }

    @Test
    void sqlPolygon_containsPolygonType() throws Exception {
      TestHelper.executeInNewDatabase("GeoDatabase", (db) -> {
        final ResultSet result = db.query("sql",
            "select geo.asGeoJson('POLYGON ((0 0, 10 0, 10 10, 0 10, 0 0))') as json");
        assertThat(result.hasNext()).isTrue();
        final String json = result.next().getProperty("json");
        assertThat(json).isNotNull();
        assertThat(json).contains("Polygon");
        assertThat(json).contains("coordinates");
      });
    }

    @Test
    void javaExecute_point_containsPointType() {
      final Object result = new SQLFunctionGeoAsGeoJson()
          .execute(null, null, null, new Object[] { "POINT (10 20)" }, null);
      assertThat(result).isNotNull();
      assertThat((String) result).contains("Point");
      assertThat((String) result).contains("coordinates");
    }

    @Test
    void javaExecute_lineString_containsLineStringType() {
      final Object result = new SQLFunctionGeoAsGeoJson()
          .execute(null, null, null, new Object[] { "LINESTRING (0 0, 10 10, 20 0)" }, null);
      assertThat(result).isNotNull();
      assertThat((String) result).contains("LineString");
      assertThat((String) result).contains("coordinates");
    }

    @Test
    void nullInput_sql_returnsNull() throws Exception {
      TestHelper.executeInNewDatabase("GeoDatabase", (db) -> {
        final ResultSet result = db.query("sql", "select geo.asGeoJson(null) as json");
        assertThat(result.hasNext()).isTrue();
        assertThat(result.next().getProperty("json")).isNull();
      });
    }

    @Test
    void nullInput_execute_returnsNull() {
      assertThat(new SQLFunctionGeoAsGeoJson()
          .execute(null, null, null, new Object[] { null }, null)).isNull();
    }
  }

  // ─── geo.x ────────────────────────────────────────────────────────────────────

  @Nested
  class X {

    @Test
    void sqlHappyPath() throws Exception {
      TestHelper.executeInNewDatabase("GeoDatabase", (db) -> {
        final ResultSet result = db.query("sql", "select geo.x('POINT (10 20)') as x");
        assertThat(result.hasNext()).isTrue();
        final Double x = result.next().getProperty("x");
        assertThat(x).isNotNull();
        assertThat(x).isEqualTo(10.0);
      });
    }

    @Test
    void javaExecuteHappyPath() {
      final Double x = (Double) new SQLFunctionGeoX()
          .execute(null, null, null, new Object[] { "POINT (10 20)" }, null);
      assertThat(x).isNotNull();
      assertThat(x).isEqualTo(10.0);
    }

    @Test
    void javaExecute_shapeInput() {
      final Shape shape = (Shape) new SQLFunctionGeoGeomFromText()
          .execute(null, null, null, new Object[] { "POINT (10 20)" }, null);
      final Double x = (Double) new SQLFunctionGeoX()
          .execute(null, null, null, new Object[] { shape }, null);
      assertThat(x).isNotNull();
      assertThat(x).isEqualTo(10.0);
    }

    @Test
    void nullInput_sql_returnsNull() throws Exception {
      TestHelper.executeInNewDatabase("GeoDatabase", (db) -> {
        final ResultSet result = db.query("sql", "select geo.x(null) as x");
        assertThat(result.hasNext()).isTrue();
        assertThat(result.next().getProperty("x")).isNull();
      });
    }

    @Test
    void nullInput_execute_returnsNull() {
      assertThat(new SQLFunctionGeoX()
          .execute(null, null, null, new Object[] { null }, null)).isNull();
    }

    @Test
    void polygonInput_sql_returnsNull() throws Exception {
      TestHelper.executeInNewDatabase("GeoDatabase", (db) -> {
        final ResultSet result = db.query("sql",
            "select geo.x('POLYGON ((0 0, 10 0, 10 10, 0 10, 0 0))') as x");
        assertThat(result.hasNext()).isTrue();
        assertThat(result.next().getProperty("x")).isNull();
      });
    }

    @Test
    void polygonInput_execute_returnsNull() {
      assertThat(new SQLFunctionGeoX()
          .execute(null, null, null,
              new Object[] { "POLYGON ((0 0, 10 0, 10 10, 0 10, 0 0))" }, null)).isNull();
    }

    @Test
    void invalidWkt_execute_returnsNullSilently() {
      // geo.x silently swallows parse errors and returns null
      assertThat(new SQLFunctionGeoX()
          .execute(null, null, null, new Object[] { "NOT_WKT_AT_ALL" }, null)).isNull();
    }

    @Test
    void roundTrip_pointXMatches() {
      final String wkt = (String) new SQLFunctionGeoPoint()
          .execute(null, null, null, new Object[] { 42.5, -7.3 }, null);
      final Double x = (Double) new SQLFunctionGeoX()
          .execute(null, null, null, new Object[] { wkt }, null);
      assertThat(x).isNotNull();
      assertThat(x).isCloseTo(42.5, Offset.offset(1e-6));
    }
  }

  // ─── geo.y ────────────────────────────────────────────────────────────────────

  @Nested
  class Y {

    @Test
    void sqlHappyPath() throws Exception {
      TestHelper.executeInNewDatabase("GeoDatabase", (db) -> {
        final ResultSet result = db.query("sql", "select geo.y('POINT (10 20)') as y");
        assertThat(result.hasNext()).isTrue();
        final Double y = result.next().getProperty("y");
        assertThat(y).isNotNull();
        assertThat(y).isEqualTo(20.0);
      });
    }

    @Test
    void javaExecuteHappyPath() {
      final Double y = (Double) new SQLFunctionGeoY()
          .execute(null, null, null, new Object[] { "POINT (10 20)" }, null);
      assertThat(y).isNotNull();
      assertThat(y).isEqualTo(20.0);
    }

    @Test
    void javaExecute_shapeInput() {
      final Shape shape = (Shape) new SQLFunctionGeoGeomFromText()
          .execute(null, null, null, new Object[] { "POINT (10 20)" }, null);
      final Double y = (Double) new SQLFunctionGeoY()
          .execute(null, null, null, new Object[] { shape }, null);
      assertThat(y).isNotNull();
      assertThat(y).isEqualTo(20.0);
    }

    @Test
    void nullInput_sql_returnsNull() throws Exception {
      TestHelper.executeInNewDatabase("GeoDatabase", (db) -> {
        final ResultSet result = db.query("sql", "select geo.y(null) as y");
        assertThat(result.hasNext()).isTrue();
        assertThat(result.next().getProperty("y")).isNull();
      });
    }

    @Test
    void nullInput_execute_returnsNull() {
      assertThat(new SQLFunctionGeoY()
          .execute(null, null, null, new Object[] { null }, null)).isNull();
    }

    @Test
    void polygonInput_sql_returnsNull() throws Exception {
      TestHelper.executeInNewDatabase("GeoDatabase", (db) -> {
        final ResultSet result = db.query("sql",
            "select geo.y('POLYGON ((0 0, 10 0, 10 10, 0 10, 0 0))') as y");
        assertThat(result.hasNext()).isTrue();
        assertThat(result.next().getProperty("y")).isNull();
      });
    }

    @Test
    void polygonInput_execute_returnsNull() {
      assertThat(new SQLFunctionGeoY()
          .execute(null, null, null,
              new Object[] { "POLYGON ((0 0, 10 0, 10 10, 0 10, 0 0))" }, null)).isNull();
    }

    @Test
    void invalidWkt_execute_returnsNullSilently() {
      assertThat(new SQLFunctionGeoY()
          .execute(null, null, null, new Object[] { "NOT_WKT_AT_ALL" }, null)).isNull();
    }

    @Test
    void roundTrip_pointYMatches() {
      final String wkt = (String) new SQLFunctionGeoPoint()
          .execute(null, null, null, new Object[] { 42.5, -7.3 }, null);
      final Double y = (Double) new SQLFunctionGeoY()
          .execute(null, null, null, new Object[] { wkt }, null);
      assertThat(y).isNotNull();
      assertThat(y).isCloseTo(-7.3, Offset.offset(1e-6));
    }
  }
}
```

**Step 2: Compile**

```
mvn test-compile -pl engine -q
```
Expected: BUILD SUCCESS

**Step 3: Run**

```
mvn test -pl engine -Dtest=GeoConversionFunctionsTest -q
```
Expected: all tests pass

**Step 4: Commit**

```bash
git add engine/src/test/java/com/arcadedb/function/sql/geo/GeoConversionFunctionsTest.java
git commit -m "test(geo): add GeoConversionFunctionsTest with SQL, execute(), and error paths"
```

---

### Task 5: Create `GeoPredicateFunctionsTest.java`

Covers: `geo.within`, `geo.intersects`, `geo.contains`, `geo.dWithin`, `geo.disjoint`, `geo.equals`, `geo.crosses`, `geo.overlaps`, `geo.touches`.

**Files:**
- Create: `engine/src/test/java/com/arcadedb/function/sql/geo/GeoPredicateFunctionsTest.java`

**Step 1: Create the file**

```java
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
import com.arcadedb.query.sql.executor.ResultSet;

import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

class GeoPredicateFunctionsTest {

  private static final String POLYGON_0_10 = "POLYGON ((0 0, 10 0, 10 10, 0 10, 0 0))";
  private static final String POINT_INSIDE  = "POINT (5 5)";
  private static final String POINT_OUTSIDE = "POINT (15 15)";

  // ─── geo.within ───────────────────────────────────────────────────────────────

  @Nested
  class Within {

    @Test
    void sql_pointInsidePolygon_returnsTrue() throws Exception {
      TestHelper.executeInNewDatabase("GeoDatabase", (db) -> {
        final ResultSet r = db.query("sql",
            "select geo.within('POINT (5 5)', 'POLYGON ((0 0, 10 0, 10 10, 0 10, 0 0))') as v");
        assertThat((Boolean) r.next().getProperty("v")).isTrue();
      });
    }

    @Test
    void sql_pointOutsidePolygon_returnsFalse() throws Exception {
      TestHelper.executeInNewDatabase("GeoDatabase", (db) -> {
        final ResultSet r = db.query("sql",
            "select geo.within('POINT (15 15)', 'POLYGON ((0 0, 10 0, 10 10, 0 10, 0 0))') as v");
        assertThat((Boolean) r.next().getProperty("v")).isFalse();
      });
    }

    @Test
    void javaExecute_pointInside_returnsTrue() {
      assertThat((Boolean) new SQLFunctionGeoWithin()
          .execute(null, null, null, new Object[] { POINT_INSIDE, POLYGON_0_10 }, null)).isTrue();
    }

    @Test
    void javaExecute_pointOutside_returnsFalse() {
      assertThat((Boolean) new SQLFunctionGeoWithin()
          .execute(null, null, null, new Object[] { POINT_OUTSIDE, POLYGON_0_10 }, null)).isFalse();
    }

    @Test
    void nullFirstArg_sql_returnsNull() throws Exception {
      TestHelper.executeInNewDatabase("GeoDatabase", (db) -> {
        final ResultSet r = db.query("sql",
            "select geo.within(null, 'POLYGON ((0 0, 10 0, 10 10, 0 10, 0 0))') as v");
        assertThat(r.next().getProperty("v")).isNull();
      });
    }

    @Test
    void nullSecondArg_execute_returnsNull() {
      assertThat(new SQLFunctionGeoWithin()
          .execute(null, null, null, new Object[] { POINT_INSIDE, null }, null)).isNull();
    }

    @Test
    void nullFirstArg_execute_returnsNull() {
      assertThat(new SQLFunctionGeoWithin()
          .execute(null, null, null, new Object[] { null, POLYGON_0_10 }, null)).isNull();
    }
  }

  // ─── geo.intersects ───────────────────────────────────────────────────────────

  @Nested
  class Intersects {

    @Test
    void sql_overlappingPolygons_returnsTrue() throws Exception {
      TestHelper.executeInNewDatabase("GeoDatabase", (db) -> {
        final ResultSet r = db.query("sql",
            "select geo.intersects('POLYGON ((0 0, 6 0, 6 6, 0 6, 0 0))', 'POLYGON ((3 3, 9 3, 9 9, 3 9, 3 3))') as v");
        assertThat((Boolean) r.next().getProperty("v")).isTrue();
      });
    }

    @Test
    void sql_disjointPolygons_returnsFalse() throws Exception {
      TestHelper.executeInNewDatabase("GeoDatabase", (db) -> {
        final ResultSet r = db.query("sql",
            "select geo.intersects('POLYGON ((0 0, 2 0, 2 2, 0 2, 0 0))', 'POLYGON ((5 5, 9 5, 9 9, 5 9, 5 5))') as v");
        assertThat((Boolean) r.next().getProperty("v")).isFalse();
      });
    }

    @Test
    void javaExecute_overlapping_returnsTrue() {
      assertThat((Boolean) new SQLFunctionGeoIntersects()
          .execute(null, null, null,
              new Object[] { "POLYGON ((0 0, 6 0, 6 6, 0 6, 0 0))", "POLYGON ((3 3, 9 3, 9 9, 3 9, 3 3))" }, null))
          .isTrue();
    }

    @Test
    void javaExecute_disjoint_returnsFalse() {
      assertThat((Boolean) new SQLFunctionGeoIntersects()
          .execute(null, null, null,
              new Object[] { "POLYGON ((0 0, 2 0, 2 2, 0 2, 0 0))", "POLYGON ((5 5, 9 5, 9 9, 5 9, 5 5))" }, null))
          .isFalse();
    }

    @Test
    void nullFirstArg_sql_returnsNull() throws Exception {
      TestHelper.executeInNewDatabase("GeoDatabase", (db) -> {
        final ResultSet r = db.query("sql",
            "select geo.intersects(null, 'POLYGON ((0 0, 10 0, 10 10, 0 10, 0 0))') as v");
        assertThat(r.next().getProperty("v")).isNull();
      });
    }

    @Test
    void nullSecondArg_execute_returnsNull() {
      assertThat(new SQLFunctionGeoIntersects()
          .execute(null, null, null, new Object[] { POLYGON_0_10, null }, null)).isNull();
    }
  }

  // ─── geo.contains ─────────────────────────────────────────────────────────────

  @Nested
  class Contains {

    @Test
    void sql_polygonContainsPoint_returnsTrue() throws Exception {
      TestHelper.executeInNewDatabase("GeoDatabase", (db) -> {
        final ResultSet r = db.query("sql",
            "select geo.contains('POLYGON ((0 0, 10 0, 10 10, 0 10, 0 0))', 'POINT (5 5)') as v");
        assertThat((Boolean) r.next().getProperty("v")).isTrue();
      });
    }

    @Test
    void sql_polygonDoesNotContainOutsidePoint_returnsFalse() throws Exception {
      TestHelper.executeInNewDatabase("GeoDatabase", (db) -> {
        final ResultSet r = db.query("sql",
            "select geo.contains('POLYGON ((0 0, 10 0, 10 10, 0 10, 0 0))', 'POINT (15 15)') as v");
        assertThat((Boolean) r.next().getProperty("v")).isFalse();
      });
    }

    @Test
    void javaExecute_contains_returnsTrue() {
      assertThat((Boolean) new SQLFunctionGeoContains()
          .execute(null, null, null, new Object[] { POLYGON_0_10, POINT_INSIDE }, null)).isTrue();
    }

    @Test
    void javaExecute_doesNotContain_returnsFalse() {
      assertThat((Boolean) new SQLFunctionGeoContains()
          .execute(null, null, null, new Object[] { POLYGON_0_10, POINT_OUTSIDE }, null)).isFalse();
    }

    @Test
    void nullFirstArg_sql_returnsNull() throws Exception {
      TestHelper.executeInNewDatabase("GeoDatabase", (db) -> {
        final ResultSet r = db.query("sql",
            "select geo.contains(null, 'POINT (5 5)') as v");
        assertThat(r.next().getProperty("v")).isNull();
      });
    }

    @Test
    void nullSecondArg_execute_returnsNull() {
      assertThat(new SQLFunctionGeoContains()
          .execute(null, null, null, new Object[] { POLYGON_0_10, null }, null)).isNull();
    }
  }

  // ─── geo.dWithin ──────────────────────────────────────────────────────────────

  @Nested
  class DWithin {

    @Test
    void sql_nearbyPoints_returnsTrue() throws Exception {
      TestHelper.executeInNewDatabase("GeoDatabase", (db) -> {
        final ResultSet r = db.query("sql",
            "select geo.dWithin('POINT (0 0)', 'POINT (1 1)', 2.0) as v");
        assertThat((Boolean) r.next().getProperty("v")).isTrue();
      });
    }

    @Test
    void sql_farPoints_returnsFalse() throws Exception {
      TestHelper.executeInNewDatabase("GeoDatabase", (db) -> {
        final ResultSet r = db.query("sql",
            "select geo.dWithin('POINT (0 0)', 'POINT (10 10)', 1.0) as v");
        assertThat((Boolean) r.next().getProperty("v")).isFalse();
      });
    }

    @Test
    void javaExecute_nearby_returnsTrue() {
      assertThat((Boolean) new SQLFunctionGeoDWithin()
          .execute(null, null, null, new Object[] { "POINT (0 0)", "POINT (1 1)", 2.0 }, null)).isTrue();
    }

    @Test
    void javaExecute_farAway_returnsFalse() {
      assertThat((Boolean) new SQLFunctionGeoDWithin()
          .execute(null, null, null, new Object[] { "POINT (0 0)", "POINT (10 10)", 1.0 }, null)).isFalse();
    }

    @Test
    void nullFirstArg_sql_returnsNull() throws Exception {
      TestHelper.executeInNewDatabase("GeoDatabase", (db) -> {
        final ResultSet r = db.query("sql",
            "select geo.dWithin(null, 'POINT (1 1)', 2.0) as v");
        assertThat(r.next().getProperty("v")).isNull();
      });
    }

    @Test
    void nullThirdArg_execute_returnsNull() {
      assertThat(new SQLFunctionGeoDWithin()
          .execute(null, null, null, new Object[] { "POINT (0 0)", "POINT (1 1)", null }, null)).isNull();
    }

    @Test
    void negativeDistance_execute_returnsFalse() {
      // No real distance is <= a negative threshold
      assertThat((Boolean) new SQLFunctionGeoDWithin()
          .execute(null, null, null, new Object[] { "POINT (0 0)", "POINT (1 0)", -1.0 }, null)).isFalse();
    }
  }

  // ─── geo.disjoint ─────────────────────────────────────────────────────────────

  @Nested
  class Disjoint {

    @Test
    void sql_farApartShapes_returnsTrue() throws Exception {
      TestHelper.executeInNewDatabase("GeoDatabase", (db) -> {
        final ResultSet r = db.query("sql",
            "select geo.disjoint('POINT (50 50)', 'POLYGON ((0 0, 10 0, 10 10, 0 10, 0 0))') as v");
        assertThat((Boolean) r.next().getProperty("v")).isTrue();
      });
    }

    @Test
    void sql_intersectingShapes_returnsFalse() throws Exception {
      TestHelper.executeInNewDatabase("GeoDatabase", (db) -> {
        final ResultSet r = db.query("sql",
            "select geo.disjoint('POINT (5 5)', 'POLYGON ((0 0, 10 0, 10 10, 0 10, 0 0))') as v");
        assertThat((Boolean) r.next().getProperty("v")).isFalse();
      });
    }

    @Test
    void javaExecute_disjoint_returnsTrue() {
      assertThat((Boolean) new SQLFunctionGeoDisjoint()
          .execute(null, null, null, new Object[] { "POINT (50 50)", POLYGON_0_10 }, null)).isTrue();
    }

    @Test
    void javaExecute_intersecting_returnsFalse() {
      assertThat((Boolean) new SQLFunctionGeoDisjoint()
          .execute(null, null, null, new Object[] { POINT_INSIDE, POLYGON_0_10 }, null)).isFalse();
    }

    @Test
    void nullFirstArg_sql_returnsNull() throws Exception {
      TestHelper.executeInNewDatabase("GeoDatabase", (db) -> {
        final ResultSet r = db.query("sql",
            "select geo.disjoint(null, 'POLYGON ((0 0, 10 0, 10 10, 0 10, 0 0))') as v");
        assertThat(r.next().getProperty("v")).isNull();
      });
    }

    @Test
    void nullSecondArg_execute_returnsNull() {
      assertThat(new SQLFunctionGeoDisjoint()
          .execute(null, null, null, new Object[] { POINT_INSIDE, null }, null)).isNull();
    }
  }

  // ─── geo.equals ───────────────────────────────────────────────────────────────

  @Nested
  class Equals {

    @Test
    void sql_identicalPoints_returnsTrue() throws Exception {
      TestHelper.executeInNewDatabase("GeoDatabase", (db) -> {
        final ResultSet r = db.query("sql",
            "select geo.equals('POINT (5 5)', 'POINT (5 5)') as v");
        assertThat((Boolean) r.next().getProperty("v")).isTrue();
      });
    }

    @Test
    void sql_differentPoints_returnsFalse() throws Exception {
      TestHelper.executeInNewDatabase("GeoDatabase", (db) -> {
        final ResultSet r = db.query("sql",
            "select geo.equals('POINT (5 5)', 'POINT (6 6)') as v");
        assertThat((Boolean) r.next().getProperty("v")).isFalse();
      });
    }

    @Test
    void javaExecute_identicalPoints_returnsTrue() {
      assertThat((Boolean) new SQLFunctionGeoEquals()
          .execute(null, null, null, new Object[] { "POINT (5 5)", "POINT (5 5)" }, null)).isTrue();
    }

    @Test
    void javaExecute_differentPoints_returnsFalse() {
      assertThat((Boolean) new SQLFunctionGeoEquals()
          .execute(null, null, null, new Object[] { "POINT (5 5)", "POINT (6 6)" }, null)).isFalse();
    }

    @Test
    void nullFirstArg_sql_returnsNull() throws Exception {
      TestHelper.executeInNewDatabase("GeoDatabase", (db) -> {
        final ResultSet r = db.query("sql",
            "select geo.equals(null, 'POINT (5 5)') as v");
        assertThat(r.next().getProperty("v")).isNull();
      });
    }

    @Test
    void nullSecondArg_execute_returnsNull() {
      assertThat(new SQLFunctionGeoEquals()
          .execute(null, null, null, new Object[] { "POINT (5 5)", null }, null)).isNull();
    }
  }

  // ─── geo.crosses ──────────────────────────────────────────────────────────────

  @Nested
  class Crosses {

    @Test
    void sql_lineCrossesPolygon_returnsTrue() throws Exception {
      TestHelper.executeInNewDatabase("GeoDatabase", (db) -> {
        final ResultSet r = db.query("sql",
            "select geo.crosses('LINESTRING (-1 5, 11 5)', 'POLYGON ((0 0, 10 0, 10 10, 0 10, 0 0))') as v");
        assertThat((Boolean) r.next().getProperty("v")).isTrue();
      });
    }

    @Test
    void javaExecute_lineCrossesPolygon_returnsTrue() {
      assertThat((Boolean) new SQLFunctionGeoCrosses()
          .execute(null, null, null,
              new Object[] { "LINESTRING (-1 5, 11 5)", POLYGON_0_10 }, null)).isTrue();
    }

    @Test
    void javaExecute_overlappingPolygons_returnsFalse() {
      // Two polygons of the same dimension cannot "cross" by the JTS DE-9IM definition
      assertThat((Boolean) new SQLFunctionGeoCrosses()
          .execute(null, null, null,
              new Object[] { "POLYGON ((0 0, 6 0, 6 6, 0 6, 0 0))", "POLYGON ((3 3, 9 3, 9 9, 3 9, 3 3))" }, null))
          .isFalse();
    }

    @Test
    void nullFirstArg_sql_returnsNull() throws Exception {
      TestHelper.executeInNewDatabase("GeoDatabase", (db) -> {
        final ResultSet r = db.query("sql",
            "select geo.crosses(null, 'POLYGON ((0 0, 10 0, 10 10, 0 10, 0 0))') as v");
        assertThat(r.next().getProperty("v")).isNull();
      });
    }

    @Test
    void nullSecondArg_execute_returnsNull() {
      assertThat(new SQLFunctionGeoCrosses()
          .execute(null, null, null, new Object[] { "LINESTRING (-1 5, 11 5)", null }, null)).isNull();
    }
  }

  // ─── geo.overlaps ─────────────────────────────────────────────────────────────

  @Nested
  class Overlaps {

    @Test
    void sql_partiallyOverlapping_returnsTrue() throws Exception {
      TestHelper.executeInNewDatabase("GeoDatabase", (db) -> {
        final ResultSet r = db.query("sql",
            "select geo.overlaps('POLYGON ((0 0, 6 0, 6 6, 0 6, 0 0))', 'POLYGON ((3 3, 9 3, 9 9, 3 9, 3 3))') as v");
        assertThat((Boolean) r.next().getProperty("v")).isTrue();
      });
    }

    @Test
    void sql_disjointPolygons_returnsFalse() throws Exception {
      TestHelper.executeInNewDatabase("GeoDatabase", (db) -> {
        final ResultSet r = db.query("sql",
            "select geo.overlaps('POLYGON ((0 0, 2 0, 2 2, 0 2, 0 0))', 'POLYGON ((5 5, 9 5, 9 9, 5 9, 5 5))') as v");
        assertThat((Boolean) r.next().getProperty("v")).isFalse();
      });
    }

    @Test
    void javaExecute_overlapping_returnsTrue() {
      assertThat((Boolean) new SQLFunctionGeoOverlaps()
          .execute(null, null, null,
              new Object[] { "POLYGON ((0 0, 6 0, 6 6, 0 6, 0 0))", "POLYGON ((3 3, 9 3, 9 9, 3 9, 3 3))" }, null))
          .isTrue();
    }

    @Test
    void javaExecute_disjoint_returnsFalse() {
      assertThat((Boolean) new SQLFunctionGeoOverlaps()
          .execute(null, null, null,
              new Object[] { "POLYGON ((0 0, 2 0, 2 2, 0 2, 0 0))", "POLYGON ((5 5, 9 5, 9 9, 5 9, 5 5))" }, null))
          .isFalse();
    }

    @Test
    void nullFirstArg_sql_returnsNull() throws Exception {
      TestHelper.executeInNewDatabase("GeoDatabase", (db) -> {
        final ResultSet r = db.query("sql",
            "select geo.overlaps(null, 'POLYGON ((0 0, 10 0, 10 10, 0 10, 0 0))') as v");
        assertThat(r.next().getProperty("v")).isNull();
      });
    }

    @Test
    void nullSecondArg_execute_returnsNull() {
      assertThat(new SQLFunctionGeoOverlaps()
          .execute(null, null, null, new Object[] { POLYGON_0_10, null }, null)).isNull();
    }
  }

  // ─── geo.touches ──────────────────────────────────────────────────────────────

  @Nested
  class Touches {

    @Test
    void sql_adjacentPolygons_returnsTrue() throws Exception {
      TestHelper.executeInNewDatabase("GeoDatabase", (db) -> {
        final ResultSet r = db.query("sql",
            "select geo.touches('POLYGON ((0 0, 5 0, 5 5, 0 5, 0 0))', 'POLYGON ((5 0, 10 0, 10 5, 5 5, 5 0))') as v");
        assertThat((Boolean) r.next().getProperty("v")).isTrue();
      });
    }

    @Test
    void sql_disjointPolygons_returnsFalse() throws Exception {
      TestHelper.executeInNewDatabase("GeoDatabase", (db) -> {
        final ResultSet r = db.query("sql",
            "select geo.touches('POLYGON ((0 0, 2 0, 2 2, 0 2, 0 0))', 'POLYGON ((5 5, 9 5, 9 9, 5 9, 5 5))') as v");
        assertThat((Boolean) r.next().getProperty("v")).isFalse();
      });
    }

    @Test
    void javaExecute_adjacent_returnsTrue() {
      assertThat((Boolean) new SQLFunctionGeoTouches()
          .execute(null, null, null,
              new Object[] { "POLYGON ((0 0, 5 0, 5 5, 0 5, 0 0))", "POLYGON ((5 0, 10 0, 10 5, 5 5, 5 0))" }, null))
          .isTrue();
    }

    @Test
    void javaExecute_disjoint_returnsFalse() {
      assertThat((Boolean) new SQLFunctionGeoTouches()
          .execute(null, null, null,
              new Object[] { "POLYGON ((0 0, 2 0, 2 2, 0 2, 0 0))", "POLYGON ((5 5, 9 5, 9 9, 5 9, 5 5))" }, null))
          .isFalse();
    }

    @Test
    void nullFirstArg_sql_returnsNull() throws Exception {
      TestHelper.executeInNewDatabase("GeoDatabase", (db) -> {
        final ResultSet r = db.query("sql",
            "select geo.touches(null, 'POLYGON ((0 0, 10 0, 10 10, 0 10, 0 0))') as v");
        assertThat(r.next().getProperty("v")).isNull();
      });
    }

    @Test
    void nullSecondArg_execute_returnsNull() {
      assertThat(new SQLFunctionGeoTouches()
          .execute(null, null, null, new Object[] { POLYGON_0_10, null }, null)).isNull();
    }
  }
}
```

**Step 2: Compile**

```
mvn test-compile -pl engine -q
```
Expected: BUILD SUCCESS

**Step 3: Run**

```
mvn test -pl engine -Dtest=GeoPredicateFunctionsTest -q
```
Expected: all tests pass

**Step 4: Commit**

```bash
git add engine/src/test/java/com/arcadedb/function/sql/geo/GeoPredicateFunctionsTest.java
git commit -m "test(geo): add GeoPredicateFunctionsTest with SQL, execute(), and error paths"
```

---

### Task 6: Delete `SQLGeoFunctionsTest.java` and verify

**Files:**
- Delete: `engine/src/test/java/com/arcadedb/function/sql/geo/SQLGeoFunctionsTest.java`

**Step 1: Delete the old file**

```bash
git rm engine/src/test/java/com/arcadedb/function/sql/geo/SQLGeoFunctionsTest.java
```

**Step 2: Compile to confirm no references remain**

```
mvn test-compile -pl engine -q
```
Expected: BUILD SUCCESS

**Step 3: Run all new geo tests together**

```
mvn test -pl engine -Dtest="GeoHashIndexTest,GeoConstructionFunctionsTest,GeoMeasurementFunctionsTest,GeoConversionFunctionsTest,GeoPredicateFunctionsTest" -q
```
Expected: all tests pass, 0 failures

**Step 4: Final commit**

```bash
git add -u
git commit -m "test(geo): remove SQLGeoFunctionsTest — superseded by 4 focused test classes"
```
