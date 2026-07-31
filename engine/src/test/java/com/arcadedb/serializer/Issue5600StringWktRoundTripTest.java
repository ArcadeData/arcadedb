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
package com.arcadedb.serializer;

import com.arcadedb.TestHelper;
import com.arcadedb.database.Document;
import com.arcadedb.database.MutableDocument;
import com.arcadedb.database.RID;
import com.arcadedb.query.sql.executor.Result;
import com.arcadedb.query.sql.executor.ResultSet;
import com.arcadedb.schema.Type;
import org.junit.jupiter.api.Test;
import org.locationtech.spatial4j.shape.Point;
import org.locationtech.spatial4j.shape.Shape;

import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicReference;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Issue #5600 (1): a value stored in a {@code STRING} property must read back as the very same {@code String}. The
 * deserializer used to sniff the first characters of every string it read and, when they looked like the head of a WKT
 * geometry, return a spatial4j {@code Shape} instead - changing the declared type of the value at the storage layer and
 * mangling any free text that happened to start with {@code POLYGON}, {@code POINT}, and friends.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class Issue5600StringWktRoundTripTest extends TestHelper {

  @Test
  void declaredStringPropertyKeepsWktText() {
    database.getSchema().createDocumentType("T").createProperty("location", Type.STRING);

    final AtomicReference<RID> rid = new AtomicReference<>();
    database.transaction(() -> {
      final MutableDocument doc = database.newDocument("T");
      doc.set("location", "POINT (10.0 45.0)");
      doc.save();
      rid.set(doc.getIdentity());
    });

    final Document reloaded = database.lookupByRID(rid.get(), true).asDocument();
    assertThat(reloaded.get("location")).isInstanceOf(String.class);
    assertThat(reloaded.getString("location")).isEqualTo("POINT (10.0 45.0)");
  }

  @Test
  void freeTextStartingWithAGeometryKeywordIsNotTouched() {
    database.getSchema().createDocumentType("Note").createProperty("description", Type.STRING);

    final String[] texts = { //
        "POLYGON shaped, see attached", //
        "POINTs of interest along the way", //
        "LINESTRING is a WKT keyword", //
        "CIRCLE the wagons", //
        "ENVELOPE (please open it)", //
        "BUFFER overflow report" };

    database.transaction(() -> {
      for (final String text : texts) {
        final MutableDocument doc = database.newDocument("Note");
        doc.set("description", text);
        doc.save();
      }
    });

    final ResultSet rs = database.query("sql", "SELECT description FROM Note ORDER BY @rid");
    int i = 0;
    while (rs.hasNext())
      assertThat(rs.next().<Object>getProperty("description")).isEqualTo(texts[i++]);
    assertThat(i).isEqualTo(texts.length);
  }

  @Test
  void wktStringSurvivesInsideListsAndMaps() {
    database.getSchema().createDocumentType("Container");

    database.transaction(() -> {
      final MutableDocument doc = database.newDocument("Container");
      doc.set("list", List.of("POINT (1 2)", "plain"));
      doc.set("map", Map.of("k", "POLYGON ((0 0, 1 0, 1 1, 0 0))"));
      doc.save();
    });

    final Result row = database.query("sql", "SELECT list, map FROM Container").next();
    assertThat(row.<List<Object>>getProperty("list")).containsExactly("POINT (1 2)", "plain");
    assertThat(row.<Map<String, Object>>getProperty("map").get("k")).isEqualTo("POLYGON ((0 0, 1 0, 1 1, 0 0))");
  }

  @Test
  void wktStringIsIndexableAndLookupableAsAString() {
    database.getSchema().createDocumentType("Indexed").createProperty("code", Type.STRING);
    database.command("sql", "CREATE INDEX ON Indexed (code) UNIQUE");

    database.transaction(() -> database.command("sql", "INSERT INTO Indexed SET code = 'POINT (3 4)'"));

    final ResultSet rs = database.query("sql", "SELECT code FROM Indexed WHERE code = 'POINT (3 4)'");
    assertThat(rs.hasNext()).isTrue();
    assertThat(rs.next().<String>getProperty("code")).isEqualTo("POINT (3 4)");
  }

  /**
   * A real spatial4j Shape has its own binary type and must keep coming back as a Shape.
   */
  @Test
  void shapeValueStillRoundTripsAsShape() {
    database.getSchema().createDocumentType("Geo");

    database.transaction(
        () -> database.command("sql", "INSERT INTO Geo SET position = geo.geomFromText('POINT (10.0 45.0)')"));

    final Object position = database.query("sql", "SELECT position FROM Geo").next().getProperty("position");
    assertThat(position).isInstanceOf(Shape.class);
    assertThat(((Point) position).getX()).isEqualTo(10.0);
    assertThat(((Point) position).getY()).isEqualTo(45.0);
  }

  /**
   * Geometry functions keep accepting a WKT string read back from a STRING property, so a pre-26.2.1 database - where
   * every shape was written as WKT text - keeps working without the deserializer converting anything.
   */
  @Test
  void geoFunctionsStillAcceptWktStoredAsText() {
    database.getSchema().createDocumentType("City").createProperty("coords", Type.STRING);

    database.transaction(() -> {
      database.command("sql", "INSERT INTO City SET name = 'Rome', coords = 'POINT (12.5 41.9)'");
      database.command("sql", "INSERT INTO City SET name = 'Milan', coords = 'POINT (9.2 45.5)'");
    });

    final ResultSet rs = database.query("sql",
        "SELECT name FROM City WHERE geo.within(coords, geo.geomFromText('POLYGON ((10 38, 16 38, 16 44, 10 44, 10 38))')) = true");
    assertThat(rs.next().<String>getProperty("name")).isEqualTo("Rome");
    assertThat(rs.hasNext()).isFalse();

    final ResultSet asText = database.query("sql", "SELECT geo.asText(coords) AS wkt FROM City WHERE name = 'Rome'");
    assertThat(asText.next().<String>getProperty("wkt")).contains("POINT");
  }

  /**
   * The two legacy SQL methods used to require an already-parsed Shape, which only the deserializer's sniff ever
   * produced from a WKT column. They now do the conversion themselves, like every geo.* function.
   */
  @Test
  void legacyShapeMethodsAcceptWktText() {
    database.getSchema().createDocumentType("Place").createProperty("coords", Type.STRING);

    database.transaction(() -> {
      database.command("sql", "INSERT INTO Place SET name = 'inside', coords = 'POINT (12.5 41.9)'");
      database.command("sql", "INSERT INTO Place SET name = 'outside', coords = 'POINT (9.2 45.5)'");
    });

    final String box = "'POLYGON ((10 38, 16 38, 16 44, 10 44, 10 38))'";

    final ResultSet within = database.query("sql",
        "SELECT name FROM Place WHERE coords.isWithin(geo.geomFromText(" + box + ")) = true");
    assertThat(within.next().<String>getProperty("name")).isEqualTo("inside");
    assertThat(within.hasNext()).isFalse();

    // The parameter may be WKT text too, not only a Shape
    final ResultSet intersects = database.query("sql",
        "SELECT name FROM Place WHERE coords.intersectsWith(" + box + ") = true");
    assertThat(intersects.next().<String>getProperty("name")).isEqualTo("inside");
    assertThat(intersects.hasNext()).isFalse();
  }

  /**
   * A malformed geometry written in the QUERY is a mistake to report, while a row that simply does not hold a geometry
   * is filtered out: failing the whole query over one bad row would be worse than skipping it.
   */
  @Test
  void aMalformedParameterIsReportedButABadRowIsJustSkipped() {
    database.getSchema().createDocumentType("Mixed").createProperty("coords", Type.STRING);

    database.transaction(() -> {
      database.command("sql", "INSERT INTO Mixed SET name = 'good', coords = 'POINT (12.5 41.9)'");
      database.command("sql", "INSERT INTO Mixed SET name = 'junk', coords = 'not a geometry at all'");
    });

    // The bad row does not match; the good one still does, and the query completes
    final ResultSet rs = database.query("sql",
        "SELECT name FROM Mixed WHERE coords.intersectsWith('POLYGON ((10 38, 16 38, 16 44, 10 44, 10 38))') = true");
    assertThat(rs.next().<String>getProperty("name")).isEqualTo("good");
    assertThat(rs.hasNext()).isFalse();

    // A typo in the literal is surfaced rather than silently answering "no match"
    assertThatThrownBy(() -> database.query("sql", "SELECT name FROM Mixed WHERE coords.isWithin('POLYGONN ((0 0))') = true")
        .hasNext()).hasMessageContaining("POLYGONN");
  }

  /**
   * The remaining operand shapes of the two relation methods: no parameter at all is a query mistake, while a null or
   * empty operand has nothing to relate and simply does not match.
   */
  @Test
  void relationMethodsHandleTheDegenerateOperands() {
    database.getSchema().createDocumentType("Deg").createProperty("coords", Type.STRING);

    database.transaction(() -> {
      database.command("sql", "INSERT INTO Deg SET name = 'here', coords = 'POINT (12.5 41.9)'");
      database.command("sql", "INSERT INTO Deg SET name = 'nowhere'");
    });

    // A missing parameter is a mistake in the query, the same way it always was
    assertThatThrownBy(() -> database.query("sql", "SELECT name FROM Deg WHERE coords.isWithin() = true").hasNext())
        .hasMessageContaining("requires a shape as parameter");
    assertThatThrownBy(() -> database.query("sql", "SELECT name FROM Deg WHERE coords.intersectsWith(null) = true")
        .hasNext()).hasMessageContaining("requires a shape as parameter");

    // An empty operand has no geometry to relate: no match, no error. The row without coords is skipped the same way.
    final ResultSet empty = database.query("sql", "SELECT name FROM Deg WHERE coords.isWithin('') = true");
    assertThat(empty.hasNext()).isFalse();

    final ResultSet present = database.query("sql",
        "SELECT name FROM Deg WHERE coords.intersectsWith('POLYGON ((10 38, 16 38, 16 44, 10 44, 10 38))') = true");
    assertThat(present.next().<String>getProperty("name")).isEqualTo("here");
    assertThat(present.hasNext()).isFalse();
  }
}
