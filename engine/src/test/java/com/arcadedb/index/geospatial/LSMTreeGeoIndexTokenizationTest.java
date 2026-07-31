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
import com.arcadedb.function.sql.geo.GeoUtils;
import com.arcadedb.index.Index;
import com.arcadedb.index.IndexCursor;
import com.arcadedb.index.IndexInternal;
import com.arcadedb.index.TypeIndex;
import com.arcadedb.log.LogManager;
import com.arcadedb.log.Logger;
import com.arcadedb.query.sql.executor.Result;
import com.arcadedb.query.sql.executor.ResultSet;
import com.arcadedb.schema.GeoIndexMetadata;
import com.arcadedb.schema.LocalSchema;
import com.arcadedb.schema.Schema;
import com.arcadedb.schema.TypeIndexBuilder;
import com.arcadedb.serializer.json.JSONObject;
import org.junit.jupiter.api.Test;
import org.locationtech.spatial4j.shape.Shape;

import java.io.File;
import java.nio.file.Files;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.logging.Level;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Regression tests for issue #5478: a GEOSPATIAL index wrote one entry per GeoHash tree level
 * ({@code precision} entries per point), which multiplied the ingest, WAL and compaction cost of a
 * bulk load by the precision and concentrated a posting per record on the handful of continent-sized
 * cells at the top of the tree.
 * <p>
 * The FRONTIER tokenization writes only the deepest cells of the shape decomposition (exactly one for
 * a point) and answers queries with a GeoHash prefix range scan. The legacy FULL tokenization is kept
 * for indexes created before the change and is covered here too, together with the cross-layout
 * equivalence of query results.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class LSMTreeGeoIndexTokenizationTest extends TestHelper {

  private static final String[] CITIES = { //
      "Rome:POINT (12.5 41.9)", //
      "Milan:POINT (9.2 45.5)", //
      "Naples:POINT (14.3 40.8)", //
      "Palermo:POINT (13.4 38.1)", //
      "Turin:POINT (7.7 45.1)" };

  /**
   * The whole point of the fix: a point costs ONE index entry, not {@code precision} of them.
   */
  @Test
  void pointCostsOneEntryPerRecord() {
    createType("Loc");
    database.command("sql", "CREATE INDEX ON Loc (coords) GEOSPATIAL");
    insertCities("Loc");

    assertThat(geoIndex("Loc").getTokenization()).isEqualTo(GeoIndexMetadata.TOKENIZATION.FRONTIER);
    assertThat(database.getSchema().getIndexByName("Loc[coords]").countEntries()).isEqualTo(CITIES.length);
  }

  /**
   * The legacy layout is still selectable and still writes the whole ancestor chain, so an index
   * created before the fix keeps behaving exactly as it did.
   */
  @Test
  void legacyTokenizationStillWritesTheWholeChain() {
    createType("LocLegacy");
    createLegacyIndex("LocLegacy");
    insertCities("LocLegacy");

    assertThat(geoIndex("LocLegacy").getTokenization()).isEqualTo(GeoIndexMetadata.TOKENIZATION.FULL);
    assertThat(database.getSchema().getIndexByName("LocLegacy[coords]").countEntries()) //
        .isEqualTo((long) CITIES.length * GeoIndexMetadata.DEFAULT_PRECISION);
  }

  /**
   * Both layouts must return the same rows for the same query, through the SQL predicate that
   * post-filters the index superset.
   */
  @Test
  void bothLayoutsReturnTheSameRows() {
    createType("LocA");
    database.command("sql", "CREATE INDEX ON LocA (coords) GEOSPATIAL");
    insertCities("LocA");

    createType("LocB");
    createLegacyIndex("LocB");
    insertCities("LocB");

    final String box = "geo.geomFromText('POLYGON ((10 38, 16 38, 16 44, 10 44, 10 38))')";
    assertThat(queryNames("LocA", box)).containsExactlyInAnyOrder("Rome", "Naples", "Palermo");
    assertThat(queryNames("LocB", box)).containsExactlyInAnyOrder("Rome", "Naples", "Palermo");
  }

  /**
   * The index is allowed to return a superset (the SQL layer post-filters it), but the FRONTIER
   * layout must not degenerate into "every record under the level-1 cell", which is what the FULL
   * layout does: a query far from the data must not drag the whole dataset out of the index.
   */
  @Test
  void frontierLayoutDoesNotReturnTheWholeContinent() {
    createType("LocSel");
    database.command("sql", "CREATE INDEX ON LocSel (coords) GEOSPATIAL");
    insertCities("LocSel");

    // A box around Palermo only
    final Shape sicily = GeoUtils.getSpatialContext().getShapeFactory().rect(12.9, 13.9, 37.6, 38.6);
    assertThat(lookup("LocSel", sicily)).hasSize(1);

    createType("LocSelLegacy");
    createLegacyIndex("LocSelLegacy");
    insertCities("LocSelLegacy");

    // The legacy layout answers the same query with every record sharing a top-level GeoHash cell
    assertThat(lookup("LocSelLegacy", sicily).size()).isGreaterThan(1);
  }

  /**
   * A non-point shape decomposes into many cells at the resolved detail level. Those are indexed as
   * the frontier of the decomposition and must be found by a query cell that is COARSER than them
   * (prefix range scan) as well as by one that is FINER (ancestor lookup).
   */
  @Test
  void polygonIsFoundByCoarserAndFinerQueries() throws Exception {
    createType("Area");
    database.command("sql", "CREATE INDEX ON Area (coords) GEOSPATIAL");

    database.transaction(() -> database.command("sql",
        "INSERT INTO Area SET name = 'lazio', coords = 'POLYGON ((11.5 41.5, 13.0 41.5, 13.0 42.5, 11.5 42.5, 11.5 41.5))'"));

    // Coarser than the indexed cells: a box covering central Italy
    final Shape coarse = GeoUtils.getSpatialContext().getShapeFactory().rect(10.0, 15.0, 40.0, 44.0);
    assertThat(lookup("Area", coarse)).hasSize(1);

    // Finer than the indexed cells: a single point well inside the polygon
    final Shape fine = GeoUtils.getSpatialContext().getFormats().getWktReader().read("POINT (12.2 42.0)");
    assertThat(lookup("Area", fine)).hasSize(1);

    // Outside: must not be found at all
    final Shape elsewhere = GeoUtils.getSpatialContext().getShapeFactory().rect(20.0, 21.0, 20.0, 21.0);
    assertThat(lookup("Area", elsewhere)).isEmpty();
  }

  /**
   * put() and remove() must tokenize identically, or a delete leaves entries behind that still resolve to a RID that no
   * longer exists. Asserted on BOTH what the index answers and {@code countEntries()}: the latter used to count
   * tombstones as live entries on every LSM index, geospatial or not, and became a usable oracle with #5601.
   */
  @Test
  void deleteRemovesEveryEntryOfTheRecord() {
    assertDeleteLeavesNothingBehind("LocDel", false);
  }

  /**
   * Same, for an AREA shape: that is the one the cell pruning of #5600 changes, and a pruned parent has to be written
   * and removed alike or a delete would leave the parent cell pointing at a gone record.
   */
  @Test
  void polygonDeleteRemovesEveryEntryOfTheRecord() {
    createType("AreaDel");
    database.command("sql", "CREATE INDEX ON AreaDel (coords) GEOSPATIAL");

    final String[] areas = { //
        "north:POLYGON ((8.0 44.0, 12.0 44.0, 12.0 46.0, 8.0 46.0, 8.0 44.0))", //
        "centre:POLYGON ((11.5 41.5, 13.0 41.5, 13.0 42.5, 11.5 42.5, 11.5 41.5))", //
        "south:POLYGON ((14.0 38.0, 16.0 38.0, 16.0 40.0, 14.0 40.0, 14.0 38.0))" };

    database.transaction(() -> {
      for (final String area : areas) {
        final String[] parts = area.split(":", 2);
        database.command("sql", "INSERT INTO AreaDel SET name = '" + parts[0] + "', coords = '" + parts[1] + "'");
      }
    });

    final Shape italy = GeoUtils.getSpatialContext().getShapeFactory().rect(6.0, 18.0, 36.0, 47.0);
    assertThat(lookup("AreaDel", italy)).hasSize(areas.length);

    for (int i = 0; i < areas.length; i++) {
      final String name = areas[i].split(":", 2)[0];
      database.transaction(() -> database.command("sql", "DELETE FROM AreaDel WHERE name = '" + name + "'"));
      assertThat(lookup("AreaDel", italy)).hasSize(areas.length - i - 1);
    }
  }

  /**
   * A jagged outline is where the cell pruning of #5600 bites hardest (measured at ~74% fewer entries): the collapsed
   * parents must still answer a query at any resolution, and must not start matching a point far outside the shape.
   */
  @Test
  void prunedJaggedPolygonIsStillFoundAtEveryResolution() throws Exception {
    createType("Jagged");
    database.command("sql", "CREATE INDEX ON Jagged (coords) GEOSPATIAL");

    final StringBuilder wkt = new StringBuilder("POLYGON ((");
    for (int i = 0; i <= 100; i++)
      wkt.append(12.0 + i * 0.01).append(' ').append(41.0 + (i % 2 == 0 ? 0.0 : 0.004)).append(", ");
    wkt.append("13.0 41.5, 12.0 41.5, 12.0 41.0))");

    database.transaction(
        () -> database.command("sql", "INSERT INTO Jagged SET name = 'coast', coords = '" + wkt + "'"));

    // Coarser than the indexed cells
    assertThat(lookup("Jagged", GeoUtils.getSpatialContext().getShapeFactory().rect(10.0, 15.0, 40.0, 44.0))).hasSize(1);
    // A point well inside the outline
    assertThat(lookup("Jagged", GeoUtils.getSpatialContext().getFormats().getWktReader().read("POINT (12.5 41.3)")))
        .hasSize(1);
    // Far away: the enlarged cover must not reach here
    assertThat(lookup("Jagged", GeoUtils.getSpatialContext().getShapeFactory().rect(20.0, 21.0, 20.0, 21.0))).isEmpty();
  }

  /**
   * Same, on the legacy layout: it must keep removing the whole chain it wrote.
   */
  @Test
  void legacyDeleteRemovesEveryEntryOfTheRecord() {
    assertDeleteLeavesNothingBehind("LocDelLegacy", true);
  }

  private void assertDeleteLeavesNothingBehind(final String typeName, final boolean legacy) {
    createType(typeName);
    if (legacy)
      createLegacyIndex(typeName);
    else
      database.command("sql", "CREATE INDEX ON " + typeName + " (coords) GEOSPATIAL");
    insertCities(typeName);

    // Every city sits inside this box
    final Shape italy = GeoUtils.getSpatialContext().getShapeFactory().rect(6.0, 16.0, 36.0, 47.0);
    assertThat(lookup(typeName, italy)).hasSize(CITIES.length);

    for (int i = 0; i < CITIES.length; i++) {
      final String city = CITIES[i].split(":")[0];
      database.transaction(() -> database.command("sql", "DELETE FROM " + typeName + " WHERE name = '" + city + "'"));
      assertThat(lookup(typeName, italy)).hasSize(CITIES.length - i - 1);
    }

    // #5601: with every record gone the index must report no live entry, tombstones or not
    assertThat(database.getSchema().getIndexByName(typeName + "[coords]").countEntries()).isZero();
  }

  /**
   * The layout is a property of the persisted index: it must survive a reopen, both ways, or a
   * database written with one layout would be read with the other.
   */
  @Test
  void tokenizationSurvivesReopen() {
    createType("LocR");
    database.command("sql", "CREATE INDEX ON LocR (coords) GEOSPATIAL");
    insertCities("LocR");

    createType("LocRLegacy");
    createLegacyIndex("LocRLegacy");
    insertCities("LocRLegacy");

    reopenDatabase();

    assertThat(geoIndex("LocR").getTokenization()).isEqualTo(GeoIndexMetadata.TOKENIZATION.FRONTIER);
    assertThat(geoIndex("LocRLegacy").getTokenization()).isEqualTo(GeoIndexMetadata.TOKENIZATION.FULL);
    assertThat(database.getSchema().getIndexByName("LocR[coords]").countEntries()).isEqualTo(CITIES.length);
    assertThat(database.getSchema().getIndexByName("LocRLegacy[coords]").countEntries()) //
        .isEqualTo((long) CITIES.length * GeoIndexMetadata.DEFAULT_PRECISION);
  }

  /**
   * A schema written before the fix carries no tokenization field: it must load as FULL, never as
   * FRONTIER, or put/remove would stop matching what is already on disk.
   */
  @Test
  void schemaWithoutTokenizationLoadsAsLegacy() throws Exception {
    createType("LocOld");
    database.command("sql", "CREATE INDEX ON LocOld (coords) GEOSPATIAL");
    insertCities("LocOld");

    database.close();
    stripTokenizationFromSchema();
    database = factory.open();

    assertThat(geoIndex("LocOld").getTokenization()).isEqualTo(GeoIndexMetadata.TOKENIZATION.FULL);
  }

  /**
   * REBUILD INDEX is the upgrade path for an index created before the fix: it re-reads every record,
   * so it can safely publish the compact layout - and it must not lose the configured precision on
   * the way (the same defect fixed for FULL_TEXT in #4732).
   */
  @Test
  void rebuildUpgradesLegacyIndexAndKeepsPrecision() {
    createType("LocUp");
    createLegacyIndex("LocUp", 7);
    insertCities("LocUp");

    assertThat(database.getSchema().getIndexByName("LocUp[coords]").countEntries()).isEqualTo((long) CITIES.length * 7);

    database.command("sql", "REBUILD INDEX `LocUp[coords]`");

    assertThat(geoIndex("LocUp").getPrecision()).isEqualTo(7);
    assertThat(geoIndex("LocUp").getTokenization()).isEqualTo(GeoIndexMetadata.TOKENIZATION.FRONTIER);
    assertThat(database.getSchema().getIndexByName("LocUp[coords]").countEntries()).isEqualTo(CITIES.length);

    final String box = "geo.geomFromText('POLYGON ((10 38, 16 38, 16 44, 10 44, 10 38))')";
    assertThat(queryNames("LocUp", box)).containsExactlyInAnyOrder("Rome", "Naples", "Palermo");
  }

  /**
   * A POINT search shape is the case the index used to answer with nothing at all: every cell of a point's covering has
   * a null {@code shapeRel}, and the lookup loop skipped those. The two SQL predicates that query with a point
   * ({@code geo.equals}, {@code geo.contains}) had to be documented as index-less because of it. The walk no longer
   * filters on {@code shapeRel} - {@code getTreeCellIterator} only ever yields cells that DO intersect the shape - so
   * both layouts answer a point query correctly now.
   */
  @Test
  void pointSearchShapeIsAnsweredByTheIndex() throws Exception {
    createType("LocPt");
    database.command("sql", "CREATE INDEX ON LocPt (coords) GEOSPATIAL");
    insertCities("LocPt");

    createType("LocPtLegacy");
    createLegacyIndex("LocPtLegacy");
    insertCities("LocPtLegacy");

    final Shape rome = GeoUtils.getSpatialContext().getFormats().getWktReader().read("POINT (12.5 41.9)");
    assertThat(lookup("LocPt", rome)).isNotEmpty();
    assertThat(lookup("LocPtLegacy", rome)).isNotEmpty();

    for (final String typeName : new String[] { "LocPt", "LocPtLegacy" }) {
      final ResultSet rs = database.query("sql",
          "SELECT name FROM " + typeName + " WHERE geo.equals(coords, geo.geomFromText('POINT (12.5 41.9)')) = true");
      final List<String> names = new ArrayList<>();
      while (rs.hasNext())
        names.add(rs.next().getProperty("name"));
      assertThat(names).as(typeName).containsExactly("Rome");
    }
  }

  /**
   * An operator has no other way to learn that a geospatial index is on the old layout, so opening the database says
   * so - once per logical index, not once per bucket sub-index and not again on every schema reload.
   */
  @Test
  void openingADatabaseWarnsOnceAboutALegacyIndex() {
    createType("LocWarn");
    createLegacyIndex("LocWarn");
    insertCities("LocWarn");
    database.close();

    final List<String> warnings = new ArrayList<>();
    final Logger previous = LogManager.instance().getLogger();
    try {
      LogManager.instance().setLogger(new CollectingLogger(warnings));
      database = factory.open();
      // A DDL statement re-reads the schema: the advice must not be repeated
      database.command("sql", "CREATE DOCUMENT TYPE Unrelated");
    } finally {
      LogManager.instance().setLogger(previous);
    }

    assertThat(warnings).hasSize(1);
    assertThat(warnings.getFirst()).contains("LocWarn[coords]").contains("REBUILD INDEX").contains("#5478");
  }

  /**
   * The same advice reaches Studio, which renders it from {@code schema:indexes}: the reason on every affected bucket
   * sub-index, and the type index name to rebuild. A current index carries neither.
   */
  @Test
  void schemaIndexesExposesTheUpgradeWarning() {
    createType("LocApi");
    createLegacyIndex("LocApi");

    createType("LocApiNew");
    database.command("sql", "CREATE INDEX ON LocApiNew (coords) GEOSPATIAL");

    int legacyRows = 0;
    final ResultSet rs = database.query("sql", "SELECT FROM schema:indexes");
    while (rs.hasNext()) {
      final Result row = rs.next();
      final String name = row.getProperty("name");
      if (name.startsWith("LocApi_")) {
        ++legacyRows;
        assertThat((String) row.getProperty("upgradeWarning")).contains("#5478");
        assertThat((String) row.getProperty("typeIndexName")).isEqualTo("LocApi[coords]");
      } else if (name.startsWith("LocApiNew_"))
        assertThat((String) row.getProperty("upgradeWarning")).isNull();
    }
    assertThat(legacyRows).isPositive();

    assertThat((String) database.query("sql", "SELECT FROM schema:index:`LocApi[coords]`").next()
        .getProperty("upgradeWarning")).contains("#5478");
    assertThat((String) database.query("sql", "SELECT FROM schema:index:`LocApiNew[coords]`").next()
        .getProperty("upgradeWarning")).isNull();
  }

  /**
   * A row limit must never reach a spatial index. Its results are a SUPERSET the geo.* predicate re-checks, so cutting
   * the candidates at N drops rows that would have survived the filter - and only on some queries, which is the worst
   * shape a bug can have. {@code get(keys, limit)} is a public {@code Index} method and {@code TypeIndex} forwards a
   * limit down to every bucket sub-index, so both layers have to ignore it.
   */
  @Test
  void aPositiveLimitIsIgnoredBecauseTheResultIsASuperset() {
    createType("LocLimit");
    database.command("sql", "CREATE INDEX ON LocLimit (coords) GEOSPATIAL");
    insertCities("LocLimit");

    final Index index = database.getSchema().getIndexByName("LocLimit[coords]");
    assertThat(((IndexInternal) index).isResultApproximate()).isTrue();

    // Every city is inside this box: asking for 2 must still hand back all 5 candidates
    final Shape italy = GeoUtils.getSpatialContext().getShapeFactory().rect(6.0, 16.0, 36.0, 47.0);
    assertThat(count(index.get(new Object[] { italy }, 2))).isEqualTo(CITIES.length);
    assertThat(count(index.get(new Object[] { italy }, -1))).isEqualTo(CITIES.length);

    // ...and the SQL LIMIT, applied after the predicate, still works
    final ResultSet rs = database.query("sql",
        "SELECT name FROM LocLimit WHERE geo.intersects(coords, geo.geomFromText('POLYGON ((10 38, 16 38, 16 44, 10 44, 10 38))')) = true LIMIT 2");
    final List<String> names = new ArrayList<>();
    while (rs.hasNext())
      names.add(rs.next().getProperty("name"));
    assertThat(names).hasSize(2);
    assertThat(List.of("Rome", "Naples", "Palermo")).containsAll(names);
  }

  // ---- helpers ----

  private int count(final IndexCursor cursor) {
    int total = 0;
    while (cursor.hasNext())
      if (cursor.next() != null)
        ++total;
    return total;
  }

  /** Minimal {@link Logger} that keeps the WARNING messages, with their arguments already substituted. */
  private record CollectingLogger(List<String> warnings) implements Logger {
    @Override
    public void log(final Object requester, final Level level, final String message, final Throwable exception,
        final String context, final Object arg1, final Object arg2, final Object arg3, final Object arg4, final Object arg5,
        final Object arg6, final Object arg7, final Object arg8, final Object arg9, final Object arg10, final Object arg11,
        final Object arg12, final Object arg13, final Object arg14, final Object arg15, final Object arg16,
        final Object arg17) {
      log(requester, level, message, exception, context,
          new Object[] { arg1, arg2, arg3, arg4, arg5, arg6, arg7, arg8, arg9, arg10, arg11, arg12, arg13, arg14, arg15,
              arg16, arg17 });
    }

    @Override
    public void log(final Object requester, final Level level, final String message, final Throwable exception,
        final String context, final Object... args) {
      if (level != Level.WARNING || message == null)
        return;
      final List<Object> bound = new ArrayList<>();
      if (args != null)
        for (final Object arg : args)
          if (arg != null)
            bound.add(arg);
      final String formatted = bound.isEmpty() ? message : String.format(message, bound.toArray());
      if (formatted.contains("should be rebuilt"))
        warnings.add(formatted);
    }

    @Override
    public void flush() {
    }
  }

  private void createType(final String typeName) {
    database.command("sql", "CREATE DOCUMENT TYPE " + typeName);
    database.command("sql", "CREATE PROPERTY " + typeName + ".name STRING");
    database.command("sql", "CREATE PROPERTY " + typeName + ".coords STRING");
  }

  private void createLegacyIndex(final String typeName) {
    createLegacyIndex(typeName, GeoIndexMetadata.DEFAULT_PRECISION);
  }

  private void createLegacyIndex(final String typeName, final int precision) {
    final TypeIndexBuilder builder = database.getSchema().buildTypeIndex(typeName, new String[] { "coords" });
    builder.withType(Schema.INDEX_TYPE.GEOSPATIAL);
    final GeoIndexMetadata meta = new GeoIndexMetadata(typeName, new String[] { "coords" }, -1);
    meta.setPrecision(precision);
    meta.setTokenization(GeoIndexMetadata.TOKENIZATION.FULL);
    builder.withMetadata(meta);
    builder.create();
  }

  private void insertCities(final String typeName) {
    database.transaction(() -> {
      for (final String city : CITIES) {
        final String[] parts = city.split(":");
        database.command("sql", "INSERT INTO " + typeName + " SET name = ?, coords = ?", parts[0], parts[1]);
      }
    });
  }

  private LSMTreeGeoIndex geoIndex(final String typeName) {
    return (LSMTreeGeoIndex) ((TypeIndex) database.getSchema().getIndexByName(typeName + "[coords]")).getSubIndexes().getFirst();
  }

  private Set<String> lookup(final String typeName, final Shape shape) {
    final Set<String> rids = new HashSet<>();
    for (final IndexCursor cursor = database.getSchema().getIndexByName(typeName + "[coords]").get(new Object[] { shape });
        cursor.hasNext(); )
      rids.add(cursor.next().getIdentity().toString());
    return rids;
  }

  /** Rewrites schema.json as a pre-#5478 database would have written it: no tokenization field at all. */
  private void stripTokenizationFromSchema() throws Exception {
    final File schemaFile = new File(getDatabasePath() + "/" + LocalSchema.SCHEMA_FILE_NAME);
    final JSONObject schema = new JSONObject(Files.readString(schemaFile.toPath()));
    final JSONObject types = schema.getJSONObject("types");
    for (final String typeName : types.keySet()) {
      final JSONObject indexes = types.getJSONObject(typeName).getJSONObject("indexes");
      if (indexes != null)
        for (final String indexName : indexes.keySet())
          indexes.getJSONObject(indexName).remove("tokenization");
    }
    Files.writeString(schemaFile.toPath(), schema.toString());
  }

  private List<String> queryNames(final String typeName, final String shapeExpression) {
    final List<String> names = new ArrayList<>();
    final ResultSet rs = database.query("sql",
        "SELECT name FROM " + typeName + " WHERE geo.intersects(coords, " + shapeExpression + ") = true");
    while (rs.hasNext())
      names.add(rs.next().getProperty("name"));
    return names;
  }
}
