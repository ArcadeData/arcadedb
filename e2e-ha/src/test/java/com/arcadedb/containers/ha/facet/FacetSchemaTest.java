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
package com.arcadedb.containers.ha.facet;

import com.arcadedb.GlobalConfiguration;
import com.arcadedb.database.Database;
import com.arcadedb.database.DatabaseComparator;
import com.arcadedb.database.DatabaseFactory;
import com.arcadedb.database.LocalDatabase;
import com.arcadedb.engine.ComponentFile;
import com.arcadedb.index.TypeIndex;
import com.arcadedb.schema.Schema;
import com.arcadedb.query.sql.executor.ResultSet;
import com.arcadedb.serializer.json.JSONArray;
import com.arcadedb.test.support.AssetGenerator;
import com.arcadedb.test.support.FacetSchema;
import com.arcadedb.test.support.FacetSnapshot;
import com.arcadedb.utility.FileUtils;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.File;
import java.io.IOException;
import java.time.Duration;
import java.util.Collection;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.Map;
import java.util.Random;
import java.util.Set;
import java.util.SortedMap;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.awaitility.Awaitility.await;

/**
 * Runs the exact facet statements of the HA tests against an embedded database and proves that the {@code FACETS} answer equals the
 * equivalent {@code GROUP BY}, so a mistake in a statement shows up here and not 20 minutes into a container run.
 */
class FacetSchemaTest {
  private static final String PATH   = "./target/facet-schema-test";
  private static final String PATH_B = "./target/facet-schema-test-b";

  private DatabaseFactory factory;
  private Database       database;

  @BeforeEach
  void setUp() {
    FileUtils.deleteRecursively(new File(PATH));
    FileUtils.deleteRecursively(new File(PATH_B));
    database = create(PATH);
  }

  @AfterEach
  void tearDown() {
    if (database != null && database.isOpen())
      database.drop();
    if (factory != null)
      factory.close();
    FileUtils.deleteRecursively(new File(PATH));
    FileUtils.deleteRecursively(new File(PATH_B));
  }

  private Database create(final String path) {
    factory = new DatabaseFactory(path);
    factory.getContextConfiguration().setValue(GlobalConfiguration.TYPE_DEFAULT_BUCKETS, 10);
    final Database db = factory.create();
    for (final String ddl : FacetSchema.DDL)
      db.command("sql", ddl);
    return db;
  }

  private void assertFacetsEqualGroupBy() {
    for (final String where : FacetSchema.CHECKED_FILTERS) {
      final SortedMap<String, Long> expected = expected(where);
      assertThat(facets(where).counts()).as("patched, where='%s'", where).isEqualTo(expected);
      await().atMost(Duration.ofSeconds(30)).until(() -> facets(where).settled());
      assertThat(facets(where).counts()).as("lag 0, where='%s'", where).isEqualTo(expected);
    }
  }

  private SortedMap<String, Long> expected(final String where) {
    try {
      return FacetSchema.expectedCounts(this::rows, where);
    } catch (final IOException e) {
      throw new IllegalStateException(e);
    }
  }

  private FacetSnapshot facets(final String where) {
    try (final ResultSet rs = database.query("sql", FacetSchema.facetQuery(where))) {
      while (rs.hasNext())
        rs.next();
      return FacetSnapshot.of(database, rs.getFacets());
    }
  }

  private JSONArray rows(final String sql) {
    final JSONArray rows = new JSONArray();
    try (final ResultSet rs = database.query("sql", sql)) {
      while (rs.hasNext())
        rows.put(rs.next().toJSON());
    }
    return rows;
  }

  private void run(final String statement, final Map<String, Object> params) {
    database.transaction(() -> database.command("sql", statement, params).close());
  }

  private void insert(final AssetGenerator generator, final long aid) {
    final Map<String, Object> params = generator.insertParams(aid);
    run(generator.omitBrand() ? FacetSchema.INSERT_NO_BRAND : FacetSchema.INSERT, params);
  }

  private long count(final String sql) {
    try (final ResultSet rs = database.query("sql", sql)) {
      return ((Number) rs.next().getProperty("c")).longValue();
    }
  }

  @Test
  void ddlIsIdempotent() {
    for (final String ddl : FacetSchema.DDL)
      database.command("sql", ddl);
    final Collection<TypeIndex> indexes = database.getSchema().getType(FacetSchema.TYPE).getAllIndexes(true);
    assertThat(indexes).hasSize(4);
    assertThat(indexes.stream().filter(i -> i.getType() == Schema.INDEX_TYPE.COLUMNAR).count()).isEqualTo(3L);
    assertThat(indexes.stream().filter(i -> i.getType() == Schema.INDEX_TYPE.LSM_TREE && i.isUnique()).count()).isEqualTo(1L);
    assertThat(database.getSchema().existsType("FacetLink")).isTrue();
  }

  @Test
  void generatorIsDeterministicAndDistributed() {
    final AssetGenerator a = new AssetGenerator(42L);
    final AssetGenerator b = new AssetGenerator(42L);
    final AssetGenerator other = new AssetGenerator(43L);
    boolean differs = false;
    for (long aid = 0; aid < 100; aid++) {
      final Map<String, Object> pa = a.insertParams(aid);
      assertThat(pa).isEqualTo(b.insertParams(aid));
      differs |= !pa.equals(other.insertParams(aid));
      assertThat(a.omitBrand()).isEqualTo(b.omitBrand());
      other.omitBrand();
    }
    assertThat(differs).isTrue();

    // 10000 draws: 5% omitted brand has sd ~22 (bound 5 sd = 110), 10% boundary sizes has sd 30 (bound 5 sd = 150)
    final AssetGenerator sampled = new AssetGenerator(7L);
    int omitted = 0;
    int boundary = 0;
    for (long aid = 0; aid < 10_000; aid++) {
      if (sampled.omitBrand())
        omitted++;
      final long size = (Long) sampled.insertParams(aid).get("fileSize");
      if (size == 1023L || size == 1024L || size == 1_048_575L || size == 1_048_576L)
        boundary++;
    }
    assertThat(omitted).isBetween(390, 610);
    assertThat(boundary).isBetween(850, 1150);
  }

  @Test
  void everyStatementRuns() {
    final AssetGenerator generator = new AssetGenerator(1L);
    run(FacetSchema.INSERT, generator.insertParams(1));
    run(FacetSchema.INSERT_NO_BRAND, generator.insertParams(2));
    run(FacetSchema.UPDATE_VALUES, generator.valuesParams(1));
    run(FacetSchema.REMOVE_BRAND, generator.keyParams(1));
    run(FacetSchema.GROW, generator.growParams(1));
    run(FacetSchema.UPDATE_GROWN, generator.valuesParams(1));
    run(FacetSchema.LINK, generator.linkParams(1, 2));
    run(FacetSchema.DELETE, generator.keyParams(2));
    assertThat(count("SELECT count(*) AS c FROM " + FacetSchema.TYPE + " WHERE aid = 1")).isEqualTo(1L);
    assertThat(count("SELECT count(*) AS c FROM " + FacetSchema.TYPE + " WHERE aid = 2")).isZero();
  }

  @Test
  void insertedAssetsMatch() {
    final AssetGenerator generator = new AssetGenerator(2L);
    for (long aid = 0; aid < 500; aid++)
      insert(generator, aid);
    assertFacetsEqualGroupBy();
  }

  @Test
  void boundaryValuesLandInTheUpperRange() {
    final AssetGenerator generator = new AssetGenerator(3L);
    long aid = 0;
    for (final long size : new long[] { 1023L, 1024L, 1048575L, 1048576L }) {
      final Map<String, Object> params = generator.insertParams(aid++);
      params.put("fileSize", size);
      run(FacetSchema.INSERT, params);
    }
    final SortedMap<String, Long> counts = facets("").counts();
    assertThat(counts.get("size=small")).isEqualTo(1L);
    assertThat(counts.get("size=medium")).isEqualTo(2L);
    assertThat(counts.get("size=large")).isEqualTo(1L);
    assertFacetsEqualGroupBy();
  }

  @Test
  void removedBrandCountsAsMissing() {
    final AssetGenerator generator = new AssetGenerator(4L);
    for (long aid = 0; aid < 10; aid++)
      run(FacetSchema.INSERT, generator.insertParams(aid));
    for (long aid = 0; aid < 3; aid++)
      run(FacetSchema.REMOVE_BRAND, generator.keyParams(aid));
    final SortedMap<String, Long> counts = facets("").counts();
    assertThat(counts.get("brand.missing")).isEqualTo(3L);
    assertThat(counts.keySet()).noneMatch(k -> k.equals("brand=null"));
    assertFacetsEqualGroupBy();
  }

  @Test
  void grownRecordUpdatedAgainIsCounted() {
    final AssetGenerator generator = new AssetGenerator(5L);
    for (long aid = 0; aid < 50; aid++)
      run(FacetSchema.INSERT, generator.insertParams(aid));
    for (long aid = 0; aid < 50; aid++)
      run(FacetSchema.GROW, generator.growParams(aid));
    for (long aid = 0; aid < 50; aid++) {
      final Map<String, Object> params = generator.valuesParams(aid);
      params.put("brand", AssetGenerator.BRANDS.get(0));
      run(FacetSchema.UPDATE_GROWN, params);
    }
    assertThat(facets("").counts().get("brand=" + AssetGenerator.BRANDS.get(0))).isEqualTo(50L);
    assertFacetsEqualGroupBy();
  }

  @Test
  void deleteThenInsertReusingTheSlotCountsOnce() {
    final AssetGenerator generator = new AssetGenerator(6L);
    for (long aid = 0; aid < 100; aid++)
      run(FacetSchema.INSERT, generator.insertParams(aid));
    for (long aid = 0; aid < 50; aid++)
      run(FacetSchema.DELETE, generator.keyParams(aid));
    for (long aid = 100; aid < 150; aid++)
      run(FacetSchema.INSERT, generator.insertParams(aid));
    final SortedMap<String, Long> counts = facets("").counts();
    long total = counts.getOrDefault("brand.missing", 0L);
    for (final Map.Entry<String, Long> e : counts.entrySet())
      if (e.getKey().startsWith("brand="))
        total += e.getValue();
    assertThat(total).isEqualTo(100L);
    assertFacetsEqualGroupBy();
  }

  @Test
  void linkingVerticesChangesNoCounts() {
    final AssetGenerator generator = new AssetGenerator(7L);
    for (long aid = 0; aid < 100; aid++)
      insert(generator, aid);
    final SortedMap<String, Long> before = facets("").counts();
    for (long aid = 0; aid < 100; aid++)
      run(FacetSchema.LINK, generator.linkParams(aid, (aid + 1) % 100));
    assertThat(facets("").counts()).isEqualTo(before);
    assertFacetsEqualGroupBy();
  }

  @Test
  void mixedRandomOperationsMatch() {
    mixedOperations();
    assertFacetsEqualGroupBy();
  }

  private void mixedOperations() {
    final AssetGenerator generator = new AssetGenerator(8L);
    final Random random = new Random(8L);
    final Set<Long> alive = new HashSet<>();
    final Set<Long> grown = new HashSet<>();
    for (int i = 0; i < 2000; i++) {
      final long aid = random.nextInt(300);
      final int op = random.nextInt(8);
      if (!alive.contains(aid)) {
        insert(generator, aid);
        alive.add(aid);
        continue;
      }
      switch (op) {
      case 0 -> {
        run(FacetSchema.DELETE, generator.keyParams(aid));
        alive.remove(aid);
        grown.remove(aid);
      }
      case 1, 2 -> run(FacetSchema.UPDATE_VALUES, generator.valuesParams(aid));
      case 3 -> run(FacetSchema.REMOVE_BRAND, generator.keyParams(aid));
      case 4 -> {
        run(FacetSchema.GROW, generator.growParams(aid));
        grown.add(aid);
      }
      case 5 -> run(grown.contains(aid) ? FacetSchema.UPDATE_GROWN : FacetSchema.UPDATE_VALUES, generator.valuesParams(aid));
      case 6 -> run(FacetSchema.LINK, generator.linkParams(aid, new ArrayList<>(alive).get(random.nextInt(alive.size()))));
      default -> run(FacetSchema.UPDATE_VALUES, generator.valuesParams(aid));
      }
    }
  }

  @Test
  void truthNeverReadsTheProjection() {
    mixedOperations();
    for (final String where : FacetSchema.CHECKED_FILTERS)
      expected(where);
    final LocalDatabase local = (LocalDatabase) database;
    assertThat(local.getColumnarIndexManager().getStats().groupByAcceleratedCount()).isZero();
    rows("SELECT brand AS v, count(*) AS c FROM " + FacetSchema.TYPE + " GROUP BY brand");
    assertThat(local.getColumnarIndexManager().getStats().groupByAcceleratedCount()).isEqualTo(1L);
  }

  /**
   * Two independently built databases never compare equal: {@code compareTypes} fails in {@code isTheSameAs} because the type JSON
   * holds the index names, which carry a creation timestamp. The second one is therefore a copy of the first, like a converged
   * replica (replicas share the index names through the replicated schema).
   */
  @Test
  void readOnlyReopenComparesEqual() throws IOException {
    final AssetGenerator generator = new AssetGenerator(9L);
    for (long aid = 0; aid < 500; aid++)
      insert(generator, aid);
    database.close();
    FileUtils.copyDirectory(new File(PATH), new File(PATH_B));

    final DatabaseFactory factoryB = new DatabaseFactory(PATH_B);
    final Database roA = factory.open(ComponentFile.MODE.READ_ONLY);
    final Database roB = factoryB.open(ComponentFile.MODE.READ_ONLY);
    try {
      new DatabaseComparator().compare(roA, roB);
    } finally {
      roA.close();
      roB.close();
    }

    // the comparison can fail: one record less on the copy
    final Database rwB = factoryB.open();
    rwB.transaction(() -> rwB.command("sql", FacetSchema.DELETE, generator.keyParams(7)).close());
    rwB.close();
    final Database roA2 = factory.open(ComponentFile.MODE.READ_ONLY);
    final Database roB2 = factoryB.open(ComponentFile.MODE.READ_ONLY);
    try {
      assertThatThrownBy(() -> new DatabaseComparator().compare(roA2, roB2)).isInstanceOf(DatabaseComparator.DatabaseAreNotIdentical.class);
    } finally {
      roA2.close();
      roB2.close();
      factoryB.close();
    }
  }
}
