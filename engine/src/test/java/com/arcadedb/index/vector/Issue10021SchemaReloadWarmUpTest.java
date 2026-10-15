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
package com.arcadedb.index.vector;

import com.arcadedb.GlobalConfiguration;
import com.arcadedb.database.Database;
import com.arcadedb.database.DatabaseFactory;
import com.arcadedb.database.DatabaseInternal;
import com.arcadedb.engine.ComponentFile;
import com.arcadedb.schema.LocalSchema;
import com.arcadedb.schema.Type;
import com.arcadedb.utility.FileUtils;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import java.io.File;
import java.util.Random;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Issue #10021: a full schema load - which an HA replica runs for every schema entry its incremental refresh declines -
 * re-instantiates every component, and an {@code LSM_VECTOR} index came back with nothing resident: the next search read
 * all of its pages to rebuild the location map and then loaded or rebuilt the graph, on the query thread, under the
 * JVM-wide rebuild permit. One unrelated DDL turned the searches that followed into a burst of those, on every vector index.
 * <p>
 * The reload now warms, in the background, the indexes whose replaced instance had a graph resident, and only those.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
@Tag("vector")
class Issue10021SchemaReloadWarmUpTest {
  private static final String DB_PATH       = "target/test-databases/Issue10021SchemaReloadWarmUpTest";
  private static final int    DIMENSIONS    = 16;
  private static final int    NUM_VECTORS   = 1_000;
  /** A bound for waiting on something that must happen, sized as a hang detector and not as a latency claim. */
  private static final long   AWAIT_MS      = 120_000L;

  @BeforeEach
  void setUp() {
    FileUtils.deleteRecursively(new File(DB_PATH));
    GlobalConfiguration.VECTOR_INDEX_WARM_UP_AFTER_SCHEMA_RELOAD_DELAY_MS.setValue(0);
    build();
  }

  @AfterEach
  void tearDown() {
    GlobalConfiguration.VECTOR_INDEX_WARM_UP_AFTER_SCHEMA_RELOAD_DELAY_MS.reset();
    FileUtils.deleteRecursively(new File(DB_PATH));
  }

  @Test
  void aWarmIndexIsWarmedInTheBackgroundAfterAFullSchemaLoad() throws Exception {
    try (final DatabaseFactory factory = new DatabaseFactory(DB_PATH)) {
      final Database db = factory.open();
      try {
        final LSMVectorIndex before = vectorIndex(db);
        before.warmUp();
        assertThat(before.isWarm()).isTrue();
        assertThat(before.getStats().get("graphNodeCount")).isEqualTo((long) NUM_VECTORS);

        fullSchemaLoad(db);

        final LSMVectorIndex after = vectorIndex(db);
        assertThat(after).as("the reload re-creates the index").isNotSameAs(before);
        assertThat(schema(db).awaitIndexWarmUp(AWAIT_MS)).isTrue();

        assertThat(after.isWarm()).as("warmed without a search having asked for it").isTrue();
        assertThat(after.getStats().get("graphNodeCount")).isEqualTo((long) NUM_VECTORS);
        assertThat(after.getStats().get("graphRebuildCount")).as("loaded from the persisted graph, not rebuilt").isZero();
        // The first search finds everything resident: it loads and builds nothing
        final long loadsBefore = after.getStats().get("graphLoadsFromOrdinalMap");
        assertThat(after.findNeighborsFromVector(embedding(3), 3, 64)).hasSize(3);
        assertThat(after.getStats().get("graphLoadsFromOrdinalMap")).isEqualTo(loadsBefore);
        assertThat(after.getStats().get("graphRebuildCount")).isZero();
      } finally {
        db.drop();
      }
    }
  }

  @Test
  void anIndexNobodySearchedIsNotWarmed() throws Exception {
    try (final DatabaseFactory factory = new DatabaseFactory(DB_PATH)) {
      final Database db = factory.open();
      try {
        assertThat(vectorIndex(db).isWarm()).as("a freshly opened database has nothing resident").isFalse();

        fullSchemaLoad(db);

        assertThat(schema(db).awaitIndexWarmUp(AWAIT_MS)).isTrue();
        final LSMVectorIndex after = vectorIndex(db);
        assertThat(after.isWarm()).as("warming it would charge the replica for an index nobody queries").isFalse();
        assertThat(after.getStats().get("graphNodeCount")).isZero();
      } finally {
        db.drop();
      }
    }
  }

  @Test
  void theWarmUpCanBeSwitchedOff() throws Exception {
    GlobalConfiguration.VECTOR_INDEX_WARM_UP_AFTER_SCHEMA_RELOAD_DELAY_MS.setValue(-1);
    try (final DatabaseFactory factory = new DatabaseFactory(DB_PATH)) {
      final Database db = factory.open();
      try {
        vectorIndex(db).warmUp();

        fullSchemaLoad(db);

        assertThat(schema(db).awaitIndexWarmUp(AWAIT_MS)).isTrue();
        assertThat(vectorIndex(db).isWarm()).as("left to the first search, as before").isFalse();
      } finally {
        db.drop();
      }
    }
  }

  @Test
  void anIndexReCreatedTwiceBeforeItGotWarmStaysInTheChain() throws Exception {
    // The first reload's warm-up is held back by a delay that never elapses in the test; the second reload, which finds an
    // instance that is not warm because the warm-up has not run, must still ask for its successor to be warmed
    try (final DatabaseFactory factory = new DatabaseFactory(DB_PATH)) {
      final Database db = factory.open();
      try {
        vectorIndex(db).warmUp();

        GlobalConfiguration.VECTOR_INDEX_WARM_UP_AFTER_SCHEMA_RELOAD_DELAY_MS.setValue(600_000);
        fullSchemaLoad(db);
        assertThat(vectorIndex(db).isWarm()).as("the first reload's warm-up is still waiting").isFalse();

        GlobalConfiguration.VECTOR_INDEX_WARM_UP_AFTER_SCHEMA_RELOAD_DELAY_MS.setValue(0);
        fullSchemaLoad(db);

        assertThat(schema(db).awaitIndexWarmUp(AWAIT_MS)).isTrue();
        assertThat(vectorIndex(db).isWarm()).as("the intent survived the second reload").isTrue();
        assertThat(vectorIndex(db).getStats().get("graphNodeCount")).isEqualTo((long) NUM_VECTORS);
      } finally {
        db.drop();
      }
    }
  }

  @Test
  void closingTheDatabaseWhileAWarmUpWaitsIsClean() throws Exception {
    try (final DatabaseFactory factory = new DatabaseFactory(DB_PATH)) {
      final Database db = factory.open();
      vectorIndex(db).warmUp();

      GlobalConfiguration.VECTOR_INDEX_WARM_UP_AFTER_SCHEMA_RELOAD_DELAY_MS.setValue(600_000);
      fullSchemaLoad(db);

      final LocalSchema schema = schema(db);
      db.close();

      assertThat(schema.awaitIndexWarmUp(AWAIT_MS)).as("the waiting warm-up was dropped with the schema").isTrue();
    }
  }

  private static LocalSchema schema(final Database db) {
    return ((DatabaseInternal) db).getSchema().getEmbedded();
  }

  private static void fullSchemaLoad(final Database db) throws Exception {
    schema(db).load(ComponentFile.MODE.READ_WRITE, true);
  }

  private static void build() {
    try (final DatabaseFactory factory = new DatabaseFactory(DB_PATH)) {
      final Database db = factory.create();
      try {
        db.transaction(() -> {
          final var type = db.getSchema().createDocumentType("Doc");
          type.createProperty("id", Type.INTEGER);
          type.createProperty("vector", Type.ARRAY_OF_FLOATS);
          db.command("sql", "CREATE INDEX ON Doc (vector) LSM_VECTOR METADATA { \"dimensions\": " + DIMENSIONS
              + ", \"similarity\": \"COSINE\" }");
        });
        db.begin();
        for (int i = 0; i < NUM_VECTORS; i++) {
          db.newDocument("Doc").set("id", i).set("vector", embedding(i)).save();
          if (i % 500 == 499) {
            db.commit();
            db.begin();
          }
        }
        db.commit();
        vectorIndex(db).buildVectorGraphNow();
      } finally {
        if (db.isOpen())
          db.close();
      }
    }
  }

  private static float[] embedding(final int id) {
    final Random random = new Random(0x10021L * 31 + id);
    final float[] vector = new float[DIMENSIONS];
    for (int d = 0; d < DIMENSIONS; d++)
      vector[d] = random.nextFloat();
    return vector;
  }

  private static LSMVectorIndex vectorIndex(final Database db) {
    return (LSMVectorIndex) db.getSchema().getType("Doc").getPolymorphicIndexByProperties("vector")
        .getIndexesOnBuckets()[0];
  }
}
