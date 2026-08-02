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
import com.arcadedb.TestHelper;
import com.arcadedb.exception.CommandSQLParsingException;
import com.arcadedb.index.IndexCursor;
import com.arcadedb.index.IndexException;
import com.arcadedb.index.TypeIndex;
import com.arcadedb.query.sql.executor.ResultSet;
import com.arcadedb.schema.LSMVectorIndexMetadata;
import com.arcadedb.schema.TypeLSMVectorIndexBuilder;
import com.arcadedb.serializer.json.JSONObject;

import org.junit.jupiter.api.Test;

import java.util.Random;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Regression test for issue #5559: a {@code locationCacheSize} on an {@code LSM_VECTOR} index used to truncate it
 * instead of bounding a cache.
 * <p>
 * The location index is not a cache in front of an authoritative set - it IS the authoritative set. A location is
 * the only record of which record a vector id belongs to and where its entry sits in the index file, and nothing on
 * disk maps a vector id back to an offset. Bounding it therefore did not spill to a slower tier: the graph was
 * rebuilt over the surviving entries only, {@code countEntries()} reported the cap instead of the live count, and a
 * search probing a vector with its own embedding answered with some other vertex. The report was taken with 200
 * vectors, 16 dimensions, INT8 quantization and a cap of 10.
 * <p>
 * Issue #5568 stopped honouring the setting and this issue removed the bounded backend outright, so the three
 * entrances now behave differently and each is pinned below:
 * <ul>
 *   <li>the {@code METADATA} clause and the Java builder <b>refuse</b> a positive value, rather than accepting one
 *       and leaving a bound in the schema that is not in force;</li>
 *   <li>the global setting is still tolerated - refusing it would stop a server booting off an existing startup
 *       line - and does not truncate the index;</li>
 *   <li>a definition persisted by an older version still loads, or the database would not open.</li>
 * </ul>
 * The self-recall probes under the global setting are the load-bearing behavioural assertions:
 * {@code countEntries()} alone would still pass on an index whose graph had been built over a truncated set,
 * because a count and a graph traversal read different state.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class Issue5559LocationCacheSizeTest extends TestHelper {

  private static final int DIMENSIONS         = 16;
  private static final int NUM_VECTORS        = 200;
  private static final int LOCATION_CACHE_CAP = 10;

  /**
   * The reported reproduction, which reached the index through the per-index METADATA clause. The statement is now
   * refused with a message that says why and what to size instead, and no index is left behind.
   */
  @Test
  void perIndexLocationCacheSizeMetadataIsRefused() {
    database.transaction(() -> {
      database.command("sql", "CREATE VERTEX TYPE Doc");
      database.command("sql", "CREATE PROPERTY Doc.embedding ARRAY_OF_FLOATS");
    });

    assertThatThrownBy(() -> database.command("sql", """
        CREATE INDEX ON Doc (embedding) LSM_VECTOR \
        METADATA {"dimensions": %d, "similarity": "COSINE", "quantization": "INT8", "locationCacheSize": %d}\
        """.formatted(DIMENSIONS, LOCATION_CACHE_CAP)))
        // A parsing exception, not an execution one: it is what AbstractServerHttpHandler's 400 arm keys on, so
        // this type is what keeps the refusal a client error over HTTP rather than a 500 carrying a helpful
        // message nobody displays. Pinned end to end by Issue5559LocationCacheSizeHttpIT in the server module.
        .isInstanceOf(CommandSQLParsingException.class)
        .hasRootCauseInstanceOf(IndexException.class)
        .hasMessageContaining("locationCacheSize")
        .hasMessageContaining("no longer supported")
        .hasMessageContaining("90 bytes per live vector");

    assertThat(database.getSchema().existsIndex("Doc[embedding]"))
        .as("a refused statement must not leave a half-built index").isFalse();

    // "no limit" is what an unset builder and a metadata copy carry, so it has to stay accepted.
    assertThatCode(() -> database.command("sql", """
        CREATE INDEX ON Doc (embedding) LSM_VECTOR \
        METADATA {"dimensions": %d, "similarity": "COSINE", "locationCacheSize": -1}\
        """.formatted(DIMENSIONS))).doesNotThrowAnyException();
  }

  /** The Java builder is the second user-facing entrance and refuses through the same setter. */
  @Test
  void theJavaBuilderRefusesALocationCacheSize() {
    database.transaction(() -> {
      database.command("sql", "CREATE VERTEX TYPE Doc");
      database.command("sql", "CREATE PROPERTY Doc.embedding ARRAY_OF_FLOATS");
    });

    assertThatThrownBy(() -> database.getSchema().buildTypeIndex("Doc", new String[] { "embedding" })
        .withLSMVectorType()
        .withDimensions(DIMENSIONS)
        .withLocationCacheSize(LOCATION_CACHE_CAP)
        .create())
        .hasMessageContaining("locationCacheSize")
        .hasMessageContaining("no longer supported");

    assertThat(database.getSchema().existsIndex("Doc[embedding]")).isFalse();

    // "no limit" keeps working on the builder too, and keeps returning the builder so an existing fluent chain
    // carrying it still composes. Only the SQL entrance was pinned for this above.
    final TypeLSMVectorIndexBuilder builder = database.getSchema().buildTypeIndex("Doc", new String[] { "embedding" })
        .withLSMVectorType()
        .withDimensions(DIMENSIONS);
    assertThat(builder.withLocationCacheSize(-1)).as("the fluent chain continues").isSameAs(builder);

    builder.create();
    final TypeIndex created = (TypeIndex) database.getSchema().getIndexByName("Doc[embedding]");
    assertThat(((LSMVectorIndex) created.getIndexesOnBuckets()[0]).getMetadata().locationCacheSize)
        .as("-1 means 'no limit', which is what the index does").isEqualTo(-1);
  }

  /**
   * A schema written by a version that still honoured the setting must keep loading: {@code fromJSON} reads a
   * complete persisted definition and does not go through the refusing setter, because refusing there would make an
   * existing database unopenable.
   */
  @Test
  void aPersistedDefinitionCarryingTheSettingStillLoads() {
    database.transaction(() -> {
      database.command("sql", "CREATE VERTEX TYPE Doc");
      database.command("sql", "CREATE PROPERTY Doc.embedding ARRAY_OF_FLOATS");
      database.command("sql", """
          CREATE INDEX ON Doc (embedding) LSM_VECTOR METADATA {"dimensions": %d, "similarity": "COSINE"}\
          """.formatted(DIMENSIONS));
    });

    final TypeIndex typeIndex = (TypeIndex) database.getSchema().getIndexByName("Doc[embedding]");
    final LSMVectorIndex lsmIndex = (LSMVectorIndex) typeIndex.getIndexesOnBuckets()[0];

    // The shape LocalSchema reads back on open, with the key an older version wrote into it.
    final JSONObject persisted = lsmIndex.toJSON().put("locationCacheSize", LOCATION_CACHE_CAP);

    final LSMVectorIndexMetadata reloaded = new LSMVectorIndexMetadata(null, new String[0], -1);
    assertThatCode(() -> reloaded.fromJSON(persisted)).as("an old schema must still open").doesNotThrowAnyException();

    assertThat(reloaded.locationCacheSize)
        .as("the value round-trips so the schema is not rewritten behind the user's back")
        .isEqualTo(LOCATION_CACHE_CAP);
  }

  /**
   * The global setting is the entrance that stays tolerated, and this is the behavioural regression guard: with the
   * cap configured, every vector must remain counted and reachable. Restoring eviction makes
   * {@code countEntries()} answer 10 instead of 200.
   */
  @Test
  void theGlobalSettingDoesNotTruncateTheIndex() {
    // Per-database, so it is dropped with the database and cannot leak into the rest of the suite.
    database.getConfiguration().setValue(GlobalConfiguration.VECTOR_INDEX_LOCATION_CACHE_SIZE, LOCATION_CACHE_CAP);

    database.transaction(() -> {
      database.command("sql", "CREATE VERTEX TYPE Doc");
      database.command("sql", "CREATE PROPERTY Doc.name STRING");
      database.command("sql", "CREATE PROPERTY Doc.embedding ARRAY_OF_FLOATS");
      database.command("sql", """
          CREATE INDEX ON Doc (embedding) LSM_VECTOR \
          METADATA {"dimensions": %d, "similarity": "COSINE", "quantization": "INT8"}\
          """.formatted(DIMENSIONS));
    });

    database.transaction(() -> {
      for (int i = 0; i < NUM_VECTORS; i++) {
        final var vertex = database.newVertex("Doc");
        vertex.set("name", "doc" + i);
        vertex.set("embedding", embeddingOf(i));
        vertex.save();
      }
    });

    final TypeIndex typeIndex = (TypeIndex) database.getSchema().getIndexByName("Doc[embedding]");
    final LSMVectorIndex lsmIndex = (LSMVectorIndex) typeIndex.getIndexesOnBuckets()[0];

    assertThat(typeIndex.countEntries())
        .as("Every indexed vector must stay resident with a location cache cap of " + LOCATION_CACHE_CAP)
        .isEqualTo(NUM_VECTORS);

    // A cap of 10 kept only the last-inserted handful reachable, so probing an early vector answered with a late
    // one. Probe across the whole insertion range, including the ids that used to be evicted first.
    for (final int probe : new int[] { 0, 5, 42, 100, 150, NUM_VECTORS - 1 }) {
      final IndexCursor cursor = lsmIndex.get(new Object[] { embeddingOf(probe) }, 1);
      assertThat(cursor.hasNext()).as("A probe for doc" + probe + " must return a neighbour").isTrue();
      assertThat(cursor.next().asVertex().getString("name"))
          .as("Probing doc" + probe + " with its own embedding must return doc" + probe)
          .isEqualTo("doc" + probe);
    }

    // The same through SQL, which is how the report exercised it.
    try (final ResultSet rs = database.query("sql",
        "SELECT name FROM (SELECT expand(vectorNeighbors('Doc[embedding]', ?, 1)))", (Object) embeddingOf(5))) {
      assertThat(rs.hasNext()).as("vectorNeighbors() must answer").isTrue();
      assertThat(rs.next().<Object>getProperty("name")).as("vectorNeighbors() must find doc5 from its own embedding")
          .isEqualTo("doc5");
    }
  }

  /**
   * Well separated embeddings: a per-vertex pseudo-random direction from a fixed seed, so the run is deterministic
   * and the nearest neighbour of {@code embeddingOf(i)} under COSINE is unambiguously vertex {@code i}. A structured
   * fixture (one dominant dimension, say) would not do - COSINE ignores magnitude, so two vertices sharing a
   * dominant dimension sit within quantization noise of each other.
   */
  private static float[] embeddingOf(final int i) {
    final Random random = new Random(i * 31L + 7);
    final float[] embedding = new float[DIMENSIONS];
    for (int d = 0; d < DIMENSIONS; d++)
      embedding[d] = random.nextFloat() * 2 - 1;
    return embedding;
  }
}
