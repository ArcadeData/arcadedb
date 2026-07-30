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
package performance;

import com.arcadedb.database.Database;
import com.arcadedb.database.DatabaseFactory;
import com.arcadedb.database.MutableDocument;
import com.arcadedb.schema.GeoIndexMetadata;
import com.arcadedb.schema.Schema;
import com.arcadedb.schema.Type;
import com.arcadedb.schema.TypeIndexBuilder;
import com.arcadedb.utility.FileUtils;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import java.io.File;
import java.util.Random;

/**
 * Measures the ingest cost of a GEOSPATIAL index (issue #5478), the workload that made a GraphBatch load slow down and
 * eventually blow the maximum replicated Raft entry size on a cluster.
 * <p>
 * Three arms over the same records, all points inside one small country-sized box so the top of the GeoHash tree is
 * shared by every record - the shape the report came from:
 * <ul>
 *   <li>{@code noIndex} - the floor: what the load costs with no index at all;</li>
 *   <li>{@code frontierTokenization} - the current layout, one index entry per point;</li>
 *   <li>{@code fullTokenization} - the pre-26.8.1 layout, {@code precision} entries per point.</li>
 * </ul>
 * Override the defaults with {@code -Darcadedb.geoIngestBenchmark.records=2000000} and
 * {@code -Darcadedb.geoIngestBenchmark.txSize=1000}.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
@Tag("benchmark")
class GeoIndexIngestBenchmark {
  private static final String DB_PATH = "target/databases/GeoIndexIngestBenchmark";
  private static final int    RECORDS = Integer.getInteger("arcadedb.geoIngestBenchmark.records", 1_000_000);
  private static final int    TX_SIZE = Integer.getInteger("arcadedb.geoIngestBenchmark.txSize", 1_000);
  private static final int    CHUNKS  = 4;

  @Test
  void noIndex() {
    run("no index", null);
  }

  @Test
  void frontierTokenization() {
    run("FRONTIER (one entry per point)", GeoIndexMetadata.TOKENIZATION.FRONTIER);
  }

  @Test
  void fullTokenization() {
    run("FULL (one entry per GeoHash level)", GeoIndexMetadata.TOKENIZATION.FULL);
  }

  private void run(final String label, final GeoIndexMetadata.TOKENIZATION tokenization) {
    FileUtils.deleteRecursively(new File(DB_PATH));

    try (final Database database = new DatabaseFactory(DB_PATH).create()) {
      database.transaction(
          () -> database.getSchema().createDocumentType("Address", 1).createProperty("location", Type.STRING));

      if (tokenization != null) {
        final TypeIndexBuilder builder = database.getSchema().buildTypeIndex("Address", new String[] { "location" });
        builder.withType(Schema.INDEX_TYPE.GEOSPATIAL);
        final GeoIndexMetadata meta = new GeoIndexMetadata("Address", new String[] { "location" }, -1);
        meta.setTokenization(tokenization);
        builder.withMetadata(meta);
        builder.create();
      }

      System.out.printf("%n=== GEOSPATIAL ingest: %s - %,d records, %,d per transaction ===%n", label, RECORDS, TX_SIZE);

      final Random rnd = new Random(42);
      final int chunkSize = RECORDS / CHUNKS;
      final long globalStart = System.currentTimeMillis();

      for (int chunk = 0; chunk < CHUNKS; chunk++) {
        final long start = System.currentTimeMillis();
        for (int i = 0; i < chunkSize / TX_SIZE; i++) {
          database.begin();
          for (int j = 0; j < TX_SIZE; j++) {
            final MutableDocument doc = database.newDocument("Address");
            // One country-sized box, so every record shares the top GeoHash cells
            doc.set("location", "POINT (" + (8.0 + rnd.nextDouble() * 5.0) + " " + (54.5 + rnd.nextDouble() * 3.0) + ")");
            doc.save();
          }
          database.commit();
        }
        final long elapsed = System.currentTimeMillis() - start;
        System.out.printf("  chunk %d: %,d records in %,d ms (%,.0f rec/s)%n", chunk, chunkSize, elapsed,
            chunkSize * 1000.0 / Math.max(1, elapsed));
      }

      System.out.printf("  TOTAL %,d records in %,d ms", RECORDS, System.currentTimeMillis() - globalStart);
      if (tokenization != null)
        System.out.printf(" - index entries %,d", database.getSchema().getIndexByName("Address[location]").countEntries());
      System.out.println();
    } finally {
      FileUtils.deleteRecursively(new File(DB_PATH));
    }
  }
}
