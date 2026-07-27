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
import com.arcadedb.database.RID;
import com.arcadedb.graph.MutableVertex;
import com.arcadedb.query.sql.executor.ResultSet;
import com.arcadedb.schema.Schema;
import com.arcadedb.schema.Type;
import com.arcadedb.utility.FileUtils;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import java.io.File;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;

/**
 * Measures the query shapes affected by planning an inline property map as the equality predicate it
 * stands for. Every shape is written twice where an equivalent {@code WHERE} spelling exists, because
 * the point of the change is that the two spellings stop diverging.
 * <p>
 * Run the same class on both revisions and compare the two tables:
 * {@code ./mvnw -pl engine -Dtest=CypherInlinePropertyPlanningBenchmark -Dgroups=benchmark test}.
 * Override the dataset with {@code -Darcadedb.inlinePropertyBenchmark.vertices=200000}.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
@Tag("benchmark")
class CypherInlinePropertyPlanningBenchmark {
  private static final String DATABASE_PATH      = "./target/databases/cypher-inline-property-benchmark";
  private static final String VERTICES_PROPERTY  = "arcadedb.inlinePropertyBenchmark.vertices";
  private static final int    DEFAULT_VERTICES   = 100_000;
  private static final int    EDGES_PER_VERTEX   = 3;
  private static final int    WARMUP_ITERATIONS  = 5;
  private static final int    MEASURED_ITERATIONS = 15;
  private static final int    PARTITIONS         = 8;
  private static final int    BATCH_SIZE         = 20_000;

  private static final String[] CITIES = { "Rome", "Milan", "Naples", "Turin", "Genoa" };

  private static Database database;
  private static int      vertexCount;

  private final Map<String, long[]> timings = new LinkedHashMap<>();

  @BeforeAll
  static void populate() {
    vertexCount = Integer.getInteger(VERTICES_PROPERTY, DEFAULT_VERTICES);

    FileUtils.deleteRecursively(new File(DATABASE_PATH));
    database = new DatabaseFactory(DATABASE_PATH).create();

    final var person = database.getSchema().createVertexType("Person");
    person.createProperty("id", Type.STRING);
    person.createProperty("name", Type.STRING);
    person.createProperty("city", Type.STRING);
    database.getSchema().createEdgeType("KNOWS");

    // Partitioned like PartitioningTestFixture does it: the strategy has to route the inserts, and it
    // demands a unique index when it is assigned. The index is dropped afterwards so the scan path -
    // the one partition pruning serves - is the one being measured.
    database.transaction(() -> {
      database.getSchema().buildVertexType().withName("Tenanted").withTotalBuckets(PARTITIONS).create();
      database.command("sql", "CREATE PROPERTY Tenanted.tenant_id STRING");
      database.command("sql", "CREATE PROPERTY Tenanted.payload STRING");
      database.command("sql", "CREATE INDEX ON Tenanted(tenant_id) UNIQUE");
      database.command("sql", "ALTER TYPE Tenanted BucketSelectionStrategy `partitioned('tenant_id')`");
    });
    // The unique index is only required when the strategy is assigned; dropping it now both allows
    // many rows per tenant and leaves the scan path - the one pruning serves - as the only option.
    database.transaction(() -> {
      for (final var index : database.getSchema().getType("Tenanted").getAllIndexes(false))
        database.getSchema().dropIndex(index.getName());
    });

    // Batched so no single transaction has to hold the whole dataset
    final RID[] people = new RID[vertexCount];
    for (int start = 0; start < vertexCount; start += BATCH_SIZE) {
      final int from = start;
      final int to = Math.min(start + BATCH_SIZE, vertexCount);
      database.transaction(() -> {
        for (int i = from; i < to; i++)
          people[i] = database.newVertex("Person")
              .set("id", "p" + i)
              .set("name", "Name" + (i % 1000))
              .set("city", CITIES[i % CITIES.length])
              .save().getIdentity();
      });
    }

    for (int start = 0; start < vertexCount; start += BATCH_SIZE) {
      final int from = start;
      final int to = Math.min(start + BATCH_SIZE, vertexCount);
      database.transaction(() -> {
        for (int i = from; i < to; i++) {
          final MutableVertex source = database.lookupByRID(people[i], true).asVertex().modify();
          for (int e = 1; e <= EDGES_PER_VERTEX; e++)
            source.newEdge("KNOWS", database.lookupByRID(people[(i + e * 7) % vertexCount], true).asVertex(),
                true, (Object[]) null).save();
        }
      });
    }

    database.transaction(() -> {
      for (int i = 0; i < 20_000; i++)
        database.newVertex("Tenanted")
            .set("tenant_id", "t" + (i % PARTITIONS))
            .set("payload", "payload-" + i)
            .save();
    });

    database.transaction(() -> database.getSchema().createTypeIndex(
        Schema.INDEX_TYPE.LSM_TREE, true, "Person", "id"));
  }

  @AfterAll
  static void tearDown() {
    if (database != null)
      database.drop();
  }

  @Test
  void inlinePropertyShapes() {
    final String anchorId = "p" + (vertexCount / 2);

    measure("indexed anchor, 1 hop            [inline]",
        "MATCH (n:Person {id: '" + anchorId + "'})-[:KNOWS]->(m:Person) RETURN count(m) AS c");
    measure("indexed anchor, 1 hop            [where ]",
        "MATCH (n:Person)-[:KNOWS]->(m:Person) WHERE n.id = '" + anchorId + "' RETURN count(m) AS c");

    measure("indexed anchor, 2 hops           [inline]",
        "MATCH (n:Person {id: '" + anchorId + "'})-[:KNOWS]->(m:Person)-[:KNOWS]->(o:Person) RETURN count(o) AS c");
    measure("indexed anchor, 2 hops           [where ]",
        "MATCH (n:Person)-[:KNOWS]->(m:Person)-[:KNOWS]->(o:Person) WHERE n.id = '" + anchorId + "' RETURN count(o) AS c");

    measure("indexed anchor, unread edge var  [inline]",
        "MATCH (n:Person {id: '" + anchorId + "'})-[r:KNOWS]->(m:Person) RETURN count(m) AS c");
    measure("indexed anchor, unread edge var  [where ]",
        "MATCH (n:Person)-[r:KNOWS]->(m:Person) WHERE n.id = '" + anchorId + "' RETURN count(m) AS c");

    measure("indexed anchor, read edge var    [inline]",
        "MATCH (n:Person {id: '" + anchorId + "'})-[r:KNOWS]->(m:Person) RETURN count(r) AS c");

    measure("anonymous indexed source, 1 hop  [inline]",
        "MATCH (:Person {id: '" + anchorId + "'})-[:KNOWS]->(m:Person) RETURN count(m) AS c");

    measure("unindexed property, scan         [inline]",
        "MATCH (n:Person {city: 'Rome'}) RETURN count(n) AS c");
    measure("unindexed property, scan         [where ]",
        "MATCH (n:Person) WHERE n.city = 'Rome' RETURN count(n) AS c");

    measure("unindexed property, 1 hop        [inline]",
        "MATCH (n:Person {city: 'Rome'})-[:KNOWS]->(m:Person) RETURN count(m) AS c");
    measure("unindexed property, 1 hop        [where ]",
        "MATCH (n:Person)-[:KNOWS]->(m:Person) WHERE n.city = 'Rome' RETURN count(m) AS c");

    measure("far-side property filter         [inline]",
        "MATCH (n:Person {id: '" + anchorId + "'})-[:KNOWS]->(m:Person {city: 'Rome'}) RETURN count(m) AS c");
    measure("far-side property filter         [where ]",
        "MATCH (n:Person)-[:KNOWS]->(m:Person) WHERE n.id = '" + anchorId + "' AND m.city = 'Rome' RETURN count(m) AS c");

    measure("partitioned type                 [inline]",
        "MATCH (t:Tenanted {tenant_id: 't3'}) RETURN count(t) AS c");
    measure("partitioned type                 [where ]",
        "MATCH (t:Tenanted) WHERE t.tenant_id = 't3' RETURN count(t) AS c");

    report();
  }

  private void measure(final String label, final String query) {
    for (int i = 0; i < WARMUP_ITERATIONS; i++)
      execute(query);

    final long[] samples = new long[MEASURED_ITERATIONS];
    for (int i = 0; i < MEASURED_ITERATIONS; i++) {
      final long begin = System.nanoTime();
      execute(query);
      samples[i] = System.nanoTime() - begin;
    }
    Arrays.sort(samples);
    timings.put(label, samples);
  }

  private long execute(final String query) {
    long rows = 0;
    try (final ResultSet resultSet = database.query("opencypher", query)) {
      while (resultSet.hasNext()) {
        resultSet.next();
        rows++;
      }
    }
    return rows;
  }

  private void report() {
    final List<String> lines = new ArrayList<>();
    lines.add(String.format(Locale.ROOT, "%-42s %10s %10s %10s", "shape", "median", "p10", "p90"));
    for (final Map.Entry<String, long[]> entry : timings.entrySet()) {
      final long[] samples = entry.getValue();
      lines.add(String.format(Locale.ROOT, "%-42s %9.3fms %9.3fms %9.3fms",
          entry.getKey(),
          samples[samples.length / 2] / 1_000_000.0,
          samples[samples.length / 10] / 1_000_000.0,
          samples[samples.length * 9 / 10] / 1_000_000.0));
    }

    System.out.printf(Locale.ROOT, "%n=== Cypher inline-property planning, %d vertices / %d edges ===%n",
        vertexCount, vertexCount * EDGES_PER_VERTEX);
    lines.forEach(System.out::println);
    System.out.println();
  }
}
