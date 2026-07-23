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
package com.arcadedb.query.opencypher;

import com.arcadedb.database.Database;
import com.arcadedb.database.DatabaseFactory;
import com.arcadedb.graph.Edge;
import com.arcadedb.graph.MutableVertex;
import com.arcadedb.query.sql.executor.ExecutionPlan;
import com.arcadedb.query.sql.executor.Result;
import com.arcadedb.query.sql.executor.ResultSet;
import com.arcadedb.schema.Schema;
import com.arcadedb.schema.Type;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

class CypherVariableLengthAnchorSelectionTest {
  private static final String DATABASE_PATH = "./target/databases/cypher-vlp-anchor-selection";

  private Database database;

  @BeforeEach
  void setUp() {
    database = new DatabaseFactory(DATABASE_PATH).create();
    database.getSchema().createVertexType("Area").createProperty("id", Type.STRING);
    database.getSchema().createEdgeType("PART_OF");
    database.getSchema().buildEdgeType().withName("PART_OF_ONE_WAY").withBidirectional(false).create();

    database.transaction(() -> {
      final MutableVertex country = area("country");
      final MutableVertex region = area("region");
      final MutableVertex city = area("city");

      country.set("region", "EU").save();
      region.set("region", "NA").save();

      city.newEdge("PART_OF", region, true, (Object[]) null).set("step", "city-region").save();
      region.newEdge("PART_OF", country, true, (Object[]) null).set("step", "region-country").save();
      city.newEdge("PART_OF_ONE_WAY", region).save();
      region.newEdge("PART_OF_ONE_WAY", country).save();

      for (int i = 0; i < 256; i++)
        area("decoy-" + i);
    });

    database.transaction(() -> database.getSchema().createTypeIndex(
        Schema.INDEX_TYPE.LSM_TREE, true, "Area", "id"));
  }

  @AfterEach
  void tearDown() {
    if (database != null)
      database.drop();
  }

  @Test
  void startsBoundedVariableLengthTraversalFromIndexedTarget() {
    final String sourceFirst = """
        MATCH (place:Area)-[:PART_OF*0..3]->(country:Area)
        WHERE country.id = $countryId
        RETURN DISTINCT place.id AS id
        ORDER BY id""";
    final String targetFirst = """
        MATCH (country:Area)<-[:PART_OF*0..3]-(place:Area)
        WHERE country.id = $countryId
        RETURN DISTINCT place.id AS id
        ORDER BY id""";
    final Map<String, Object> parameters = Map.of("countryId", "country");

    assertThat(queryIds(sourceFirst, parameters)).containsExactly("city", "country", "region");
    assertThat(queryIds(targetFirst, parameters)).containsExactly("city", "country", "region");

    final ExecutionPlan plan;
    try (ResultSet resultSet = database.query("opencypher", "PROFILE " + sourceFirst, parameters)) {
      while (resultSet.hasNext())
        resultSet.next();
      plan = resultSet.getExecutionPlan().orElseThrow();
    }

    assertThat(plan.getSteps()).isNotEmpty();
    assertThat(plan.getSteps().get(0).getDescription())
        .contains("MATCH NODE (country:Area)")
        .contains("[index: Area[id]]")
        .contains("1 rows");
    assertThat(plan.prettyPrint(0, 2))
        .contains("Execution Plan (Traditional)")
        .contains("MATCH NODE (country:Area)")
        .doesNotContain("Execution Plan (Cost-Based Optimizer)");

    try (ResultSet resultSet = database.query("opencypher", "EXPLAIN " + sourceFirst, parameters)) {
      assertThat(resultSet.getExecutionPlan().orElseThrow().prettyPrint(0, 2))
          .contains("Using Traditional Execution (Non-Optimized)")
          .doesNotContain("Using Cost-Based Query Optimizer");
    }
  }

  @Test
  void startsUnboundedVariableLengthTraversalFromIndexedTarget() {
    final String query = """
        MATCH (place:Area)-[:PART_OF*]->(country:Area)
        WHERE country.id = $countryId
        RETURN DISTINCT place.id AS id
        ORDER BY id""";
    final Map<String, Object> parameters = Map.of("countryId", "country");

    assertThat(queryIds(query, parameters)).containsExactly("city", "region");

    try (ResultSet resultSet = database.query("opencypher", "PROFILE " + query, parameters)) {
      while (resultSet.hasNext())
        resultSet.next();
      assertThat(resultSet.getExecutionPlan().orElseThrow().getSteps().get(0).getDescription())
          .contains("MATCH NODE (country:Area)")
          .contains("[index: Area[id]]")
          .contains("1 rows");
    }
  }

  @Test
  void startsOpenEndedTraversalWithMinimumHopsFromIndexedTarget() {
    final String query = """
        MATCH (place:Area)-[:PART_OF*2..]->(country:Area)
        WHERE country.id = $countryId
        RETURN DISTINCT place.id AS id
        ORDER BY id""";
    final Map<String, Object> parameters = Map.of("countryId", "country");

    assertThat(queryIds(query, parameters)).containsExactly("city");

    try (ResultSet resultSet = database.query("opencypher", "PROFILE " + query, parameters)) {
      while (resultSet.hasNext())
        resultSet.next();
      assertThat(resultSet.getExecutionPlan().orElseThrow().getSteps().get(0).getDescription())
          .contains("MATCH NODE (country:Area)")
          .contains("[index: Area[id]]")
          .contains("1 rows");
    }
  }

  @Test
  void startsAnonymousSourceTraversalFromIndexedTarget() {
    final String query = """
        MATCH (:Area)-[:PART_OF*0..3]->(country:Area)
        WHERE country.id = $countryId
        RETURN country.id AS id, count(*) AS places""";
    final Map<String, Object> parameters = Map.of("countryId", "country");

    try (ResultSet resultSet = database.query("opencypher", "PROFILE " + query, parameters)) {
      assertThat(resultSet.hasNext()).isTrue();
      final Result result = resultSet.next();
      assertThat(result.<String>getProperty("id")).isEqualTo("country");
      assertThat(result.<Long>getProperty("places")).isEqualTo(3L);
      assertThat(resultSet.hasNext()).isFalse();
      assertThat(resultSet.getExecutionPlan().orElseThrow().getSteps().get(0).getDescription())
          .contains("MATCH NODE (country:Area)")
          .contains("[index: Area[id]]")
          .contains("1 rows");
    }
  }

  @Test
  void startsAnonymousUnboundedTraversalFromIndexedTarget() {
    final String query = """
        MATCH (:Area)-[:PART_OF*2..]->(country:Area)
        WHERE country.id = $countryId
        RETURN country.id AS id, count(*) AS places""";
    final Map<String, Object> parameters = Map.of("countryId", "country");

    try (ResultSet resultSet = database.query("opencypher", "PROFILE " + query, parameters)) {
      assertThat(resultSet.hasNext()).isTrue();
      final Result result = resultSet.next();
      assertThat(result.<String>getProperty("id")).isEqualTo("country");
      assertThat(result.<Long>getProperty("places")).isEqualTo(1L);
      assertThat(resultSet.hasNext()).isFalse();
      assertThat(resultSet.getExecutionPlan().orElseThrow().getSteps().get(0).getDescription())
          .contains("MATCH NODE (country:Area)")
          .contains("[index: Area[id]]")
          .contains("1 rows");
    }
  }

  @Test
  void startsAnonymousUnboundedTraversalFromIndexedInListTargets() {
    final String query = """
        MATCH (:Area)-[:PART_OF*]->(country:Area)
        WHERE country.id IN $countryIds
        RETURN country.id AS country, count(*) AS places
        ORDER BY country""";
    final Map<String, Object> parameters = Map.of("countryIds", List.of("country", "region", "absent"));

    try (ResultSet resultSet = database.query("opencypher", "PROFILE " + query, parameters)) {
      assertThat(resultSet.stream()
          .map(row -> row.<String>getProperty("country") + ":" + row.<Long>getProperty("places"))
          .toList())
          .containsExactly("country:2", "region:1");
      assertThat(resultSet.getExecutionPlan().orElseThrow().getSteps().get(0).getDescription())
          .contains("INDEX SEEK (country:Area)")
          .contains("[index: Area[id]]")
          .contains("2 rows");
    }
  }

  @Test
  void startsBoundedVariableLengthTraversalFromIndexedInListTarget() {
    final String query = """
        MATCH (place:Area)-[:PART_OF*0..3]->(country:Area)
        WHERE country.id IN $countryIds
        RETURN country.id AS country, place.id AS place
        ORDER BY country, place""";
    final Map<String, Object> parameters = Map.of("countryIds", List.of("country", "region", "absent"));

    try (ResultSet resultSet = database.query("opencypher", query, parameters)) {
      assertThat(resultSet.stream()
          .map(row -> row.<String>getProperty("country") + ":" + row.<String>getProperty("place"))
          .toList())
          .containsExactly("country:city", "country:country", "country:region", "region:city", "region:region");
    }

    try (ResultSet resultSet = database.query("opencypher", "PROFILE " + query, parameters)) {
      while (resultSet.hasNext())
        resultSet.next();
      assertThat(resultSet.getExecutionPlan().orElseThrow().getSteps().get(0).getDescription())
          .contains("INDEX SEEK (country:Area)")
          .contains("[index: Area[id]]")
          .contains("2 rows");
    }
  }

  @Test
  void preservesInlinePropertiesOnIndexedInListAnchor() {
    final String query = """
        MATCH (place:Area)-[:PART_OF*0..3]->(country:Area {region: 'EU'})
        WHERE country.id IN $countryIds
        RETURN country.id AS country, place.id AS place
        ORDER BY country, place""";
    final Map<String, Object> parameters = Map.of("countryIds", List.of("country", "region"));

    try (ResultSet resultSet = database.query("opencypher", query, parameters)) {
      assertThat(resultSet.stream()
          .map(row -> row.<String>getProperty("country") + ":" + row.<String>getProperty("place"))
          .toList())
          .containsExactly("country:city", "country:country", "country:region");
    }

    try (ResultSet resultSet = database.query("opencypher", "PROFILE " + query, parameters)) {
      while (resultSet.hasNext())
        resultSet.next();
      assertThat(resultSet.getExecutionPlan().orElseThrow().prettyPrint(0, 2))
          .contains("MATCH NODE (place:Area)")
          .doesNotContain("INDEX SEEK (country:Area)");
    }
  }

  @Test
  void preservesAdditionalLabelsOnIndexedInListAnchor() {
    final String query = """
        MATCH (place:Area)-[:PART_OF*0..3]->(country:Area:Selected)
        WHERE country.id IN $countryIds
        RETURN country.id AS country, place.id AS place
        ORDER BY country, place""";
    final Map<String, Object> parameters = Map.of("countryIds", List.of("country", "region"));

    try (ResultSet resultSet = database.query("opencypher", query, parameters)) {
      assertThat(resultSet.stream()).isEmpty();
    }

    try (ResultSet resultSet = database.query("opencypher", "PROFILE " + query, parameters)) {
      while (resultSet.hasNext())
        resultSet.next();
      assertThat(resultSet.getExecutionPlan().orElseThrow().prettyPrint(0, 2))
          .contains("MATCH NODE (place:Area)")
          .doesNotContain("INDEX SEEK (country:Area)");
    }
  }

  @Test
  void keepsBoundTargetReversalIndependentOfIndexedAnchor() {
    final String query = """
        MATCH (country:Area {id: 'country'})
        OPTIONAL MATCH (place:Area)-[:PART_OF]->(country)
        WHERE place.id = 'region'
        WITH country, count(place) AS places
        RETURN country.id AS id, places""";

    try (ResultSet resultSet = database.query("opencypher", query)) {
      assertThat(resultSet.hasNext()).isTrue();
      final Result result = resultSet.next();
      assertThat(result.<String>getProperty("id")).isEqualTo("country");
      assertThat(result.<Long>getProperty("places")).isEqualTo(1L);
      assertThat(resultSet.hasNext()).isFalse();
    }
  }

  @Test
  void preservesRelationshipListInWrittenPathOrder() {
    final String query = """
        MATCH (place:Area)-[relationships:PART_OF*1..3]->(country:Area)
        WHERE country.id = $countryId
        RETURN place.id AS id, relationships AS relationships
        ORDER BY id""";

    final List<Result> rows;
    final ExecutionPlan plan;
    try (ResultSet resultSet = database.query("opencypher", "PROFILE " + query, Map.of("countryId", "country"))) {
      rows = resultSet.stream().toList();
      plan = resultSet.getExecutionPlan().orElseThrow();
    }

    assertThat(plan.getSteps().get(0).getDescription())
        .contains("MATCH NODE (country:Area)")
        .contains("[index: Area[id]]");

    final Result city = rows.stream()
        .filter(row -> "city".equals(row.<String>getProperty("id")))
        .findFirst()
        .orElseThrow();
    final List<Edge> relationships = city.getProperty("relationships");
    assertThat(relationships)
        .extracting(edge -> edge.<String>get("step"))
        .containsExactly("city-region", "region-country");
  }

  @Test
  void preservesUnboundedRelationshipListInWrittenPathOrder() {
    final String query = """
        MATCH (place:Area)-[relationships:PART_OF*1..]->(country:Area)
        WHERE country.id = $countryId
        RETURN place.id AS id, relationships AS relationships
        ORDER BY id""";

    final List<Result> rows;
    final ExecutionPlan plan;
    try (ResultSet resultSet = database.query("opencypher", "PROFILE " + query, Map.of("countryId", "country"))) {
      rows = resultSet.stream().toList();
      plan = resultSet.getExecutionPlan().orElseThrow();
    }

    assertThat(plan.getSteps().get(0).getDescription())
        .contains("MATCH NODE (country:Area)")
        .contains("[index: Area[id]]");

    final Result city = rows.stream()
        .filter(row -> "city".equals(row.<String>getProperty("id")))
        .findFirst()
        .orElseThrow();
    final List<Edge> relationships = city.getProperty("relationships");
    assertThat(relationships)
        .extracting(edge -> edge.<String>get("step"))
        .containsExactly("city-region", "region-country");
  }

  @Test
  void keepsSourceScanWhenIncomingAdjacencyIsUnavailable() {
    final String query = """
        MATCH (place:Area)-[:PART_OF_ONE_WAY*0..3]->(country:Area)
        WHERE country.id = $countryId
        RETURN DISTINCT place.id AS id
        ORDER BY id""";
    final Map<String, Object> parameters = Map.of("countryId", "country");

    assertThat(queryIds(query, parameters)).containsExactly("city", "country", "region");

    try (ResultSet resultSet = database.query("opencypher", "PROFILE " + query, parameters)) {
      while (resultSet.hasNext())
        resultSet.next();
      assertThat(resultSet.getExecutionPlan().orElseThrow().getSteps().get(0).getDescription())
          .contains("MATCH NODE (place:Area)")
          .doesNotContain("[index:");
    }
  }

  @Test
  void keepsSourceScanForUntypedRelationships() {
    final String query = """
        MATCH (place:Area)-[*0..3]->(country:Area)
        WHERE country.id = $countryId
        RETURN DISTINCT place.id AS id
        ORDER BY id""";
    final Map<String, Object> parameters = Map.of("countryId", "country");

    assertThat(queryIds(query, parameters)).containsExactly("city", "country", "region");

    try (ResultSet resultSet = database.query("opencypher", "PROFILE " + query, parameters)) {
      while (resultSet.hasNext())
        resultSet.next();
      assertThat(resultSet.getExecutionPlan().orElseThrow().getSteps().get(0).getDescription())
          .contains("MATCH NODE (place:Area)")
          .doesNotContain("[index:");
    }
  }

  private MutableVertex area(final String id) {
    return database.newVertex("Area").set("id", id).save();
  }

  private List<String> queryIds(final String query, final Map<String, Object> parameters) {
    try (ResultSet resultSet = database.query("opencypher", query, parameters)) {
      return resultSet.stream()
          .map((Result result) -> result.<String>getProperty("id"))
          .toList();
    }
  }
}
