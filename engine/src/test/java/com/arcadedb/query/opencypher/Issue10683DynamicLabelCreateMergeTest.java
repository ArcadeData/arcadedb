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

import com.arcadedb.TestHelper;
import com.arcadedb.query.sql.executor.Result;
import com.arcadedb.query.sql.executor.ResultSet;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Issue #10683: {@code CREATE (n:$($label))} and {@code MERGE (n:$($label) {...})} accepted the Cypher 25 dynamic-label syntax
 * and created the node with no label at all (in the {@code ~NO_LABEL~} type), with no error. {@code SET}, {@code REMOVE},
 * {@code MATCH} and {@code WHERE} already interpolated the expression (#7059, #7151). Both write clauses read
 * {@code NodePattern.getLabels()} only and never looked at {@code getDynamicLabels()}.
 */
class Issue10683DynamicLabelCreateMergeTest extends TestHelper {

  @Test
  void createWithDynamicLabelCreatesTheLabelledNode() {
    final List<Object> labels = firstColumn(
        "CREATE (n:$($label) {uuid: 'd'}) RETURN labels(n) AS l", Map.of("label", "Finding"));
    assertThat(labels).containsExactly(List.of("Finding"));

    assertThat(uuids("MATCH (n:Finding) RETURN n.uuid AS uuid")).containsExactly("d");
    assertThat(uuids("MATCH (n:$($label)) RETURN n.uuid AS uuid", Map.of("label", "Finding"))).containsExactly("d");
    assertThat(schemaTypeNames()).doesNotContain(Labels.NO_LABEL_TYPE).noneMatch(name -> name.contains("$("));
  }

  @Test
  void mergeWithDynamicLabelCreatesTheLabelledNodeOnceAndMatchesIt() {
    final Map<String, Object> params = Map.of("label", "Finding");
    final List<Object> first = firstColumn("MERGE (n:$($label) {uuid: 'e'}) RETURN labels(n) AS l", params);
    final List<Object> second = firstColumn("MERGE (n:$($label) {uuid: 'e'}) RETURN labels(n) AS l", params);
    assertThat(first).containsExactly(List.of("Finding"));
    assertThat(second).containsExactly(List.of("Finding"));

    // the second MERGE matched the node the first created
    assertThat(uuids("MATCH (n:Finding) RETURN n.uuid AS uuid")).containsExactly("e");
    assertThat(schemaTypeNames()).doesNotContain(Labels.NO_LABEL_TYPE);
  }

  @Test
  void mergeWithDynamicLabelDoesNotMatchANodeOfAnotherLabel() {
    database.transaction(() -> database.command("opencypher", "CREATE (n:Other {uuid: 'x'})").close());

    database.transaction(() -> database.command("opencypher", "MERGE (n:$($label) {uuid: 'x'})",
        Map.of("label", "Finding")).close());

    assertThat(uuids("MATCH (n:Finding) RETURN n.uuid AS uuid")).containsExactly("x");
    assertThat(uuids("MATCH (n:Other) RETURN n.uuid AS uuid")).containsExactly("x");
  }

  @Test
  void aListOfDynamicLabelsAndStaticLabelsAreCombined() {
    final List<Object> labels = firstColumn(
        "CREATE (n:Base:$($labels) {uuid: 'l'}) RETURN labels(n) AS l", Map.of("labels", List.of("Finding", "Person")));
    assertThat(labels).hasSize(1);
    @SuppressWarnings("unchecked") final List<String> carried = (List<String>) labels.getFirst();
    assertThat(carried).containsExactlyInAnyOrder("Base", "Finding", "Person");

    assertThat(uuids("MATCH (n:Base:Finding:Person) RETURN n.uuid AS uuid")).containsExactly("l");
  }

  @Test
  void theLabelIsEvaluatedPerRow() {
    final List<Map<String, Object>> rows = List.of(Map.of("id", 1, "label", "Alpha"), Map.of("id", 2, "label", "Beta"));
    database.transaction(() -> {
      database.command("opencypher", "UNWIND $rows AS r CREATE (n:$(r.label) {id: r.id})", Map.of("rows", rows)).close();
      database.command("opencypher", "UNWIND $rows AS r MERGE (n:$(r.label) {id: r.id + 10})", Map.of("rows", rows)).close();
    });

    assertThat(uuids("MATCH (n:Alpha) RETURN toString(n.id) AS uuid")).containsExactlyInAnyOrder("1", "11");
    assertThat(uuids("MATCH (n:Beta) RETURN toString(n.id) AS uuid")).containsExactlyInAnyOrder("2", "12");
    assertThat(schemaTypeNames()).doesNotContain(Labels.NO_LABEL_TYPE);
  }

  @Test
  void aDynamicLabelInAPathPatternIsResolved() {
    database.transaction(() -> database.command("opencypher",
        "CREATE (a:$($from) {uuid: 'a'})-[:KNOWS]->(b:$($to) {uuid: 'b'})", Map.of("from", "Person", "to", "Finding")).close());

    assertThat(uuids("MATCH (a:Person)-[:KNOWS]->(b:Finding) RETURN a.uuid + b.uuid AS uuid")).containsExactly("ab");
  }

  @Test
  void aValueThatIsNotALabelFailsTheStatement() {
    assertThatThrownBy(() -> database.transaction(() -> database.command("opencypher",
        "CREATE (n:$($label) {uuid: 'z'})", Map.of("label", 42)).close())).hasMessageContaining("dynamic label");
    assertThatThrownBy(() -> database.transaction(() -> database.command("opencypher",
        "MERGE (n:$($label) {uuid: 'z'})", Map.of("label", "")).close())).hasMessageContaining("label");

    assertThat(database.getSchema().existsType(Labels.NO_LABEL_TYPE)).as("no node was created").isFalse();
  }

  private List<Object> firstColumn(final String query, final Map<String, Object> params) {
    final List<Object> out = new ArrayList<>();
    database.transaction(() -> {
      try (final ResultSet rs = database.command("opencypher", query, params)) {
        while (rs.hasNext())
          out.add(rs.next().getProperty("l"));
      }
    });
    return out;
  }

  private List<String> uuids(final String query) {
    return uuids(query, Map.of());
  }

  private List<String> uuids(final String query, final Map<String, Object> params) {
    final List<String> out = new ArrayList<>();
    try (final ResultSet rs = database.query("opencypher", query, params)) {
      while (rs.hasNext()) {
        final Result row = rs.next();
        out.add(row.getProperty("uuid"));
      }
    }
    return out;
  }

  private List<String> schemaTypeNames() {
    return database.getSchema().getTypes().stream().map(t -> t.getName()).toList();
  }
}
