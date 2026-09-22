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
import com.arcadedb.graph.Vertex;
import com.arcadedb.query.sql.executor.ResultSet;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Regression test for GitHub issue #8117: {@code merge.node} declared {@code getMaxArgs() == 3} and read only
 * {@code (labels, matchProps, createProps)}, so APOC's fourth parameter - {@code onMatchProps}, applied when the
 * node already existed - had no implementation behind it at all: the four-argument call failed outright with
 * "expects 3 arguments but got 4", with no way to express "set these properties when the node already existed".
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class CypherMergeNodeOnMatchPropsIssue8117Test extends TestHelper {
  private static final String MERGE =
      "CALL merge.node($labels, $match, $create, $onMatch) YIELD node RETURN node";

  @Test
  void onMatchPropsIsAppliedWhenTheNodeAlreadyExists() {
    merge(List.of("Person8117"), Map.of("id", 1), Map.of("createdOnly", true), Map.of());
    final Vertex node = merge(List.of("Person8117"), Map.of("id", 1), Map.of("createdOnly", true),
        Map.of("visits", 2));

    assertThat(node.get("visits")).isEqualTo(2);
    // matchProps/createProps of the second (matching) call must not overwrite what the first call set.
    assertThat(node.get("createdOnly")).isEqualTo(true);
    assertThat(countOf("Person8117")).isEqualTo(1);
  }

  @Test
  void onMatchPropsIsNotAppliedOnTheCreateBranch() {
    final Vertex node = merge(List.of("Person8117Created"), Map.of("id", 1), Map.of(), Map.of("visits", 99));

    assertThat(node.getPropertyNames()).doesNotContain("visits");
  }

  @Test
  void omittingOnMatchPropsKeepsThePreviousThreeArgumentBehaviour() {
    try (final ResultSet resultSet = database.command("opencypher",
        "CALL merge.node($labels, $match, $create) YIELD node RETURN node",
        Map.of("labels", List.of("Person8117Legacy"), "match", Map.of("id", 1), "create", Map.of()))) {
      assertThat(resultSet.hasNext()).isTrue();
    }
    assertThat(countOf("Person8117Legacy")).isEqualTo(1);
  }

  @Test
  void aFifthArgumentIsStillRejected() {
    assertThatThrownBy(() -> database.command("opencypher",
        "CALL merge.node($labels, $match, $create, $onMatch, $extra) YIELD node RETURN node",
        Map.of("labels", List.of("Person8117"), "match", Map.of(), "create", Map.of(), "onMatch", Map.of(),
            "extra", Map.of())).close())
        .hasMessageContaining("5");
  }

  private Vertex merge(final List<String> labels, final Map<String, Object> matchProps,
                       final Map<String, Object> createProps, final Map<String, Object> onMatchProps) {
    try (final ResultSet resultSet = database.command("opencypher", MERGE,
        Map.of("labels", labels, "match", matchProps, "create", createProps, "onMatch", onMatchProps))) {
      assertThat(resultSet.hasNext()).isTrue();
      return resultSet.next().getProperty("node");
    }
  }

  private long countOf(final String typeName) {
    return database.countType(typeName, false);
  }
}
