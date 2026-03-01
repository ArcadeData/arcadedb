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
package com.arcadedb.query.select;

import com.arcadedb.TestHelper;
import com.arcadedb.graph.Vertex;
import com.arcadedb.schema.Schema;
import com.arcadedb.schema.Type;

import org.junit.jupiter.api.Test;

import java.util.*;
import java.util.stream.*;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Tests for graph traversal via the Select API.
 *
 * Graph structure:
 * Alice --FRIENDS--> Bob --FRIENDS--> Charlie
 * Alice --FRIENDS--> Diana
 * Bob --WORKS_WITH--> Eve
 */
public class SelectTraversalTest extends TestHelper {

  public SelectTraversalTest() {
    autoStartTx = false;
  }

  @Override
  protected void beginTest() {
    database.getSchema().createVertexType("Person")//
        .createProperty("name", Type.STRING)//
        .createIndex(Schema.INDEX_TYPE.LSM_TREE, true);
    database.getSchema().createEdgeType("FRIENDS");
    database.getSchema().createEdgeType("WORKS_WITH");

    database.transaction(() -> {
      final Vertex alice = database.newVertex("Person").set("name", "Alice").save();
      final Vertex bob = database.newVertex("Person").set("name", "Bob").save();
      final Vertex charlie = database.newVertex("Person").set("name", "Charlie").save();
      final Vertex diana = database.newVertex("Person").set("name", "Diana").save();
      final Vertex eve = database.newVertex("Person").set("name", "Eve").save();

      alice.newEdge("FRIENDS", bob).save();
      alice.newEdge("FRIENDS", diana).save();
      bob.newEdge("FRIENDS", charlie).save();
      bob.newEdge("WORKS_WITH", eve).save();
    });
  }

  @Test
  void okTraverseOut() {
    final List<Vertex> friends = database.select().fromType("Person")//
        .where().property("name").eq().value("Alice")//
        .vertices()//
        .traverseOut("FRIENDS")//
        .toList();

    assertThat(friends).hasSize(2);
    final Set<String> names = friends.stream().map(v -> v.getString("name")).collect(Collectors.toSet());
    assertThat(names).containsExactlyInAnyOrder("Bob", "Diana");
  }

  @Test
  void okTraverseIn() {
    final List<Vertex> result = database.select().fromType("Person")//
        .where().property("name").eq().value("Bob")//
        .vertices()//
        .traverseIn("FRIENDS")//
        .toList();

    assertThat(result).hasSize(1);
    assertThat(result.getFirst().getString("name")).isEqualTo("Alice");
  }

  @Test
  void okTraverseBoth() {
    final List<Vertex> result = database.select().fromType("Person")//
        .where().property("name").eq().value("Bob")//
        .vertices()//
        .traverseBoth("FRIENDS")//
        .toList();

    assertThat(result).hasSize(2);
    final Set<String> names = result.stream().map(v -> v.getString("name")).collect(Collectors.toSet());
    assertThat(names).containsExactlyInAnyOrder("Alice", "Charlie");
  }

  @Test
  void okMultiHopChaining() {
    // Alice -> (Bob, Diana) -> (Charlie, Eve via FRIENDS only = Charlie)
    final Set<String> names = database.select().fromType("Person")//
        .where().property("name").eq().value("Alice")//
        .vertices()//
        .traverseOut("FRIENDS")//
        .thenOut("FRIENDS")//
        .stream()//
        .map(v -> v.getString("name"))//
        .collect(Collectors.toSet());

    assertThat(names).containsExactly("Charlie");
  }

  @Test
  void okEdgeTypeFilter() {
    // Bob has FRIENDS (Alice in, Charlie out) and WORKS_WITH (Eve out)
    // Traverse OUT without filter => Charlie + Eve
    final List<Vertex> allOut = database.select().fromType("Person")//
        .where().property("name").eq().value("Bob")//
        .vertices()//
        .traverseOut()//
        .toList();

    assertThat(allOut).hasSize(2);

    // Traverse OUT with WORKS_WITH only
    final List<Vertex> worksWithOnly = database.select().fromType("Person")//
        .where().property("name").eq().value("Bob")//
        .vertices()//
        .traverseOut("WORKS_WITH")//
        .toList();

    assertThat(worksWithOnly).hasSize(1);
    assertThat(worksWithOnly.getFirst().getString("name")).isEqualTo("Eve");
  }

  @Test
  void okTraverseStream() {
    final long count = database.select().fromType("Person")//
        .where().property("name").eq().value("Alice")//
        .vertices()//
        .traverseOut("FRIENDS")//
        .stream()//
        .count();

    assertThat(count).isEqualTo(2);
  }

  @Test
  void okEmptySource() {
    final List<Vertex> result = database.select().fromType("Person")//
        .where().property("name").eq().value("NonExistent")//
        .vertices()//
        .traverseOut("FRIENDS")//
        .toList();

    assertThat(result).isEmpty();
  }
}
