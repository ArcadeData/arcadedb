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
package com.arcadedb.gremlin;

import com.arcadedb.gremlin.support.TraversalPlans;
import com.arcadedb.schema.DocumentType;
import com.arcadedb.schema.Schema;
import com.arcadedb.schema.Type;
import org.apache.tinkerpop.gremlin.process.traversal.P;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversalSource;
import org.apache.tinkerpop.gremlin.structure.Element;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * The {@code hasLabel(X)} rewrite over a type hierarchy. ArcadeDB reads {@code hasLabel(X)} polymorphically - X and its
 * sub-types, as SQL's {@code FROM X} does - and every rewrite of it must agree:
 * <ul>
 *   <li>#8249: with an index declared on a SUPER type of X, the index push-down answered with the parent's and the
 *   siblings' elements too, because the index spans the whole super type and the label filter had been removed.</li>
 *   <li>#8250: {@code hasLabel(X).count()} counted only X's own buckets while {@code hasLabel(X)} iterates X's
 *   sub-types too, so the count disagreed with the number of elements yielded.</li>
 * </ul>
 * Schema: {@code Person <- Employee <- Manager} and {@code Person <- Contractor}, with ONE index, on
 * {@code Person(name)}; one element of each type, all with {@code name = 'x'}. The edge hierarchy mirrors it.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class Issue8249HasLabelTypeHierarchyTest {

  private ArcadeGraph graph;

  @BeforeEach
  void setup() {
    graph = ArcadeGraph.open("./target/test-gremlin-8249-hierarchy");
    final Schema schema = graph.getDatabase().getSchema();

    final DocumentType person = schema.createVertexType("Person");
    person.createProperty("name", Type.STRING);
    person.createProperty("age", Type.INTEGER);
    person.createTypeIndex(Schema.INDEX_TYPE.LSM_TREE, false, "name");
    schema.createVertexType("Employee").addSuperType("Person");
    schema.createVertexType("Manager").addSuperType("Employee");
    schema.createVertexType("Contractor").addSuperType("Person");

    final DocumentType link = schema.createEdgeType("Link");
    link.createProperty("name", Type.STRING);
    link.createTypeIndex(Schema.INDEX_TYPE.LSM_TREE, false, "name");
    schema.createEdgeType("Reports").addSuperType("Link");
    schema.createEdgeType("Escalates").addSuperType("Reports");
    schema.createEdgeType("Contracts").addSuperType("Link");

    graph.getDatabase().transaction(() -> {
      int age = 10;
      ArcadeVertex previous = null;
      for (final String type : List.of("Person", "Employee", "Manager", "Contractor")) {
        final ArcadeVertex v = graph.addVertex(type);
        v.property("name", "x");
        v.property("age", age);
        age += 10;
        previous = v;
      }
      for (final String type : List.of("Link", "Reports", "Escalates", "Contracts"))
        previous.addEdge(type, previous, "name", "x");
    });
  }

  @AfterEach
  void teardown() {
    if (graph != null)
      graph.drop();
  }

  @Test
  void anIndexOnASuperTypeDoesNotLeakTheParentOrTheSiblings() {
    assertThat(TraversalPlans.hasStepOfType(g().V().hasLabel("Employee").has("name", "x"), ArcadeFilterByIndexStep.class))
        .as("the super type's index must still be used as a candidate generator").isTrue();

    assertThat(labels(g().V().hasLabel("Employee").has("name", "x").toList())).containsExactlyInAnyOrder("Employee", "Manager");
    assertThat(labels(g().V().hasLabel("Manager").has("name", "x").toList())).containsExactly("Manager");
    assertThat(labels(g().V().hasLabel("Contractor").has("name", "x").toList())).containsExactly("Contractor");
    assertThat(labels(g().V().hasLabel("Person").has("name", "x").toList()))
        .containsExactlyInAnyOrder("Person", "Employee", "Manager", "Contractor");
  }

  @Test
  void theIndexedAndTheUnindexedFormsAgree() {
    // age IS NOT INDEXED: THE SAME TRAVERSAL GOES THROUGH ArcadeFilterByTypeStep AND IS THE REFERENCE ANSWER
    for (final String type : List.of("Person", "Employee", "Manager", "Contractor"))
      assertThat(labels(g().V().hasLabel(type).has("name", "x").toList()))
          .as(type).containsExactlyInAnyOrderElementsOf(labels(g().V().hasLabel(type).has("age", P.gte(0)).toList()));
  }

  @Test
  void theSqlAnswerIsTheSame() {
    for (final String type : List.of("Person", "Employee", "Manager", "Contractor"))
      assertThat(g().V().hasLabel(type).has("name", "x").count().next())
          .as(type).isEqualTo(graph.getDatabase().query("sql", "SELECT count(*) AS c FROM " + type + " WHERE name = 'x'")
              .next().<Long>getProperty("c"));
  }

  @Test
  void anIndexedRangeOnASuperTypeIsFilteredToo() {
    assertThat(labels(g().V().hasLabel("Employee").has("name", P.gte("a")).toList())).containsExactlyInAnyOrder("Employee", "Manager");
  }

  @Test
  void edgesGoThroughTheSameRewrite() {
    assertThat(labels(g().E().hasLabel("Reports").has("name", "x").toList())).containsExactlyInAnyOrder("Reports", "Escalates");
    assertThat(labels(g().E().hasLabel("Contracts").has("name", "x").toList())).containsExactly("Contracts");
  }

  @Test
  void countAgreesWithTheElementsYielded() {
    // #8250
    for (final String type : List.of("Person", "Employee", "Manager", "Contractor"))
      assertThat(g().V().hasLabel(type).count().next()).as(type).isEqualTo((long) g().V().hasLabel(type).toList().size());
    assertThat(g().V().hasLabel("Person").count().next()).isEqualTo(4L);
    assertThat(g().V().hasLabel("Employee").count().next()).isEqualTo(2L);

    for (final String type : List.of("Link", "Reports", "Escalates", "Contracts"))
      assertThat(g().E().hasLabel(type).count().next()).as(type).isEqualTo((long) g().E().hasLabel(type).toList().size());
    assertThat(g().E().hasLabel("Link").count().next()).isEqualTo(4L);

    // THE UNLABELLED COUNT STAYS DE-DUPLICATED: EVERY ELEMENT ONCE, NOT ONCE PER ANCESTOR
    assertThat(g().V().count().next()).isEqualTo(4L);
    assertThat(g().E().count().next()).isEqualTo(4L);
  }

  @Test
  void aBucketLabelCountsThatBucket() {
    final String bucket = graph.getDatabase().getSchema().getType("Employee").getBuckets(false).getFirst().getName();
    final long inBucket = graph.getDatabase().countBucket(bucket);
    assertThat(g().V().hasLabel("bucket:" + bucket).count().next()).isEqualTo(inBucket);
    assertThat(g().V().hasLabel("bucket:" + bucket).toList()).hasSize((int) inBucket);
    assertThat(g().E().hasLabel("bucket:" + bucket).count().next()).as("a vertex bucket on an edge traversal").isEqualTo(0L);
  }

  private GraphTraversalSource g() {
    return graph.traversal();
  }

  private static List<String> labels(final List<? extends Element> elements) {
    return elements.stream().map(Element::label).toList();
  }
}
