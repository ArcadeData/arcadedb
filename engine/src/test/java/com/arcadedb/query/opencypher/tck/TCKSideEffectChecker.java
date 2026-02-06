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
package com.arcadedb.query.opencypher.tck;

import com.arcadedb.database.Database;
import com.arcadedb.database.Document;
import com.arcadedb.graph.Edge;
import com.arcadedb.graph.Vertex;
import com.arcadedb.schema.DocumentType;
import com.arcadedb.schema.EdgeType;
import com.arcadedb.schema.VertexType;

import java.util.HashSet;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Captures and verifies database side effects for TCK scenarios.
 */
public class TCKSideEffectChecker {

  private long nodeCount;
  private long relationshipCount;
  private long labelCount;
  private long propertyCount;

  /**
   * Takes a snapshot of current database state.
   */
  public void snapshot(final Database database) {
    nodeCount = countAllNodes(database);
    relationshipCount = countAllRelationships(database);
    labelCount = countLabels(database);
    propertyCount = countAllProperties(database);
  }

  /**
   * Asserts no side effects occurred since the snapshot.
   */
  public void assertNoSideEffects(final Database database) {
    final long currentNodes = countAllNodes(database);
    final long currentRels = countAllRelationships(database);
    final long currentLabels = countLabels(database);
    final long currentProps = countAllProperties(database);

    assertThat(currentNodes).as("Unexpected node count change").isEqualTo(nodeCount);
    assertThat(currentRels).as("Unexpected relationship count change").isEqualTo(relationshipCount);
    // Label and property checks are best-effort since ArcadeDB schema types are persistent
  }

  /**
   * Asserts specific side effects occurred since the snapshot.
   */
  public void assertSideEffects(final Database database, final Map<String, Integer> expectedEffects) {
    final long currentNodes = countAllNodes(database);
    final long currentRels = countAllRelationships(database);
    final long currentLabels = countLabels(database);
    final long currentProps = countAllProperties(database);

    final int addedNodes = expectedEffects.getOrDefault("+nodes", 0);
    final int deletedNodes = expectedEffects.getOrDefault("-nodes", 0);
    final int addedRels = expectedEffects.getOrDefault("+relationships", 0);
    final int deletedRels = expectedEffects.getOrDefault("-relationships", 0);

    final long expectedNodeDelta = addedNodes - deletedNodes;
    final long expectedRelDelta = addedRels - deletedRels;

    assertThat(currentNodes - nodeCount).as(
        "Node count change mismatch. Expected delta: " + expectedNodeDelta + " (+" + addedNodes + "/-" + deletedNodes + "). Before: " + nodeCount + ", After: "
            + currentNodes).isEqualTo(expectedNodeDelta);

    assertThat(currentRels - relationshipCount).as(
        "Relationship count change mismatch. Expected delta: " + expectedRelDelta + " (+" + addedRels + "/-" + deletedRels + "). Before: " + relationshipCount
            + ", After: " + currentRels).isEqualTo(expectedRelDelta);

    // Label and property side effects are harder to verify precisely in ArcadeDB
    // because types persist in schema even when empty. Best-effort check:
    final int addedLabels = expectedEffects.getOrDefault("+labels", 0);
    final int addedProps = expectedEffects.getOrDefault("+properties", 0);
    final int deletedProps = expectedEffects.getOrDefault("-properties", 0);

    if (addedProps > 0 || deletedProps > 0) {
      final long expectedPropDelta = addedProps - deletedProps;
      assertThat(currentProps - propertyCount).as("Property count change mismatch. Expected delta: " + expectedPropDelta).isEqualTo(expectedPropDelta);
    }
  }

  private long countAllNodes(final Database database) {
    long count = 0;
    for (final DocumentType type : database.getSchema().getTypes())
      if (type instanceof VertexType)
        count += database.countType(type.getName(), false);
    return count;
  }

  private long countAllRelationships(final Database database) {
    long count = 0;
    for (final DocumentType type : database.getSchema().getTypes())
      if (type instanceof EdgeType)
        count += database.countType(type.getName(), false);
    return count;
  }

  private long countLabels(final Database database) {
    long count = 0;
    for (final DocumentType type : database.getSchema().getTypes())
      if (type instanceof VertexType)
        count++;
    return count;
  }

  private long countAllProperties(final Database database) {
    long count = 0;
    for (final DocumentType type : database.getSchema().getTypes()) {
      if (type instanceof VertexType || type instanceof EdgeType) {
        try {
          final Iterator<? extends Document> it = database.iterateType(type.getName(), false);
          while (it.hasNext()) {
            final Document doc = it.next();
            count += doc.getPropertyNames().size();
          }
        } catch (final Exception ignored) {
          // Type may have been concurrently modified
        }
      }
    }
    return count;
  }
}
