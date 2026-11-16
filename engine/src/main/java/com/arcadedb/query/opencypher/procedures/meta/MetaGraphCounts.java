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
package com.arcadedb.query.opencypher.procedures.meta;

import com.arcadedb.database.Database;
import com.arcadedb.database.Record;
import com.arcadedb.graph.Edge;
import com.arcadedb.schema.DocumentType;
import com.arcadedb.schema.EdgeType;
import com.arcadedb.schema.VertexType;

import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;

/**
 * The counts {@code meta.stats} and {@code meta.graph} report, as APOC reads them from Neo4j's count store: nodes per
 * label, relationships per type, per (start label, type), per (type, end label), and which (start label, type, end
 * label) combinations occur. ArcadeDB keeps no per-label relationship counts, so the relationships of each readable type
 * are scanned once; their endpoints' labels come from the endpoint RIDs' buckets, without loading the nodes.
 */
final class MetaGraphCounts {
  /** label - nodes carrying it, labels with at least one node only */
  final Map<String, Long>                           labels    = new TreeMap<>();
  /** relationship type - relationships, types with at least one relationship only */
  final Map<String, Long>                           relTypes  = new TreeMap<>();
  /** type - start label - relationships of the type leaving a node with the label */
  final Map<String, Map<String, Long>>              outCounts = new TreeMap<>();
  /** type - end label - relationships of the type reaching a node with the label */
  final Map<String, Map<String, Long>>              inCounts  = new TreeMap<>();
  /** type - start label - end label - relationships of the type between the two labels */
  final Map<String, Map<String, Map<String, Long>>> between   = new TreeMap<>();
  /** the property keys declared by a readable type or seen on a record read, when asked for */
  final Set<String>                                 propertyKeys = new HashSet<>();
  long nodeCount;
  long relCount;

  /**
   * @param collectPropertyKeys whether to fill {@link #propertyKeys}: the declared properties, those of every relationship
   *                            scanned, and those of a sample of each vertex type's records (as many as APOC's default
   *                            {@code sample} picks, see {@link MetaConfig#skipFor})
   */
  MetaGraphCounts(final Database database, final MetaLabels metaLabels, final boolean collectPropertyKeys) {
    for (final VertexType type : metaLabels.vertexTypes()) {
      final long count = database.countType(type.getName(), false);
      nodeCount += count;
      if (collectPropertyKeys)
        collectPropertyKeys(database, type, count);
      if (count == 0L)
        continue;
      for (final String label : metaLabels.labelsOf(type))
        labels.merge(label, count, Long::sum);
    }

    for (final EdgeType type : metaLabels.edgeTypes()) {
      final String typeName = type.getName();
      final long count = database.countType(typeName, false);
      relCount += count;
      if (collectPropertyKeys)
        propertyKeys.addAll(type.getPolymorphicPropertyNames());
      if (count == 0L)
        continue;
      relTypes.put(typeName, count);

      final Map<String, Long> out = outCounts.computeIfAbsent(typeName, k -> new TreeMap<>());
      final Map<String, Long> in = inCounts.computeIfAbsent(typeName, k -> new TreeMap<>());
      final Map<String, Map<String, Long>> pairs = between.computeIfAbsent(typeName, k -> new TreeMap<>());
      final Iterator<Record> records = database.iterateType(typeName, false);
      while (records.hasNext()) {
        final Edge edge = records.next().asEdge();
        if (collectPropertyKeys)
          propertyKeys.addAll(edge.getPropertyNames());
        final List<String> startLabels = metaLabels.labelsOf(edge.getOut());
        final List<String> endLabels = metaLabels.labelsOf(edge.getIn());
        for (final String startLabel : startLabels) {
          out.merge(startLabel, 1L, Long::sum);
          final Map<String, Long> ends = pairs.computeIfAbsent(startLabel, k -> new TreeMap<>());
          for (final String endLabel : endLabels)
            ends.merge(endLabel, 1L, Long::sum);
        }
        for (final String endLabel : endLabels)
          in.merge(endLabel, 1L, Long::sum);
      }
    }
  }

  private void collectPropertyKeys(final Database database, final DocumentType type, final long count) {
    propertyKeys.addAll(type.getPolymorphicPropertyNames());
    if (count == 0L)
      return;
    final long skip = MetaConfig.DEFAULT.skipFor(count);
    final Iterator<Record> records = database.iterateType(type.getName(), false);
    for (long i = 0L; records.hasNext(); i++) {
      final Record record = records.next();
      if (i % skip == 0L)
        propertyKeys.addAll(record.asDocument().getPropertyNames());
    }
  }
}
