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
import com.arcadedb.graph.Edge;
import com.arcadedb.graph.Vertex;
import com.arcadedb.graph.VirtualEdge;
import com.arcadedb.graph.VirtualVertex;
import com.arcadedb.query.sql.executor.CommandContext;
import com.arcadedb.query.sql.executor.Result;
import com.arcadedb.query.sql.executor.ResultInternal;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Stream;

/**
 * Procedure: meta.graph([config])
 * <p>
 * Returns the graph's schema as a virtual graph, as APOC's {@code apoc.meta.graph}: one virtual node per label with nodes,
 * carrying {@code name} and {@code count}, and one virtual relationship per (start label, type, end label) that occurs,
 * carrying {@code type}, {@code out} (relationships of the type leaving a node with the start label), {@code in}
 * (reaching a node with the end label) and {@code count} (all relationships of the type). Nodes are sorted by label,
 * relationships by type, start label and end label. The optional config map is accepted and, as APOC 5.26 does with
 * it for this procedure, does not narrow the answer.
 * </p>
 * <p>
 * Cost: the relationship records of every readable edge type are scanned once (linear in the number of relationships, where
 * Neo4j reads its count store), because ArcadeDB keeps no per-label relationship counts. The scan stops at the command's
 * timeout, on termination and on interrupt.
 * </p>
 * <p>
 * Example Cypher usage:
 * <pre>
 * CALL meta.graph()
 * YIELD nodes, relationships
 * </pre>
 * </p>
 *
 * @author Luca Garulli (l.garulli--(at)--arcadedata.com)
 */
public class MetaGraph extends AbstractMetaProcedure {
  public static final String NAME = "meta.graph";

  @Override
  public String getName() {
    return NAME;
  }

  @Override
  public int getMinArgs() {
    return 0;
  }

  @Override
  public int getMaxArgs() {
    return 1;
  }

  @Override
  public String getDescription() {
    return "Returns a virtual graph of the labels and relationship types in the database, with their counts";
  }

  @Override
  public List<String> getYieldFields() {
    return List.of("nodes", "relationships");
  }

  @Override
  public Stream<Result> execute(final Object[] args, final Result inputRow, final CommandContext context) {
    final Database database = context.getDatabase();
    // Validated as a map, otherwise unused: APOC answers the whole graph whatever includeLabels/includeRels say
    optionalMapArg(args, 0, "config");
    final MetaGraphCounts counts = new MetaGraphCounts(database, new MetaLabels(database), false, newWorkGuard(context));

    final Map<String, Vertex> nodesByLabel = new LinkedHashMap<>();
    for (final Map.Entry<String, Long> label : counts.labels.entrySet()) {
      final Map<String, Object> properties = new LinkedHashMap<>();
      properties.put("name", label.getKey());
      properties.put("count", label.getValue());
      nodesByLabel.put(label.getKey(), new VirtualVertex(database, List.of(label.getKey()), properties));
    }

    final List<Edge> relationships = new ArrayList<>();
    for (final Map.Entry<String, Map<String, Map<String, Long>>> type : counts.between.entrySet()) {
      final String typeName = type.getKey();
      for (final Map.Entry<String, Map<String, Long>> start : type.getValue().entrySet())
        for (final String endLabel : start.getValue().keySet()) {
          final Vertex from = nodesByLabel.get(start.getKey());
          final Vertex to = nodesByLabel.get(endLabel);
          if (from == null || to == null)
            continue;
          final Map<String, Object> properties = new LinkedHashMap<>();
          properties.put("type", typeName);
          properties.put("out", counts.outCounts.get(typeName).get(start.getKey()));
          properties.put("in", counts.inCounts.get(typeName).get(endLabel));
          properties.put("count", counts.relTypes.get(typeName));
          relationships.add(new VirtualEdge(database, from, typeName, properties, to));
        }
    }

    final ResultInternal result = new ResultInternal();
    result.setProperty("nodes", new ArrayList<>(nodesByLabel.values()));
    result.setProperty("relationships", relationships);
    return Stream.of(result);
  }
}
