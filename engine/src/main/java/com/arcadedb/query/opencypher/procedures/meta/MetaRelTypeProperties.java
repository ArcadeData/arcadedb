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
import com.arcadedb.query.sql.executor.CommandContext;
import com.arcadedb.query.sql.executor.Result;
import com.arcadedb.query.sql.executor.ResultInternal;
import com.arcadedb.query.sql.executor.WorkGuard;
import com.arcadedb.schema.EdgeType;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Stream;

/**
 * Procedure: meta.relTypeProperties([config])
 * <p>
 * Returns, as APOC's {@code apoc.meta.relTypeProperties}, one row per property of each relationship type between two
 * label sets found in the data: {@code relType} ({@code :`T`}), {@code sourceNodeLabels}, {@code targetNodeLabels},
 * {@code propertyName}, {@code propertyTypes}, {@code mandatory}, {@code propertyObservations} and
 * {@code totalObservations}. The relationships read are the outgoing ones of the nodes sampled label by label with APOC's
 * {@code sample} (default 1000, -1 reads them all), at most {@code maxRels} (default 100) per node; a relationship group
 * with no property gets one row with a null property. Properties the schema declares are reported too, with 0
 * observations when no relationship read carried them. The config also takes {@code includeLabels},
 * {@code excludeLabels}, {@code includeRels} and {@code excludeRels}.
 * </p>
 * <p>
 * Example Cypher usage:
 * <pre>
 * CALL meta.relTypeProperties()
 * YIELD relType, sourceNodeLabels, targetNodeLabels, propertyName, propertyTypes
 * </pre>
 * </p>
 *
 * @author Luca Garulli (l.garulli--(at)--arcadedata.com)
 */
public class MetaRelTypeProperties extends AbstractMetaProcedure {
  public static final String NAME = "meta.reltypeproperties";

  /** One relationship type between one start label set and one end label set. */
  private record RelGroup(String type, List<String> sourceLabels, List<String> targetLabels) {
  }

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
    return "Returns the properties found on each relationship type between label sets, with their types and how often they occur";
  }

  @Override
  public List<String> getYieldFields() {
    return List.of("relType", "sourceNodeLabels", "targetNodeLabels", "propertyName", "propertyTypes", "mandatory",
        "propertyObservations", "totalObservations");
  }

  @Override
  public Stream<Result> execute(final Object[] args, final Result inputRow, final CommandContext context) {
    final Database database = context.getDatabase();
    final MetaConfig config = config(args);
    final MetaLabels labels = new MetaLabels(database);
    final long maxRels = config.maxRels();
    final WorkGuard guard = newWorkGuard(context);

    final Map<RelGroup, MetaPropertyProfile> groups = new LinkedHashMap<>();
    forEachSampledNode(database, labels, config, guard, (type, record) -> {
      final Vertex vertex = record.asVertex();
      final List<String> sourceLabels = labels.labelsOf(type);
      long read = 0L;
      int walked = 0;
      for (final Edge edge : vertex.getEdges(Vertex.DIRECTION.OUT)) {
        if (read >= maxRels)
          break;
        // a supernode whose relationships are mostly of excluded types is walked to the end without counting any
        guard.checkPeriodically(walked++);
        final String relType = edge.getTypeName();
        if (!config.acceptsRelType(relType) || !labels.isReadableEdgeType(relType))
          continue;
        ++read;
        groups.computeIfAbsent(new RelGroup(relType, sourceLabels, labels.labelsOf(edge.getIn())),
            k -> new MetaPropertyProfile()).observe(edge);
      }
    });

    for (final EdgeType type : labels.edgeTypes()) {
      if (type.getPolymorphicPropertyNames().isEmpty() || !config.acceptsRelType(type.getName()))
        continue;
      boolean seen = false;
      for (final Map.Entry<RelGroup, MetaPropertyProfile> group : groups.entrySet())
        if (group.getKey().type().equals(type.getName())) {
          group.getValue().declare(type);
          seen = true;
        }
      if (!seen)
        groups.computeIfAbsent(new RelGroup(type.getName(), List.of(), List.of()), k -> new MetaPropertyProfile()).declare(type);
    }

    final List<Result> results = new ArrayList<>();
    for (final Map.Entry<RelGroup, MetaPropertyProfile> group : groups.entrySet()) {
      final Map<String, Object> columns = new LinkedHashMap<>();
      columns.put("relType", MetaLabels.pattern(List.of(group.getKey().type())));
      columns.put("sourceNodeLabels", group.getKey().sourceLabels());
      columns.put("targetNodeLabels", group.getKey().targetLabels());
      results.addAll(group.getValue().rows(columns));
    }
    return results.stream();
  }
}
