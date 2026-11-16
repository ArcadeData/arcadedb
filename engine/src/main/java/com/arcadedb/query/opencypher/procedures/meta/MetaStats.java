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
import com.arcadedb.query.sql.executor.CommandContext;
import com.arcadedb.query.sql.executor.Result;
import com.arcadedb.query.sql.executor.ResultInternal;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.stream.Stream;

/**
 * Procedure: meta.stats()
 * <p>
 * Returns statistics about the database with APOC's columns: {@code labelCount}, {@code relTypeCount},
 * {@code propertyKeyCount}, {@code nodeCount}, {@code relCount}, {@code labels} (nodes per label), {@code relTypes}
 * (relationships per {@code ()-[:T]->()}, {@code (:L)-[:T]->()} and {@code ()-[:T]->(:L)} pattern), {@code relTypesCount}
 * (relationships per type) and {@code stats} (the first seven in one map). {@code value} is kept for the callers written
 * before these columns: a map of labelCount, relTypeCount, propertyKeyCount, nodeCount, relCount, labels and relTypes
 * keyed by type.
 * </p>
 * <p>
 * Example Cypher usage:
 * <pre>
 * CALL meta.stats()
 * YIELD nodeCount, relCount, labels
 * </pre>
 * </p>
 *
 * @author Luca Garulli (l.garulli--(at)--arcadedata.com)
 */
public class MetaStats extends AbstractMetaProcedure {
  public static final String NAME = "meta.stats";

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
    return 0;
  }

  @Override
  public String getDescription() {
    return "Returns statistics about the database including counts of nodes and relationships";
  }

  @Override
  public List<String> getYieldFields() {
    return List.of("labelCount", "relTypeCount", "propertyKeyCount", "nodeCount", "relCount", "labels", "relTypes",
        "relTypesCount", "stats", "value");
  }

  @Override
  public Stream<Result> execute(final Object[] args, final Result inputRow, final CommandContext context) {
    final Database database = context.getDatabase();
    final MetaGraphCounts counts = new MetaGraphCounts(database, new MetaLabels(database), true);

    // APOC's relTypes: every type under ()-[:T]->(), and once per label of a start or end node that has some
    final Map<String, Long> relTypes = new TreeMap<>();
    for (final Map.Entry<String, Long> type : counts.relTypes.entrySet()) {
      final String typeName = type.getKey();
      relTypes.put("()-[:" + typeName + "]->()", type.getValue());
      for (final Map.Entry<String, Long> out : counts.outCounts.get(typeName).entrySet())
        relTypes.put("(:" + out.getKey() + ")-[:" + typeName + "]->()", out.getValue());
      for (final Map.Entry<String, Long> in : counts.inCounts.get(typeName).entrySet())
        relTypes.put("()-[:" + typeName + "]->(:" + in.getKey() + ")", in.getValue());
    }

    final long labelCount = counts.labels.size();
    final long relTypeCount = counts.relTypes.size();
    final long propertyKeyCount = counts.propertyKeys.size();

    final Map<String, Object> stats = new LinkedHashMap<>();
    stats.put("labelCount", labelCount);
    stats.put("relTypeCount", relTypeCount);
    stats.put("propertyKeyCount", propertyKeyCount);
    stats.put("nodeCount", counts.nodeCount);
    stats.put("relCount", counts.relCount);
    stats.put("labels", counts.labels);
    stats.put("relTypes", relTypes);

    final Map<String, Object> value = new LinkedHashMap<>(stats);
    value.put("relTypes", counts.relTypes);

    final ResultInternal result = new ResultInternal();
    result.setProperty("labelCount", labelCount);
    result.setProperty("relTypeCount", relTypeCount);
    result.setProperty("propertyKeyCount", propertyKeyCount);
    result.setProperty("nodeCount", counts.nodeCount);
    result.setProperty("relCount", counts.relCount);
    result.setProperty("labels", counts.labels);
    result.setProperty("relTypes", relTypes);
    result.setProperty("relTypesCount", counts.relTypes);
    result.setProperty("stats", stats);
    result.setProperty("value", value);
    return Stream.of(result);
  }
}
