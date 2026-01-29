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
import com.arcadedb.schema.DocumentType;
import com.arcadedb.schema.EdgeType;
import com.arcadedb.schema.Schema;
import com.arcadedb.schema.VertexType;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Stream;

/**
 * Procedure: meta.stats()
 * <p>
 * Returns statistics about the database including counts of nodes and relationships.
 * </p>
 * <p>
 * Example Cypher usage:
 * <pre>
 * CALL meta.stats()
 * YIELD value
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
    return List.of("value");
  }

  @Override
  public Stream<Result> execute(final Object[] args, final Result inputRow, final CommandContext context) {
    final Database database = context.getDatabase();
    final Schema schema = database.getSchema();

    final Map<String, Object> stats = new HashMap<>();

    long totalNodes = 0;
    long totalRelationships = 0;
    final Map<String, Long> labelCounts = new HashMap<>();
    final Map<String, Long> relTypeCounts = new HashMap<>();

    for (final DocumentType type : schema.getTypes()) {
      final long count = database.countType(type.getName(), true);

      if (type instanceof VertexType) {
        totalNodes += count;
        labelCounts.put(type.getName(), count);
      } else if (type instanceof EdgeType) {
        totalRelationships += count;
        relTypeCounts.put(type.getName(), count);
      }
    }

    stats.put("labelCount", labelCounts.size());
    stats.put("relTypeCount", relTypeCounts.size());
    stats.put("nodeCount", totalNodes);
    stats.put("relCount", totalRelationships);
    stats.put("labels", labelCounts);
    stats.put("relTypes", relTypeCounts);

    final ResultInternal result = new ResultInternal();
    result.setProperty("value", stats);

    return Stream.of(result);
  }
}
