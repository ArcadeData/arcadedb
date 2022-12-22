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
package com.arcadedb.query.sql.executor;

import com.arcadedb.database.Document;
import com.arcadedb.exception.TimeoutException;
import com.arcadedb.graph.Edge;
import com.arcadedb.graph.Vertex;
import com.arcadedb.index.Index;
import com.arcadedb.schema.DocumentType;
import com.arcadedb.schema.Schema;

import java.util.*;
import java.util.stream.*;

/**
 * Returns an OResult containing metadata regarding the schema types.
 *
 * @author Luigi Dell'Aquila (luigi.dellaquila-(at)-gmail.com)
 */
public class FetchFromSchemaTypesStep extends AbstractExecutionStep {

  private final List<ResultInternal> result = new ArrayList<>();

  private int  cursor = 0;
  private long cost   = 0;

  public FetchFromSchemaTypesStep(final CommandContext ctx, final boolean profilingEnabled) {
    super(ctx, profilingEnabled);
  }

  @Override
  public ResultSet syncPull(final CommandContext ctx, final int nRecords) throws TimeoutException {
    getPrev().ifPresent(x -> x.syncPull(ctx, nRecords));

    if (cursor == 0) {
      long begin = profilingEnabled ? System.nanoTime() : 0;
      try {
        final Schema schema = ctx.getDatabase().getSchema();

        final List<String> orderedTypes = schema.getTypes().stream().map(x -> x.getName()).sorted(String::compareToIgnoreCase).collect(Collectors.toList());
        for (String typeName : orderedTypes) {
          final DocumentType type = schema.getType(typeName);

          final ResultInternal r = new ResultInternal();
          result.add(r);

          r.setProperty("name", type.getName());

          String t = "?";

          if (type.getType() == Document.RECORD_TYPE)
            t = "document";
          else if (type.getType() == Vertex.RECORD_TYPE)
            t = "vertex";
          else if (type.getType() == Edge.RECORD_TYPE)
            t = "edge";

          r.setProperty("type", t);

          List<String> parents = type.getSuperTypes().stream().map(pt -> pt.getName()).collect(Collectors.toList());
          r.setProperty("parentTypes", parents);

          final List<ResultInternal> propertiesTypes = type.getPropertyNames().stream().sorted(String::compareToIgnoreCase).map(name -> type.getProperty(name))
              .map(property -> {
                final ResultInternal propRes = new ResultInternal();
                propRes.setProperty("id", property.getId());
                propRes.setProperty("name", property.getName());
                propRes.setProperty("type", property.getType());

                final Map<String, Object> customs = new HashMap<>();
                for (Object customKey : property.getCustomKeys().stream().sorted(String::compareToIgnoreCase).toArray())
                  customs.put((String) customKey, type.getCustomValue((String) customKey));
                propRes.setProperty("custom", customs);

                return propRes;
              }).collect(Collectors.toList());
          r.setProperty("properties", propertiesTypes);

          final List<ResultInternal> indexes = type.getAllIndexes(false).stream().sorted(Comparator.comparing(Index::getName)).map(typeIndex -> {
            final ResultInternal propRes = new ResultInternal();
            propRes.setProperty("name", typeIndex.getName());
            propRes.setProperty("typeName", typeIndex.getTypeName());
            propRes.setProperty("type", typeIndex.getType());
            propRes.setProperty("properties", typeIndex.getPropertyNames());
            propRes.setProperty("automatic", typeIndex.isAutomatic());
            propRes.setProperty("unique", typeIndex.isUnique());
            return propRes;
          }).collect(Collectors.toList());
          r.setProperty("indexes", indexes);

          final Map<String, Object> customs = new HashMap<>();
          for (Object customKey : type.getCustomKeys().stream().sorted(String::compareToIgnoreCase).toArray())
            customs.put((String) customKey, type.getCustomValue((String) customKey));
          r.setProperty("custom", customs);

        }
      } finally {
        if (profilingEnabled) {
          cost += (System.nanoTime() - begin);
        }
      }
    }
    return new ResultSet() {
      @Override
      public boolean hasNext() {
        return cursor < result.size();
      }

      @Override
      public Result next() {
        return result.get(cursor++);
      }

      @Override
      public void close() {
        result.clear();
      }

      @Override
      public Optional<ExecutionPlan> getExecutionPlan() {
        return Optional.empty();
      }

      @Override
      public Map<String, Long> getQueryStats() {
        return null;
      }

      @Override
      public void reset() {
        cursor = 0;
      }
    };
  }

  @Override
  public String prettyPrint(int depth, int indent) {
    String spaces = ExecutionStepInternal.getIndent(depth, indent);
    String result = spaces + "+ FETCH DATABASE METADATA TYPES";
    if (profilingEnabled) {
      result += " (" + getCostFormatted() + ")";
    }
    return result;
  }

  @Override
  public long getCost() {
    return cost;
  }
}
