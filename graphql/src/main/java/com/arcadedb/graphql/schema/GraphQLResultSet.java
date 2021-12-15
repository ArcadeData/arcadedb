/*
 * Copyright 2021 Arcade Data Ltd
 *
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package com.arcadedb.graphql.schema;

import com.arcadedb.database.Document;
import com.arcadedb.graph.Vertex;
import com.arcadedb.graphql.parser.Argument;
import com.arcadedb.graphql.parser.Directive;
import com.arcadedb.graphql.parser.Directives;
import com.arcadedb.graphql.parser.Field;
import com.arcadedb.graphql.parser.FieldDefinition;
import com.arcadedb.graphql.parser.ObjectTypeDefinition;
import com.arcadedb.graphql.parser.Selection;
import com.arcadedb.graphql.parser.SelectionSet;
import com.arcadedb.query.sql.executor.ExecutionPlan;
import com.arcadedb.query.sql.executor.Result;
import com.arcadedb.query.sql.executor.ResultInternal;
import com.arcadedb.query.sql.executor.ResultSet;

import java.util.*;

/**
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
public class GraphQLResultSet implements ResultSet {
  private final ResultSet            resultSet;
  private final SelectionSet         projections;
  private final ObjectTypeDefinition returnType;

  public GraphQLResultSet(final ResultSet resultSet, final SelectionSet projections, final ObjectTypeDefinition returnType) {
    if (resultSet == null)
      throw new IllegalArgumentException("NULL resultSet");
    if (projections == null)
      throw new IllegalArgumentException("NULL projections");

    this.resultSet = resultSet;
    this.projections = projections;
    this.returnType = returnType;
  }

  @Override
  public boolean hasNext() {
    return resultSet.hasNext();
  }

  @Override
  public Result next() {
    return mapSelectionSet(resultSet.next(), projections.getSelections());
  }

  private GraphQLResult mapSelectionSet(final Result current, final List<Selection> selections) {
    final Map<String, Object> map = new HashMap<>();

    if (current.getElement().isPresent()) {
      final Document element = current.getElement().get();
      map.put("@rid", element.getIdentity());
    }

    for (Selection sel : selections) {
      final String projName = sel.getName();
      final Field field = sel.getField();

      Object projection = current.getProperty(projName);

      if (projection == null) {
        // SEARCH IN THE SCHEMA
        final FieldDefinition fieldDefinition = returnType.getFieldDefinitionByName(projName);
        if (fieldDefinition != null) {
          final Directives directives = fieldDefinition.getDirectives();
          if (directives != null) {
            for (Directive directive : directives.getDirectives()) {
              if ("relationship".equals(directive.getName())) {
                if (directive.getArguments() != null) {
                  String type = null;
                  Vertex.DIRECTION direction = Vertex.DIRECTION.BOTH;
                  for (Argument argument : directive.getArguments().getList()) {
                    if ("type".equals(argument.getName())) {
                      type = argument.getValueWithVariable().getValue().getValue().toString();
                    } else if ("direction".equals(argument.getName())) {
                      direction = Vertex.DIRECTION.valueOf(argument.getValueWithVariable().getValue().getValue().toString());
                    }
                  }

                  if (current.getElement().isPresent()) {
                    Vertex vertex = current.getElement().get().asVertex();
                    final Iterable<Vertex> connected = type != null ? vertex.getVertices(direction, type) : vertex.getVertices(direction);
                    projection = connected;
                  }
                }
              }
            }
          }
        }
      }

      if (projection == null) {
        if (field.getDirectives() != null) {
          for (Directive directive : field.getDirectives().getDirectives()) {
            if ("rid".equals(directive.getName())) {
              if (current.getElement().isPresent())
                projection = current.getElement().get().getIdentity();
            } else if ("type".equals(directive.getName())) {
              if (current.getElement().isPresent())
                projection = current.getElement().get().getTypeName();
            }
          }
        }
      }

      if (field.getSelectionSet() != null) {
        if (projection instanceof Map)
          projection = mapSelectionSet(new ResultInternal((Map<String, Object>) projection), field.getSelectionSet().getSelections());
        else if (projection instanceof Result)
          projection = mapSelectionSet((Result) projection, field.getSelectionSet().getSelections());
        else if (projection instanceof Iterable) {
          final List<Result> subResults = new ArrayList<>();
          for (Object o : ((Iterable) projection)) {
            Result item;
            if (o instanceof Document)
              item = mapSelectionSet(new ResultInternal((Document) o), field.getSelectionSet().getSelections());
            else if (o instanceof Result)
              item = mapSelectionSet((Result) projection, field.getSelectionSet().getSelections());
            else
              continue;

            subResults.add(item);
          }
          projection = subResults;
        } else
          continue;
      }

      map.put(projName, projection);
    }

    return new GraphQLResult(map);
  }

  @Override
  public void close() {
    resultSet.close();
  }

  @Override
  public Optional<ExecutionPlan> getExecutionPlan() {
    return Optional.empty();
  }

  @Override
  public Map<String, Long> getQueryStats() {
    return new HashMap<>();
  }
}
