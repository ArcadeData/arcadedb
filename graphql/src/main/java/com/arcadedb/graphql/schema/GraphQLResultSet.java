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
package com.arcadedb.graphql.schema;

import com.arcadedb.database.Document;
import com.arcadedb.graph.Vertex;
import com.arcadedb.graphql.parser.AbstractField;
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

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

/**
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
public class GraphQLResultSet implements ResultSet {
  private final GraphQLSchema        schema;
  private final ResultSet            resultSet;
  private final List<Selection>      projections;
  private final ObjectTypeDefinition returnType;

  private static class Projection {
    final String               name;
    final AbstractField        field;
    final ObjectTypeDefinition type;
    final List<Selection>      set;

    private Projection(final String name, final Field field, final ObjectTypeDefinition type, final List<Selection> set) {
      this.name = name;
      this.field = field;
      this.type = type;
      this.set = set;
    }
  }

  public GraphQLResultSet(final GraphQLSchema schema, final ResultSet resultSet, final List<Selection> projections, final ObjectTypeDefinition returnType) {
    if (resultSet == null)
      throw new IllegalArgumentException("NULL resultSet");

    this.schema = schema;
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
    return projections != null ? mapBySelections(resultSet.next(), projections) : mapByReturnType(resultSet.next(), returnType);
  }

  private GraphQLResult mapByReturnType(final Result current, final ObjectTypeDefinition type) {
    final List<Projection> projections = new ArrayList<>(type.getFieldDefinitions().size());
    // ADD ALL THE TYPE FIELDS AUTOMATICALLY
    for (FieldDefinition fieldDefinition : type.getFieldDefinitions()) {
      final ObjectTypeDefinition subType = schema.getTypeFromField(fieldDefinition);
      projections.add(new Projection(fieldDefinition.getName(), null, subType, null));
    }
    return mapProjections(current, projections);
  }

  private GraphQLResult mapBySelections(final Result current, final List<Selection> definedProjections) {
    final List<Projection> projections = new ArrayList<>(definedProjections.size());
    for (Selection fieldDefinition : definedProjections) {
      final SelectionSet set = fieldDefinition.getField().getSelectionSet();
      projections.add(new Projection(fieldDefinition.getName(), fieldDefinition.getField(), null, set != null ? set.getSelections() : null));
    }
    return mapProjections(current, projections);
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

  private Object evaluateDirectives(final Result current, final AbstractField fieldDefinition) {
    Object projectionValue = null;

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
                final Vertex vertex = current.getElement().get().asVertex();
                final Iterable<Vertex> connected = type != null ? vertex.getVertices(direction, type) : vertex.getVertices(direction);
                projectionValue = connected;
              } else if (current.getIdentity().isPresent()) {
                final Vertex vertex = current.getIdentity().get().asVertex();
                final Iterable<Vertex> connected = type != null ? vertex.getVertices(direction, type) : vertex.getVertices(direction);
                projectionValue = connected;
              }
            }
          }
        }
      }
    }
    return projectionValue;
  }

  private GraphQLResult mapProjections(final Result current, final List<Projection> projections) {
    final Map<String, Object> map = new HashMap<>();

    if (current.getElement().isPresent()) {
      final Document element = current.getElement().get();
      map.put("@rid", element.getIdentity());
    }

    for (Projection entry : projections) {
      final String projName = entry.name;

      Object projectionValue = current.getProperty(projName);

      if (projectionValue == null) {
        // TRY THE FIELD FIRST
        projectionValue = evaluateDirectives(current, entry.field);
        if (projectionValue == null) {
          // SEARCH IN THE SCHEMA
          final AbstractField fieldDefinition = returnType.getFieldDefinitionByName(projName);
          projectionValue = evaluateDirectives(current, fieldDefinition);
        }
      }

      final AbstractField field = entry.field;
      if (projectionValue == null && field != null) {
        if (field.getDirectives() != null) {
          for (Directive directive : field.getDirectives().getDirectives()) {
            if ("rid".equals(directive.getName())) {
              if (current.getElement().isPresent())
                projectionValue = current.getElement().get().getIdentity();
            } else if ("type".equals(directive.getName())) {
              if (current.getElement().isPresent())
                projectionValue = current.getElement().get().getTypeName();
            }
          }
        }
      }

      final List<Selection> selectionSet = entry.set;
      final ObjectTypeDefinition projectionType = entry.type;

      if (selectionSet != null) {
        if (projectionValue instanceof Map)
          projectionValue = mapBySelections(new ResultInternal((Map<String, Object>) projectionValue), selectionSet);
        else if (projectionValue instanceof Result)
          projectionValue = mapBySelections((Result) projectionValue, selectionSet);
        else if (projectionValue instanceof Iterable) {
          final List<Result> subResults = new ArrayList<>();
          for (Object o : ((Iterable) projectionValue)) {
            Result item;
            if (o instanceof Document)
              item = mapBySelections(new ResultInternal((Document) o), selectionSet);
            else if (o instanceof Result)
              item = mapBySelections((Result) o, selectionSet);
            else
              continue;

            subResults.add(item);
          }
          projectionValue = subResults;
        } else
          continue;
      } else if (projectionType != null) {
        if (projectionValue instanceof Map)
          projectionValue = mapByReturnType(new ResultInternal((Map<String, Object>) projectionValue), projectionType);
        else if (projectionValue instanceof Result)
          projectionValue = mapByReturnType((Result) projectionValue, projectionType);
        else if (projectionValue instanceof Iterable) {
          final List<Result> subResults = new ArrayList<>();
          for (Object o : ((Iterable) projectionValue)) {
            Result item;
            if (o instanceof Document)
              item = mapByReturnType(new ResultInternal((Document) o), projectionType);
            else if (o instanceof Result)
              item = mapByReturnType((Result) o, projectionType);
            else
              continue;

            subResults.add(item);
          }
          projectionValue = subResults;
        } else
          continue;
      }

      map.put(projName, projectionValue);
    }

    return new GraphQLResult(map);
  }
}
