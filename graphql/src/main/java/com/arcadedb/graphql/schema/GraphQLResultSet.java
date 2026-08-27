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
import com.arcadedb.database.EmbeddedDocument;
import com.arcadedb.database.RID;
import com.arcadedb.graph.Edge;
import com.arcadedb.graph.Vertex;
import com.arcadedb.graphql.parser.AbstractField;
import com.arcadedb.graphql.parser.Argument;
import com.arcadedb.graphql.parser.Directive;
import com.arcadedb.graphql.parser.Directives;
import com.arcadedb.graphql.parser.Field;
import com.arcadedb.graphql.parser.FieldDefinition;
import com.arcadedb.graphql.parser.FieldWithAlias;
import com.arcadedb.graphql.parser.ObjectTypeDefinition;
import com.arcadedb.graphql.parser.Selection;
import com.arcadedb.graphql.parser.SelectionSet;
import com.arcadedb.query.sql.executor.ExecutionPlan;
import com.arcadedb.query.sql.executor.Result;
import com.arcadedb.query.sql.executor.ResultInternal;
import com.arcadedb.query.sql.executor.ResultSet;

import java.util.*;

import static com.arcadedb.schema.Property.CAT_PROPERTY;
import static com.arcadedb.schema.Property.RID_PROPERTY;
import static com.arcadedb.schema.Property.TYPE_PROPERTY;

/**
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
public class GraphQLResultSet implements ResultSet {
  private final GraphQLSchema        schema;
  private final ResultSet            resultSet;
  private final List<Selection>      projections;
  private final ObjectTypeDefinition returnType;

  /**
   * The variable values of the operation, for the directives written in the query document. Never used for a
   * directive declared in the schema: see {@link #evaluateDirectives}.
   */
  private final Map<String, Object> variables;

  /**
   * The types currently being expanded from the schema by {@link #mapByReturnType}, innermost last. It guards the
   * automatic expansion against a cyclic schema (e.g. {@code Book.authors -> Author.wrote -> Book}), which would
   * otherwise recurse until the stack overflows once directives are resolved against the right type. It is only
   * touched by {@code mapByReturnType}, always in a push/pop pair, so it is empty again at the end of every
   * {@link #next()}: an explicit selection set states its own depth and is never limited by it.
   */
  private final List<ObjectTypeDefinition> expansionPath = new ArrayList<>(4);

  /**
   * @param name        output key: the alias when present, otherwise the field name
   * @param fieldName   real field/property name to resolve, ignoring any alias
   * @param field       the field as written in the query document, carrying any inline directive
   * @param schemaField the field of the schema type this selection belongs to, carrying any schema-declared
   *                    directive. Resolved against the type of the enclosing selection, not against the top-level
   *                    query return type: see issue #6833
   * @param type        the object type this field returns, when the schema declares one
   * @param set         the sub-selections written in the query document, if any
   */
  private record Projection(String name, String fieldName, AbstractField field, FieldDefinition schemaField,
                            ObjectTypeDefinition type, List<Selection> set) {
  }

  public GraphQLResultSet(final GraphQLSchema schema, final ResultSet resultSet, final List<Selection> projections,
      final ObjectTypeDefinition returnType, final Map<String, Object> variables) {
    if (resultSet == null)
      throw new IllegalArgumentException("NULL resultSet");

    this.schema = schema;
    this.resultSet = resultSet;
    this.projections = projections;
    this.returnType = returnType;
    this.variables = variables;
  }

  @Override
  public boolean hasNext() {
    return resultSet.hasNext();
  }

  @Override
  public Result next() {
    return projections != null ?
        mapBySelections(resultSet.next(), projections, returnType) :
        mapByReturnType(resultSet.next(), returnType);
  }

  private GraphQLResult mapByReturnType(final Result current, final ObjectTypeDefinition type) {
    expansionPath.add(type);
    try {
      final List<Projection> projections = new ArrayList<>(type.getFieldDefinitions().size());
      // ADD ALL THE TYPE FIELDS AUTOMATICALLY
      for (final FieldDefinition fieldDefinition : type.getFieldDefinitions()) {
        final ObjectTypeDefinition subType = schema.getTypeFromField(fieldDefinition);
        if (subType != null && isBeingExpanded(subType))
          // THE SCHEMA IS CYCLIC ON THIS FIELD: STOP THE AUTOMATIC EXPANSION HERE RATHER THAN RECURSE FOREVER.
          // ONLY A QUERY THAT ASKS FOR THE FIELD EXPLICITLY GETS IT, AND THEN AT THE DEPTH IT ASKS FOR
          continue;

        projections.add(
            new Projection(fieldDefinition.getName(), fieldDefinition.getName(), null, fieldDefinition, subType, null));
      }
      return mapProjections(current, projections);
    } finally {
      expansionPath.removeLast();
    }
  }

  /**
   * @param parentType the schema type the selections are written against - the type of the enclosing field, not the
   *                   top-level query return type, so a schema directive declared two levels deep is found (#6833)
   */
  private GraphQLResult mapBySelections(final Result current, final List<Selection> definedProjections,
      final ObjectTypeDefinition parentType) {
    final List<Projection> projections = new ArrayList<>(definedProjections.size());
    for (final Selection selection : definedProjections) {
      // A selection written as `alias: field` parses into fieldWithAlias (name = the real field,
      // alias carried by Selection.getName()); an unaliased selection parses into field instead.
      // Neither is set for an ellipsis selection (fragment spread / inline fragment).
      final FieldWithAlias aliasedField = selection.getFieldWithAlias();
      final Field          plainField = selection.getField();
      final AbstractField  field = aliasedField != null ? aliasedField : plainField;
      final String         fieldName = aliasedField != null ? aliasedField.getName() : selection.getName();
      final SelectionSet   set;
      if (aliasedField != null)
        set = aliasedField.getSelectionSet();
      else
        set = plainField != null ? plainField.getSelectionSet() : null;

      final FieldDefinition schemaField = parentType != null ? parentType.getFieldDefinitionByName(fieldName) : null;
      final ObjectTypeDefinition subType = schemaField != null ? schema.getTypeFromField(schemaField) : null;

      projections.add(new Projection(selection.getName(), fieldName, field, schemaField, subType,
          set != null ? set.getSelections() : null));
    }
    return mapProjections(current, projections);
  }

  /**
   * Identity lookup over the (at most a handful of entries deep) automatic-expansion path. The schema keeps one
   * {@link ObjectTypeDefinition} instance per type name, so reference equality is the right comparison and costs
   * nothing.
   */
  private boolean isBeingExpanded(final ObjectTypeDefinition type) {
    for (int i = 0; i < expansionPath.size(); i++)
      if (expansionPath.get(i) == type)
        return true;
    return false;
  }

  @Override
  public void close() {
    resultSet.close();
  }

  @Override
  public Optional<ExecutionPlan> getExecutionPlan() {
    return Optional.empty();
  }

  /**
   * @param variables the operation's variable values when {@code fieldDefinition} is a field of the query document,
   *                  whose inline directives can reference them; {@code null} for a field of the schema, whose
   *                  directives are authored in the SDL and have no operation in scope to take a variable from
   */
  private Object evaluateDirectives(final Result current, final AbstractField fieldDefinition,
      final Map<String, Object> variables) {
    Object projectionValue = null;

    if (fieldDefinition != null) {
      final Directives directives = fieldDefinition.getDirectives();
      if (directives != null) {
        for (final Directive directive : directives.getDirectives()) {
          if ("relationship".equals(directive.getName())) {
            if (directive.getArguments() != null) {
              String type = null;
              Vertex.DIRECTION direction = Vertex.DIRECTION.BOTH;
              for (final Argument argument : directive.getArguments().getList()) {
                if ("type".equals(argument.getName())) {
                  final Object value = GraphQLSchema.resolveValue(argument.getValueWithVariable(), variables);
                  type = value != null ? value.toString() : null;
                } else if ("direction".equals(argument.getName())) {
                  final Object value = GraphQLSchema.resolveValue(argument.getValueWithVariable(), variables);
                  if (value != null)
                    direction = Vertex.DIRECTION.valueOf(value.toString());
                }
              }

              if (current.getElement().isPresent()) {
                final Vertex vertex = current.getElement().get().asVertex();
                final Iterable<Vertex> connected =
                    type != null ? vertex.getVertices(direction, type) : vertex.getVertices(direction);
                projectionValue = connected;
              } else if (current.getIdentity().isPresent()) {
                final Vertex vertex = current.getIdentity().get().asVertex();
                final Iterable<Vertex> connected =
                    type != null ? vertex.getVertices(direction, type) : vertex.getVertices(direction);
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
      final RID rid = element.getIdentity();
      if (rid != null)
        map.put(RID_PROPERTY, rid);
      map.put(TYPE_PROPERTY, element.getTypeName());
      map.put(CAT_PROPERTY, element instanceof Vertex ? "v" : element instanceof Edge ? "e" : "d");
    }

    for (final Projection entry : projections) {
      final String projName = entry.name();
      final String realName = entry.fieldName();

      Object projectionValue = current.getProperty(realName);

      if (projectionValue == null && current.getElement().isPresent())
        // PROPERTY NOT FOUND IN PROJECTION, TRY DIRECTLY FROM THE ELEMENT (E.G. CYPHER RETURN)
        projectionValue = current.getElement().get().get(realName);

      if (projectionValue == null) {
        // TRY THE FIELD FIRST
        // AN INLINE DIRECTIVE IS WRITTEN IN THE QUERY DOCUMENT, SO IT CAN REFERENCE THE OPERATION'S VARIABLES
        projectionValue = evaluateDirectives(current, entry.field(), variables);
        if (projectionValue == null)
          // SEARCH IN THE SCHEMA, IN THE TYPE THIS SELECTION BELONGS TO. A DIRECTIVE DECLARED THERE IS PART OF THE
          // SDL, NOT OF THE OPERATION, SO NO VARIABLE IS IN SCOPE FOR IT
          projectionValue = evaluateDirectives(current, entry.schemaField(), null);
      }

      final AbstractField field = entry.field();
      if (projectionValue == null && field != null) {
        if (field.getDirectives() != null) {
          for (final Directive directive : field.getDirectives().getDirectives()) {
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

      final List<Selection> selectionSet = entry.set();
      final ObjectTypeDefinition projectionType = entry.type();

      if (selectionSet != null) {
        switch (projectionValue) {
        case Map m -> projectionValue = mapBySelections(new ResultInternal(m), selectionSet, projectionType);
        case EmbeddedDocument emb -> projectionValue = mapBySelections(new ResultInternal(emb), selectionSet, projectionType);
        case Result result -> projectionValue = mapBySelections(result, selectionSet, projectionType);
        case Iterable iterable -> {
          final List<Result> subResults = new ArrayList<>();
          for (final Object o : iterable) {
            final Result item;
            if (o instanceof Document document)
              item = mapBySelections(new ResultInternal(document), selectionSet, projectionType);
            else if (o instanceof Result result)
              item = mapBySelections(result, selectionSet, projectionType);
            else
              continue;

            subResults.add(item);
          }
          projectionValue = subResults;
        } else {
          continue;
        }
      } else if (projectionType != null) {
        switch (projectionValue) {
        case Map m -> projectionValue = mapByReturnType(new ResultInternal(m), projectionType);
        // MIRRORS THE Map/Result ARMS: THIS BRANCH IS THE ONE WHERE selectionSet IS NULL BY CONSTRUCTION, SO
        // DELEGATING TO mapBySelections() WITH IT WAS A GUARANTEED NPE. SEE ISSUE #6835
        case EmbeddedDocument emb -> projectionValue = mapByReturnType(new ResultInternal(emb), projectionType);
        case Result result -> projectionValue = mapByReturnType(result, projectionType);
        case Iterable iterable -> {
          final List<Result> subResults = new ArrayList<>();
          for (final Object o : iterable) {
            final Result item;
            if (o instanceof Document document)
              item = mapByReturnType(new ResultInternal(document), projectionType);
            else if (o instanceof Result result)
              item = mapByReturnType(result, projectionType);
            else
              continue;

            subResults.add(item);
          }
          projectionValue = subResults;
        } else {
          continue;
        }
      }

      map.put(projName, projectionValue);
    }

    return new GraphQLResult(map);
  }
}
