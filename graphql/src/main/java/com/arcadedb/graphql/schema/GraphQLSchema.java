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

import com.arcadedb.database.Database;
import com.arcadedb.exception.QueryParsingException;
import com.arcadedb.graphql.parser.Argument;
import com.arcadedb.graphql.parser.Arguments;
import com.arcadedb.graphql.parser.Definition;
import com.arcadedb.graphql.parser.Directive;
import com.arcadedb.graphql.parser.Directives;
import com.arcadedb.graphql.parser.Document;
import com.arcadedb.graphql.parser.Field;
import com.arcadedb.graphql.parser.FieldDefinition;
import com.arcadedb.graphql.parser.GraphQLParser;
import com.arcadedb.graphql.parser.InputValueDefinition;
import com.arcadedb.graphql.parser.ObjectTypeDefinition;
import com.arcadedb.graphql.parser.OperationDefinition;
import com.arcadedb.graphql.parser.ParseException;
import com.arcadedb.graphql.parser.Selection;
import com.arcadedb.graphql.parser.SelectionSet;
import com.arcadedb.graphql.parser.TypeDefinition;
import com.arcadedb.graphql.parser.TypeSystemDefinition;
import com.arcadedb.query.sql.executor.InternalResultSet;
import com.arcadedb.query.sql.executor.ResultSet;

import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

public class GraphQLSchema {
  private final Database                          database;
  private final Map<String, ObjectTypeDefinition> objectTypeDefinitionMap = new HashMap<>();
  private ObjectTypeDefinition              queryDefinition;

  public GraphQLSchema(Database database) {
    this.database = database;
  }

  public ResultSet execute(final String query) throws ParseException {
    final Document ast = GraphQLParser.parse(query);

    final List<Definition> definitions = ast.getDefinitions();
    if (!definitions.isEmpty()) {
      for (Definition definition : definitions) {
        if (definition instanceof TypeSystemDefinition) {
          TypeSystemDefinition typeSystemDefinition = (TypeSystemDefinition) definition;

          final TypeDefinition type = typeSystemDefinition.getTypeDefinition();
          if (type instanceof ObjectTypeDefinition) {
            final ObjectTypeDefinition obj = (ObjectTypeDefinition) type;
            objectTypeDefinitionMap.put(obj.getName(), obj);

            if ("Query".equals(obj.getName()))
              // SPECIAL TYPE QUERY
              queryDefinition = obj;
          }

        } else if (definition instanceof OperationDefinition) {
          final OperationDefinition op = ((OperationDefinition) definition);
          if (op.isQuery()) {
            return executeQuery(op);
          } else
            throw new UnsupportedOperationException("GraphQL mutations not supported yet");
        }
      }
    }

    return new InternalResultSet();
  }

  private ResultSet executeQuery(final OperationDefinition op) {
    String from = null;

    SelectionSet projection = null;
    ObjectTypeDefinition returnType = null;
    FieldDefinition typeDefinition = null;
    final Set<String> typeArgumentNames = new HashSet<>();

    if (op.getSelectionSet().getSelections().size() > 1)
      throw new QueryParsingException("Error on executing multiple queries");

    String queryName = null;
    Map<String, Object> arguments = null;

    try {
      Selection selection = op.getSelectionSet().getSelections().get(0);
      queryName = selection.getName();
      if (queryDefinition != null) {
        for (FieldDefinition f : queryDefinition.getFieldDefinitions()) {
          if (queryName.equals(f.getName())) {
            typeDefinition = f;
            returnType = getTypeFromField(f);
            for (InputValueDefinition d : f.getArgumentsDefinition().getInputValueDefinitions())
              typeArgumentNames.add(d.getName().getValue());
            if (returnType != null)
              from = returnType.getName();
          }
        }
      }

      if (from == null)
        throw new QueryParsingException("Target type not defined for GraphQL query '" + queryName + "'");

      if (typeDefinition != null) {
        final Directives directives = typeDefinition.getDirectives();
        if (directives != null) {
          for (Directive directive : directives.getDirectives()) {
            final String directiveName = directive.getName();

            // TODO: REFACTOR THIS IN MULTIPLE PLUGGABLE CLASSES
            if ("sql".equals(directiveName) ||
                "gremlin".equals(directiveName) ||
                "cypher".equals(directiveName))
              return parseNativeQueryDirective(directiveName, directive, selection, returnType);
          }
        }
      }

      String where = "";
      final Field field = selection.getField();
      if (field != null) {
        final Arguments queryArguments = field.getArguments();
        if (queryArguments != null)
          for (Argument queryArgument : queryArguments.getList()) {
            final String argName = queryArgument.getName();
            if (!typeArgumentNames.contains(argName))
              throw new QueryParsingException("Parameter '" + argName + "' not defined in query");

            final Object argValue = queryArgument.getValueWithVariable().getValue().getValue();

            if (where.length() > 0)
              where += " and ";

            if ("where".equals(argName)) {
              where += argValue;
            } else {
              where += argName;
              where += " = ";

              if (argValue instanceof String)
                where += "\"";

              where += argValue;

              if (argValue instanceof String)
                where += "\"";
            }
          }

        projection = field.getSelectionSet();
      }

      String query = "select from " + from;
      if (!where.isEmpty())
        query += " where " + where;

      final ResultSet resultSet = arguments != null ? database.query("sql", query, arguments) : database.query("sql", query);

      return new GraphQLResultSet(this, resultSet, projection != null ? projection.getSelections() : null, returnType);

    } catch (QueryParsingException e) {
      throw e;
    } catch (Exception e) {
      if (queryName != null)
        throw new QueryParsingException("Error on executing GraphQL query '" + queryName + "'", e);
      throw new QueryParsingException("Error on executing GraphQL query", e);
    }

  }

  private GraphQLResultSet parseNativeQueryDirective(final String language, Directive directive, Selection selection, ObjectTypeDefinition returnType) {
    if (directive.getArguments() == null)
      throw new QueryParsingException(language.toUpperCase() + " directive has no `statement` argument");

    String statement = null;
    Map<String, Object> arguments = null;
    SelectionSet projection = null;

    for (Argument argument : directive.getArguments().getList()) {
      if ("statement".equals(argument.getName())) {
        statement = argument.getValueWithVariable().getValue().getValue().toString();

        final Field field = selection.getField();
        if (field != null) {
          arguments = getArguments(field.getArguments());
          projection = field.getSelectionSet();
        }
      }
    }

    if (statement == null)
      throw new QueryParsingException(language.toUpperCase() + " directive has no `statement` argument");

    final ResultSet resultSet = arguments != null ? database.query(language, statement, arguments) : database.query(language, statement);

    return new GraphQLResultSet(this, resultSet, projection != null ? projection.getSelections() : null, returnType);
  }

  private Map<String, Object> getArguments(final Arguments queryArguments) {
    Map<String, Object> arguments = null;

    if (queryArguments != null) {
      for (Argument queryArgument : queryArguments.getList()) {
        final String argName = queryArgument.getName();
        final Object argValue = queryArgument.getValueWithVariable().getValue().getValue();

        if (arguments == null)
          arguments = new HashMap<>();

        arguments.put(argName, argValue);
      }
    }

    return arguments;
  }

  public ObjectTypeDefinition getTypeFromField(final FieldDefinition fieldDefinition) {
    ObjectTypeDefinition returnType = null;
    if (fieldDefinition.getType().getTypeName() != null) {
      final String returnTypeName = fieldDefinition.getType().getTypeName().getName();
      returnType = objectTypeDefinitionMap.get(returnTypeName);

    } else if (fieldDefinition.getType().getListType() != null) {
      final String returnTypeName = fieldDefinition.getType().getListType().getType().getTypeName().getName();
      returnType = objectTypeDefinitionMap.get(returnTypeName);
    }
    return returnType;
  }
}
