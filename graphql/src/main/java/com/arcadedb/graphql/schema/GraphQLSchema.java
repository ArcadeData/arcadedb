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

import com.arcadedb.database.Database;
import com.arcadedb.exception.QueryParsingException;
import com.arcadedb.graphql.parser.Argument;
import com.arcadedb.graphql.parser.Arguments;
import com.arcadedb.graphql.parser.Definition;
import com.arcadedb.graphql.parser.Document;
import com.arcadedb.graphql.parser.Field;
import com.arcadedb.graphql.parser.FieldDefinition;
import com.arcadedb.graphql.parser.GraphQLParser;
import com.arcadedb.graphql.parser.ObjectTypeDefinition;
import com.arcadedb.graphql.parser.OperationDefinition;
import com.arcadedb.graphql.parser.ParseException;
import com.arcadedb.graphql.parser.Selection;
import com.arcadedb.graphql.parser.SelectionSet;
import com.arcadedb.graphql.parser.TypeDefinition;
import com.arcadedb.graphql.parser.TypeSystemDefinition;
import com.arcadedb.query.sql.executor.InternalResultSet;
import com.arcadedb.query.sql.executor.ResultSet;

import java.util.*;

public class GraphQLSchema {
  private Database                          database;
  private Map<String, ObjectTypeDefinition> objectTypeDefinitionMap = new HashMap<>();
  private ObjectTypeDefinition              queryDefinition;

  public GraphQLSchema(Database database) {
    this.database = database;
  }

  public ResultSet execute(final String query) throws ParseException {
    final Document ast = GraphQLParser.parse(query);

    //ast.dump("");

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
    String where = "";

    SelectionSet projection = null;
    ObjectTypeDefinition returnType = null;

    try {
      for (Selection selection : op.getSelectionSet().getSelections()) {
        final String queryName = selection.getName();
        if (queryDefinition != null) {
          for (FieldDefinition f : queryDefinition.getFieldDefinitions()) {
            if (queryName.equals(f.getName())) {
              final String returnTypeName = f.getType().getTypeName().getName();
              returnType = objectTypeDefinitionMap.get(returnTypeName);
              if (returnType == null)
                throw new QueryParsingException("Returning type '" + returnTypeName + "' not defined in GraphQL schema");
              from = returnType.getName();
              break;
            }
          }
        }

        final Field field = selection.getField();
        if (field != null) {
          final Arguments arguments = field.getArguments();
          for (Argument argument : arguments.getList()) {
            final String argName = argument.getName();
            final Object argValue = argument.getValueWithVariable().getValue().getValue();

            if (where.length() > 0)
              where += " and ";
            where += argName;
            where += " = ";

            if (argValue instanceof String)
              where += "\"";

            where += argValue;

            if (argValue instanceof String)
              where += "\"";
          }

          projection = field.getSelectionSet();
        }
      }

      if (from == null)
        throw new QueryParsingException("Target type not defined for GraphQL query");

      String query = "select from " + from;
      if (!where.isEmpty())
        query += " where " + where;

      return new GraphQLResultSet(database.query("sql", query), projection, returnType);

    } catch (QueryParsingException e) {
      throw e;
    } catch (Exception e) {
      if (op.getName() != null)
        throw new QueryParsingException("Error on executing GraphQL query '" + op.getName() + "'", e);
      throw new QueryParsingException("Error on executing GraphQL query", e);
    }
  }
}
