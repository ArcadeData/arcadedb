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
import com.arcadedb.exception.CommandParsingException;
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
import com.arcadedb.query.sql.executor.ResultInternal;
import com.arcadedb.query.sql.executor.ResultSet;
import com.arcadedb.schema.DocumentType;
import com.arcadedb.schema.Property;

import java.util.*;

public class GraphQLSchema {
  private final Database                          database;
  private final Map<String, ObjectTypeDefinition> objectTypeDefinitionMap = new HashMap<>();
  private ObjectTypeDefinition              queryDefinition;

  public GraphQLSchema(final Database database) {
    this.database = database;
  }

  public ResultSet execute(final String query) throws ParseException {
    final Document ast = GraphQLParser.parse(query);

    final List<Definition> definitions = ast.getDefinitions();
    if (!definitions.isEmpty()) {
      for (final Definition definition : definitions) {
        if (definition instanceof TypeSystemDefinition typeSystemDefinition) {

          final TypeDefinition type = typeSystemDefinition.getTypeDefinition();
          if (type instanceof ObjectTypeDefinition obj) {
            objectTypeDefinitionMap.put(obj.getName(), obj);

            if ("Query".equals(obj.getName()))
              // SPECIAL TYPE QUERY
              queryDefinition = obj;
          }

        } else if (definition instanceof OperationDefinition operationDefinition) {
          final OperationDefinition op = operationDefinition;
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
      throw new CommandParsingException("Error on executing multiple queries");

    String queryName = null;
    final Map<String, Object> arguments = null;

    try {
      final Selection selection = op.getSelectionSet().getSelections().getFirst();
      queryName = selection.getName();

      // HANDLE INTROSPECTION QUERIES
      if ("__schema".equals(queryName))
        return executeIntrospectionSchema(selection);
      else if ("__type".equals(queryName))
        return executeIntrospectionType(selection);
      else if ("__typename".equals(queryName))
        return executeIntrospectionTypename();
      if (queryDefinition != null) {
        for (final FieldDefinition f : queryDefinition.getFieldDefinitions()) {
          if (queryName.equals(f.getName())) {
            typeDefinition = f;
            returnType = getTypeFromField(f);
            for (final InputValueDefinition d : f.getArgumentsDefinition().getInputValueDefinitions())
              typeArgumentNames.add(d.getName().getValue());
            if (returnType != null)
              from = returnType.getName();
          }
        }
      }

      if (from == null)
        throw new CommandParsingException("Target type not defined for GraphQL query '" + queryName + "'");

      if (typeDefinition != null) {
        final Directives directives = typeDefinition.getDirectives();
        if (directives != null) {
          for (final Directive directive : directives.getDirectives()) {
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
          for (final Argument queryArgument : queryArguments.getList()) {
            final String argName = queryArgument.getName();
            if (!typeArgumentNames.contains(argName))
              throw new CommandParsingException("Parameter '" + argName + "' not defined in query");

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

    } catch (final CommandParsingException e) {
      throw e;
    } catch (final Exception e) {
      if (queryName != null)
        throw new CommandParsingException("Error on executing GraphQL query '" + queryName + "'", e);
      throw new CommandParsingException("Error on executing GraphQL query", e);
    }

  }

  private GraphQLResultSet parseNativeQueryDirective(final String language, final Directive directive, final Selection selection, final ObjectTypeDefinition returnType) {
    if (directive.getArguments() == null)
      throw new CommandParsingException(language.toUpperCase(Locale.ENGLISH) + " directive has no `statement` argument");

    String statement = null;
    Map<String, Object> arguments = null;
    SelectionSet projection = null;

    for (final Argument argument : directive.getArguments().getList()) {
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
      throw new CommandParsingException(language.toUpperCase() + " directive has no `statement` argument");

    final ResultSet resultSet = arguments != null ? database.query(language, statement, arguments) : database.query(language, statement);

    return new GraphQLResultSet(this, resultSet, projection != null ? projection.getSelections() : null, returnType);
  }

  private Map<String, Object> getArguments(final Arguments queryArguments) {
    Map<String, Object> arguments = null;

    if (queryArguments != null) {
      for (final Argument queryArgument : queryArguments.getList()) {
        final String argName = queryArgument.getName();
        final Object argValue = queryArgument.getValueWithVariable().getValue().getValue();

        if (arguments == null)
          arguments = new HashMap<>();

        arguments.put(argName, argValue);
      }
    }

    return arguments;
  }

  private ResultSet executeIntrospectionSchema(final Selection selection) {
    final InternalResultSet resultSet = new InternalResultSet();
    final ResultInternal schemaResult = new ResultInternal();

    final Field field = selection.getField();
    final SelectionSet selectionSet = field != null ? field.getSelectionSet() : null;

    if (selectionSet != null) {
      for (final Selection sub : selectionSet.getSelections()) {
        final String fieldName = sub.getName();
        if ("types".equals(fieldName))
          schemaResult.setProperty("types", buildTypeList(sub));
        else if ("queryType".equals(fieldName))
          schemaResult.setProperty("queryType", buildNameResult("Query"));
        else if ("mutationType".equals(fieldName))
          schemaResult.setProperty("mutationType", null);
        else if ("subscriptionType".equals(fieldName))
          schemaResult.setProperty("subscriptionType", null);
        else if ("directives".equals(fieldName))
          schemaResult.setProperty("directives", Collections.emptyList());
      }
    }

    resultSet.add(schemaResult);
    return resultSet;
  }

  private ResultSet executeIntrospectionType(final Selection selection) {
    final Field field = selection.getField();
    String typeName = null;

    if (field != null && field.getArguments() != null)
      for (final Argument arg : field.getArguments().getList())
        if ("name".equals(arg.getName()))
          typeName = arg.getValueWithVariable().getValue().getValue().toString();

    if (typeName == null)
      throw new CommandParsingException("__type query requires a 'name' argument");

    final SelectionSet selectionSet = field != null ? field.getSelectionSet() : null;
    final ResultInternal typeResult = buildTypeResult(typeName, selectionSet);

    if (typeResult == null)
      throw new CommandParsingException("Type '" + typeName + "' not found");

    final InternalResultSet resultSet = new InternalResultSet();
    resultSet.add(typeResult);
    return resultSet;
  }

  private ResultSet executeIntrospectionTypename() {
    final InternalResultSet resultSet = new InternalResultSet();
    final ResultInternal result = new ResultInternal();
    result.setProperty("__typename", "Query");
    resultSet.add(result);
    return resultSet;
  }

  private List<ResultInternal> buildTypeList(final Selection selection) {
    final List<ResultInternal> types = new ArrayList<>();
    final Set<String> addedTypes = new HashSet<>();

    final Field field = selection.getField();
    final SelectionSet selectionSet = field != null ? field.getSelectionSet() : null;

    // Add GraphQL-defined types
    for (final Map.Entry<String, ObjectTypeDefinition> entry : objectTypeDefinitionMap.entrySet()) {
      types.add(buildTypeResult(entry.getKey(), selectionSet));
      addedTypes.add(entry.getKey());
    }

    // Add database types not already covered by GraphQL definitions
    for (final DocumentType dbType : database.getSchema().getTypes()) {
      if (!addedTypes.contains(dbType.getName())) {
        types.add(buildDatabaseTypeResult(dbType, selectionSet));
        addedTypes.add(dbType.getName());
      }
    }

    // Add GraphQL built-in scalar types
    for (final String scalar : new String[] { "String", "Int", "Float", "Boolean", "ID" }) {
      if (!addedTypes.contains(scalar)) {
        final ResultInternal scalarResult = new ResultInternal();
        scalarResult.setProperty("name", scalar);
        scalarResult.setProperty("kind", "SCALAR");
        types.add(scalarResult);
      }
    }

    return types;
  }

  private ResultInternal buildTypeResult(final String typeName, final SelectionSet selectionSet) {
    final ObjectTypeDefinition objType = objectTypeDefinitionMap.get(typeName);
    if (objType != null)
      return buildGraphQLTypeResult(objType, selectionSet);

    // Check database types
    if (database.getSchema().existsType(typeName))
      return buildDatabaseTypeResult(database.getSchema().getType(typeName), selectionSet);

    // Check scalars
    if (Set.of("String", "Int", "Float", "Boolean", "ID").contains(typeName)) {
      final ResultInternal result = new ResultInternal();
      result.setProperty("name", typeName);
      result.setProperty("kind", "SCALAR");
      return result;
    }

    return null;
  }

  private ResultInternal buildGraphQLTypeResult(final ObjectTypeDefinition objType, final SelectionSet selectionSet) {
    final ResultInternal result = new ResultInternal();
    result.setProperty("name", objType.getName());
    result.setProperty("kind", "OBJECT");

    if (selectionSet != null) {
      for (final Selection sub : selectionSet.getSelections()) {
        if ("fields".equals(sub.getName())) {
          final List<ResultInternal> fields = new ArrayList<>();
          for (final FieldDefinition fd : objType.getFieldDefinitions()) {
            final ResultInternal fieldResult = new ResultInternal();
            fieldResult.setProperty("name", fd.getName());

            final Field subField = sub.getField();
            final SelectionSet fieldSelectionSet = subField != null ? subField.getSelectionSet() : null;
            if (fieldSelectionSet != null) {
              for (final Selection fieldSub : fieldSelectionSet.getSelections()) {
                if ("type".equals(fieldSub.getName()))
                  fieldResult.setProperty("type", buildFieldTypeInfo(fd));
              }
            }

            fields.add(fieldResult);
          }
          result.setProperty("fields", fields);
        }
      }
    }

    return result;
  }

  private ResultInternal buildDatabaseTypeResult(final DocumentType dbType, final SelectionSet selectionSet) {
    final ResultInternal result = new ResultInternal();
    result.setProperty("name", dbType.getName());
    result.setProperty("kind", "OBJECT");

    if (selectionSet != null) {
      for (final Selection sub : selectionSet.getSelections()) {
        if ("fields".equals(sub.getName())) {
          final List<ResultInternal> fields = new ArrayList<>();
          for (final Property prop : dbType.getProperties()) {
            final ResultInternal fieldResult = new ResultInternal();
            fieldResult.setProperty("name", prop.getName());

            final Field subField = sub.getField();
            final SelectionSet fieldSelectionSet = subField != null ? subField.getSelectionSet() : null;
            if (fieldSelectionSet != null) {
              for (final Selection fieldSub : fieldSelectionSet.getSelections()) {
                if ("type".equals(fieldSub.getName())) {
                  final ResultInternal typeInfo = new ResultInternal();
                  typeInfo.setProperty("name", mapDatabaseTypeToGraphQL(prop.getType()));
                  typeInfo.setProperty("kind", "SCALAR");
                  fieldResult.setProperty("type", typeInfo);
                }
              }
            }

            fields.add(fieldResult);
          }
          result.setProperty("fields", fields);
        }
      }
    }

    return result;
  }

  private ResultInternal buildFieldTypeInfo(final FieldDefinition fd) {
    final ResultInternal typeInfo = new ResultInternal();
    if (fd.getType().getListType() != null) {
      final String name = fd.getType().getListType().getType().getTypeName().getName();
      typeInfo.setProperty("name", name);
      typeInfo.setProperty("kind", "LIST");
    } else if (fd.getType().getTypeName() != null) {
      final String name = fd.getType().getTypeName().getName();
      typeInfo.setProperty("name", name);
      typeInfo.setProperty("kind", objectTypeDefinitionMap.containsKey(name) ? "OBJECT" : "SCALAR");
    }
    return typeInfo;
  }

  private ResultInternal buildNameResult(final String name) {
    final ResultInternal result = new ResultInternal();
    result.setProperty("name", name);
    return result;
  }

  private static String mapDatabaseTypeToGraphQL(final com.arcadedb.schema.Type type) {
    if (type == null)
      return "String";
    return switch (type) {
      case INTEGER, SHORT, BYTE -> "Int";
      case LONG -> "Long";
      case FLOAT -> "Float";
      case DOUBLE -> "Float";
      case BOOLEAN -> "Boolean";
      default -> "String";
    };
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
