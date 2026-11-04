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

import com.arcadedb.graphql.parser.FieldDefinition;
import com.arcadedb.graphql.parser.ObjectTypeDefinition;
import com.arcadedb.graphql.parser.Type;

import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * The object types an SDL registered on a database, as one immutable value (issue #10764). One {@link GraphQLSchema} serves every
 * request thread of its database, so the registry is never changed in place: a document that declares types builds the next
 * registry beside the current one and publishes it with a single volatile write, and an operation reads the registry once and
 * resolves every type it meets against that one. A reader therefore sees either the whole SDL before a document or the whole SDL
 * after it, never a map being resized under its iterator nor a document applied halfway.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
final class GraphQLTypeRegistry {
  static final GraphQLTypeRegistry EMPTY = new GraphQLTypeRegistry(Collections.emptyMap());

  private static final String QUERY_TYPE = "Query";

  private final Map<String, ObjectTypeDefinition> types;
  private final ObjectTypeDefinition              queryDefinition;

  private GraphQLTypeRegistry(final Map<String, ObjectTypeDefinition> types) {
    this.types = types;
    this.queryDefinition = types.get(QUERY_TYPE);
  }

  /**
   * The registry with {@code definitions} declared on top of this one, in order, a later declaration of a name replacing the
   * earlier one; this registry when there is nothing to declare.
   */
  GraphQLTypeRegistry with(final List<ObjectTypeDefinition> definitions) {
    if (definitions.isEmpty())
      return this;
    final Map<String, ObjectTypeDefinition> next = new HashMap<>(types);
    for (final ObjectTypeDefinition definition : definitions)
      next.put(definition.getName(), definition);
    return new GraphQLTypeRegistry(Collections.unmodifiableMap(next));
  }

  ObjectTypeDefinition get(final String name) {
    return types.get(name);
  }

  /** The {@code Query} root type, or null when the SDL declares none. */
  ObjectTypeDefinition getQueryDefinition() {
    return queryDefinition;
  }

  boolean isObjectType(final String name) {
    return types.containsKey(name);
  }

  Collection<ObjectTypeDefinition> getTypes() {
    return types.values();
  }

  /**
   * The object type a field returns, through one list wrapper, or null when it returns anything else (a scalar, a type the SDL
   * does not declare).
   */
  ObjectTypeDefinition getTypeFromField(final FieldDefinition fieldDefinition) {
    final Type type = fieldDefinition.getType();
    if (type.getTypeName() != null)
      return types.get(type.getTypeName().getName());
    if (type.getListType() != null && type.getListType().getType().getTypeName() != null)
      return types.get(type.getListType().getType().getTypeName().getName());
    return null;
  }
}
