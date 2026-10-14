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
package com.arcadedb.query.opencypher.procedures;

import com.arcadedb.schema.DocumentType;
import com.arcadedb.schema.EdgeType;
import com.arcadedb.schema.Schema;
import com.arcadedb.schema.VertexType;

/**
 * Checks the type names a procedure receives as filters against the schema, so a name that is not a type of the expected
 * kind is refused instead of being answered with an empty graph: a typo ({@code 'KNOWN'} for {@code 'KNOWS'}) would
 * otherwise walk nothing and return no rows, which reads as a correct answer (issues #9769 and #10025).
 * <p>
 * Shared by the {@code algo.*} and the {@code path.*} procedures, which refuse the same mistakes with the same words.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
public final class ProcedureSchemaTypes {
  private ProcedureSchemaTypes() {
  }

  /**
   * Refuses any name that is not an edge type of the schema. A null or empty name is skipped: it is how the callers spell
   * "every type".
   */
  public static void requireEdgeTypes(final String procedureName, final Schema schema, final String... names) {
    requireTypes(procedureName, schema, names, true);
  }

  /** As {@link #requireEdgeTypes}, for node labels: every name must be a vertex type of the schema. */
  public static void requireVertexTypes(final String procedureName, final Schema schema, final String... names) {
    requireTypes(procedureName, schema, names, false);
  }

  private static void requireTypes(final String procedureName, final Schema schema, final String[] names, final boolean edges) {
    final String expected = edges ? "relationship type" : "node label";
    for (final String name : names) {
      if (name == null || name.isEmpty())
        continue;
      final DocumentType type = schema.getTypeOrNull(name);
      if (type == null)
        throw new IllegalArgumentException(
            procedureName + "(): " + expected + " '" + name + "' does not exist" + (edges ? edgePropertyHint(schema, name) : ""));
      if (edges ? !(type instanceof EdgeType) : !(type instanceof VertexType))
        throw new IllegalArgumentException(procedureName + "(): '" + name + "' is not a " + expected + ", it is " + describeKind(type));
    }
  }

  private static String describeKind(final DocumentType type) {
    if (type instanceof VertexType)
      return "a vertex type";
    if (type instanceof EdgeType)
      return "an edge type";
    return "a document type";
  }

  /**
   * When the name that is not an edge type is a property declared on one, the caller has most likely passed the weight
   * property where the relationship types go, the order {@code algo.mst} and {@code algo.apsp} took before #9769.
   */
  private static String edgePropertyHint(final Schema schema, final String name) {
    for (final DocumentType type : schema.getTypes())
      if (type instanceof EdgeType && type.existsProperty(name))
        return " ('" + name + "' is a property of the edge type '" + type.getName()
            + "': the relationship types come first, then the weight property)";
    return "";
  }
}
