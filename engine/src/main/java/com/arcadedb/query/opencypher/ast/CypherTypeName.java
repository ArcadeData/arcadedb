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
package com.arcadedb.query.opencypher.ast;

/**
 * A Cypher type as written after {@code IS ::} / {@code IS TYPED}, in both the predicate and a property type constraint.
 * <p>
 * The name is canonical: every synonym the grammar accepts for one type ({@code BOOL}, {@code VARCHAR}, {@code INT64},
 * {@code TIMESTAMP WITH TIME ZONE}, {@code ARRAY}, ...) is reduced to the one Neo4j renders, so the predicate and the
 * constraint cannot read the same spelling two ways (#9709). A list type, whether written {@code LIST<T>} or with the
 * postfix {@code T LIST}, is {@code LIST} with {@code T} as its element.
 *
 * @param name           the canonical type name, {@code LIST} for a list type
 * @param elementName    the canonical element type of a list, or null for any other type and for a bare {@code LIST}
 * @param nonNull        whether the type was written {@code NOT NULL} (or {@code !})
 * @param elementNonNull whether the element type was written {@code NOT NULL}
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
public record CypherTypeName(String name, String elementName, boolean nonNull, boolean elementNonNull) {

  public static final String LIST = "LIST";

  public boolean isList() {
    return LIST.equals(name);
  }

  /** The type as Neo4j renders it, e.g. {@code LIST<STRING NOT NULL>} or {@code ZONED DATETIME NOT NULL}. */
  @Override
  public String toString() {
    final StringBuilder sb = new StringBuilder(name);
    if (elementName != null) {
      sb.append('<').append(elementName);
      if (elementNonNull)
        sb.append(" NOT NULL");
      sb.append('>');
    }
    if (nonNull)
      sb.append(" NOT NULL");
    return sb.toString();
  }

  /**
   * Reduces a type name, its tokens upper-cased and separated by one space, to the canonical name of the type. The integer
   * and float widths stay apart, because ArcadeDB stores each at its own width: {@code INT} is {@code INT32}, while
   * {@code INTEGER} is the 64-bit generic Cypher integer.
   */
  public static String canonical(final String name) {
    return switch (name) {
      case "BOOL" -> "BOOLEAN";
      case "VARCHAR" -> "STRING";
      case "INTEGER8" -> "INT8";
      case "INTEGER16" -> "INT16";
      case "INT", "INTEGER32" -> "INT32";
      case "SIGNED INTEGER", "INT64", "INTEGER64" -> "INTEGER";
      case "FLOAT64" -> "FLOAT";
      case "LOCAL TIMESTAMP", "TIMESTAMP WITHOUT TIMEZONE", "TIMESTAMP WITHOUT TIME ZONE" -> "LOCAL DATETIME";
      case "ZONED TIMESTAMP", "TIMESTAMP WITH TIMEZONE", "TIMESTAMP WITH TIME ZONE" -> "ZONED DATETIME";
      case "TIME WITHOUT TIMEZONE", "TIME WITHOUT TIME ZONE" -> "LOCAL TIME";
      case "TIME WITH TIMEZONE", "TIME WITH TIME ZONE" -> "ZONED TIME";
      case "ARRAY" -> LIST;
      case "VERTEX", "ANY NODE", "ANY VERTEX" -> "NODE";
      case "EDGE", "ANY RELATIONSHIP", "ANY EDGE" -> "RELATIONSHIP";
      case "PATHS" -> "PATH";
      case "ANY VALUE", "PROPERTY VALUE", "ANY PROPERTY VALUE" -> "ANY";
      case "ANY MAP" -> "MAP";
      default -> name;
    };
  }
}
