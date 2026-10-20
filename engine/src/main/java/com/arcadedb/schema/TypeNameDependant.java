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
package com.arcadedb.schema;

import com.arcadedb.exception.SchemaException;

import java.util.Collection;

/**
 * A schema object that stores the NAME of a type: a materialized view, a continuous aggregate, a trigger, a Graph Analytical
 * View. The rule every one of them follows is the same - a type a dependant is built from cannot be dropped from under it,
 * and a rename of the type either carries to the dependant or is refused before a single file moves - and
 * {@link LocalSchema} enforces it by walking {@link LocalSchema#getTypeNameDependants()}, the one list every kind is
 * collected into (issue #10368). A new kind is protected by implementing this interface and joining that list; there is no
 * second place to remember.
 * <p>
 * The three methods run under the schema's monitor. {@link #checkTypeDrop} runs both as an early-out and again under the
 * database write lock that every DDL takes, {@link #carryTypeRename} inside the schema recording session that publishes the
 * rename, before the schema is saved: whatever it rewrites reaches {@code schema.json} with the new name.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
public interface TypeNameDependant {

  /**
   * Refuses, with a {@link SchemaException} that names this dependant and the statement that removes it, the drop of the type
   * {@code canonicalTypeName} when this dependant is built from it. Also refuses when a name it stores resolves to no type at
   * all, since it cannot then be told whether that is an old name of the type being dropped (issue #10096): see
   * {@link #checkNamesResolve}. A dependant that the type takes with it when dropped, such as a trigger, refuses nothing.
   *
   * @param typeName          the name the drop was asked with, possibly an alias, quoted in the message
   * @param canonicalTypeName the name of the type itself, the one to compare with
   */
  void checkTypeDrop(LocalSchema schema, String typeName, String canonicalTypeName);

  /**
   * Refuses, before a single file moves, the rename of {@code oldName} to {@code newName} when this dependant could not follow
   * it with certainty. Does nothing by default, for a dependant whose rename is always exact.
   */
  default void checkTypeRename(final LocalSchema schema, final String oldName, final String newName) {
  }

  /**
   * Makes this dependant name {@code newName} wherever it named {@code oldName}. {@link #checkTypeRename} already refused what
   * cannot be carried, so this does not throw: the files have moved by now. A name stored as one of the type's aliases needs
   * nothing, the alias staying with the type across the rename.
   */
  void carryTypeRename(LocalSchema schema, String oldName, String newName);

  /**
   * The fail-safe of {@link #checkTypeDrop}: refuses the drop of {@code droppedTypeName} when one of the type names a dependant
   * stores resolves to no type, saying which dependant is out of date and what to do about it. A dependant in that state cannot
   * be refreshed or rebuilt anyway.
   */
  static void checkNamesResolve(final LocalSchema schema, final String droppedTypeName, final String kind,
      final String dependantName, final String dropStatement, final Collection<String> typeNames) {
    for (final String typeName : typeNames)
      if (schema.getTypeOrNull(typeName) == null)
        throw new SchemaException("Cannot drop type '" + droppedTypeName + "' because the " + kind + " '" + dependantName
            + "' names the type '" + typeName + "', which does not exist (the type was probably renamed before a rename "
            + "carried its dependants), so it cannot be told whether this is a type the " + kind + " depends on. Drop the "
            + kind + " first with: " + dropStatement + ", and create it again if it is still needed");
  }

  /**
   * The name of the type {@code typeName} resolves to, which is {@code typeName} itself unless it is an alias, or
   * {@code typeName} when it resolves to none. A dependant stores names as it was defined with them, and a definition can name a
   * type by an alias: each stored name is resolved before it is compared, so a type is protected whichever of its names the
   * dependant uses.
   */
  static String canonicalNameOf(final LocalSchema schema, final String typeName) {
    final LocalDocumentType type = schema.getTypeOrNull(typeName);
    return type != null ? type.getName() : typeName;
  }
}
