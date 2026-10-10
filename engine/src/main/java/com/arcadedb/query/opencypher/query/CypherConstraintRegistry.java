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
package com.arcadedb.query.opencypher.query;

import com.arcadedb.query.opencypher.ast.CypherDDLStatement.ConstraintKind;
import com.arcadedb.schema.DocumentType;
import com.arcadedb.schema.Property;
import com.arcadedb.schema.Schema;
import com.arcadedb.serializer.json.JSONArray;
import com.arcadedb.serializer.json.JSONObject;

import java.util.ArrayList;
import java.util.List;

/**
 * Remembers what a NAMED Cypher constraint created, so {@code DROP CONSTRAINT <name>} can undo exactly that (#9646).
 * <p>
 * ArcadeDB has no constraint object: a UNIQUE constraint is a unique index named after its type and properties, a NOT
 * NULL constraint is the MANDATORY flag of a property, and a NODE KEY is both. Neither carries the name the statement
 * gave, so the name is recorded as a custom value of the constrained type ({@value #KEY_PREFIX}{@code <name>}), which
 * persists with the schema and replicates with it. The entry holds:
 * <ul>
 *   <li>{@code kind} and {@code properties} - what the constraint requires</li>
 *   <li>{@code index} - the unique index this constraint created, absent when the index was already there</li>
 *   <li>{@code mandatory} - the properties whose MANDATORY flag this constraint turned on</li>
 * </ul>
 * Only what the statement CREATED is owned, so dropping the constraint never removes an index or a flag that was declared
 * before it. Ownership passes to another named constraint on the same type that still requires the same thing (a NOT
 * NULL and a NODE KEY over one property, a UNIQUE and a NODE KEY over one property set), so dropping one of the two
 * leaves the other enforced and dropping the second removes what neither needs any more.
 */
final class CypherConstraintRegistry {
  static final String KEY_PREFIX = "cypher.constraint.";

  private CypherConstraintRegistry() {
  }

  /**
   * Returns the type holding the constraint with that name, or null if no named constraint has it. Names are unique
   * across the database, as in Neo4j.
   */
  static DocumentType findOwner(final Schema schema, final String name) {
    if (name == null)
      return null;
    final String key = KEY_PREFIX + name;
    for (final DocumentType type : schema.getTypes())
      if (type.getCustomValue(key) != null)
        return type;
    return null;
  }

  static void register(final DocumentType type, final String name, final ConstraintKind kind, final String[] properties,
      final String ownedIndex, final List<String> ownedMandatory) {
    final JSONObject entry = new JSONObject();
    entry.put("kind", kind.name());
    entry.put("properties", new JSONArray(properties));
    if (ownedIndex != null)
      entry.put("index", ownedIndex);
    entry.put("mandatory", new JSONArray(ownedMandatory));
    type.setCustomValue(KEY_PREFIX + name, entry.toString());
  }

  /**
   * Drops the named constraint, undoing what it owns unless another named constraint on the same type still requires
   * it.
   * <p>
   * The steps are separate schema changes, not one atomic one. Each is safe to repeat, which is what makes a retry after a
   * failure part-way through converge: the entry is removed last so the name is still there to retry with, an index
   * already gone is skipped, a flag already cleared is left alone, and a property already handed to an heir is not added
   * to it twice. An entry whose index or property was removed by other means (SQL {@code DROP INDEX}, {@code DROP
   * PROPERTY}) is still a constraint by that name, and dropping it succeeds with nothing left to undo.
   *
   * @return false if no named constraint has that name
   */
  static boolean drop(final Schema schema, final String name) {
    final DocumentType type = findOwner(schema, name);
    if (type == null)
      return false;

    final String key = KEY_PREFIX + name;
    final JSONObject entry = new JSONObject((String) type.getCustomValue(key));

    final List<String> properties = toStrings(entry.getJSONArray("properties", new JSONArray()));

    final String index = entry.getString("index", null);
    if (index != null) {
      final String heir = findHeir(type, key, null, properties);
      if (heir != null) {
        final JSONObject heirEntry = entry(type, heir);
        heirEntry.put("index", index);
        type.setCustomValue(heir, heirEntry.toString());
      } else if (schema.existsIndex(index))
        schema.dropIndex(index);
    }

    for (final String propertyName : toStrings(entry.getJSONArray("mandatory", new JSONArray()))) {
      final String heir = findHeir(type, key, propertyName, null);
      if (heir != null) {
        final JSONObject heirEntry = entry(type, heir);
        final JSONArray heirMandatory = heirEntry.getJSONArray("mandatory", new JSONArray());
        if (!toStrings(heirMandatory).contains(propertyName)) {
          heirEntry.put("mandatory", heirMandatory.put(propertyName));
          type.setCustomValue(heir, heirEntry.toString());
        }
      } else {
        final Property property = type.getPropertyIfExists(propertyName);
        if (property != null && property.isMandatory())
          property.setMandatory(false);
      }
    }
    // Forgotten last, so a failure undoing the above leaves the name in place for a retry
    type.setCustomValue(key, null);
    return true;
  }

  /**
   * Finds a named constraint on the type, other than the one being dropped, that still needs what is being released:
   * with {@code uniqueOver}, a UNIQUE or NODE KEY over exactly those properties IN THAT ORDER (an index over (a, b) is
   * not the index over (b, a), so the order is part of the index's identity); with {@code mandatoryProperty}, a NOT NULL
   * or NODE KEY covering it.
   */
  private static String findHeir(final DocumentType type, final String droppedKey, final String mandatoryProperty,
      final List<String> uniqueOver) {
    for (final String key : type.getCustomKeys()) {
      if (!key.startsWith(KEY_PREFIX) || key.equals(droppedKey))
        continue;
      final JSONObject candidate = entry(type, key);
      final ConstraintKind kind;
      try {
        kind = ConstraintKind.valueOf(candidate.getString("kind", ""));
      } catch (final IllegalArgumentException e) {
        // an entry this version does not know (written by a newer one) claims nothing here, and must not abort the drop
        continue;
      }
      final List<String> properties = toStrings(candidate.getJSONArray("properties", new JSONArray()));
      if (uniqueOver != null) {
        if ((kind == ConstraintKind.UNIQUE || kind == ConstraintKind.KEY) && properties.equals(uniqueOver))
          return key;
      } else if ((kind == ConstraintKind.NOT_NULL || kind == ConstraintKind.KEY) && properties.contains(mandatoryProperty))
        return key;
    }
    return null;
  }

  private static JSONObject entry(final DocumentType type, final String key) {
    return new JSONObject((String) type.getCustomValue(key));
  }

  private static List<String> toStrings(final JSONArray array) {
    final List<String> result = new ArrayList<>(array.length());
    for (int i = 0; i < array.length(); i++)
      result.add(array.getString(i));
    return result;
  }
}
