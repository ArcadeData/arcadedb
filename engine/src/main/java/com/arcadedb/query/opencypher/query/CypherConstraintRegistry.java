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

import com.arcadedb.database.Database;
import com.arcadedb.database.DatabaseInternal;
import com.arcadedb.exception.SchemaException;
import com.arcadedb.index.TypeIndex;
import com.arcadedb.log.LogManager;
import com.arcadedb.query.opencypher.ast.CypherDDLStatement.ConstraintKind;
import com.arcadedb.schema.DocumentType;
import com.arcadedb.schema.Property;
import com.arcadedb.schema.Schema;
import com.arcadedb.schema.Type;
import com.arcadedb.serializer.json.JSONArray;
import com.arcadedb.serializer.json.JSONObject;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.WeakHashMap;
import java.util.logging.Level;

/**
 * Remembers what a Cypher constraint created, so {@code DROP CONSTRAINT <name>} can undo exactly that (#9646, #9706).
 * <p>
 * ArcadeDB has no constraint object: a UNIQUE constraint is a unique index named after its type and properties, a NOT
 * NULL constraint is the MANDATORY flag of a property, a NODE KEY is both, and a property type constraint is the declared
 * type of the property. None carries the name of the constraint, so the name is recorded as a custom value of the
 * constrained type ({@value #KEY_PREFIX}{@code <name>}), which persists with the schema and replicates with it. A constraint
 * created without a name is recorded under the name {@link #generatedName} gives it, so every constraint Cypher creates can
 * be listed and dropped (#9706). The entry holds:
 * <ul>
 *   <li>{@code kind} and {@code properties} - what the constraint requires</li>
 *   <li>{@code index} - the unique index this constraint created, absent when the index was already there</li>
 *   <li>{@code mandatory} - the properties whose MANDATORY flag this constraint turned on</li>
 *   <li>{@code declared} - the properties this constraint declared, which were undeclared before it (#9706)</li>
 *   <li>{@code previous} - for a type constraint that changed the type of a declared property, the declaration it
 *   replaced, so dropping it puts that declaration back (#9706)</li>
 *   <li>{@code declaredType} and {@code propertyType} - for a type constraint, the ArcadeDB type it declared and the Cypher
 *   type it was written with</li>
 * </ul>
 * Only what the statement CREATED is owned, so dropping the constraint never removes an index, a flag or a declaration
 * that was there before it. Ownership passes to another constraint on the same type that still requires the same thing (a
 * NOT NULL and a NODE KEY over one property, a UNIQUE and a NODE KEY over one property set, any two constraints over a
 * property one of them declared), so dropping one of the two leaves the other enforced and dropping the second removes what
 * neither needs any more.
 */
final class CypherConstraintRegistry {
  static final String KEY_PREFIX = "cypher.constraint.";
  // One monitor per open database, held only by the constraint statements themselves: never the schema's own monitor, which
  // the schema takes internally while it changes files. Weakly keyed by the database instance, so a database closed or
  // dropped takes its monitor with it
  private static final Map<Database, Object> LOCKS = Collections.synchronizedMap(new WeakHashMap<>());

  /** Changes the declared type of a property, keeping the rest of its declaration: the engine's retype. */
  @FunctionalInterface
  interface Retyper {
    void retype(DocumentType type, Property property, Type newType, String newOfType);
  }

  /**
   * What a constraint statement created, recorded under the constraint's name so dropping it undoes exactly that.
   *
   * @param ownedIndex     the unique index it created, or null
   * @param ownedMandatory the properties whose MANDATORY flag it turned on
   * @param declared       the properties it declared, undeclared before it
   * @param previous       for a type constraint, the declarations it replaced, by property
   * @param declaredType   for a type constraint, the ArcadeDB type it declared, otherwise null
   * @param declaredOfType for a type constraint over a list, its element type
   * @param propertyType   for a type constraint, the Cypher type it was written with
   */
  record Ownership(String ownedIndex, List<String> ownedMandatory, List<String> declared, JSONObject previous, Type declaredType,
                   String declaredOfType, String propertyType) {
  }

  /** A constraint as recorded, for listing. */
  record Entry(String name, ConstraintKind kind, List<String> properties, String index, List<String> declared, String propertyType) {
  }


  private CypherConstraintRegistry() {
  }

  /**
   * The monitor that serializes the constraint statements of a database, so checking a name and recording it are one step.
   * It is local to this JVM, which is enough: a constraint statement changes the schema, so in an HA cluster a replica forwards
   * it to the leader and only the leader runs it, and the replicas receive the recorded entries with the schema.
   */
  static Object lockFor(final Database database) {
    return LOCKS.computeIfAbsent(database instanceof DatabaseInternal internal ? internal.getEmbedded() : database, k -> new Object());
  }

  /**
   * The name a constraint created without one is recorded under: the type, the properties and what is required, as in
   * {@code constraint_Person_email_unique}. It is deterministic, so the same unnamed statement names the same constraint, and
   * it is the name {@code SHOW CONSTRAINTS} lists the same requirement under when it was declared outside Cypher.
   */
  static String generatedName(final String typeName, final ConstraintKind kind, final List<String> properties) {
    return generatedName(typeName, properties, switch (kind) {
      case UNIQUE -> "unique";
      case KEY -> "key";
      case NOT_NULL -> "existence";
      case TYPED -> "type";
    });
  }

  static String generatedName(final String typeName, final List<String> properties, final String suffix) {
    return "constraint_" + typeName + "_" + String.join("_", properties) + "_" + suffix;
  }

  /**
   * A generated name no recorded constraint and no index holds yet. The name joins the properties with {@code _}, so two
   * property sets can spell it the same ({@code [a_b, c]} and {@code [a, b_c]}): the second one takes the first free
   * {@code <name>_2}, {@code <name>_3}, ... rather than being mistaken for the first.
   */
  static String freeGeneratedName(final Schema schema, final String typeName, final ConstraintKind kind,
      final List<String> properties) {
    final String base = generatedName(typeName, kind, properties);
    String candidate = base;
    for (int i = 2; findOwner(schema, candidate) != null || schema.existsIndex(candidate); i++)
      candidate = base + "_" + i;
    return candidate;
  }

  /**
   * Returns the type holding the constraint with that name, or null if no recorded constraint has it. Names are unique
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
      final Ownership owned) {
    final JSONObject entry = new JSONObject();
    entry.put("kind", kind.name());
    entry.put("properties", new JSONArray(properties));
    if (owned.ownedIndex() != null)
      entry.put("index", owned.ownedIndex());
    entry.put("mandatory", new JSONArray(owned.ownedMandatory()));
    if (!owned.declared().isEmpty())
      entry.put("declared", new JSONArray(owned.declared()));
    if (owned.previous() != null && !owned.previous().isEmpty())
      entry.put("previous", owned.previous());
    if (owned.declaredType() != null)
      entry.put("declaredType", declaration(owned.declaredType(), owned.declaredOfType()));
    if (owned.propertyType() != null)
      entry.put("propertyType", owned.propertyType());
    type.setCustomValue(KEY_PREFIX + name, entry.toString());
  }

  /** A property declaration as recorded: its type and, for a collection, its element type. */
  static JSONObject declaration(final Type type, final String ofType) {
    final JSONObject declaration = new JSONObject().put("type", type.name());
    if (ofType != null)
      declaration.put("of", ofType);
    return declaration;
  }

  /** The constraints recorded on the type, in no particular order; an unreadable entry is skipped. */
  static List<Entry> entries(final DocumentType type) {
    final List<Entry> entries = new ArrayList<>();
    for (final String key : type.getCustomKeys()) {
      if (!key.startsWith(KEY_PREFIX))
        continue;
      final JSONObject entry = entry(type, key);
      final ConstraintKind kind = entry != null ? kindOf(entry) : null;
      if (kind == null)
        continue;
      entries.add(new Entry(key.substring(KEY_PREFIX.length()), kind, toStrings(entry.getJSONArray("properties", new JSONArray())),
          entry.getString("index", null), toStrings(entry.getJSONArray("declared", new JSONArray())),
          entry.getString("propertyType", null)));
    }
    return entries;
  }

  /**
   * The name of a constraint on the type that requires exactly the same thing, or null. A type constraint is the same only
   * with the same declared type.
   */
  static String findEquivalent(final DocumentType type, final ConstraintKind kind, final List<String> properties,
      final Type declaredType, final String declaredOfType) {
    for (final String key : type.getCustomKeys()) {
      if (!key.startsWith(KEY_PREFIX))
        continue;
      final JSONObject entry = entry(type, key);
      if (entry == null || kindOf(entry) != kind || !toStrings(entry.getJSONArray("properties", new JSONArray())).equals(properties))
        continue;
      if (kind != ConstraintKind.TYPED || sameDeclaration(entry.getJSONObject("declaredType", null), declaredType, declaredOfType))
        return key.substring(KEY_PREFIX.length());
    }
    return null;
  }

  /**
   * The name of a type constraint on the type that requires another type of one of these properties, or null. A property
   * has one declared type, so two type constraints that disagree cannot both hold: as in Neo4j, the second is refused rather
   * than silently replacing the first.
   */
  static String findConflictingTyped(final DocumentType type, final List<String> properties, final Type declaredType,
      final String declaredOfType) {
    for (final String key : type.getCustomKeys()) {
      if (!key.startsWith(KEY_PREFIX))
        continue;
      final JSONObject entry = entry(type, key);
      if (entry == null || kindOf(entry) != ConstraintKind.TYPED)
        continue;
      for (final String property : toStrings(entry.getJSONArray("properties", new JSONArray())))
        if (properties.contains(property) && !sameDeclaration(entry.getJSONObject("declaredType", null), declaredType, declaredOfType))
          return key.substring(KEY_PREFIX.length());
    }
    return null;
  }

  /** The name of the constraint that created this index, or null when no recorded constraint owns it. */
  static String findIndexOwner(final Schema schema, final String indexName) {
    for (final DocumentType type : schema.getTypes())
      for (final String key : type.getCustomKeys()) {
        if (!key.startsWith(KEY_PREFIX))
          continue;
        final JSONObject entry = entry(type, key);
        if (entry != null && indexName.equals(entry.getString("index", null)))
          return key.substring(KEY_PREFIX.length());
      }
    return null;
  }

  /**
   * Drops the named constraint, undoing what it owns unless another constraint on the same type still requires it.
   * <p>
   * A type constraint that replaced the declaration of a property puts that declaration back first, before anything else is
   * undone: it is the one step that can be refused (a record written since does not fit the old type), and a refusal then
   * leaves the constraint whole. A property the constraint declared is undeclared, which is what lifts a type restriction in
   * a schema that has no "any" type, unless another constraint still needs it declared (a unique index is built over the
   * declared type, a MANDATORY flag lives on the declaration): that one then owns the declaration, and the property is
   * undeclared when the last of them goes. A declaration that also carries what no constraint created (a READONLY flag, a
   * DEFAULT, bounds set with SQL) is left in place, as is anything a statement outside Cypher still depends on.
   * <p>
   * The steps are separate schema changes, not one atomic one. Each is safe to repeat, which is what makes a retry after a
   * failure part-way through converge: the entry is removed last so the name is still there to retry with, an index
   * already gone is skipped, a flag already cleared is left alone, and a property already handed to an heir is not added
   * to it twice. An entry whose index or property was removed by other means (SQL {@code DROP INDEX}, {@code DROP
   * PROPERTY}) is still a constraint by that name, and dropping it succeeds with nothing left to undo.
   *
   * @return false if no recorded constraint has that name
   */
  static boolean drop(final Schema schema, final String name, final Retyper retyper) {
    final DocumentType type = findOwner(schema, name);
    if (type == null)
      return false;

    final String key = KEY_PREFIX + name;
    final JSONObject entry = entry(type, key);
    if (entry == null) {
      // unreadable: it can be forgotten, but there is nothing it can be trusted to undo
      LogManager.instance().log(CypherConstraintRegistry.class, Level.WARNING,
          "Constraint '%s' on type '%s' has an unreadable record: dropping its name without undoing anything", name, type.getName());
      type.setCustomValue(key, null);
      return true;
    }

    restorePreviousDeclarations(type, entry, retyper);

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

    for (final String propertyName : toStrings(entry.getJSONArray("declared", new JSONArray()))) {
      final String heir = findUser(type, key, propertyName);
      if (heir != null)
        handDeclarationTo(type, heir, propertyName);
      else
        undeclareIfUnused(type, propertyName);
    }

    // Forgotten last, so a failure undoing the above leaves the name in place for a retry
    type.setCustomValue(key, null);
    return true;
  }

  /**
   * Puts back the declarations a type constraint replaced, as long as the property still has the type the constraint gave
   * it: a type changed since by other means is not this constraint's to undo.
   */
  private static void restorePreviousDeclarations(final DocumentType type, final JSONObject entry, final Retyper retyper) {
    final JSONObject previous = entry.getJSONObject("previous", null);
    final JSONObject declared = entry.getJSONObject("declaredType", null);
    if (previous == null || declared == null)
      return;
    for (final String propertyName : previous.keySet()) {
      final Property property = type.getPropertyIfExists(propertyName);
      if (property == null || !sameDeclaration(declared, property.getType(), property.getOfType())) {
        LogManager.instance().log(CypherConstraintRegistry.class, Level.FINE,
            "Type constraint on '%s.%s': the property no longer has the type the constraint declared, so the declaration it "
                + "replaced is not restored", type.getName(), propertyName);
        continue;
      }
      final JSONObject old = previous.getJSONObject(propertyName, null);
      final Type oldType = old != null ? typeOf(old.getString("type", null)) : null;
      if (oldType == null)
        continue;
      try {
        retyper.retype(type, property, oldType, old.getString("of", null));
      } catch (final RuntimeException e) {
        throw new SchemaException("Cannot drop the type constraint on '" + type.getName() + "." + propertyName
            + "': it replaced the declaration " + oldType + (old.has("of") ? " OF " + old.getString("of") : "")
            + ", which cannot be restored: " + e.getMessage() + ". The constraint is still in place", e);
      }
    }
  }

  /**
   * Hands the declaration of a property to another constraint that uses it. A type constraint that recorded the declaration
   * it replaced forgets it, since that declaration is the one leaving: dropping the heir later undeclares the property instead
   * of restoring it.
   */
  private static void handDeclarationTo(final DocumentType type, final String heir, final String propertyName) {
    final JSONObject heirEntry = entry(type, heir);
    final JSONObject heirPrevious = heirEntry.getJSONObject("previous", null);
    if (heirPrevious != null)
      heirPrevious.remove(propertyName);
    final JSONArray heirDeclared = heirEntry.getJSONArray("declared", new JSONArray());
    if (!toStrings(heirDeclared).contains(propertyName))
      heirEntry.put("declared", heirDeclared.put(propertyName));
    type.setCustomValue(heir, heirEntry.toString());
  }

  /**
   * Removes the declaration of a property when nothing but the declared type is left on it. Whatever else it carries - a
   * MANDATORY or NOT NULL flag, READONLY, a DEFAULT, bounds, a pattern, HIDDEN - was set by something other than the dropped
   * constraint and goes with the declaration, so the declaration stays; an index over the property keeps it too.
   */
  static boolean undeclareIfUnused(final DocumentType type, final String propertyName) {
    final Property property = type.getPropertyIfExists(propertyName);
    if (property == null)
      return true;
    if (!carriesOnlyItsType(property))
      return false;
    try {
      type.dropProperty(propertyName);
      return true;
    } catch (final SchemaException e) {
      // an index over the property, or a declared TIMESERIES column: the schema refuses, and the declaration stays
      return false;
    }
  }

  static boolean carriesOnlyItsType(final Property property) {
    return !property.isMandatory() && !property.isNotNull() && !property.isReadonly() && !property.isHidden()
        && property.getDefaultValueDefinition() == null && property.getMin() == null && property.getMax() == null
        && property.getRegexp() == null;
  }

  /**
   * Finds a constraint on the type, other than the one being dropped, that still needs what is being released: with
   * {@code uniqueOver}, a UNIQUE or NODE KEY over exactly those properties IN THAT ORDER (an index over (a, b) is not the
   * index over (b, a), so the order is part of the index's identity); with {@code mandatoryProperty}, a NOT NULL or NODE KEY
   * covering it.
   */
  private static String findHeir(final DocumentType type, final String droppedKey, final String mandatoryProperty,
      final List<String> uniqueOver) {
    for (final String key : type.getCustomKeys()) {
      if (!key.startsWith(KEY_PREFIX) || key.equals(droppedKey))
        continue;
      final JSONObject candidate = entry(type, key);
      final ConstraintKind kind = candidate != null ? kindOf(candidate) : null;
      if (kind == null)
        continue;
      final List<String> properties = toStrings(candidate.getJSONArray("properties", new JSONArray()));
      if (uniqueOver != null) {
        if ((kind == ConstraintKind.UNIQUE || kind == ConstraintKind.KEY) && properties.equals(uniqueOver))
          return key;
      } else if ((kind == ConstraintKind.NOT_NULL || kind == ConstraintKind.KEY) && properties.contains(mandatoryProperty))
        return key;
    }
    return null;
  }

  /** Finds a constraint on the type, other than the one being dropped, over the property, of any kind. */
  private static String findUser(final DocumentType type, final String droppedKey, final String propertyName) {
    for (final String key : type.getCustomKeys()) {
      if (!key.startsWith(KEY_PREFIX) || key.equals(droppedKey))
        continue;
      final JSONObject candidate = entry(type, key);
      if (candidate != null && kindOf(candidate) != null && toStrings(candidate.getJSONArray("properties", new JSONArray())).contains(
          propertyName))
        return key;
    }
    return null;
  }

  /** Whether a recorded declaration is the given type and element type. */
  private static boolean sameDeclaration(final JSONObject declaration, final Type type, final String ofType) {
    if (declaration == null)
      return false;
    final String recordedOf = declaration.getString("of", null);
    return type.name().equals(declaration.getString("type", null)) && (recordedOf == null ? ofType == null : recordedOf.equals(ofType));
  }

  /** An entry this version does not know (written by a newer one) claims nothing here, and must not abort a drop. */
  private static ConstraintKind kindOf(final JSONObject entry) {
    try {
      return ConstraintKind.valueOf(entry.getString("kind", ""));
    } catch (final IllegalArgumentException e) {
      return null;
    }
  }

  private static Type typeOf(final String name) {
    if (name == null)
      return null;
    try {
      return Type.valueOf(name);
    } catch (final IllegalArgumentException e) {
      return null;
    }
  }

  /**
   * Parses an entry, or returns null when the value is not one this class wrote (edited by hand, corrupted): such an entry
   * claims nothing, and must not abort the drop of another constraint.
   */
  private static JSONObject entry(final DocumentType type, final String key) {
    final Object value = type.getCustomValue(key);
    if (!(value instanceof String))
      return null;
    try {
      return new JSONObject((String) value);
    } catch (final RuntimeException e) {
      return null;
    }
  }

  private static List<String> toStrings(final JSONArray array) {
    final List<String> result = new ArrayList<>(array.length());
    for (int i = 0; i < array.length(); i++)
      result.add(array.getString(i));
    return result;
  }

  /** The unique index of the type over exactly these properties, in this order, or null. */
  static TypeIndex uniqueIndexOver(final DocumentType type, final List<String> properties) {
    for (final TypeIndex index : type.getAllIndexes(false))
      if (index.isUnique() && index.getPropertyNames().equals(properties))
        return index;
    return null;
  }
}
