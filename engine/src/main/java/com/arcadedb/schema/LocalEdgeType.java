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

import com.arcadedb.engine.Bucket;
import com.arcadedb.exception.SchemaException;

import static com.arcadedb.schema.Property.IN_PROPERTY;
import static com.arcadedb.schema.Property.OUT_PROPERTY;

public class LocalEdgeType extends LocalDocumentType implements EdgeType {
  private final boolean bidirectional;
  // Not final: both are settable through ALTER TYPE, unlike bidirectional. Volatile because ALTER TYPE reassigns them
  // on the live instance while the edge-creation path (GraphEngine, GraphBatch) reads them with no lock held; for a
  // LIGHTWEIGHT type no index backs UNIQUE, so a stale read of unique admits duplicates nothing else rejects (#7866).
  private volatile boolean lightweight;
  private volatile boolean unique;

  public LocalEdgeType(final LocalSchema schema, final String name, final boolean bidirectional) {
    this(schema, name, bidirectional, false, false);
  }

  public LocalEdgeType(final LocalSchema schema, final String name, final boolean bidirectional,
                       final boolean lightweight, final boolean unique) {
    super(schema, name);
    this.bidirectional = bidirectional;
    this.lightweight = lightweight;
    this.unique = unique;
  }

  @Override
  public boolean isBidirectional() {
    return bidirectional;
  }

  @Override
  public boolean isLightweight() {
    return lightweight;
  }

  @Override
  public boolean isUnique() {
    return unique;
  }

  /**
   * Flips the lightweight flag. Only legal while the type holds no edge records: switching an already-populated type
   * would leave those records addressable but unreachable from any creation path. Existing lightweight entries in
   * vertex edge lists are unaffected either way, because the flag is not consulted on read.
   */
  public void setLightweight(final boolean lightweight) {
    checkForSchemaMutation();
    if (lightweight && !this.lightweight)
      for (final Bucket bucket : getBuckets(false))
        if (bucket.count() > 0)
          throw new SchemaException("Cannot declare edge type '" + getName()
              + "' LIGHTWEIGHT because it already contains edge records. Create a new lightweight type instead");
    // Inside a schema recording session, which saves schema.json and, under HA, ships it to the followers (issue #9747).
    // The inherited-constraint check is part of it, under the lock that orders it against a CREATE PROPERTY or an
    // addSuperType elsewhere in the hierarchy (issue #9846)
    getSchema().getEmbedded().recordChange(() -> {
      if (lightweight && !this.lightweight)
        checkCanBeLightweight(getName(), this);
      this.lightweight = lightweight;
    });
  }

  /**
   * Refuses a LIGHTWEIGHT edge type that would declare or inherit a property a record must carry or is given (issue #9846).
   * <p>
   * A lightweight edge is a pair of pointers inside the two vertices: there is no record, so {@code DocumentValidator}
   * never runs for it and a MANDATORY or default-valued property would never be applied or enforced - the same constraint
   * that refuses the property-less edge of a regular type. The type's own {@code createProperty} already refuses ANY
   * property on a lightweight type; this is the same rule for the properties that arrive from the other side, through a
   * super type, and only for the ones that apply to an edge without properties. An optional property cannot be violated by
   * an edge that holds none, so it stays legal.
   *
   * Public because the SQL {@code CREATE EDGE TYPE} statement, in the parser package, asks it before it creates anything.
   *
   * @param edgeTypeName the type that is, or is about to be, LIGHTWEIGHT
   * @param type         the type whose polymorphic properties it would carry: itself, or the super type it is about to extend
   */
  public static void checkCanBeLightweight(final String edgeTypeName, final DocumentType type) {
    final Property constraint = type.findPropertyAppliedToEmptyRecord();
    if (constraint == null)
      return;

    final String how = type.getName().equals(edgeTypeName) ? "declares or inherits" : "would inherit from '" + type.getName() + "'";
    throw new SchemaException("Edge type '" + edgeTypeName + "' cannot be LIGHTWEIGHT because it " + how + " the "
        + constraintKind(constraint) + " property '" + constraint.getName() + "': a lightweight edge has no record, so the "
        + "constraint would never be applied to it. " + capitalize(remedy(constraint)) + ", or declare the type without LIGHTWEIGHT");
  }

  /** What makes a property apply to a record that does not carry it, for the messages. */
  static String constraintKind(final Property property) {
    return property.isMandatory() ? "MANDATORY" : "default-valued";
  }

  /**
   * The change to {@code property} that stops it applying to a record that does not carry it: a MANDATORY one must become
   * optional, a default-valued one must lose its default, and one that is both needs both (making a default-valued property
   * optional leaves its default in place).
   */
  static String remedy(final Property property) {
    final boolean defaulted = property.hasDefaultFillingInAValue();
    if (property.isMandatory())
      return defaulted ? "make the property optional and remove its default value (DEFAULT null)" : "make the property optional";
    return "remove the property's default value (DEFAULT null)";
  }

  private static String capitalize(final String text) {
    return Character.toUpperCase(text.charAt(0)) + text.substring(1);
  }

  /**
   * The warning for a LIGHTWEIGHT edge type that carries a property {@link #checkCanBeLightweight} would refuse, or
   * {@code null} when it carries none. Every DDL path refuses the combination since #9846, but the schema load deliberately
   * does not, so a database written before can still open; this is how such a database says that a constraint its schema
   * declares is not enforced (issue #9975). The schema load logs it once per type and {@code CHECK DATABASE} reports it.
   *
   * @return the message, or {@code null}
   */
  public static String unenforcedConstraintWarning(final DocumentType type) {
    if (!(type instanceof EdgeType edgeType) || !edgeType.isLightweight() || type.findPropertyAppliedToEmptyRecord() == null)
      return null;

    // EVERY such property, each with its own way out, so one pass of fixes clears the finding rather than one per restart
    final StringBuilder message = new StringBuilder("Edge type '").append(type.getName())
        .append("' is LIGHTWEIGHT, and a lightweight edge has no record, so these constraints are not enforced on its edges: ");
    boolean first = true;
    for (final Property property : type.getPolymorphicProperties()) {
      if (!property.isMandatory() && !property.hasDefaultFillingInAValue())
        continue;
      final DocumentType declaring = property instanceof AbstractProperty p && p.owner != null ? p.owner : type;
      if (!first)
        message.append("; ");
      first = false;
      message.append("the ").append(constraintKind(property)).append(" property '").append(declaring.getName()).append('.')
          .append(property.getName()).append("' (").append(declaring == type ? "declared" : "inherited").append(": ")
          .append(remedy(property)).append(')');
    }
    return message.append(". Or declare the type without LIGHTWEIGHT").toString();
  }

  public void setUnique(final boolean unique) {
    checkForSchemaMutation();
    // See setLightweight: a lightweight type builds no index, so nothing else would have shipped this flag (issue #9747)
    getSchema().getEmbedded().recordChange(() -> this.unique = unique);
  }

  /**
   * Applies the storage shape and the {@code UNIQUE} declaration of an {@code ALTER TYPE} as ONE schema change (issue #9848).
   * <p>
   * The flag and the index that enforces it used to change in separate critical sections: the flag was flipped in one, the
   * index created or dropped in the next. In the gap another session saw a type declaring UNIQUE with no index behind it, or
   * the reverse, and a concurrent {@code DROP INDEX} of that index judged the constraint against the flag as it stood before
   * the alter - so it was allowed, and it then dropped the index the alter had built. Here the flags and the index change
   * inside one schema recording session, so the database write lock is held across all of it and the session saves
   * {@code schema.json} (and, under HA, ships it) once.
   * <p>
   * When the change is refused (an already populated type that holds a duplicate pair makes the index build fail) the flags
   * go back to what they were, so the type never advertises a constraint nothing enforces. The index build removes what it
   * had started to create, and the {@code out}/{@code in} endpoint properties {@link #applyUniqueConstraint} may have declared
   * before it are left in place: they are additive and harmless, as {@link #removeUniqueConstraint} documents.
   *
   * @param lightweight the requested storage shape, or {@code null} to leave it alone
   * @param unique      the requested {@code UNIQUE} declaration, or {@code null} to leave it alone
   */
  public static void alterShape(final LocalSchema schema, final LocalEdgeType type, final Boolean lightweight,
      final Boolean unique) {
    schema.recordChange(() -> {
      final boolean previousLightweight = type.lightweight;
      final boolean previousUnique = type.unique;
      try {
        // Order matters when both arrive together: the storage shape decides how UNIQUE is enforced, so it is settled before
        // the constraint is materialised.
        if (lightweight != null)
          type.setLightweight(lightweight);
        if (unique != null)
          // Withdrawn first so that the index drop below passes the guard that protects a live constraint's index
          type.setUnique(unique);

        if (type.isUnique())
          applyUniqueConstraint(schema, type);
        else
          removeUniqueConstraint(schema, type);
      } catch (final RuntimeException e) {
        type.lightweight = previousLightweight;
        type.unique = previousUnique;
        throw e;
      }
    });
  }

  /**
   * Name of the index that backs {@link #isUnique()} on a regular (non-lightweight) edge type.
   */
  public static String uniqueIndexName(final String typeName) {
    return typeName + "[" + OUT_PROPERTY + "," + IN_PROPERTY + "]";
  }

  /**
   * Materialises the {@code UNIQUE} declaration on a regular edge type as a unique index over the two endpoints, so
   * the constraint is enforced by a O(log n) index probe on insert.
   * <p>
   * A lightweight edge type is skipped: it has no records, so there is nothing to index, and the same constraint is
   * enforced by scanning the source vertex's edge list instead (see
   * {@code GraphEngine.checkLightEdgeUniqueness}).
   * <p>
   * Building the index on an already-populated type validates what is there and fails on existing duplicates, which
   * is the desired behaviour for {@code ALTER TYPE ... WITH unique = true}.
   */
  public static void applyUniqueConstraint(final LocalSchema schema, final LocalEdgeType type) {
    if (!type.isUnique() || type.isLightweight())
      return;

    if (schema.existsIndex(uniqueIndexName(type.getName())))
      return;

    if (!type.existsProperty(OUT_PROPERTY))
      type.createProperty(OUT_PROPERTY, Type.LINK);
    if (!type.existsProperty(IN_PROPERTY))
      type.createProperty(IN_PROPERTY, Type.LINK);

    schema.createTypeIndex(Schema.INDEX_TYPE.LSM_TREE, true, type.getName(), OUT_PROPERTY, IN_PROPERTY);
  }

  /**
   * Drops the backing index when {@code UNIQUE} is withdrawn. The endpoint properties are left declared: they are
   * harmless, and re-deriving whether the application declared them itself is not worth the risk of removing one it
   * relies on.
   */
  public static void removeUniqueConstraint(final LocalSchema schema, final LocalEdgeType type) {
    if (type.isUnique())
      throw new SchemaException("Withdraw the UNIQUE declaration on edge type '" + type.getName()
          + "' before dropping the index that enforces it");

    final String indexName = uniqueIndexName(type.getName());
    if (schema.existsIndex(indexName))
      schema.dropIndex(indexName);
  }
}
