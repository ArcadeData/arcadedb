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
  // Not final: both are settable through ALTER TYPE, unlike bidirectional.
  private       boolean lightweight;
  private       boolean unique;

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
    if (lightweight && !this.lightweight)
      for (final Bucket bucket : getBuckets(false))
        if (bucket.count() > 0)
          throw new SchemaException("Cannot declare edge type '" + getName()
              + "' LIGHTWEIGHT because it already contains edge records. Create a new lightweight type instead");
    this.lightweight = lightweight;
  }

  public void setUnique(final boolean unique) {
    this.unique = unique;
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
