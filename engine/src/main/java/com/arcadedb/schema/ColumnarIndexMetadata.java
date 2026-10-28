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

import com.arcadedb.serializer.json.JSONObject;

import java.util.Set;

/**
 * Settings of a {@link Schema.INDEX_TYPE#COLUMNAR COLUMNAR} index: the optional hierarchy a facet over the property rolls up
 * along, and the distinct-value limit enforced when the index is created.
 * <p>
 * The hierarchy is stored as given, after a shape check: an object with the strings {@code vertexType}, {@code key} and
 * {@code parentEdge}, plus an optional string {@code parentDirection}. Its meaning is validated where it is used.
 */
public class ColumnarIndexMetadata extends IndexMetadata {
  public static final String HIERARCHY       = "hierarchy";
  public static final String MAX_CARDINALITY = "maxCardinality";

  private static final Set<String> USER_METADATA_KEYS     = Set.of(HIERARCHY, MAX_CARDINALITY);
  private static final Set<String> HIERARCHY_REQUIRED_KEYS = Set.of("vertexType", "key", "parentEdge");
  private static final String      HIERARCHY_DIRECTION     = "parentDirection";

  private JSONObject hierarchy;
  private int        maxCardinality;

  public ColumnarIndexMetadata(final String typeName, final String[] propertyNames, final int bucketId) {
    super(typeName, propertyNames, bucketId);
  }

  /**
   * The hierarchy definition ({@code {"vertexType","key","parentEdge"[,"parentDirection"]}}), or null when the index has none. The
   * object is this definition's own: callers must not modify it.
   */
  public JSONObject getHierarchy() {
    return hierarchy;
  }

  /** The distinct-value limit checked at creation, 0 to use {@code arcadedb.columnar.maxCardinality}. */
  public int getMaxCardinality() {
    return maxCardinality;
  }

  /** Sets the hierarchy after the same shape check the METADATA clause gets; null removes it. */
  public void setHierarchy(final JSONObject hierarchy) {
    this.hierarchy = hierarchy == null ? null : checkHierarchy(hierarchy);
  }

  public void setMaxCardinality(final int maxCardinality) {
    if (maxCardinality < 1)
      throw new IllegalArgumentException("Index metadata '" + MAX_CARDINALITY + "' must be greater than 0, got: " + maxCardinality);
    this.maxCardinality = maxCardinality;
  }

  /**
   * Reads a persisted definition. The bucket-level index JSON carries no {@code typeName}, so the base fields are read only from a
   * type-level one, as the other wrapper index types do. Not validated: a definition that loads today must keep loading.
   */
  @Override
  public void fromJSON(final JSONObject metadata) {
    if (metadata.has("typeName"))
      super.fromJSON(metadata);
    final JSONObject persisted = metadata.getJSONObject(HIERARCHY, null);
    hierarchy = persisted == null ? null : persisted.copy();
    maxCardinality = metadata.getInt(MAX_CARDINALITY, 0);
  }

  /** Writes the settings into a persisted index definition; an absent setting writes nothing. */
  public void toJSON(final JSONObject json) {
    if (hierarchy != null)
      json.put(HIERARCHY, hierarchy.copy());
    if (maxCardinality > 0)
      json.put(MAX_CARDINALITY, maxCardinality);
  }

  @Override
  public ColumnarIndexMetadata copy(final String typeName, final String[] propertyNames, final int bucketId) {
    final ColumnarIndexMetadata copy = copyCommonTo(new ColumnarIndexMetadata(typeName, propertyNames, bucketId));
    copy.hierarchy = hierarchy == null ? null : hierarchy.copy();
    copy.maxCardinality = maxCardinality;
    return copy;
  }

  @Override
  public Set<String> getUserMetadataKeys() {
    return USER_METADATA_KEYS;
  }

  @Override
  protected void applyUserMetadata(final JSONObject json) {
    if (json.has(HIERARCHY)) {
      final Object value = json.get(HIERARCHY);
      if (!(value instanceof JSONObject object))
        throw new IllegalArgumentException("Index metadata '" + HIERARCHY + "' must be an object with " + HIERARCHY_REQUIRED_KEYS
            + ", got: " + value);
      setHierarchy(object);
    }
    if (json.has(MAX_CARDINALITY))
      setMaxCardinality(metadataInt(json, MAX_CARDINALITY));
  }

  @Override
  protected Object getUserMetadataValue(final String key) {
    return switch (key) {
      case HIERARCHY -> hierarchy;
      case MAX_CARDINALITY -> maxCardinality;
      default -> null;
    };
  }

  private static JSONObject checkHierarchy(final JSONObject hierarchy) {
    for (final String key : hierarchy.keySet()) {
      if (!HIERARCHY_REQUIRED_KEYS.contains(key) && !HIERARCHY_DIRECTION.equals(key))
        throw new IllegalArgumentException("Unsupported key '" + key + "' in index metadata '" + HIERARCHY + "'. Supported keys: "
            + HIERARCHY_REQUIRED_KEYS + " and the optional '" + HIERARCHY_DIRECTION + "'");
      if (!(hierarchy.get(key) instanceof String))
        throw new IllegalArgumentException("Key '" + key + "' of index metadata '" + HIERARCHY + "' must be a string, got: "
            + hierarchy.get(key));
    }
    for (final String key : HIERARCHY_REQUIRED_KEYS)
      if (!hierarchy.has(key))
        throw new IllegalArgumentException("Index metadata '" + HIERARCHY + "' requires the key '" + key + "'");
    return hierarchy.copy();
  }
}
