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

import com.arcadedb.GlobalConfiguration;
import com.arcadedb.exception.SchemaException;
import com.arcadedb.index.Index;
import com.arcadedb.index.TypeIndex;
import com.arcadedb.index.columnar.ColumnDefinition;
import com.arcadedb.index.columnar.ColumnarIndex;
import com.arcadedb.index.columnar.ColumnarIndexFactoryHandler;
import com.arcadedb.index.columnar.HierarchyDefinition;
import com.arcadedb.security.SecurityDatabaseUser;
import com.arcadedb.serializer.json.JSONObject;

import java.util.HashSet;
import java.util.Set;

/**
 * Builder for {@link Schema.INDEX_TYPE#COLUMNAR COLUMNAR} indexes: carries the {@link ColumnarIndexMetadata} and refuses, before
 * anything is created, a definition the columnar projection cannot hold.
 */
public class TypeColumnarIndexBuilder extends TypeIndexBuilder {
  protected TypeColumnarIndexBuilder(final TypeIndexBuilder copyFrom) {
    // Every common setting is carried over by the shared copy constructor, which is the ONE place a new builder field has to be
    // added - see TypeIndexBuilder(TypeIndexBuilder, INDEX_TYPE, IndexMetadata) and issue #5606.
    super(copyFrom, Schema.INDEX_TYPE.COLUMNAR,
        new ColumnarIndexMetadata(copyFrom.metadata.typeName, copyFrom.metadata.propertyNames.toArray(new String[0]),
            copyFrom.metadata.associatedBucketId));
  }

  /**
   * Configures the builder from the {@code METADATA} clause of {@code CREATE INDEX}: {@code hierarchy} and {@code maxCardinality}.
   * Any other key is refused.
   */
  public TypeColumnarIndexBuilder withMetadata(final JSONObject json) {
    columnarMetadata().fromUserMetadata(json, Schema.INDEX_TYPE.COLUMNAR);
    return this;
  }

  /**
   * Restores the builder from a PERSISTED index definition, the JSON {@code ColumnarIndex.toJSON()} writes into
   * {@code schema.json} and the exporters copy verbatim. Unlike {@link #withMetadata(JSONObject)} it tolerates the structural keys
   * of such a definition ({@code type}, {@code bucket}, {@code properties}, {@code nullStrategy}, {@code unique}).
   */
  public TypeColumnarIndexBuilder withPersistedMetadata(final JSONObject json) {
    if (json != null)
      columnarMetadata().fromJSON(json);
    return this;
  }

  @Override
  public TypeColumnarIndexBuilder withMetadata(final IndexMetadata metadata) {
    if (metadata != null && !(metadata instanceof ColumnarIndexMetadata))
      throw new IllegalArgumentException("A COLUMNAR index requires ColumnarIndexMetadata but got " + metadata.getClass().getName());
    this.metadata = metadata;
    return this;
  }

  /** Sets the hierarchy a facet over the property rolls up along. */
  public TypeColumnarIndexBuilder withHierarchy(final String vertexType, final String key, final String parentEdge) {
    columnarMetadata().setHierarchy(new JSONObject().put("vertexType", vertexType).put("key", key).put("parentEdge", parentEdge));
    return this;
  }

  /** Sets the distinct-value limit checked at creation, see {@link ColumnarIndexMetadata#getMaxCardinality()}. */
  public TypeColumnarIndexBuilder withMaxCardinality(final int max) {
    columnarMetadata().setMaxCardinality(max);
    return this;
  }

  /**
   * Refuses a unique index, more than one property (one index per property), BY KEY or BY VALUE, a property type the projection
   * cannot hold and, for a dictionary-encoded property, a type that already holds more distinct values than the limit. The
   * structural checks always run; only the scan is skipped when a COLUMNAR index already sits on the property, since
   * {@link TypeIndexBuilder#create()} then decides the request over that index.
   * <p>
   * The scan reads without a lock: a write committed between it and the build can take the property over the limit. That is
   * accepted, a projection past its limit keeps working and reports it through its cardinality gauge.
   * <p>
   * Final review C-I2: a property takes one COLUMNAR index per type hierarchy, BY ITEM or not, since the projection of a bucket keys
   * its dictionaries by property. Refused, before anything is created and again under the schema lock, when another COLUMNAR index on
   * the same base property already reaches a bucket this one would (declared above the type, by the type, below it, or above one of
   * its subtypes). The index on the very same properties that the type declares or inherits is left to {@link TypeIndexBuilder#create()},
   * which answers {@code IF NOT EXISTS} with it.
   */
  @Override
  public TypeIndex create() {
    database.checkPermissionsOnDatabase(SecurityDatabaseUser.DATABASE_ACCESS.UPDATE_SCHEMA);
    ColumnarIndexFactoryHandler.checkDefinition(unique, metadata.propertyNames);

    final LocalDocumentType type = database.getSchema().getEmbedded().getType(metadata.typeName);
    final String indexProperty = metadata.propertyNames.getFirst();
    final boolean byItem = indexProperty.endsWith(ColumnarIndex.BY_ITEM_SUFFIX);
    final String property = Index.basePropertyName(indexProperty);
    checkNoOtherColumnarIndex(type, property, type.getPolymorphicIndexByProperties(metadata.propertyNames, Schema.INDEX_TYPE.COLUMNAR));
    final HierarchyDefinition hierarchy = columnarMetadata().getHierarchyDefinition();
    if (hierarchy != null)
      hierarchy.validate(database.getSchema(), metadata.typeName, property);
    final Type propertyType = resolvePropertyType(type, property, byItem);
    if (propertyType != null) {
      final ColumnDefinition column = ColumnarIndex.defineColumn(-1, property, propertyType, byItem);
      if (type.getPolymorphicIndexByProperties(metadata.propertyNames, Schema.INDEX_TYPE.COLUMNAR) == null) {
        final int configured = columnarMetadata().getMaxCardinality();
        ColumnarIndexFactoryHandler.checkCardinality(database, metadata.typeName, column, configured > 0 ?
            configured :
            database.getConfiguration().getValueAsInteger(GlobalConfiguration.COLUMNAR_MAX_CARDINALITY));
      }
    }
    return super.create();
  }

  @Override
  protected void checkUnderSchemaLock(final LocalDocumentType type) {
    // any index on the requested properties was decided over (answered, refused, or dropped to be replaced) before this point
    checkNoOtherColumnarIndex(type, Index.basePropertyName(metadata.propertyNames.getFirst()), null);
  }

  private void checkNoOtherColumnarIndex(final LocalDocumentType type, final String property, final TypeIndex answering) {
    final TypeIndex other = findColumnarIndexReaching(type, property, answering);
    if (other != null)
      throw columnarConflict("Cannot create a COLUMNAR index on property '" + property + "' of type '" + type.getName() + "'", other,
          property);
  }

  /**
   * Final review C-I2: the COLUMNAR index on base property {@code property}, other than {@code except}, that projects a bucket of
   * {@code type} or of a type below it: declared by any of those types or by a type above any of them (a diamond included). Null for
   * none. Reads the hierarchy as it is; the callers that act on the answer hold the schema lock.
   */
  static TypeIndex findColumnarIndexReaching(final LocalDocumentType type, final String property, final TypeIndex except) {
    return findColumnarIndexReaching(type, property, except, new HashSet<>());
  }

  private static TypeIndex findColumnarIndexReaching(final LocalDocumentType type, final String property, final TypeIndex except,
      final Set<String> visited) {
    if (!visited.add(type.getName()))
      return null;
    for (final TypeIndex index : type.getAllIndexes(true))
      // getType() first: an index with no sub-index left answers null, and has no property names to ask
      if (index != except && index.getType() == Schema.INDEX_TYPE.COLUMNAR && property.equals(
          Index.basePropertyName(index.getPropertyNames().getFirst())))
        return index;
    for (final DocumentType subType : type.getSubTypes()) {
      final TypeIndex found = findColumnarIndexReaching((LocalDocumentType) subType, property, except, visited);
      if (found != null)
        return found;
    }
    return null;
  }

  /** The refusal of a second COLUMNAR index on one property of a hierarchy, for {@code what} (the statement refused). */
  static SchemaException columnarConflict(final String what, final TypeIndex existing, final String property) {
    return new SchemaException(what + ": the COLUMNAR index '" + existing.getName() + "' of type '" + existing.getTypeName()
        + "' already projects property '" + property + "' into the same buckets, and a property takes one COLUMNAR index per type "
        + "hierarchy (BY ITEM or not). Drop that index first");
  }

  /**
   * The type the column of {@code property} is defined with, the way {@link TypeIndexBuilder#create()} resolves the key: the
   * declared type; for a nested path the root's type BY ITEM (it must be a LIST) and STRING otherwise; for an undeclared property
   * the default key type the caller set. Null when the property cannot be resolved, which {@link TypeIndexBuilder#create()}
   * refuses.
   */
  private Type resolvePropertyType(final LocalDocumentType type, final String property, final boolean byItem) {
    final Property declared = type.getPolymorphicPropertyIfExists(property);
    if (declared != null)
      return declared.getType();
    if (property.contains(".")) {
      final Property root = type.getPolymorphicPropertyIfExists(property.substring(0, property.indexOf('.')));
      if (root != null)
        return byItem ? root.getType() : Type.STRING;
    }
    final Type[] defaults = getDefaultKeyTypesForUndeclaredProperties();
    if (defaults != null && defaults.length > 0 && defaults[0] != null)
      return byItem ? Type.LIST : defaults[0];
    return null;
  }

  private ColumnarIndexMetadata columnarMetadata() {
    if (metadata instanceof ColumnarIndexMetadata m)
      return m;
    throw new IllegalStateException(
        "Columnar index metadata expected but was " + (metadata == null ? "null" : metadata.getClass().getName()));
  }
}
