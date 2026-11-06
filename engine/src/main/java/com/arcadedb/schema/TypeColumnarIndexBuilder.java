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
import com.arcadedb.index.Index;
import com.arcadedb.index.TypeIndex;
import com.arcadedb.index.columnar.ColumnDefinition;
import com.arcadedb.index.columnar.ColumnarIndex;
import com.arcadedb.index.columnar.ColumnarIndexFactoryHandler;
import com.arcadedb.security.SecurityDatabaseUser;
import com.arcadedb.serializer.json.JSONObject;

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
   */
  @Override
  public TypeIndex create() {
    database.checkPermissionsOnDatabase(SecurityDatabaseUser.DATABASE_ACCESS.UPDATE_SCHEMA);
    ColumnarIndexFactoryHandler.checkDefinition(unique, metadata.propertyNames);

    final LocalDocumentType type = database.getSchema().getEmbedded().getType(metadata.typeName);
    final String indexProperty = metadata.propertyNames.getFirst();
    final boolean byItem = indexProperty.endsWith(ColumnarIndex.BY_ITEM_SUFFIX);
    final String property = Index.basePropertyName(indexProperty);
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
