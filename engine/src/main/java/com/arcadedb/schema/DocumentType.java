package com.arcadedb.schema;/*
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
 */

import com.arcadedb.database.Document;
import com.arcadedb.database.MutableDocument;
import com.arcadedb.database.RecordEvents;
import com.arcadedb.database.bucketselectionstrategy.BucketSelectionStrategy;
import com.arcadedb.engine.Bucket;
import com.arcadedb.exception.SchemaException;
import com.arcadedb.index.Index;
import com.arcadedb.index.IndexInternal;
import com.arcadedb.index.TypeIndex;
import com.arcadedb.index.lsm.LSMTreeIndexAbstract;
import com.arcadedb.serializer.json.JSONObject;
import com.arcadedb.utility.ExcludeFromJacocoGeneratedReport;

import java.util.*;

/**
 * Schema Document Type.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
@ExcludeFromJacocoGeneratedReport
public interface DocumentType {
  String getName();

  MutableDocument newRecord();

  default byte getType() {
    return Document.RECORD_TYPE;
  }

  RecordEvents getEvents();

  Set<String> getPolymorphicPropertiesWithDefaultDefined();

  DocumentType addSuperType(String superName);

  DocumentType addSuperType(DocumentType superType);

  DocumentType removeSuperType(String superTypeName);

  DocumentType removeSuperType(DocumentType superType);

  boolean instanceOf(String type);

  List<DocumentType> getSuperTypes();

  DocumentType setSuperTypes(List<DocumentType> newSuperTypes);

  List<DocumentType> getSubTypes();

  Set<String> getPropertyNames();

  Collection<? extends Property> getProperties();

  Collection<? extends Property> getPolymorphicProperties();

  Set<String> getPolymorphicPropertyNames();

  Property createProperty(String propertyName, String propertyType);

  Property createProperty(String propertyName, Class<?> propertyType);

  default Property createProperty(String propName, JSONObject prop) {
    final Property p = createProperty(propName, prop.getString("type"));

    if (prop.has("of"))
      p.setOfType(prop.getString("of"));
    if (prop.has("default"))
      p.setDefaultValue(prop.get("default"));
    if (prop.has("readonly"))
      p.setReadonly(prop.getBoolean("readonly"));
    if (prop.has("mandatory"))
      p.setMandatory(prop.getBoolean("mandatory"));
    if (prop.has("notNull"))
      p.setNotNull(prop.getBoolean("notNull"));
    if (prop.has("hidden"))
      p.setHidden(prop.getBoolean("hidden"));
    if (prop.has("max"))
      p.setMax(prop.getString("max"));
    if (prop.has("min"))
      p.setMin(prop.getString("min"));
    if (prop.has("regexp"))
      p.setRegexp(prop.getString("regexp"));

    if (prop.has("custom")) {
      for (Map.Entry<String, Object> entry : prop.getJSONObject("custom").toMap().entrySet())
        p.setCustomValue(entry.getKey(), entry.getValue());
    }

    return p;
  }

  Property createProperty(String propertyName, Type propertyType);

  Property createProperty(String propertyName, Type propertyType, String ofType);

  Property getOrCreateProperty(String propertyName, String propertyType);

  Property getOrCreateProperty(String propertyName, String propertyType, String ofType);

  Property getOrCreateProperty(String propertyName, Class<?> propertyType);

  Property getOrCreateProperty(String propertyName, Type propertyType);

  Property getOrCreateProperty(String propertyName, Type propertyType, String ofType);

  Property dropProperty(String propertyName);

  TypeIndex createTypeIndex(Schema.INDEX_TYPE indexType, boolean unique, String... propertyNames);

  TypeIndex createTypeIndex(Schema.INDEX_TYPE indexType, boolean unique, String[] propertyNames, int pageSize);

  TypeIndex createTypeIndex(Schema.INDEX_TYPE indexType, boolean unique, String[] propertyNames, int pageSize,
      Index.BuildIndexCallback callback);

  TypeIndex createTypeIndex(Schema.INDEX_TYPE indexType, boolean unique, String[] propertyNames, int pageSize,
      LSMTreeIndexAbstract.NULL_STRATEGY nullStrategy, Index.BuildIndexCallback callback);

  TypeIndex getOrCreateTypeIndex(Schema.INDEX_TYPE indexType, boolean unique, String... propertyNames);

  TypeIndex getOrCreateTypeIndex(Schema.INDEX_TYPE indexType, boolean unique, String[] propertyNames, int pageSize);

  TypeIndex getOrCreateTypeIndex(Schema.INDEX_TYPE indexType, boolean unique, String[] propertyNames, int pageSize,
      Index.BuildIndexCallback callback);

  TypeIndex getOrCreateTypeIndex(Schema.INDEX_TYPE indexType, boolean unique, String[] propertyNames, int pageSize,
      LSMTreeIndexAbstract.NULL_STRATEGY nullStrategy, Index.BuildIndexCallback callback);

  List<Bucket> getInvolvedBuckets();

  List<Bucket> getBuckets(boolean polymorphic);

  List<Integer> getBucketIds(boolean polymorphic);

  DocumentType addBucket(Bucket bucket);

  DocumentType removeBucket(Bucket bucket);

  Bucket getBucketIdByRecord(Document record, boolean async);

  int getBucketIndexByKeys(Object[] keys, boolean async);

  BucketSelectionStrategy getBucketSelectionStrategy();

  DocumentType setBucketSelectionStrategy(BucketSelectionStrategy selectionStrategy);

  DocumentType setBucketSelectionStrategy(String selectionStrategyName, Object... args);

  boolean existsProperty(String propertyName);

  boolean existsPolymorphicProperty(String propertyName);

  default Property getPolymorphicPropertyIfExists(final String propertyName) {
    Property prop = getPropertyIfExists(propertyName);
    if (prop != null)
      return prop;

    for (final DocumentType superType : getSuperTypes()) {
      prop = superType.getPolymorphicPropertyIfExists(propertyName);
      if (prop != null)
        return prop;
    }

    return null;
  }

  Property getPropertyIfExists(String propertyName);

  default Property getProperty(final String propertyName) {
    final Property prop = getPropertyIfExists(propertyName);
    if (prop == null)
      throw new SchemaException("Cannot find property '" + propertyName + "' in type '" + getName() + "'");
    return prop;
  }

  default Property getPolymorphicProperty(final String propertyName) {
    final Property prop = getPolymorphicPropertyIfExists(propertyName);
    if (prop == null)
      throw new SchemaException("Cannot find property '" + propertyName + "' in type '" + getName() + "'");
    return prop;
  }

  Collection<TypeIndex> getAllIndexes(boolean polymorphic);

  List<IndexInternal> getPolymorphicBucketIndexByBucketId(int bucketId, List<String> filterByProperties);

  List<TypeIndex> getIndexesByProperties(String property1, String... propertiesN);

  List<TypeIndex> getIndexesByProperties(Collection<String> properties);

  TypeIndex getPolymorphicIndexByProperties(String... properties);

  TypeIndex getPolymorphicIndexByProperties(List<String> properties);

  Schema getSchema();

  boolean isTheSameAs(Object o);

  boolean hasBucket(String bucketName);

  int getFirstBucketId();

  boolean isSubTypeOf(String type);

  boolean isSuperTypeOf(String type);

  Set<String> getCustomKeys();

  Object getCustomValue(String key);

  Object setCustomValue(String key, Object value);

  JSONObject toJSON();
}
