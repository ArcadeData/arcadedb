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
package com.arcadedb.remote;

import com.arcadedb.database.Document;
import com.arcadedb.database.MutableDocument;
import com.arcadedb.database.RecordEvents;
import com.arcadedb.database.bucketselectionstrategy.BucketSelectionStrategy;
import com.arcadedb.engine.Bucket;
import com.arcadedb.index.Index;
import com.arcadedb.index.IndexInternal;
import com.arcadedb.index.TypeIndex;
import com.arcadedb.index.lsm.LSMTreeIndexAbstract;
import com.arcadedb.query.sql.executor.Result;
import com.arcadedb.query.sql.executor.ResultSet;
import com.arcadedb.schema.DocumentType;
import com.arcadedb.schema.Property;
import com.arcadedb.schema.Schema;
import com.arcadedb.schema.Type;
import com.arcadedb.serializer.json.JSONObject;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * Document type used by {@link RemoteDatabase} class. The metadata are cached from the server until the schema is changed or
 * {@link RemoteSchema#reload()} is called.
 * <p>
 * This class is not thread safe. For multi-thread usage create one instance of RemoteDatabase per thread.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */

public class RemoteDocumentType implements DocumentType {
  protected final RemoteDatabase              remoteDatabase;
  protected final String                      name;
  private         int                         count;
  private         List<String>                buckets;
  private         String                      bucketSelectionStrategy;
  private         List<String>                parentTypes;
  private         Map<String, RemoteProperty> properties;
  private         Map<String, Object>         custom = new HashMap<>();

  RemoteDocumentType(final RemoteDatabase remoteDatabase, final Result record) {
    this.remoteDatabase = remoteDatabase;
    this.name = record.getProperty("name");
    reload(record);
  }

  void reload(final Result record) {
    count = record.getProperty("records");
    buckets = record.getProperty("buckets");
    bucketSelectionStrategy = record.getProperty("bucketSelectionStrategy");
    parentTypes = record.getProperty("parentTypes");

    final List<Map<String, Object>> propertiesMap = record.getProperty("properties");

    if (properties == null)
      properties = new HashMap<>(propertiesMap.size());

    for (Map<String, Object> entry : propertiesMap) {
      final String propertyName = (String) entry.get("name");

      RemoteProperty p = properties.get(propertyName);
      if (p == null) {
        p = new RemoteProperty(this, entry);
        properties.put(propertyName, p);
      } else
        p.reload(entry);
    }

    if (record.hasProperty("custom"))
      custom = record.getProperty("custom");
  }

  @Override
  public Schema getSchema() {
    return remoteDatabase.getSchema();
  }

  @Override
  public String toString() {
    return name;
  }

  @Override
  public boolean isTheSameAs(final Object o) {
    return equals(o);
  }

  @Override
  public boolean equals(final Object o) {
    return o instanceof RemoteDocumentType rdt && rdt.getName().equals(name);
  }

  @Override
  public int hashCode() {
    return name.hashCode();
  }

  @Override
  public String getName() {
    return name;
  }

  @Override
  public MutableDocument newRecord() {
    return new RemoteMutableDocument(remoteDatabase, name);
  }

  public int count() {
    return count;
  }

  @Override
  public Property createProperty(final String propertyName, final String propertyType) {
    remoteDatabase.command("sql", "create property `" + name + "`.`" + propertyName + "` " + propertyType);
    remoteDatabase.getSchema().reload();
    return getProperty(propertyName);
  }

  @Override
  public Property createProperty(final String propertyName, final Class<?> propertyType) {
    remoteDatabase.command("sql",
        "create property `" + name + "`.`" + propertyName + "` " + Type.getTypeByClass(propertyType).name());
    remoteDatabase.getSchema().reload();
    return getProperty(propertyName);
  }

  @Override
  public Property createProperty(String propertyName, Type propertyType) {
    remoteDatabase.command("sql", "create property `" + name + "`.`" + propertyName + "` " + propertyType.name());
    remoteDatabase.getSchema().reload();
    return getProperty(propertyName);
  }

  @Override
  public Property createProperty(final String propertyName, final Type propertyType, final String ofType) {
    remoteDatabase.command("sql", "create property `" + name + "`.`" + propertyName + "` " + propertyType.name() + " of " + ofType);
    remoteDatabase.getSchema().reload();
    return getProperty(propertyName);
  }

  @Override
  public Property getOrCreateProperty(final String propertyName, final String propertyType) {
    remoteDatabase.command("sql", "create property `" + name + "`.`" + propertyName + "` if not exists " + propertyType);
    remoteDatabase.getSchema().reload();
    return getProperty(propertyName);
  }

  @Override
  public Property getOrCreateProperty(final String propertyName, final String propertyType, final String ofType) {
    remoteDatabase.command("sql",
        "create property `" + name + "`.`" + propertyName + "` if not exists " + propertyType + " of " + ofType);
    remoteDatabase.getSchema().reload();
    return getProperty(propertyName);
  }

  @Override
  public Property getOrCreateProperty(final String propertyName, final Class<?> propertyType) {
    remoteDatabase.command("sql",
        "create property `" + name + "`.`" + propertyName + "` if not exists " + Type.getTypeByClass(propertyType).name());
    remoteDatabase.getSchema().reload();
    return getProperty(propertyName);
  }

  @Override
  public Property getOrCreateProperty(final String propertyName, final Type propertyType) {
    remoteDatabase.command("sql", "create property `" + name + "`.`" + propertyName + "` if not exists " + propertyType.name());
    remoteDatabase.getSchema().reload();
    return getProperty(propertyName);
  }

  @Override
  public Property getOrCreateProperty(final String propertyName, final Type propertyType, final String ofType) {
    remoteDatabase.command("sql",
        "create property `" + name + "`.`" + propertyName + "` if not exists " + propertyType.name() + " of " + ofType);
    remoteDatabase.getSchema().reload();
    return getProperty(propertyName);
  }

  @Override
  public Property dropProperty(final String propertyName) {
    final Property p = getProperty(propertyName);
    remoteDatabase.command("sql", "drop property `" + name + "`.`" + propertyName + "`");
    remoteDatabase.getSchema().reload();
    return p;
  }

  @Override
  public TypeIndex createTypeIndex(final Schema.INDEX_TYPE indexType, final boolean unique, final String... propertyNames) {
    remoteDatabase.getSchema().createTypeIndex(indexType, unique, name, propertyNames);
    remoteDatabase.getSchema().invalidateSchema();
    return null;
  }

  @Override
  public TypeIndex getOrCreateTypeIndex(final Schema.INDEX_TYPE indexType, final boolean unique, final String... propertyNames) {
    remoteDatabase.getSchema().getOrCreateTypeIndex(indexType, unique, name, propertyNames);
    remoteDatabase.getSchema().invalidateSchema();
    return null;
  }

  @Override
  public DocumentType addSuperType(final String superName) {
    remoteDatabase.command("sql", "alter type `" + name + "` supertype +`" + superName + "`");
    remoteDatabase.getSchema().reload();
    return this;
  }

  @Override
  public DocumentType addSuperType(final DocumentType superType) {
    remoteDatabase.command("sql", "alter type `" + name + "` supertype +`" + superType.getName() + "`");
    remoteDatabase.getSchema().reload();
    return this;
  }

  @Override
  public DocumentType removeSuperType(final String superTypeName) {
    remoteDatabase.command("sql", "alter type `" + name + "` supertype -`" + superTypeName + "`");
    remoteDatabase.getSchema().reload();
    return this;
  }

  @Override
  public DocumentType removeSuperType(final DocumentType superType) {
    remoteDatabase.command("sql", "alter type `" + name + "` supertype -`" + superType.getName() + "`");
    remoteDatabase.getSchema().reload();
    return this;
  }

  @Override
  public boolean existsProperty(final String propertyName) {
    return properties.containsKey(propertyName);
  }

  @Override
  public boolean existsPolymorphicProperty(final String propertyName) {
    if (existsProperty(propertyName))
      return true;
    for (DocumentType s : getSuperTypes())
      if (s.existsProperty(propertyName))
        return true;
    return false;
  }

  @Override
  public boolean isSubTypeOf(final String type) {
    if (parentTypes != null) {
      if (parentTypes.contains(type))
        return true;

      for (DocumentType s : getSuperTypes())
        if (s.isSubTypeOf(type))
          return true;
    }
    return false;
  }

  @Override
  public List<DocumentType> getSuperTypes() {
    return parentTypes.stream().map(p -> remoteDatabase.getSchema().getType(p)).collect(Collectors.toList());
  }

  @Override
  public Property getPropertyIfExists(String propertyName) {
    return properties.get(propertyName);
  }

  @Override
  public boolean isSuperTypeOf(final String type) {
    return remoteDatabase.getSchema().getType(type).isSubTypeOf(name);
  }

  @Override
  public List<Bucket> getBuckets(final boolean polymorphic) {
    if (!polymorphic)
      return buckets.stream().map((bucketName) -> getSchema().getBucketByName(bucketName)).collect(Collectors.toList());

    final List<Bucket> result = new ArrayList<>();
    result.addAll(buckets.stream().map((bucketName) -> getSchema().getBucketByName(bucketName)).collect(Collectors.toList()));
    for (String parent : parentTypes)
      result.addAll(getSchema().getType(parent).getBuckets(true));
    return result;
  }

  @Override
  public List<TypeIndex> getIndexesByProperties(Collection<String> properties) {
    return Collections.emptyList();
  }

  @Override
  public TypeIndex getPolymorphicIndexByProperties(String... properties) {
    // NEVER USES THE INDEX
    return null;
  }

  @Override
  public TypeIndex getPolymorphicIndexByProperties(List<String> properties) {
    // NEVER USES THE INDEX
    return null;
  }

  @Override
  public boolean hasBucket(final String bucketName) {
    return buckets.contains(bucketName);
  }

  // UNSUPPORTED METHODS. OPEN A NEW ISSUE TO REQUEST THE SUPPORT OF ADDITIONAL METHODS IN REMOTE
  @Override
  public boolean instanceOf(final String type) {
    throw new UnsupportedOperationException();
  }

  @Override
  public List<DocumentType> getSubTypes() {
    throw new UnsupportedOperationException();
  }

  @Override
  public Set<String> getPropertyNames() {
    return properties.keySet();
  }

  @Override
  public TypeIndex getOrCreateTypeIndex(final Schema.INDEX_TYPE indexType, final boolean unique, final String[] propertyNames,
      final int pageSize) {
    throw new UnsupportedOperationException();
  }

  @Override
  public TypeIndex getOrCreateTypeIndex(final Schema.INDEX_TYPE indexType, final boolean unique, final String[] propertyNames,
      final int pageSize, Index.BuildIndexCallback callback) {
    throw new UnsupportedOperationException();
  }

  @Override
  public TypeIndex getOrCreateTypeIndex(Schema.INDEX_TYPE indexType, boolean unique, String[] propertyNames, int pageSize,
      LSMTreeIndexAbstract.NULL_STRATEGY nullStrategy, Index.BuildIndexCallback callback) {
    throw new UnsupportedOperationException();
  }

  @Override
  public TypeIndex createTypeIndex(final Schema.INDEX_TYPE indexType, final boolean unique, final String[] propertyNames,
      final int pageSize) {
    throw new UnsupportedOperationException();
  }

  @Override
  public TypeIndex createTypeIndex(final Schema.INDEX_TYPE indexType, final boolean unique, final String[] propertyNames,
      final int pageSize, final Index.BuildIndexCallback callback) {
    throw new UnsupportedOperationException();
  }

  @Override
  public TypeIndex createTypeIndex(final Schema.INDEX_TYPE indexType, final boolean unique, final String[] propertyNames,
      final int pageSize, final LSMTreeIndexAbstract.NULL_STRATEGY nullStrategy, final Index.BuildIndexCallback callback) {
    throw new UnsupportedOperationException();
  }

  @Override
  public RecordEvents getEvents() {
    throw new UnsupportedOperationException();
  }

  @Override
  public Set<String> getPolymorphicPropertiesWithDefaultDefined() {
    throw new UnsupportedOperationException();
  }

  @Override
  public DocumentType setSuperTypes(List<DocumentType> newSuperTypes) {
    throw new UnsupportedOperationException();
  }

  @Override
  public Collection<? extends Property> getProperties() {
    return properties.values();
  }

  @Override
  public Collection<? extends Property> getPolymorphicProperties() {
    if (parentTypes.isEmpty())
      return getProperties();

    final Set<Property> allProperties = new HashSet<>(getProperties());
    for (final String parentName : parentTypes)
      allProperties.addAll(getSchema().getType(parentName).getPolymorphicProperties());
    return allProperties;
  }

  @Override
  public Set<String> getPolymorphicPropertyNames() {
    if (parentTypes.isEmpty())
      return getPropertyNames();

    final Set<String> allProperties = new HashSet<>(getPropertyNames());
    for (final String parentName : parentTypes)
      allProperties.addAll(getSchema().getType(parentName).getPropertyNames());
    return allProperties;
  }

  @Override
  public List<Bucket> getInvolvedBuckets() {
    throw new UnsupportedOperationException();
  }

  @Override
  public List<Integer> getBucketIds(boolean polymorphic) {
    throw new UnsupportedOperationException();
  }

  @Override
  public DocumentType addBucket(Bucket bucket) {
    remoteDatabase.command("sql", "alter type `" + name + "` bucket +`" + bucket.getName() + "`");
    return remoteDatabase.getSchema().reload().getType(name);
  }

  @Override
  public DocumentType removeBucket(Bucket bucket) {
    throw new UnsupportedOperationException();
  }

  @Override
  public Bucket getBucketIdByRecord(Document record, boolean async) {
    throw new UnsupportedOperationException();
  }

  @Override
  public int getBucketIndexByKeys(Object[] keys, boolean async) {
    throw new UnsupportedOperationException();
  }

  @Override
  public BucketSelectionStrategy getBucketSelectionStrategy() {
    throw new UnsupportedOperationException();
  }

  @Override
  public DocumentType setBucketSelectionStrategy(BucketSelectionStrategy selectionStrategy) {
    throw new UnsupportedOperationException();
  }

  @Override
  public DocumentType setBucketSelectionStrategy(String selectionStrategyName, Object... args) {
    throw new UnsupportedOperationException();
  }

  @Override
  public Collection<TypeIndex> getAllIndexes(boolean polymorphic) {
    throw new UnsupportedOperationException();
  }

  @Override
  public List<IndexInternal> getPolymorphicBucketIndexByBucketId(int bucketId, List<String> filterByProperties) {
    throw new UnsupportedOperationException();
  }

  @Override
  public List<TypeIndex> getIndexesByProperties(String property1, String... propertiesN) {
    throw new UnsupportedOperationException();
  }

  @Override
  public int getFirstBucketId() {
    throw new UnsupportedOperationException();
  }

  @Override
  public Set<String> getCustomKeys() {
    return custom.keySet();
  }

  @Override
  public Object getCustomValue(final String key) {
    return custom.get(key);
  }

  @Override
  public Object setCustomValue(final String key, Object value) {
    remoteDatabase.command("sql", "alter type `" + name + "` custom " + key + " = ?", value);
    return custom.put(key, value);
  }

  @Override
  public JSONObject toJSON() {
    throw new UnsupportedOperationException();
  }
}
