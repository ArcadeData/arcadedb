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
 */
package com.arcadedb.schema;

import com.arcadedb.database.Document;
import com.arcadedb.database.RecordEvents;
import com.arcadedb.database.RecordEventsRegistry;
import com.arcadedb.database.bucketselectionstrategy.BucketSelectionStrategy;
import com.arcadedb.database.bucketselectionstrategy.RoundRobinBucketSelectionStrategy;
import com.arcadedb.engine.Bucket;
import com.arcadedb.exception.SchemaException;
import com.arcadedb.index.Index;
import com.arcadedb.index.IndexInternal;
import com.arcadedb.index.TypeIndex;
import com.arcadedb.index.lsm.LSMTreeIndexAbstract;
import com.arcadedb.log.LogManager;

import java.util.*;
import java.util.concurrent.*;
import java.util.logging.*;

public class DocumentType {
  protected final EmbeddedSchema                    schema;
  protected final String                            name;
  protected final List<DocumentType>                superTypes              = new ArrayList<>();
  protected final List<DocumentType>                subTypes                = new ArrayList<>();
  protected final List<Bucket>                      buckets                 = new ArrayList<>();
  protected       BucketSelectionStrategy           bucketSelectionStrategy = new RoundRobinBucketSelectionStrategy();
  protected final Map<String, Property>             properties              = new HashMap<>();
  protected       Map<Integer, List<IndexInternal>> bucketIndexesByBucket   = new HashMap<>();
  protected       Map<List<String>, TypeIndex>      indexesByProperties     = new HashMap<>();
  protected final RecordEventsRegistry              events                  = new RecordEventsRegistry();
  protected final Map<String, Object>               custom                  = new HashMap<>();

  public DocumentType(final EmbeddedSchema schema, final String name) {
    this.schema = schema;
    this.name = name;
  }

  public String getName() {
    return name;
  }

  public byte getType() {
    return Document.RECORD_TYPE;
  }

  public RecordEvents getEvents() {
    return events;
  }

  public DocumentType addSuperType(final String superName) {
    return addSuperType(schema.getType(superName));
  }

  public DocumentType addSuperType(final DocumentType superType) {
    if (superTypes.indexOf(superType) > -1)
      // ALREADY PARENT
      return this;

    // CHECK FOR CONFLICT WITH PROPERTIES NAMES
    final Set<String> allProperties = getPolymorphicPropertyNames();
    for (String p : superType.getPolymorphicPropertyNames())
      if (allProperties.contains(p)) {
        LogManager.instance().log(this, Level.WARNING, "Property '" + p + "' is already defined in type '" + name + "' or any super types");
        //throw new IllegalArgumentException("Property '" + p + "' is already defined in type '" + name + "' or any super types");
      }

    recordFileChanges(() -> {
      superTypes.add(superType);
      superType.subTypes.add(this);

      // CREATE INDEXES AUTOMATICALLY ON PROPERTIES DEFINED IN SUPER TYPES
      final List<TypeIndex> indexes = getAllIndexes(true);
      indexes.removeAll(indexesByProperties.values());

      schema.getDatabase().transaction(() -> {
        for (TypeIndex index : indexes) {
          for (int i = 0; i < buckets.size(); i++) {
            final Bucket bucket = buckets.get(i);
            schema.createBucketIndex(schema.getType(index.getTypeName()), index.getKeyTypes(), bucket, name, index.getType(), index.isUnique(),
                LSMTreeIndexAbstract.DEF_PAGE_SIZE, index.getNullStrategy(), null,
                index.getPropertyNames().toArray(new String[index.getPropertyNames().size()]));
          }
        }
      }, false);

      return null;
    });
    return this;
  }

  public void removeSuperType(final String superTypeName) {
    removeSuperType(schema.getType(superTypeName));
  }

  public void removeSuperType(final DocumentType superType) {
    recordFileChanges(() -> {
      if (!superTypes.remove(superType))
        // ALREADY REMOVED SUPER TYPE
        return null;

      superType.subTypes.remove(this);
      return null;
    });
  }

  public boolean instanceOf(final String type) {
    if (name.equals(type))
      return true;

    for (DocumentType t : superTypes) {
      if (t.instanceOf(type))
        return true;
    }

    return false;
  }

  public List<DocumentType> getSuperTypes() {
    return Collections.unmodifiableList(superTypes);
  }

  public void setSuperTypes(List<DocumentType> newSuperTypes) {
    if (newSuperTypes == null)
      newSuperTypes = Collections.emptyList();

    final List<DocumentType> commonSuperTypes = new ArrayList<>(superTypes);
    commonSuperTypes.retainAll(newSuperTypes);

    final List<DocumentType> toRemove = new ArrayList<>(superTypes);
    toRemove.removeAll(commonSuperTypes);
    toRemove.forEach(this::removeSuperType);

    final List<DocumentType> toAdd = new ArrayList<>(newSuperTypes);
    toAdd.removeAll(commonSuperTypes);
    toAdd.forEach(this::addSuperType);
  }

  public List<DocumentType> getSubTypes() {
    return Collections.unmodifiableList(subTypes);
  }

  public Set<String> getPropertyNames() {
    return properties.keySet();
  }

  public Set<String> getPolymorphicPropertyNames() {
    final Set<String> allProperties = new HashSet<>();
    allProperties.addAll(getPropertyNames());
    for (DocumentType p : superTypes)
      allProperties.addAll(p.getPolymorphicPropertyNames());
    return allProperties;
  }

  public Property createProperty(final String propertyName, final String propertyType) {
    return createProperty(propertyName, Type.getTypeByName(propertyType));
  }

  public Property createProperty(final String propertyName, final Class<?> propertyType) {
    return createProperty(propertyName, Type.getTypeByClass(propertyType));
  }

  public Property createProperty(final String propertyName, final Type propertyType) {
    if (properties.containsKey(propertyName))
      throw new SchemaException("Cannot create the property '" + propertyName + "' in type '" + name + "' because it already exists");

    if (getPolymorphicPropertyNames().contains(propertyName))
      throw new SchemaException("Cannot create the property '" + propertyName + "' in type '" + name + "' because it was already defined in a super type");

    final Property property = new Property(this, propertyName, propertyType);

    recordFileChanges(() -> {
      properties.put(propertyName, property);
      return null;
    });
    return property;
  }

  public Property getOrCreateProperty(final String propertyName, final String propertyType) {
    return getOrCreateProperty(propertyName, Type.getTypeByName(propertyType));
  }

  public Property getOrCreateProperty(final String propertyName, final Class<?> propertyType) {
    return getOrCreateProperty(propertyName, Type.getTypeByClass(propertyType));
  }

  public Property getOrCreateProperty(final String propertyName, final Type propertyType) {
    Property p = properties.get(propertyName);
    if (p != null) {
      if (p.getType().equals(propertyType))
        return p;

      // DIFFERENT TYPE: DROP THE PROPERTY AND CREATE A NEW ONE
      dropProperty(propertyName);
    }
    return createProperty(propertyName, propertyType);
  }

  public void dropProperty(final String propertyName) {
    recordFileChanges(() -> {
      properties.remove(propertyName);
      return null;
    });
  }

  public Index createTypeIndex(final EmbeddedSchema.INDEX_TYPE indexType, final boolean unique, final String... propertyNames) {
    return schema.createTypeIndex(indexType, unique, name, propertyNames, LSMTreeIndexAbstract.DEF_PAGE_SIZE, LSMTreeIndexAbstract.NULL_STRATEGY.SKIP, null);
  }

  public Index createTypeIndex(final EmbeddedSchema.INDEX_TYPE indexType, final boolean unique, String[] propertyNames, final int pageSize) {
    return schema.createTypeIndex(indexType, unique, name, propertyNames, pageSize);
  }

  public Index createTypeIndex(final EmbeddedSchema.INDEX_TYPE indexType, final boolean unique, final String[] propertyNames, final int pageSize,
      final Index.BuildIndexCallback callback) {
    return schema.createTypeIndex(indexType, unique, name, propertyNames, pageSize, callback);
  }

  public Index createTypeIndex(final EmbeddedSchema.INDEX_TYPE indexType, final boolean unique, final String[] propertyNames, final int pageSize,
      LSMTreeIndexAbstract.NULL_STRATEGY nullStrategy, final Index.BuildIndexCallback callback) {
    return schema.createTypeIndex(indexType, unique, name, propertyNames, pageSize, nullStrategy, callback);
  }

  public Index getOrCreateTypeIndex(final EmbeddedSchema.INDEX_TYPE indexType, final boolean unique, final String... propertyNames) {
    return schema.getOrCreateTypeIndex(indexType, unique, name, propertyNames);
  }

  public Index getOrCreateTypeIndex(final EmbeddedSchema.INDEX_TYPE indexType, final boolean unique, String[] propertyNames, final int pageSize) {
    return schema.getOrCreateTypeIndex(indexType, unique, name, propertyNames, pageSize);
  }

  public Index getOrCreateTypeIndex(final EmbeddedSchema.INDEX_TYPE indexType, final boolean unique, final String[] propertyNames, final int pageSize,
      final Index.BuildIndexCallback callback) {
    return schema.getOrCreateTypeIndex(indexType, unique, name, propertyNames, pageSize, callback);
  }

  public Index getOrCreateTypeIndex(final EmbeddedSchema.INDEX_TYPE indexType, final boolean unique, final String[] propertyNames, final int pageSize,
      LSMTreeIndexAbstract.NULL_STRATEGY nullStrategy, final Index.BuildIndexCallback callback) {
    return schema.getOrCreateTypeIndex(indexType, unique, name, propertyNames, pageSize, nullStrategy, callback);
  }

  public List<Bucket> getBuckets(final boolean polymorphic) {
    if (!polymorphic || subTypes.isEmpty())
      return Collections.unmodifiableList(buckets);

    final List<Bucket> allBuckets = new ArrayList<>(buckets);
    for (DocumentType p : subTypes)
      allBuckets.addAll(p.getBuckets(true));
    return allBuckets;
  }

  public DocumentType addBucket(final Bucket bucket) {
    recordFileChanges(() -> {
      addBucketInternal(bucket);
      return null;
    });
    return this;
  }

  public DocumentType removeBucket(final Bucket bucket) {
    recordFileChanges(() -> {
      removeBucketInternal(bucket);
      return null;
    });
    return this;
  }

  public Bucket getBucketIdByRecord(final Document record, final boolean async) {
    if (buckets.isEmpty())
      throw new SchemaException("Cannot retrieve a bucket for type '" + name + "' because there are no buckets associated");
    return buckets.get(bucketSelectionStrategy.getBucketIdByRecord(record, async));
  }

  public int getBucketIndexByKeys(final Object[] keys, final boolean async) {
    if (buckets.isEmpty())
      throw new SchemaException("Cannot retrieve a bucket for keys '" + Arrays.toString(keys) + "' because there are no buckets associated");
    return bucketSelectionStrategy.getBucketIdByKeys(keys, async);
  }

  public BucketSelectionStrategy getBucketSelectionStrategy() {
    return bucketSelectionStrategy;
  }

  public void setBucketSelectionStrategy(final BucketSelectionStrategy selectionStrategy) {
    this.bucketSelectionStrategy = selectionStrategy;
    this.bucketSelectionStrategy.setType(this);
  }

  public boolean existsProperty(final String propertyName) {
    return properties.containsKey(propertyName);
  }

  public boolean existsPolymorphicProperty(final String propertyName) {
    return getPolymorphicPropertyNames().contains(propertyName);
  }

  public Property getPolymorphicPropertyIfExists(final String propertyName) {
    Property prop = properties.get(propertyName);
    if (prop != null)
      return prop;

    for (DocumentType superType : superTypes) {
      prop = superType.getPolymorphicPropertyIfExists(propertyName);
      if (prop != null)
        return prop;
    }

    return null;
  }

  public Property getPropertyIfExists(final String propertyName) {
    return properties.get(propertyName);
  }

  public Property getProperty(final String propertyName) {
    final Property prop = properties.get(propertyName);
    if (prop == null)
      throw new SchemaException("Cannot find property '" + propertyName + "' in type '" + name + "'");
    return prop;
  }

  public Property getPolymorphicProperty(final String propertyName) {
    final Property prop = getPolymorphicPropertyIfExists(propertyName);
    if (prop == null)
      throw new SchemaException("Cannot find property '" + propertyName + "' in type '" + name + "'");
    return prop;
  }

  public List<TypeIndex> getAllIndexes(final boolean polymorphic) {
    if (!polymorphic || superTypes.isEmpty())
      return new ArrayList<>(indexesByProperties.values());

    final List<TypeIndex> list = new ArrayList<>(indexesByProperties.values());

    if (polymorphic)
      for (DocumentType t : superTypes)
        list.addAll(t.getAllIndexes(true));

    return list;
  }

  public List<Index> getPolymorphicBucketIndexByBucketId(final int bucketId) {
    final List<IndexInternal> r = bucketIndexesByBucket.get(bucketId);
    if (r != null && superTypes.isEmpty())
      // MOST COMMON CASE, SAVE CREATING AND COPYING TO A NEW ARRAY
      return Collections.unmodifiableList(r);

    final List<Index> result = r != null ? new ArrayList<>(r) : new ArrayList<>();
    for (DocumentType t : superTypes)
      result.addAll(t.getPolymorphicBucketIndexByBucketId(bucketId));

    return result;
  }

  public List<TypeIndex> getIndexesByProperties(final String property1, final String... propertiesN) {
    final List<TypeIndex> result = new ArrayList<>();

    final Set<String> properties = new HashSet<>(propertiesN.length + 1);
    properties.add(property1);
    for (String p : propertiesN)
      properties.add(p);

    for (Map.Entry<List<String>, TypeIndex> entry : indexesByProperties.entrySet()) {
      for (String prop : entry.getKey()) {
        if (properties.contains(prop)) {
          result.add(entry.getValue());
          break;
        }
      }
    }

    return result;
  }

  public TypeIndex getPolymorphicIndexByProperties(final String... properties) {
    return getPolymorphicIndexByProperties(Arrays.asList(properties));
  }

  public TypeIndex getPolymorphicIndexByProperties(final List<String> properties) {
    TypeIndex idx = indexesByProperties.get(properties);

    if (idx == null)
      for (DocumentType t : superTypes) {
        idx = t.getPolymorphicIndexByProperties(properties);
        if (idx != null)
          break;
      }

    return idx;
  }

  public Schema getSchema() {
    return schema;
  }

  @Override
  public String toString() {
    return name;
  }

  public boolean isTheSameAs(final Object o) {
    if (this == o)
      return true;
    if (o == null || getClass() != o.getClass())
      return false;

    final DocumentType that = (DocumentType) o;
    if (!Objects.equals(name, that.name))
      return false;

    if (superTypes.size() != that.superTypes.size())
      return false;

    final Set<String> set = new HashSet<>();
    for (DocumentType t : superTypes)
      set.add(t.name);

    for (DocumentType t : that.superTypes)
      set.remove(t.name);

    if (!set.isEmpty())
      return false;

    if (subTypes.size() != that.subTypes.size())
      return false;

    set.clear();

    for (DocumentType t : subTypes)
      set.add(t.name);

    for (DocumentType t : that.subTypes)
      set.remove(t.name);

    if (!set.isEmpty())
      return false;

    if (buckets.size() != that.buckets.size())
      return false;

    set.clear();

    for (Bucket t : buckets)
      set.add(t.getName());

    for (Bucket t : that.buckets)
      set.remove(t.getName());

    if (!set.isEmpty())
      return false;

    if (properties.size() != that.properties.size())
      return false;

    set.clear();

    for (Property p : properties.values())
      set.add(p.getName());

    for (Property p : that.properties.values())
      set.remove(p.getName());

    if (!set.isEmpty())
      return false;

    for (Property p1 : properties.values()) {
      final Property p2 = that.properties.get(p1.getName());
      if (!p1.equals(p2))
        return false;
    }

    if (bucketIndexesByBucket.size() != that.bucketIndexesByBucket.size())
      return false;

    for (Map.Entry<Integer, List<IndexInternal>> entry1 : bucketIndexesByBucket.entrySet()) {
      final List<IndexInternal> value2 = that.bucketIndexesByBucket.get(entry1.getKey());
      if (value2 == null)
        return false;
      if (entry1.getValue().size() != value2.size())
        return false;

      for (int i = 0; i < value2.size(); ++i) {
        final Index m1 = entry1.getValue().get(i);
        final Index m2 = value2.get(i);

        if (m1.getAssociatedBucketId() != m2.getAssociatedBucketId())
          return false;
        if (!m1.getName().equals(m2.getName()))
          return false;
        if (m1.getPropertyNames().size() != m2.getPropertyNames().size())
          return false;

        for (int p = 0; p < m1.getPropertyNames().size(); ++p) {
          if (!m1.getPropertyNames().get(p).equals(m2.getPropertyNames().get(p)))
            return false;
        }
      }
    }

    if (indexesByProperties.size() != that.indexesByProperties.size())
      return false;

    for (Map.Entry<List<String>, TypeIndex> entry1 : indexesByProperties.entrySet()) {
      final TypeIndex index2 = that.indexesByProperties.get(entry1.getKey());
      if (index2 == null)
        return false;

      final TypeIndex index1 = entry1.getValue();

      if (!index1.equals(index2))
        return false;
    }

    return true;
  }

  @Override
  public boolean equals(final Object o) {
    if (this == o)
      return true;
    if (o == null || getClass() != o.getClass())
      return false;
    final DocumentType that = (DocumentType) o;
    return name.equals(that.name);
  }

  @Override
  public int hashCode() {
    return Objects.hash(name);
  }

  protected void addIndexInternal(final IndexInternal index, final int bucketId, final String[] propertyNames) {
    index.setMetadata(name, propertyNames, bucketId);

    List<IndexInternal> list = bucketIndexesByBucket.get(bucketId);
    if (list == null) {
      list = new ArrayList<>();
      bucketIndexesByBucket.put(bucketId, list);
    }
    list.add(index);

    final List<String> propertyList = Arrays.asList(propertyNames);

    TypeIndex propIndex = indexesByProperties.get(propertyList);
    if (propIndex == null) {
      // CREATE THE TYPE-INDEX FOR THE 1ST TIME
      propIndex = new TypeIndex(name + Arrays.toString(propertyNames).replace(" ", ""), this);
      schema.indexMap.put(propIndex.getName(), propIndex);
      indexesByProperties.put(propertyList, propIndex);
    }

    // ADD AS SUB-INDEX
    propIndex.addIndexOnBucket(index);
    this.bucketSelectionStrategy.setType(this);
  }

  public void removeIndexInternal(final TypeIndex index) {
    for (Iterator<TypeIndex> it = indexesByProperties.values().iterator(); it.hasNext(); ) {
      final TypeIndex idx = it.next();
      if (idx == index) {
        it.remove();
        break;
      }
    }

    for (IndexInternal idx : index.getIndexesOnBuckets()) {
      final List<IndexInternal> list = bucketIndexesByBucket.get(idx.getAssociatedBucketId());
      if (list != null) {
        list.remove(idx);
        if (list.isEmpty())
          bucketIndexesByBucket.remove(idx.getAssociatedBucketId());
      }
    }

    for (DocumentType superType : superTypes)
      superType.removeIndexInternal(index);
  }

  protected void addBucketInternal(final Bucket bucket) {
    for (DocumentType cl : schema.getTypes()) {
      if (cl.hasBucket(bucket.getName()))
        throw new SchemaException(
            "Cannot add the bucket '" + bucket.getName() + "' to the type '" + name + "', because the bucket is already associated to the type '" + cl.getName()
                + "'");
    }

    buckets.add(bucket);
    bucketSelectionStrategy.setType(this);
  }

  protected void removeBucketInternal(final Bucket bucket) {
    if (!buckets.contains(bucket))
      throw new SchemaException(
          "Cannot remove the bucket '" + bucket.getName() + "' to the type '" + name + "', because the bucket is not associated to the type '" + getName()
              + "'");

    buckets.remove(bucket);
  }

  protected Map<String, Property> getPolymorphicProperties() {
    final Map<String, Property> allProperties = new HashMap<>();
    allProperties.putAll(properties);

    for (DocumentType p : superTypes)
      allProperties.putAll(p.getPolymorphicProperties());

    return allProperties;
  }

  public boolean hasBucket(final String bucketName) {
    for (Bucket b : buckets)
      if (b.getName().equals(bucketName))
        return true;
    return false;
  }

  public int getFirstBucketId() {
    return buckets.get(0).getId();
  }

  public boolean isSubTypeOf(final String type) {
    if (type == null)
      return false;

    if (type.equalsIgnoreCase(getName()))
      return true;
    for (DocumentType superType : superTypes) {
      if (superType.isSubTypeOf(type))
        return true;
    }
    return false;
  }

  public boolean isSuperTypeOf(final String type) {
    if (type == null)
      return false;

    if (type.equalsIgnoreCase(getName()))
      return true;
    for (DocumentType subType : subTypes) {
      if (subType.isSuperTypeOf(type))
        return true;
    }
    return false;
  }

  public Set<String> getCustomKeys() {
    return Collections.unmodifiableSet(custom.keySet());
  }

  public Object getCustomValue(final String key) {
    return custom.get(key);
  }

  public Object setCustom(final String key, final Object value) {
    if (value == null)
      return custom.remove(key);
    return custom.put(key, value);
  }

  protected <RET> RET recordFileChanges(final Callable<Object> callback) {
    return schema.recordFileChanges(callback);
  }
}
