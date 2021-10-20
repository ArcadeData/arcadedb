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
  protected final List<DocumentType>                parentTypes             = new ArrayList<>();
  protected final List<DocumentType>                subTypes                = new ArrayList<>();
  protected final List<Bucket>                      buckets                 = new ArrayList<>();
  protected       BucketSelectionStrategy           bucketSelectionStrategy = new RoundRobinBucketSelectionStrategy();
  protected final Map<String, Property>             properties              = new HashMap<>();
  protected       Map<Integer, List<IndexInternal>> bucketIndexesByBucket   = new HashMap<>();
  protected       Map<List<String>, TypeIndex>      indexesByProperties     = new HashMap<>();
  protected final RecordEventsRegistry              events                  = new RecordEventsRegistry();

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

  public DocumentType addParentType(final String parentName) {
    return addParentType(schema.getType(parentName));
  }

  public DocumentType addParentType(final DocumentType parent) {
    if (parentTypes.indexOf(parent) > -1)
      // ALREADY PARENT
      return this;

    final Set<String> allProperties = getPolymorphicPropertyNames();
    for (String p : parent.getPolymorphicPropertyNames())
      if (allProperties.contains(p)) {
        LogManager.instance().log(this, Level.WARNING, "Property '" + p + "' is already defined in type '" + name + "' or any parent types");
        //throw new IllegalArgumentException("Property '" + p + "' is already defined in type '" + name + "' or any parent types");
      }

    recordFileChanges(() -> {
      parentTypes.add(parent);
      parent.subTypes.add(this);
      return null;
    });
    return this;
  }

  public void removeParentType(final String parentName) {
    removeParentType(schema.getType(parentName));
  }

  public void removeParentType(final DocumentType parent) {
    recordFileChanges(() -> {
      if (!parentTypes.remove(parent))
        // ALREADY REMOVED PARENT
        return null;

      parent.subTypes.remove(this);
      return null;
    });
  }

  public boolean instanceOf(final String type) {
    if (name.equals(type))
      return true;

    for (DocumentType t : parentTypes) {
      if (t.instanceOf(type))
        return true;
    }

    return false;
  }

  public List<DocumentType> getParentTypes() {
    return Collections.unmodifiableList(parentTypes);
  }

  public void setParentTypes(List<DocumentType> newParents) {
    if (newParents == null)
      newParents = Collections.emptyList();
    List<DocumentType> commonParents = new ArrayList<>(parentTypes);
    commonParents.retainAll(newParents);
    List<DocumentType> toRemove = new ArrayList<>(parentTypes);
    toRemove.removeAll(commonParents);
    toRemove.forEach(this::removeParentType);
    List<DocumentType> toAdd = new ArrayList<>(newParents);
    toAdd.removeAll(commonParents);
    toAdd.forEach(this::addParentType);
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
    for (DocumentType p : parentTypes)
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
      throw new SchemaException("Cannot create the property '" + propertyName + "' in type '" + name + "' because it was already defined in a parent type");

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

    for (DocumentType parent : parentTypes) {
      prop = parent.getPolymorphicPropertyIfExists(propertyName);
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

  public List<Index> getAllIndexes(final boolean polymorphic) {
    if (!polymorphic || parentTypes.isEmpty())
      return new ArrayList<>(indexesByProperties.values());

    final List<Index> list = new ArrayList<>();
    for (TypeIndex idx : indexesByProperties.values())
      list.add(idx);

    for (DocumentType t : parentTypes)
      list.addAll(t.getAllIndexes(polymorphic));

    return list;
  }

  public List<Index> getPolymorphicBucketIndexByBucketId(final int bucketId) {
    final List<IndexInternal> r = bucketIndexesByBucket.get(bucketId);
    if (r != null && parentTypes.isEmpty())
      // MOST COMMON CASE, SAVE CREATING AND COPYING TO A NEW ARRAY
      return Collections.unmodifiableList(r);

    final List<Index> result = r != null ? new ArrayList<>(r) : new ArrayList<>();
    for (DocumentType t : parentTypes)
      result.addAll(t.getPolymorphicBucketIndexByBucketId(bucketId));

    return result;
  }

  public List<TypeIndex> getIndexesByProperty(final String property) {
    final List<TypeIndex> result = new ArrayList<>();

    for (Map.Entry<List<String>, TypeIndex> entry : indexesByProperties.entrySet()) {
      for (String prop : entry.getKey()) {
        if (property.equals(prop)) {
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
      for (DocumentType t : parentTypes) {
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

    if (parentTypes.size() != that.parentTypes.size())
      return false;

    final Set<String> set = new HashSet<>();
    for (DocumentType t : parentTypes)
      set.add(t.name);

    for (DocumentType t : that.parentTypes)
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

    for (DocumentType parent : parentTypes)
      parent.removeIndexInternal(index);
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

    for (DocumentType p : parentTypes)
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
    for (DocumentType parentType : parentTypes) {
      if (parentType.isSubTypeOf(type))
        return true;
    }
    return false;
  }

  protected <RET> RET recordFileChanges(final Callable<Object> callback) {
    return schema.recordFileChanges(callback);
  }
}
