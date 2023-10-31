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

import com.arcadedb.database.Document;
import com.arcadedb.database.MutableDocument;
import com.arcadedb.database.RecordEvents;
import com.arcadedb.database.RecordEventsRegistry;
import com.arcadedb.database.bucketselectionstrategy.BucketSelectionStrategy;
import com.arcadedb.database.bucketselectionstrategy.PartitionedBucketSelectionStrategy;
import com.arcadedb.database.bucketselectionstrategy.RoundRobinBucketSelectionStrategy;
import com.arcadedb.database.bucketselectionstrategy.ThreadBucketSelectionStrategy;
import com.arcadedb.engine.Bucket;
import com.arcadedb.exception.SchemaException;
import com.arcadedb.index.Index;
import com.arcadedb.index.IndexException;
import com.arcadedb.index.IndexInternal;
import com.arcadedb.index.TypeIndex;
import com.arcadedb.index.lsm.LSMTreeIndexAbstract;
import com.arcadedb.log.LogManager;
import com.arcadedb.serializer.json.JSONObject;
import com.arcadedb.utility.CollectionUtils;
import com.arcadedb.utility.FileUtils;

import java.util.*;
import java.util.concurrent.*;
import java.util.logging.*;

public class DocumentType {
  protected final EmbeddedSchema                    schema;
  protected final String                            name;
  protected final List<DocumentType>                superTypes                   = new ArrayList<>();
  protected final List<DocumentType>                subTypes                     = new ArrayList<>();
  protected       List<Bucket>                      buckets                      = new ArrayList<>();
  protected       List<Bucket>                      cachedPolymorphicBuckets     = new ArrayList<>(); // PRE COMPILED LIST TO SPEED UP RUN-TIME OPERATIONS
  protected       List<Integer>                     bucketIds                    = new ArrayList<>();
  protected       List<Integer>                     cachedPolymorphicBucketIds   = new ArrayList<>(); // PRE COMPILED LIST TO SPEED UP RUN-TIME OPERATIONS
  protected       BucketSelectionStrategy           bucketSelectionStrategy      = new RoundRobinBucketSelectionStrategy();
  protected final Map<String, Property>             properties                   = new HashMap<>();
  protected final Map<Integer, List<IndexInternal>> bucketIndexesByBucket        = new HashMap<>();
  protected final Map<List<String>, TypeIndex>      indexesByProperties          = new HashMap<>();
  protected final RecordEventsRegistry              events                       = new RecordEventsRegistry();
  protected final Map<String, Object>               custom                       = new HashMap<>();
  protected       Set<String>                       propertiesWithDefaultDefined = Collections.emptySet();

  public DocumentType(final EmbeddedSchema schema, final String name) {
    this.schema = schema;
    this.name = name;
  }

  public String getName() {
    return name;
  }

  public MutableDocument newRecord() {
    return schema.getDatabase().newDocument(name);
  }

  public byte getType() {
    return Document.RECORD_TYPE;
  }

  public RecordEvents getEvents() {
    return events;
  }

  public Set<String> getPolymorphicPropertiesWithDefaultDefined() {
    if (superTypes.isEmpty())
      return propertiesWithDefaultDefined;

    final HashSet<String> set = new HashSet<>(propertiesWithDefaultDefined);
    for (final DocumentType superType : superTypes)
      set.addAll(superType.propertiesWithDefaultDefined);
    return set;
  }

  public DocumentType addSuperType(final String superName) {
    return addSuperType(schema.getType(superName));
  }

  public DocumentType addSuperType(final DocumentType superType) {
    return addSuperType(superType, true);
  }

  protected DocumentType addSuperType(final DocumentType superType, final boolean createIndexes) {
    if (superTypes.contains(superType))
      // ALREADY PARENT
      return this;

    // CHECK FOR CONFLICT WITH PROPERTIES NAMES
    final Set<String> allProperties = getPolymorphicPropertyNames();
    for (final String p : superType.getPolymorphicPropertyNames())
      if (allProperties.contains(p)) {
        LogManager.instance()
            .log(this, Level.WARNING, "Property '" + p + "' is already defined in type '" + name + "' or any super types");
        //throw new IllegalArgumentException("Property '" + p + "' is already defined in type '" + name + "' or any super types");
      }

    recordFileChanges(() -> {
      superTypes.add(superType);
      superType.subTypes.add(this);

      // UPDATE THE LIST OF POLYMORPHIC BUCKETS TREE
      superType.updatePolymorphicBucketsCache(true, cachedPolymorphicBuckets, cachedPolymorphicBucketIds);

      // CREATE INDEXES AUTOMATICALLY ON PROPERTIES DEFINED IN SUPER TYPES
      final Collection<TypeIndex> indexes = new ArrayList<>(getAllIndexes(true));
      indexes.removeAll(indexesByProperties.values());

      if (createIndexes) {
        try {
          schema.getDatabase().transaction(() -> {
            for (final TypeIndex index : indexes) {
              if (index.getType() == null) {
                LogManager.instance().log(this, Level.WARNING,
                    "Error on creating implicit indexes from super type '" + superType.getName() + "': key types is null");
              } else {
                for (int i = 0; i < buckets.size(); i++) {
                  final Bucket bucket = buckets.get(i);
                  schema.createBucketIndex(this, index.getKeyTypes(), bucket, name, index.getType(), index.isUnique(),
                      LSMTreeIndexAbstract.DEF_PAGE_SIZE, index.getNullStrategy(), null,
                      index.getPropertyNames().toArray(new String[index.getPropertyNames().size()]), index,
                      IndexBuilder.BUILD_BATCH_SIZE);
                }
              }
            }
          }, false);
        } catch (final IndexException e) {
          LogManager.instance()
              .log(this, Level.WARNING, "Error on creating implicit indexes from super type '" + superType.getName() + "'", e);
          throw e;
        }
      }

      if (!superType.getBucketSelectionStrategy().getName().equalsIgnoreCase(getBucketSelectionStrategy().getName()))
        // INHERIT THE BUCKET SELECTION STRATEGY FROM THE SUPER TYPE
        setBucketSelectionStrategy(superType.getBucketSelectionStrategy().copy());

      return null;
    });
    return this;
  }

  /**
   * Removes a super type (by its name) from the current type.
   *
   * @see #removeSuperType(DocumentType)
   * @see #addSuperType(DocumentType)
   * @see #addSuperType(String)
   * @see #addSuperType(DocumentType, boolean)
   */
  public DocumentType removeSuperType(final String superTypeName) {
    removeSuperType(schema.getType(superTypeName));
    return this;
  }

  /**
   * Removes a super type from the current type.
   *
   * @see #removeSuperType(String)
   * @see #addSuperType(DocumentType)
   * @see #addSuperType(String)
   * @see #addSuperType(DocumentType, boolean)
   */
  public DocumentType removeSuperType(final DocumentType superType) {
    recordFileChanges(() -> {
      if (!superTypes.remove(superType))
        // ALREADY REMOVED SUPER TYPE
        return null;

      superType.subTypes.remove(this);

      // TODO: CHECK THE EDGE CASE WHERE THE SAME TYPE IS INHERITED ON MULTIPLE LEVEL AND YOU DON'T NEED TO REMOVE THE BUCKETS
      // UPDATE THE LIST OF POLYMORPHIC BUCKETS TREE
      superType.updatePolymorphicBucketsCache(false, buckets, bucketIds);

      return null;
    });
    return this;
  }

  /**
   * Returns true if the current type is the same or a subtype of `type` parameter.
   *
   * @param type the type name to check
   *
   * @see #addSuperType(DocumentType)
   * @see #addSuperType(String)
   * @see #addSuperType(DocumentType, boolean)
   */
  public boolean instanceOf(final String type) {
    if (name.equals(type))
      return true;

    for (final DocumentType t : superTypes) {
      if (t.instanceOf(type))
        return true;
    }

    return false;
  }

  /**
   * Returns the list of super types if any, otherwise an empty collection.
   *
   * @see #addSuperType(DocumentType)
   * @see #addSuperType(String)
   * @see #addSuperType(DocumentType, boolean)
   */
  public List<DocumentType> getSuperTypes() {
    return Collections.unmodifiableList(superTypes);
  }

  /**
   * Set the type super types. Any previous configuration about supertypes will be replaced with this new list.
   *
   * @param newSuperTypes List of super types to assign
   *
   * @see #addSuperType(DocumentType)
   * @see #addSuperType(String)
   * @see #addSuperType(DocumentType, boolean)
   */
  public DocumentType setSuperTypes(List<DocumentType> newSuperTypes) {
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

    return this;
  }

  /**
   * Returns the list of subtypes, in any, or an empty list in case the type has not subtypes defined.
   *
   * @see #addSuperType(DocumentType)
   * @see #addSuperType(String)
   * @see #addSuperType(DocumentType, boolean)
   */
  public List<DocumentType> getSubTypes() {
    return Collections.unmodifiableList(subTypes);
  }

  /**
   * Returns all the properties defined in the type, not considering the ones inherited from subtypes.
   *
   * @return Set containing all the names
   *
   * @see #getPolymorphicPropertyNames()
   */
  public Set<String> getPropertyNames() {
    return properties.keySet();
  }

  public Collection<Property> getProperties() {
    return properties.values();
  }

  /**
   * Returns all the properties defined in the type and subtypes.
   *
   * @return Set containing all the names
   *
   * @see #getPropertyNames()
   */
  public Set<String> getPolymorphicPropertyNames() {
    if (superTypes.isEmpty())
      return getPropertyNames();

    final Set<String> allProperties = new HashSet<>(getPropertyNames());
    for (final DocumentType p : superTypes)
      allProperties.addAll(p.getPolymorphicPropertyNames());
    return allProperties;
  }

  /**
   * Creates a new property with type `propertyType`.
   *
   * @param propertyName Property name to remove
   * @param propertyType Property type by type name @{@link String}
   */
  public Property createProperty(final String propertyName, final String propertyType) {
    return createProperty(propertyName, Type.getTypeByName(propertyType));
  }

  /**
   * Creates a new property with type `propertyType`.
   *
   * @param propertyName Property name to remove
   * @param propertyType Property type as Java @{@link Class}
   */
  public Property createProperty(final String propertyName, final Class<?> propertyType) {
    return createProperty(propertyName, Type.getTypeByClass(propertyType));
  }

  public Property createProperty(final String propName, final JSONObject prop) {
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
    if (prop.has("max"))
      p.setMax(prop.getString("max"));
    if (prop.has("min"))
      p.setMin(prop.getString("min"));
    if (prop.has("regexp"))
      p.setRegexp(prop.getString("regexp"));

    p.custom.clear();
    if (prop.has("custom"))
      p.custom.putAll(prop.getJSONObject("custom").toMap());

    return p;
  }

  /**
   * Creates a new property with type `propertyType`.
   *
   * @param propertyName Property name to remove
   * @param propertyType Property type as @{@link Type}
   */
  public Property createProperty(final String propertyName, final Type propertyType) {
    return createProperty(propertyName, propertyType, null);
  }

  /**
   * Creates a new property with type `propertyType`.
   *
   * @param propertyName Property name to remove
   * @param propertyType Property type as @{@link Type}
   * @param ofType       Linked type. For List the type contained in the list. For RID the schema type name.
   */
  public Property createProperty(final String propertyName, final Type propertyType, final String ofType) {
    if (properties.containsKey(propertyName))
      throw new SchemaException(
          "Cannot create the property '" + propertyName + "' in type '" + name + "' because it already exists");

    if (getPolymorphicPropertyNames().contains(propertyName))
      throw new SchemaException("Cannot create the property '" + propertyName + "' in type '" + name
          + "' because it was already defined in a super type");

    final Property property = new Property(this, propertyName, propertyType);

    if (ofType != null)
      property.setOfType(ofType);

    recordFileChanges(() -> {
      properties.put(propertyName, property);
      return null;
    });
    return property;
  }

  /**
   * Returns a property by its name. If the property does not exist, it is created with type `propertyType`.
   *
   * @param propertyName Property name to remove
   * @param propertyType Property type, by type name @{@link String}, to use in case the property does not exist and will be created
   */
  public Property getOrCreateProperty(final String propertyName, final String propertyType) {
    return getOrCreateProperty(propertyName, Type.getTypeByName(propertyType), null);
  }

  /**
   * Returns a property by its name. If the property does not exist, it is created with type `propertyType`.
   *
   * @param propertyName Property name to remove
   * @param propertyType Property type, by type name @{@link String}, to use in case the property does not exist and will be created
   * @param ofType       Linked type. For List the type contained in the list. For RID the schema type name.
   */
  public Property getOrCreateProperty(final String propertyName, final String propertyType, final String ofType) {
    return getOrCreateProperty(propertyName, Type.getTypeByName(propertyType), ofType);
  }

  /**
   * Returns a property by its name. If the property does not exist, it is created with type `propertyType`.
   *
   * @param propertyName Property name to remove
   * @param propertyType Property type, as Java @{@link Class}, to use in case the property does not exist and will be created
   */
  public Property getOrCreateProperty(final String propertyName, final Class<?> propertyType) {
    return getOrCreateProperty(propertyName, Type.getTypeByClass(propertyType), null);
  }

  /**
   * Returns a property by its name. If the property does not exist, it is created with type `propertyType`.
   *
   * @param propertyName Property name to remove
   * @param propertyType Property type, as @{@link Type}, to use in case the property does not exist and will be created
   */
  public Property getOrCreateProperty(final String propertyName, final Type propertyType) {
    return getOrCreateProperty(propertyName, propertyType, null);
  }

  /**
   * Returns a property by its name. If the property does not exist, it is created with type `propertyType`.
   *
   * @param propertyName Property name to remove
   * @param propertyType Property type, as @{@link Type}, to use in case the property does not exist and will be created
   * @param ofType       Linked type. For List the type contained in the list. For RID the schema type name.
   */
  public Property getOrCreateProperty(final String propertyName, final Type propertyType, final String ofType) {
    final Property p = getPolymorphicPropertyIfExists(propertyName);
    if (p != null) {
      if (p.getType().equals(propertyType))
        return p;

      // DIFFERENT TYPE: DROP THE PROPERTY AND CREATE A NEW ONE
      dropProperty(propertyName);
    }
    return createProperty(propertyName, propertyType, ofType);
  }

  /**
   * Drops a property from the type. If there is any index on the property a @{@link SchemaException} is thrown.
   *
   * @param propertyName Property name to remove
   *
   * @return the property dropped if found
   */
  public Property dropProperty(final String propertyName) {
    for (final TypeIndex index : getAllIndexes(true)) {
      if (index.getPropertyNames().contains(propertyName))
        throw new SchemaException(
            "Error on dropping property '" + propertyName + "' because used by index '" + index.getName() + "'");
    }

    return recordFileChanges(() -> {
      return properties.remove(propertyName);
    });
  }

  public TypeIndex createTypeIndex(final EmbeddedSchema.INDEX_TYPE indexType, final boolean unique, final String... propertyNames) {
    return schema.buildTypeIndex(name, propertyNames).withType(indexType).withUnique(unique).create();
  }

  public TypeIndex createTypeIndex(final EmbeddedSchema.INDEX_TYPE indexType, final boolean unique, final String[] propertyNames,
      final int pageSize) {
    return schema.buildTypeIndex(name, propertyNames).withType(indexType).withUnique(unique).withPageSize(pageSize).create();
  }

  public TypeIndex createTypeIndex(final EmbeddedSchema.INDEX_TYPE indexType, final boolean unique, final String[] propertyNames,
      final int pageSize, final Index.BuildIndexCallback callback) {
    return schema.buildTypeIndex(name, propertyNames).withType(indexType).withUnique(unique).withPageSize(pageSize)
        .withCallback(callback).create();
  }

  public TypeIndex createTypeIndex(final EmbeddedSchema.INDEX_TYPE indexType, final boolean unique, final String[] propertyNames,
      final int pageSize, final LSMTreeIndexAbstract.NULL_STRATEGY nullStrategy, final Index.BuildIndexCallback callback) {
    return schema.buildTypeIndex(name, propertyNames).withType(indexType).withUnique(unique).withPageSize(pageSize)
        .withNullStrategy(nullStrategy).withCallback(callback).create();
  }

  public TypeIndex getOrCreateTypeIndex(final EmbeddedSchema.INDEX_TYPE indexType, final boolean unique,
      final String... propertyNames) {
    return schema.buildTypeIndex(name, propertyNames).withType(indexType).withUnique(unique).withIgnoreIfExists(true).create();
  }

  public TypeIndex getOrCreateTypeIndex(final EmbeddedSchema.INDEX_TYPE indexType, final boolean unique,
      final String[] propertyNames, final int pageSize) {
    return schema.buildTypeIndex(name, propertyNames).withType(indexType).withUnique(unique).withPageSize(pageSize)
        .withIgnoreIfExists(true).create();
  }

  public TypeIndex getOrCreateTypeIndex(final EmbeddedSchema.INDEX_TYPE indexType, final boolean unique,
      final String[] propertyNames, final int pageSize, final Index.BuildIndexCallback callback) {
    return schema.buildTypeIndex(name, propertyNames).withType(indexType).withUnique(unique).withPageSize(pageSize)
        .withCallback(callback).withIgnoreIfExists(true).create();
  }

  public TypeIndex getOrCreateTypeIndex(final EmbeddedSchema.INDEX_TYPE indexType, final boolean unique,
      final String[] propertyNames, final int pageSize, final LSMTreeIndexAbstract.NULL_STRATEGY nullStrategy,
      final Index.BuildIndexCallback callback) {
    return schema.buildTypeIndex(name, propertyNames).withType(indexType).withUnique(unique).withPageSize(pageSize)
        .withNullStrategy(nullStrategy).withCallback(callback).withIgnoreIfExists(true).create();
  }

  public List<Bucket> getInvolvedBuckets() {
    return getBuckets(false);
  }

  public List<Bucket> getBuckets(final boolean polymorphic) {
    return polymorphic ? cachedPolymorphicBuckets : buckets;
  }

  public List<Integer> getBucketIds(final boolean polymorphic) {
    return polymorphic ? cachedPolymorphicBucketIds : bucketIds;
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
      throw new SchemaException(
          "Cannot retrieve a bucket for keys '" + Arrays.toString(keys) + "' because there are no buckets associated");
    return bucketSelectionStrategy.getBucketIdByKeys(keys, async);
  }

  public BucketSelectionStrategy getBucketSelectionStrategy() {
    return bucketSelectionStrategy;
  }

  public DocumentType setBucketSelectionStrategy(final BucketSelectionStrategy selectionStrategy) {
    this.bucketSelectionStrategy = selectionStrategy;
    this.bucketSelectionStrategy.setType(this);
    return this;
  }

  public DocumentType setBucketSelectionStrategy(final String selectionStrategyName, final Object... args) {
    BucketSelectionStrategy selectionStrategy;
    if (selectionStrategyName.equalsIgnoreCase("thread"))
      selectionStrategy = new ThreadBucketSelectionStrategy();
    else if (selectionStrategyName.equalsIgnoreCase("round-robin"))
      selectionStrategy = new RoundRobinBucketSelectionStrategy();
    else if (selectionStrategyName.equalsIgnoreCase("partitioned")) {
      final List<String> convertedParams = new ArrayList<>(args.length);
      for (int i = 0; i < args.length; i++)
        convertedParams.add(FileUtils.getStringContent(args[i]));

      selectionStrategy = new PartitionedBucketSelectionStrategy(convertedParams);
    } else if (selectionStrategyName.startsWith("partitioned(") && selectionStrategyName.endsWith(")")) {
      final String[] params = selectionStrategyName.substring("partitioned(".length(), selectionStrategyName.length() - 1)
          .split(",");
      final List<String> convertedParams = new ArrayList<>(params.length);
      for (int i = 0; i < params.length; i++)
        convertedParams.add(FileUtils.getStringContent(params[i]));

      selectionStrategy = new PartitionedBucketSelectionStrategy(convertedParams);
    } else {
      // GET THE VALUE AS FULL-CLASS-NAME
      try {
        selectionStrategy = (BucketSelectionStrategy) Class.forName(selectionStrategyName).getConstructor().newInstance();
      } catch (Exception e) {
        throw new SchemaException("Cannot find bucket selection strategy class '" + selectionStrategyName + "'", e);
      }
    }

    this.bucketSelectionStrategy = selectionStrategy;
    this.bucketSelectionStrategy.setType(this);
    return this;
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

    for (final DocumentType superType : superTypes) {
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

  public Collection<TypeIndex> getAllIndexes(final boolean polymorphic) {
    if (!polymorphic || superTypes.isEmpty())
      return Collections.unmodifiableCollection(indexesByProperties.values());

    final List<TypeIndex> list = new ArrayList<>(indexesByProperties.values());

    for (final DocumentType t : superTypes)
      list.addAll(t.getAllIndexes(true));

    return Collections.unmodifiableCollection(list);
  }

  public List<IndexInternal> getPolymorphicBucketIndexByBucketId(final int bucketId, final List<String> filterByProperties) {
    List<IndexInternal> r = bucketIndexesByBucket.get(bucketId);

    if (r != null && filterByProperties != null) {
      // FILTER BY PROPERTY NAMES
      r = new ArrayList<>(r);
      r.removeIf((idx) -> !idx.getPropertyNames().equals(filterByProperties));
    }

    if (superTypes.isEmpty()) {
      // MOST COMMON CASES, OPTIMIZATION AVOIDING CREATING NEW LISTS
      if (r == null)
        return Collections.emptyList();
      else
        // MOST COMMON CASE, SAVE CREATING AND COPYING TO A NEW ARRAY
        return Collections.unmodifiableList(r);
    }

    final List<IndexInternal> result = r != null ? new ArrayList<>(r) : new ArrayList<>();
    for (final DocumentType t : superTypes)
      result.addAll(t.getPolymorphicBucketIndexByBucketId(bucketId, filterByProperties));

    return result;
  }

  public List<TypeIndex> getIndexesByProperties(final String property1, final String... propertiesN) {
    final Set<String> properties = new HashSet<>(propertiesN.length + 1);
    properties.add(property1);
    Collections.addAll(properties, propertiesN);
    return getIndexesByProperties(properties);
  }

  public List<TypeIndex> getIndexesByProperties(final Collection<String> properties) {
    final List<TypeIndex> result = new ArrayList<>();

    for (final Map.Entry<List<String>, TypeIndex> entry : indexesByProperties.entrySet()) {
      for (final String prop : entry.getKey()) {
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
      for (final DocumentType t : superTypes) {
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
    for (final DocumentType t : superTypes)
      set.add(t.name);

    for (final DocumentType t : that.superTypes)
      set.remove(t.name);

    if (!set.isEmpty())
      return false;

    if (subTypes.size() != that.subTypes.size())
      return false;

    for (final DocumentType t : subTypes)
      set.add(t.name);

    for (final DocumentType t : that.subTypes)
      set.remove(t.name);

    if (!set.isEmpty())
      return false;

    if (buckets.size() != that.buckets.size())
      return false;

    for (final Bucket t : buckets)
      set.add(t.getName());

    for (final Bucket t : that.buckets)
      set.remove(t.getName());

    if (!set.isEmpty())
      return false;

    if (properties.size() != that.properties.size())
      return false;

    for (final Property p : properties.values())
      set.add(p.getName());

    for (final Property p : that.properties.values())
      set.remove(p.getName());

    if (!set.isEmpty())
      return false;

    for (final Property p1 : properties.values()) {
      final Property p2 = that.properties.get(p1.getName());
      if (!p1.equals(p2))
        return false;
    }

    if (bucketIndexesByBucket.size() != that.bucketIndexesByBucket.size())
      return false;

    for (final Map.Entry<Integer, List<IndexInternal>> entry1 : bucketIndexesByBucket.entrySet()) {
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

    for (final Map.Entry<List<String>, TypeIndex> entry1 : indexesByProperties.entrySet()) {
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

  protected void addIndexInternal(final IndexInternal index, final int bucketId, final String[] propertyNames,
      TypeIndex propIndex) {
    index.setMetadata(name, propertyNames, bucketId);

    final List<IndexInternal> list = bucketIndexesByBucket.computeIfAbsent(bucketId, k -> new ArrayList<>());
    list.add(index);

    if (propIndex == null) {
      final List<String> propertyList = Arrays.asList(propertyNames);
      propIndex = indexesByProperties.get(propertyList);
      if (propIndex == null) {
        // CREATE THE TYPE-INDEX FOR THE 1ST TIME
        propIndex = new TypeIndex(name + Arrays.toString(propertyNames).replace(" ", ""), this);
        schema.indexMap.put(propIndex.getName(), propIndex);
        indexesByProperties.put(propertyList, propIndex);
      }
    }

    // ADD AS SUB-INDEX
    propIndex.addIndexOnBucket(index);
    this.bucketSelectionStrategy.setType(this);
  }

  public void removeTypeIndexInternal(final TypeIndex index) {
    for (final Iterator<TypeIndex> it = indexesByProperties.values().iterator(); it.hasNext(); ) {
      final TypeIndex idx = it.next();
      if (idx == index) {
        it.remove();
        break;
      }
    }

    for (final IndexInternal idx : index.getIndexesOnBuckets())
      removeBucketIndexInternal(idx);

    for (final DocumentType superType : superTypes)
      superType.removeTypeIndexInternal(index);
  }

  public void removeBucketIndexInternal(final Index index) {
    final List<IndexInternal> list = bucketIndexesByBucket.get(index.getAssociatedBucketId());
    if (list != null) {
      list.remove(index);
      if (list.isEmpty())
        bucketIndexesByBucket.remove(index.getAssociatedBucketId());
    }
  }

  protected void addBucketInternal(final Bucket bucket) {
    for (final DocumentType cl : schema.getTypes()) {
      if (cl.hasBucket(bucket.getName()))
        throw new SchemaException("Cannot add the bucket '" + bucket.getName() + "' to the type '" + name
            + "', because the bucket is already associated to the type '" + cl.getName() + "'");
    }

    buckets = CollectionUtils.addToUnmodifiableList(buckets, bucket);
    cachedPolymorphicBuckets = CollectionUtils.addToUnmodifiableList(cachedPolymorphicBuckets, bucket);

    bucketIds = CollectionUtils.addToUnmodifiableList(bucketIds, bucket.getFileId());
    cachedPolymorphicBucketIds = CollectionUtils.addToUnmodifiableList(cachedPolymorphicBucketIds, bucket.getFileId());

    bucketSelectionStrategy.setType(this);

    // AUTOMATICALLY CREATES THE INDEX ON THE NEW BUCKET
    final Collection<TypeIndex> existentIndexes = getAllIndexes(false);

    if (!existentIndexes.isEmpty()) {
      schema.getDatabase().transaction(() -> {
        for (TypeIndex idx : existentIndexes) {
          schema.createBucketIndex(this, idx.getKeyTypes(), bucket, name, idx.getType(), idx.isUnique(), idx.getPageSize(),
              idx.getNullStrategy(), null, idx.getPropertyNames().toArray(new String[idx.getPropertyNames().size()]), idx,
              IndexBuilder.BUILD_BATCH_SIZE);
        }
      });
    }
  }

  protected void removeBucketInternal(final Bucket bucket) {
    if (!buckets.contains(bucket))
      throw new SchemaException("Cannot remove the bucket '" + bucket.getName() + "' to the type '" + name
          + "', because the bucket is not associated to the type '" + getName() + "'");

    buckets = CollectionUtils.removeFromUnmodifiableList(buckets, bucket);
    cachedPolymorphicBuckets = CollectionUtils.removeFromUnmodifiableList(cachedPolymorphicBuckets, bucket);

    bucketIds = CollectionUtils.removeFromUnmodifiableList(bucketIds, bucket.getFileId());
    cachedPolymorphicBucketIds = CollectionUtils.removeFromUnmodifiableList(cachedPolymorphicBucketIds, bucket.getFileId());

    // AUTOMATICALLY DROP THE INDEX ON THE REMOVED BUCKET
    final Collection<TypeIndex> existentIndexes = getAllIndexes(false);

    if (!existentIndexes.isEmpty()) {
      schema.getDatabase().transaction(() -> {
        for (TypeIndex idx : existentIndexes) {
          for (IndexInternal subIndex : idx.getIndexesOnBuckets())
            if (subIndex.getAssociatedBucketId() == bucket.getFileId())
              schema.dropIndex(subIndex.getName());
        }
      });
    }
  }

  public boolean hasBucket(final String bucketName) {
    for (final Bucket b : buckets)
      if (b.getName().equals(bucketName))
        return true;
    return false;
  }

  public int getFirstBucketId() {
    return buckets.get(0).getFileId();
  }

  public boolean isSubTypeOf(final String type) {
    if (type == null)
      return false;

    if (type.equalsIgnoreCase(getName()))
      return true;
    for (final DocumentType superType : superTypes) {
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
    for (final DocumentType subType : subTypes) {
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

  public Object setCustomValue(final String key, final Object value) {
    if (value == null)
      return custom.remove(key);
    return custom.put(key, value);
  }

  public JSONObject toJSON() {
    final JSONObject type = new JSONObject();

    final String kind;
    if (this instanceof VertexType)
      kind = "v";
    else if (this instanceof EdgeType)
      kind = "e";
    else
      kind = "d";
    type.put("type", kind);

    final String[] parents = new String[getSuperTypes().size()];
    for (int i = 0; i < parents.length; ++i)
      parents[i] = getSuperTypes().get(i).getName();
    type.put("parents", parents);

    final List<Bucket> originalBuckets = getBuckets(false);
    final String[] buckets = new String[originalBuckets.size()];
    for (int i = 0; i < buckets.length; ++i)
      buckets[i] = originalBuckets.get(i).getName();

    type.put("buckets", buckets);

    final JSONObject properties = new JSONObject();
    type.put("properties", properties);

    for (final String propName : getPropertyNames())
      properties.put(propName, getProperty(propName).toJSON());

    final JSONObject indexes = new JSONObject();
    type.put("indexes", indexes);

    final BucketSelectionStrategy strategy = getBucketSelectionStrategy();
    if (!strategy.getName().equals(RoundRobinBucketSelectionStrategy.NAME))
      // WRITE ONLY IF NOT DEFAULT
      type.put("bucketSelectionStrategy", strategy.toJSON());

    for (final TypeIndex i : getAllIndexes(false)) {
      for (final IndexInternal entry : i.getIndexesOnBuckets())
        indexes.put(entry.getMostRecentFileName(), entry.toJSON());
    }

    type.put("custom", new JSONObject(custom));
    return type;
  }

  protected <RET> RET recordFileChanges(final Callable<Object> callback) {
    return schema.recordFileChanges(callback);
  }

  private void updatePolymorphicBucketsCache(final boolean add, final List<Bucket> buckets, final List<Integer> bucketIds) {
    if (add) {
      cachedPolymorphicBuckets = CollectionUtils.addAllToUnmodifiableList(cachedPolymorphicBuckets, buckets);
      cachedPolymorphicBucketIds = CollectionUtils.addAllToUnmodifiableList(cachedPolymorphicBucketIds, bucketIds);
    } else {
      cachedPolymorphicBuckets = CollectionUtils.removeAllFromUnmodifiableList(cachedPolymorphicBuckets, buckets);
      cachedPolymorphicBucketIds = CollectionUtils.removeAllFromUnmodifiableList(cachedPolymorphicBucketIds, bucketIds);
    }

    for (DocumentType s : superTypes)
      s.updatePolymorphicBucketsCache(add, cachedPolymorphicBuckets, cachedPolymorphicBucketIds);
  }
}
