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
import com.arcadedb.engine.Component;
import com.arcadedb.engine.Dictionary;
import com.arcadedb.exception.SchemaException;
import com.arcadedb.function.FunctionDefinition;
import com.arcadedb.function.FunctionLibraryDefinition;
import com.arcadedb.index.Index;
import com.arcadedb.index.IndexInternal;
import com.arcadedb.index.TypeIndex;
import com.arcadedb.index.lsm.LSMTreeIndexAbstract;
import com.arcadedb.query.sql.executor.Result;
import com.arcadedb.query.sql.executor.ResultSet;
import com.arcadedb.schema.BucketIndexBuilder;
import com.arcadedb.schema.DocumentType;
import com.arcadedb.schema.EdgeType;
import com.arcadedb.schema.EmbeddedSchema;
import com.arcadedb.schema.ManualIndexBuilder;
import com.arcadedb.schema.Property;
import com.arcadedb.schema.Schema;
import com.arcadedb.schema.Type;
import com.arcadedb.schema.TypeBuilder;
import com.arcadedb.schema.TypeIndexBuilder;
import com.arcadedb.schema.VectorIndexBuilder;
import com.arcadedb.schema.VertexType;
import com.arcadedb.serializer.json.JSONObject;

import java.time.*;
import java.util.*;
import java.util.concurrent.*;
import java.util.stream.*;

/**
 * Remote Document Type implementation used by Remote Database. It's not thread safe. For multi-thread usage create one instance of RemoteDatabase per thread.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
public class RemoteDocumentType extends DocumentType {
  private final RemoteDatabase remoteDatabase;

  public RemoteDocumentType(final RemoteDatabase remoteDatabase, final String typeName) {
    super(null, typeName);
    this.remoteDatabase = remoteDatabase;
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
    return o instanceof RemoteDocumentType && ((RemoteDocumentType) o).getName().equals(name);
  }

  @Override
  public int hashCode() {
    return name.hashCode();
  }

  @Override
  public MutableDocument newRecord() {
    return new RemoteMutableDocument(remoteDatabase, name);
  }

  @Override
  public Property createProperty(final String propertyName, final String propertyType) {
    remoteDatabase.command("sql", "create property `" + name + "`.`" + propertyName + "` " + propertyType);
    return null;
  }

  @Override
  public Property createProperty(final String propertyName, final Class<?> propertyType) {
    remoteDatabase.command("sql",
        "create property `" + name + "`.`" + propertyName + "` " + Type.getTypeByClass(propertyType).name());
    return null;
  }

  @Override
  public Property createProperty(String propName, JSONObject prop) {
    throw new UnsupportedOperationException();
  }

  @Override
  public Property createProperty(String propertyName, Type propertyType) {
    remoteDatabase.command("sql", "create property `" + name + "`.`" + propertyName + "` " + propertyType.name());
    return null;
  }

  @Override
  public Property createProperty(final String propertyName, final Type propertyType, final String ofType) {
    remoteDatabase.command("sql", "create property `" + name + "`.`" + propertyName + "` " + propertyType.name() + " of " + ofType);
    return null;
  }

  @Override
  public Property getOrCreateProperty(final String propertyName, final String propertyType) {
    remoteDatabase.command("sql", "create property `" + name + "`.`" + propertyName + "` if not exists " + propertyType);
    return null;
  }

  @Override
  public Property getOrCreateProperty(final String propertyName, final String propertyType, final String ofType) {
    remoteDatabase.command("sql",
        "create property `" + name + "`.`" + propertyName + "` if not exists " + propertyType + " of " + ofType);
    return null;
  }

  @Override
  public Property getOrCreateProperty(final String propertyName, final Class<?> propertyType) {
    remoteDatabase.command("sql",
        "create property `" + name + "`.`" + propertyName + "` if not exists " + Type.getTypeByClass(propertyType).name());
    return null;
  }

  @Override
  public Property getOrCreateProperty(final String propertyName, final Type propertyType) {
    remoteDatabase.command("sql", "create property `" + name + "`.`" + propertyName + "` if not exists " + propertyType.name());
    return null;
  }

  @Override
  public Property getOrCreateProperty(final String propertyName, final Type propertyType, final String ofType) {
    remoteDatabase.command("sql",
        "create property `" + name + "`.`" + propertyName + "` if not exists " + propertyType.name() + " of " + ofType);
    return null;
  }

  @Override
  public Property dropProperty(final String propertyName) {
    remoteDatabase.command("sql", "drop property `" + name + "`.`" + propertyName + "`");
    return null;
  }

  @Override
  public TypeIndex createTypeIndex(final EmbeddedSchema.INDEX_TYPE indexType, final boolean unique, final String... propertyNames) {
    remoteDatabase.getSchema().createTypeIndex(indexType, unique, name, propertyNames);
    return null;
  }

  @Override
  public TypeIndex getOrCreateTypeIndex(final EmbeddedSchema.INDEX_TYPE indexType, final boolean unique,
      final String... propertyNames) {
    remoteDatabase.getSchema().getOrCreateTypeIndex(indexType, unique, name, propertyNames);
    return null;
  }

  @Override
  public DocumentType addSuperType(final String superName) {
    remoteDatabase.command("sql", "alter type `" + name + "` supertype +" + superName);
    return this;
  }

  @Override
  public DocumentType addSuperType(final DocumentType superType) {
    remoteDatabase.command("sql", "alter type `" + name + "` supertype +" + superType.getName());
    return this;
  }

  @Override
  public DocumentType removeSuperType(final String superTypeName) {
    remoteDatabase.command("sql", "alter type `" + name + "` supertype -" + superTypeName);
    return this;
  }

  @Override
  public DocumentType removeSuperType(final DocumentType superType) {
    remoteDatabase.command("sql", "alter type `" + name + "` supertype -" + superType.getName());
    return this;
  }

  // UNSUPPORTED METHODS. OPEN A NEW ISSUE TO REQUEST THE SUPPORT OF ADDITIONAL METHODS IN REMOTE
  @Override
  public boolean instanceOf(final String type) {
    throw new UnsupportedOperationException();
  }

  @Override
  public List<DocumentType> getSuperTypes() {
    throw new UnsupportedOperationException();
  }

  @Override
  public List<DocumentType> getSubTypes() {
    throw new UnsupportedOperationException();
  }

  @Override
  public Set<String> getPropertyNames() {
    throw new UnsupportedOperationException();
  }

  @Override
  public TypeIndex getOrCreateTypeIndex(final EmbeddedSchema.INDEX_TYPE indexType, final boolean unique,
      final String[] propertyNames, final int pageSize) {
    throw new UnsupportedOperationException();
  }

  @Override
  public TypeIndex getOrCreateTypeIndex(final EmbeddedSchema.INDEX_TYPE indexType, final boolean unique,
      final String[] propertyNames, final int pageSize, Index.BuildIndexCallback callback) {
    throw new UnsupportedOperationException();
  }

  @Override
  public TypeIndex getOrCreateTypeIndex(EmbeddedSchema.INDEX_TYPE indexType, boolean unique, String[] propertyNames, int pageSize,
      LSMTreeIndexAbstract.NULL_STRATEGY nullStrategy, Index.BuildIndexCallback callback) {
    throw new UnsupportedOperationException();
  }

  @Override
  public TypeIndex createTypeIndex(final EmbeddedSchema.INDEX_TYPE indexType, final boolean unique, final String[] propertyNames,
      final int pageSize) {
    throw new UnsupportedOperationException();
  }

  @Override
  public TypeIndex createTypeIndex(final EmbeddedSchema.INDEX_TYPE indexType, final boolean unique, final String[] propertyNames,
      final int pageSize, final Index.BuildIndexCallback callback) {
    throw new UnsupportedOperationException();
  }

  @Override
  public TypeIndex createTypeIndex(final EmbeddedSchema.INDEX_TYPE indexType, final boolean unique, final String[] propertyNames,
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
  public Collection<Property> getProperties() {
    throw new UnsupportedOperationException();
  }

  @Override
  public Set<String> getPolymorphicPropertyNames() {
    throw new UnsupportedOperationException();
  }

  @Override
  public List<Bucket> getInvolvedBuckets() {
    throw new UnsupportedOperationException();
  }

  @Override
  public List<Bucket> getBuckets(boolean polymorphic) {
    throw new UnsupportedOperationException();
  }

  @Override
  public List<Integer> getBucketIds(boolean polymorphic) {
    throw new UnsupportedOperationException();
  }

  @Override
  public DocumentType addBucket(Bucket bucket) {
    throw new UnsupportedOperationException();
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
  public boolean existsProperty(String propertyName) {
    throw new UnsupportedOperationException();
  }

  @Override
  public boolean existsPolymorphicProperty(String propertyName) {
    throw new UnsupportedOperationException();
  }

  @Override
  public Property getPolymorphicPropertyIfExists(String propertyName) {
    throw new UnsupportedOperationException();
  }

  @Override
  public Property getPropertyIfExists(String propertyName) {
    throw new UnsupportedOperationException();
  }

  @Override
  public Property getProperty(String propertyName) {
    throw new UnsupportedOperationException();
  }

  @Override
  public Property getPolymorphicProperty(String propertyName) {
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
  public List<TypeIndex> getIndexesByProperties(Collection<String> properties) {
    throw new UnsupportedOperationException();
  }

  @Override
  public TypeIndex getPolymorphicIndexByProperties(String... properties) {
    throw new UnsupportedOperationException();
  }

  @Override
  public TypeIndex getPolymorphicIndexByProperties(List<String> properties) {
    throw new UnsupportedOperationException();
  }

  @Override
  protected void addIndexInternal(IndexInternal index, int bucketId, String[] propertyNames, TypeIndex propIndex) {
    throw new UnsupportedOperationException();
  }

  @Override
  public void removeTypeIndexInternal(TypeIndex index) {
    throw new UnsupportedOperationException();
  }

  @Override
  public void removeBucketIndexInternal(Index index) {
    throw new UnsupportedOperationException();
  }

  @Override
  protected void addBucketInternal(Bucket bucket) {
    throw new UnsupportedOperationException();
  }

  @Override
  protected void removeBucketInternal(Bucket bucket) {
    throw new UnsupportedOperationException();
  }

  @Override
  public boolean hasBucket(String bucketName) {
    throw new UnsupportedOperationException();
  }

  @Override
  public int getFirstBucketId() {
    throw new UnsupportedOperationException();
  }

  @Override
  public boolean isSubTypeOf(String type) {
    throw new UnsupportedOperationException();
  }

  @Override
  public boolean isSuperTypeOf(String type) {
    throw new UnsupportedOperationException();
  }

  @Override
  public Set<String> getCustomKeys() {
    throw new UnsupportedOperationException();
  }

  @Override
  public Object getCustomValue(String key) {
    throw new UnsupportedOperationException();
  }

  @Override
  public Object setCustomValue(String key, Object value) {
    throw new UnsupportedOperationException();
  }

  @Override
  public JSONObject toJSON() {
    throw new UnsupportedOperationException();
  }

  @Override
  protected <RET> RET recordFileChanges(Callable<Object> callback) {
    throw new UnsupportedOperationException();
  }
}
