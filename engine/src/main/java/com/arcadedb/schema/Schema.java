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
import com.arcadedb.engine.Component;
import com.arcadedb.engine.Dictionary;
import com.arcadedb.function.FunctionDefinition;
import com.arcadedb.function.FunctionLibraryDefinition;
import com.arcadedb.index.Index;
import com.arcadedb.index.TypeIndex;
import com.arcadedb.index.lsm.LSMTreeIndexAbstract;
import com.arcadedb.utility.ExcludeFromJacocoGeneratedReport;

import java.io.*;
import java.time.ZoneId;
import java.util.Collection;
import java.util.List;
import java.util.TimeZone;

@ExcludeFromJacocoGeneratedReport
public interface Schema {

  Component getFileById(int id);

  boolean existsBucket(String bucketName);

  Bucket getBucketByName(String name);

  Component getFileByIdIfExists(int id);

  Collection<? extends Bucket> getBuckets();

  Bucket getBucketById(int id);

  Bucket createBucket(String bucketName);

  boolean existsIndex(String indexName);

  DocumentType copyType(String typeName, String newTypeName, Class<? extends DocumentType> newType, int buckets, int pageSize,
      int transactionBatchSize);

  Index[] getIndexes();

  Index getIndexByName(String indexName);

  TypeIndexBuilder buildTypeIndex(String typeName, String[] propertyNames);

  BucketIndexBuilder buildBucketIndex(String typeName, String bucketName, String[] propertyNames);

  ManualIndexBuilder buildManualIndex(String indexName, Type[] keyTypes);

  TypeIndex createTypeIndex(Schema.INDEX_TYPE indexType, boolean unique, String typeName, String... propertyNames);

  /**
   * @Deprecated. Use `buildTypeIndex(typeName, propertyNames).withUnique(unique).withPageSize(pageSize).create()` instead.
   */
  @Deprecated
  TypeIndex createTypeIndex(Schema.INDEX_TYPE indexType, boolean unique, String typeName, String[] propertyNames, int pageSize);

  /**
   * @Deprecated. Use `buildTypeIndex(typeName, propertyNames).withUnique(unique).withPageSize(pageSize).withCallback(callback).create()` instead.
   */
  @Deprecated
  TypeIndex createTypeIndex(Schema.INDEX_TYPE indexType, boolean unique, String typeName, String[] propertyNames, int pageSize,
      Index.BuildIndexCallback callback);

  /**
   * @Deprecated. Use `buildTypeIndex(typeName, propertyNames).withUnique(unique).withPageSize(pageSize).withCallback(callback).withNullStrategy(nullStrategy).create()` instead.
   */
  @Deprecated
  TypeIndex createTypeIndex(Schema.INDEX_TYPE indexType, boolean unique, String typeName, String[] propertyNames, int pageSize,
      LSMTreeIndexAbstract.NULL_STRATEGY nullStrategy, Index.BuildIndexCallback callback);

  TypeIndex getOrCreateTypeIndex(Schema.INDEX_TYPE indexType, boolean unique, String typeName, String... propertyNames);

  /**
   * @Deprecated. Use `buildTypeIndex(typeName, propertyNames).withUnique(unique).withPageSize(pageSize).withIgnoreIfExists(true).create()` instead.
   */
  @Deprecated
  TypeIndex getOrCreateTypeIndex(Schema.INDEX_TYPE indexType, boolean unique, String typeName, String[] propertyNames,
      int pageSize);

  /**
   * @Deprecated. Use `buildTypeIndex(typeName, propertyNames).withUnique(unique).withPageSize(pageSize).withCallback(callback).withIgnoreIfExists(true).create()` instead.
   */
  @Deprecated
  TypeIndex getOrCreateTypeIndex(Schema.INDEX_TYPE indexType, boolean unique, String typeName, String[] propertyNames, int pageSize,
      Index.BuildIndexCallback callback);

  /**
   * @Deprecated. Use `buildTypeIndex(typeName, propertyNames).withUnique(unique).withPageSize(pageSize).withCallback(callback).withNullStrategy(nullStrategy).withIgnoreIfExists(true).create()` instead.
   */
  @Deprecated
  TypeIndex getOrCreateTypeIndex(Schema.INDEX_TYPE indexType, boolean unique, String typeName, String[] propertyNames, int pageSize,
      LSMTreeIndexAbstract.NULL_STRATEGY nullStrategy, Index.BuildIndexCallback callback);

  /**
   * @Deprecated. Use `buildBucketIndex(typeName, propertyNames).withUnique(unique).withPageSize(pageSize).withCallback(callback).withNullStrategy(nullStrategy).create()` instead.
   */
  @Deprecated
  Index createBucketIndex(Schema.INDEX_TYPE indexType, boolean unique, String typeName, String bucketName, String[] propertyNames,
      int pageSize, LSMTreeIndexAbstract.NULL_STRATEGY nullStrategy, Index.BuildIndexCallback callback);

  /**
   * @Deprecated. Use `buildManualIndex(indexName, keyTypes).withType(indexType).withUnique(unique).withPageSize(pageSize).withNullStrategy(nullStrategy).create()` instead.
   */
  @Deprecated
  Index createManualIndex(Schema.INDEX_TYPE indexType, boolean unique, String indexName, Type[] keyTypes, int pageSize,
      LSMTreeIndexAbstract.NULL_STRATEGY nullStrategy);

  Dictionary getDictionary();

  Collection<? extends DocumentType> getTypes();

  DocumentType getType(String typeName);

  void dropType(String typeName);

  String getTypeNameByBucketId(int bucketId);

  DocumentType getTypeByBucketId(int bucketId);

  DocumentType getInvolvedTypeByBucketId(int bucketId);

  DocumentType getTypeByBucketName(String bucketName);

  boolean existsType(String typeName);

  void dropBucket(String bucketName);

  void dropIndex(String indexName);

  // TRIGGER MANAGEMENT

  /**
   * Check if a trigger with the given name exists.
   */
  boolean existsTrigger(String triggerName);

  /**
   * Get a trigger by name.
   *
   * @return The trigger or null if not found
   */
  Trigger getTrigger(String triggerName);

  /**
   * Get all triggers defined in the schema.
   */
  Trigger[] getTriggers();

  /**
   * Get all triggers defined for a specific type.
   */
  Trigger[] getTriggersForType(String typeName);

  /**
   * Create a new trigger and register it in the schema.
   * The trigger will be automatically registered as an event listener.
   *
   * @throws com.arcadedb.exception.CommandExecutionException if a trigger with the same name already exists
   * @throws com.arcadedb.exception.CommandExecutionException if the type does not exist
   */
  void createTrigger(Trigger trigger);

  /**
   * Drop an existing trigger and unregister it from the event system.
   *
   * @throws com.arcadedb.exception.CommandExecutionException if the trigger does not exist
   */
  void dropTrigger(String triggerName);

  // -- Materialized View management --

  boolean existsMaterializedView(String viewName);

  MaterializedView getMaterializedView(String viewName);

  MaterializedView[] getMaterializedViews();

  void dropMaterializedView(String viewName);

  void alterMaterializedView(String viewName, MaterializedViewRefreshMode newMode, long newIntervalMs);

  MaterializedViewBuilder buildMaterializedView();

  // -- Continuous Aggregate management --

  boolean existsContinuousAggregate(String name);

  ContinuousAggregate getContinuousAggregate(String name);

  ContinuousAggregate[] getContinuousAggregates();

  void dropContinuousAggregate(String name);

  ContinuousAggregateBuilder buildContinuousAggregate();

  TypeBuilder<? extends DocumentType> buildDocumentType();

  TypeBuilder<VertexType> buildVertexType();

  TypeBuilder<EdgeType> buildEdgeType();

  TimeSeriesTypeBuilder buildTimeSeriesType();

  /**
   * Creates a new document type with the default settings of buckets.
   * This is the same as using `buildDocumentType().withName(typeName).create()`.
   */
  DocumentType createDocumentType(String typeName);

  /**
   * @Deprecated. Use `buildDocumentType().withName(typeName).withTotalBuckets(buckets).create()` instead.
   */
  @Deprecated
  DocumentType createDocumentType(String typeName, int buckets);

  /**
   * @Deprecated. Use `buildDocumentType().withName(typeName).withBuckets(buckets).create()` instead.
   */
  @Deprecated
  DocumentType createDocumentType(String typeName, List<Bucket> buckets);

  /**
   * @Deprecated. Use `buildDocumentType().withName(typeName).withTotalBuckets(buckets).withPageSize(pageSize).create()` instead.
   */
  @Deprecated
  DocumentType createDocumentType(String typeName, int buckets, int pageSize);

  /**
   * @Deprecated. Use `buildDocumentType().withName(typeName).withBuckets(buckets).withPageSize(pageSize).create()` instead.
   */
  @Deprecated
  DocumentType createDocumentType(String typeName, List<Bucket> buckets, int pageSize);

  /**
   * Returns a document type if already defined otherwise creates it in the schema.
   * This is equivalent to `buildDocumentType().withName(typeName).withIgnoreIfExists(true).create()`.
   */
  DocumentType getOrCreateDocumentType(String typeName);

  /**
   * @Deprecated. Use `buildDocumentType().withName(typeName).withTotalBuckets(buckets).withIgnoreIfExists(true).create()` instead.
   */
  @Deprecated
  DocumentType getOrCreateDocumentType(String typeName, int buckets);

  /**
   * @Deprecated. Use `buildDocumentType().withName(typeName).withTotalBuckets(buckets).withPageSize(pageSize).withIgnoreIfExists(true).create()` instead.
   */
  @Deprecated
  DocumentType getOrCreateDocumentType(String typeName, int buckets, int pageSize);

  /**
   * Creates a new vertex type with the default settings of buckets.
   * This is the same as using `buildVertexType().withName(typeName).create()`.
   */
  VertexType createVertexType(String typeName);

  /**
   * @Deprecated. Use `buildVertexType().withName(typeName).withTotalBuckets(buckets).create()` instead.
   */
  @Deprecated
  VertexType createVertexType(String typeName, int buckets);

  /**
   * @Deprecated. Use `buildVertexType().withName(typeName).withBuckets(buckets).create()` instead.
   */
  @Deprecated
  VertexType createVertexType(String typeName, List<Bucket> buckets);

  /**
   * @Deprecated. Use `buildVertexType().withName(typeName).withTotalBuckets(buckets).withPageSize(pageSize).create()` instead.
   */
  @Deprecated
  VertexType createVertexType(String typeName, int buckets, int pageSize);

  /**
   * @Deprecated. Use `buildVertexType().withName(typeName).withBuckets(buckets).withPageSize(pageSize).create()` instead.
   */
  @Deprecated
  VertexType createVertexType(String typeName, List<Bucket> buckets, final int pageSize);

  /**
   * Returns a vertex type if already defined otherwise creates it in the schema.
   * This is equivalent to `buildVertexType().withName(typeName).withIgnoreIfExists(true).create()`.
   */
  VertexType getOrCreateVertexType(String typeName);

  /**
   * @Deprecated. Use `buildVertexType().withName(typeName).withTotalBuckets(buckets).withIgnoreIfExists(true).create()` instead.
   */
  @Deprecated
  VertexType getOrCreateVertexType(String typeName, int buckets);

  /**
   * @Deprecated. Use `buildVertexType().withName(typeName).withTotalBuckets(buckets).withPageSize(pageSize).withIgnoreIfExists(true).create()` instead.
   */
  @Deprecated
  VertexType getOrCreateVertexType(String typeName, int buckets, int pageSize);

  /**
   * Creates a new edge type with the default settings of buckets.
   * This is the same as using `buildEdgeType().withName(typeName).create()`.
   */
  EdgeType createEdgeType(String typeName);

  /**
   * @Deprecated. Use `buildEdgeType().withName(typeName).withTotalBuckets(buckets).create()` instead.
   */
  @Deprecated
  EdgeType createEdgeType(String typeName, int buckets);

  /**
   * @Deprecated. Use `buildEdgeType().withName(typeName).withBuckets(buckets).create()` instead.
   */
  @Deprecated
  EdgeType createEdgeType(String typeName, List<Bucket> buckets);

  /**
   * @Deprecated. Use `buildEdgeType().withName(typeName).withTotalBuckets(buckets).withPageSize(pageSize).create()` instead.
   */
  @Deprecated
  EdgeType createEdgeType(String typeName, int buckets, int pageSize);

  /**
   * @Deprecated. Use `buildEdgeType().withName(typeName).withBuckets(buckets).withPageSize(pageSize)` instead.
   */
  @Deprecated
  EdgeType createEdgeType(String typeName, List<Bucket> buckets, int pageSize);

  /**
   * Returns an edge type if already defined otherwise creates it in the schema.
   * This is equivalent to `buildEdgeType().withName(typeName).withIgnoreIfExists(true).create()`.
   */
  EdgeType getOrCreateEdgeType(String typeName);

  /**
   * @Deprecated. Use `buildEdgeType().withName(typeName).withTotalBuckets(buckets).withIgnoreIfExists(true).create()` instead.
   */
  @Deprecated
  EdgeType getOrCreateEdgeType(String typeName, int buckets);

  /**
   * @Deprecated. Use `buildEdgeType().withName(typeName).withTotalBuckets(buckets).withPageSize(pageSize).withIgnoreIfExists(true).create()` instead.
   */
  @Deprecated
  EdgeType getOrCreateEdgeType(String typeName, int buckets, int pageSize);

  TimeZone getTimeZone();

  void setTimeZone(TimeZone timeZone);

  ZoneId getZoneId();

  void setZoneId(ZoneId zoneId);

  String getDateFormat();

  void setDateFormat(String dateFormat);

  String getDateTimeFormat();

  void setDateTimeFormat(String dateTimeFormat);

  String getEncoding();

  void setEncoding(String encoding);

  LocalSchema getEmbedded();

  /**
   * Registers a function library.
   *
   * @param library Function library definition implementation
   *
   * @return The current schema instance
   */
  Schema registerFunctionLibrary(FunctionLibraryDefinition library);

  /**
   * Unregister a function library previously defined.
   *
   * @param name Name of the function to unregister
   *
   * @return The current schema instance
   */
  Schema unregisterFunctionLibrary(String name);

  /**
   * Returns all the registered function libraries.
   */
  Iterable<FunctionLibraryDefinition> getFunctionLibraries();

  /**
   * Returns true if the a function library is defined, otherwise false.
   *
   * @param name Function library nme
   */
  boolean hasFunctionLibrary(String name);

  /**
   * Returns the requested function library if defined or IllegalArgumentException if not
   *
   * @param name Function library to search
   *
   * @throws IllegalArgumentException if the library is not defined
   */
  FunctionLibraryDefinition getFunctionLibrary(String name) throws IllegalArgumentException;

  /**
   * Returns a function defined in a library.
   *
   * @param libraryName  Name of the function library
   * @param functionName Name of the function
   *
   * @return The function definition if found
   *
   * @throws IllegalArgumentException if the library or the function is not defined
   */
  FunctionDefinition getFunction(String libraryName, String functionName) throws IllegalArgumentException;

  enum INDEX_TYPE {
    LSM_TREE, FULL_TEXT, LSM_VECTOR, GEOSPATIAL
  }
}
