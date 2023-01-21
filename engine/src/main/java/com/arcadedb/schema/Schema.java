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
import com.arcadedb.engine.Dictionary;
import com.arcadedb.engine.PaginatedComponent;
import com.arcadedb.function.FunctionDefinition;
import com.arcadedb.function.FunctionLibraryDefinition;
import com.arcadedb.index.Index;
import com.arcadedb.index.TypeIndex;
import com.arcadedb.index.lsm.LSMTreeIndexAbstract;

import java.time.*;
import java.util.*;

public interface Schema {

  PaginatedComponent getFileById(int id);

  boolean existsBucket(String bucketName);

  Bucket getBucketByName(String name);

  PaginatedComponent getFileByIdIfExists(int id);

  Collection<Bucket> getBuckets();

  Bucket getBucketById(int id);

  Bucket createBucket(String bucketName);

  boolean existsIndex(String indexName);

  DocumentType copyType(String typeName, String newTypeName, Class<? extends DocumentType> newType, int buckets, int pageSize, int transactionBatchSize);

  Index[] getIndexes();

  Index getIndexByName(String indexName);

  TypeIndex createTypeIndex(EmbeddedSchema.INDEX_TYPE indexType, boolean unique, String typeName, String... propertyNames);

  TypeIndex createTypeIndex(EmbeddedSchema.INDEX_TYPE indexType, boolean unique, String typeName, String[] propertyNames, int pageSize);

  TypeIndex createTypeIndex(EmbeddedSchema.INDEX_TYPE indexType, boolean unique, String typeName, String[] propertyNames, int pageSize,
      Index.BuildIndexCallback callback);

  TypeIndex createTypeIndex(EmbeddedSchema.INDEX_TYPE indexType, boolean unique, String typeName, String[] propertyNames, int pageSize,
      LSMTreeIndexAbstract.NULL_STRATEGY nullStrategy, Index.BuildIndexCallback callback);

  TypeIndex getOrCreateTypeIndex(EmbeddedSchema.INDEX_TYPE indexType, boolean unique, String typeName, String... propertyNames);

  TypeIndex getOrCreateTypeIndex(EmbeddedSchema.INDEX_TYPE indexType, boolean unique, String typeName, String[] propertyNames, int pageSize);

  TypeIndex getOrCreateTypeIndex(EmbeddedSchema.INDEX_TYPE indexType, boolean unique, String typeName, String[] propertyNames, int pageSize,
      Index.BuildIndexCallback callback);

  TypeIndex getOrCreateTypeIndex(EmbeddedSchema.INDEX_TYPE indexType, boolean unique, String typeName, String[] propertyNames, int pageSize,
      LSMTreeIndexAbstract.NULL_STRATEGY nullStrategy, Index.BuildIndexCallback callback);

  Index createBucketIndex(EmbeddedSchema.INDEX_TYPE indexType, boolean unique, String typeName, String bucketName, String[] propertyNames, int pageSize,
      LSMTreeIndexAbstract.NULL_STRATEGY nullStrategy, Index.BuildIndexCallback callback);

  Index createManualIndex(EmbeddedSchema.INDEX_TYPE indexType, boolean unique, String indexName, Type[] keyTypes, int pageSize,
      LSMTreeIndexAbstract.NULL_STRATEGY nullStrategy);

  Dictionary getDictionary();

  Collection<DocumentType> getTypes();

  DocumentType getType(String typeName);

  void dropType(String typeName);

  String getTypeNameByBucketId(int bucketId);

  DocumentType getTypeByBucketId(int bucketId);

  DocumentType getTypeByBucketName(String bucketName);

  boolean existsType(String typeName);

  void dropBucket(String bucketName);

  void dropIndex(String indexName);

  DocumentType createDocumentType(String typeName);

  DocumentType createDocumentType(String typeName, int buckets);

  DocumentType createDocumentType(String typeName, List<Bucket> buckets);

  DocumentType createDocumentType(String typeName, int buckets, int pageSize);

  DocumentType createDocumentType(String typeName, List<Bucket> buckets, int pageSize);

  DocumentType getOrCreateDocumentType(String typeName);

  DocumentType getOrCreateDocumentType(String typeName, int buckets);

  DocumentType getOrCreateDocumentType(String typeName, int buckets, int pageSize);

  VertexType createVertexType(String typeName);

  VertexType createVertexType(String typeName, int buckets);

  VertexType createVertexType(String typeName, List<Bucket> buckets);

  VertexType createVertexType(String typeName, int buckets, int pageSize);

  VertexType createVertexType(String typeName, List<Bucket> buckets, final int pageSize);

  VertexType getOrCreateVertexType(String typeName);

  VertexType getOrCreateVertexType(String typeName, int buckets);

  VertexType getOrCreateVertexType(String typeName, int buckets, int pageSize);

  EdgeType createEdgeType(String typeName);

  EdgeType createEdgeType(String typeName, int buckets);

  EdgeType createEdgeType(String typeName, List<Bucket> buckets);

  EdgeType createEdgeType(String typeName, int buckets, int pageSize);

  EdgeType createEdgeType(String typeName, List<Bucket> buckets, int pageSize);

  EdgeType getOrCreateEdgeType(String typeName);

  EdgeType getOrCreateEdgeType(String typeName, int buckets);

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

  EmbeddedSchema getEmbedded();

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
    LSM_TREE, FULL_TEXT
  }
}
