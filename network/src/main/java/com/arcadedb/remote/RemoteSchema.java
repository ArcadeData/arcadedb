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

import com.arcadedb.engine.Bucket;
import com.arcadedb.engine.Component;
import com.arcadedb.engine.Dictionary;
import com.arcadedb.engine.LocalBucket;
import com.arcadedb.exception.SchemaException;
import com.arcadedb.function.FunctionDefinition;
import com.arcadedb.function.FunctionLibraryDefinition;
import com.arcadedb.index.Index;
import com.arcadedb.index.TypeIndex;
import com.arcadedb.index.lsm.LSMTreeIndexAbstract;
import com.arcadedb.query.sql.executor.Result;
import com.arcadedb.query.sql.executor.ResultSet;
import com.arcadedb.schema.BucketIndexBuilder;
import com.arcadedb.schema.DocumentType;
import com.arcadedb.schema.EdgeType;
import com.arcadedb.schema.LocalSchema;
import com.arcadedb.schema.ManualIndexBuilder;
import com.arcadedb.schema.Schema;
import com.arcadedb.schema.Type;
import com.arcadedb.schema.TypeBuilder;
import com.arcadedb.schema.TypeIndexBuilder;
import com.arcadedb.schema.VectorIndexBuilder;
import com.arcadedb.schema.VertexType;

import java.time.*;
import java.util.*;
import java.util.stream.*;

/**
 * Remote Schema implementation used by Remote Database. The types are loaded from the server the first time
 * are needed and cached in RAM until the schema is changed, then it is automatically reloaded from the server.
 * You can manually reload the schema by calling the {@link #reload()} method.
 * <p>
 * This class is not thread safe. For multi-thread usage create one instance of RemoteDatabase per thread.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
public class RemoteSchema implements Schema {
  private final RemoteDatabase                  remoteDatabase;
  private       Map<String, RemoteDocumentType> types   = null;
  private       Map<String, RemoteBucket>       buckets = null;

  public RemoteSchema(final RemoteDatabase remoteDatabase) {
    this.remoteDatabase = remoteDatabase;
  }

  @Override
  public boolean existsType(final String typeName) {
    checkSchemaIsLoaded();
    return types.containsKey(typeName);
  }

  @Override
  public boolean existsBucket(final String bucketName) {
    return remoteDatabase.command("sql", "select from schema:types where '" + bucketName + "' IN buckets").hasNext();
  }

  @Override
  public boolean existsIndex(final String indexName) {
    return remoteDatabase.command("sql", "select from schema:indexes where name = `" + indexName + "`").hasNext();
  }

  @Override
  public void dropBucket(final String bucketName) {
    remoteDatabase.command("sql", "drop bucket `" + bucketName + "`");
  }

  @Override
  public void dropType(final String typeName) {
    remoteDatabase.command("sql", "drop type `" + typeName + "`");
    invalidateSchema();
  }

  @Override
  public void dropIndex(final String indexName) {
    remoteDatabase.command("sql", "drop index `" + indexName + "`");
  }

  @Override
  public Bucket createBucket(final String bucketName) {
    final ResultSet result = remoteDatabase.command("sql", "create bucket `" + bucketName + "`");
    return new RemoteBucket(result.next().getProperty("bucketName"));
  }

  @Override
  public TypeIndex createTypeIndex(final INDEX_TYPE indexType, final boolean unique, final String typeName,
      final String... propertyNames) {
    final String propList = Arrays.stream(propertyNames).collect(Collectors.joining(","));
    remoteDatabase.command("sql", "create index on `" + typeName +//
        "`(" + propList + ") " +//
        (unique ? "UNIQUE" : "NOTUNIQUE") +//
        " ENGINE " + indexType.name());
    return null;
  }

  @Override
  public TypeIndex getOrCreateTypeIndex(final INDEX_TYPE indexType, final boolean unique, final String typeName,
      final String... propertyNames) {
    final String propList = Arrays.stream(propertyNames).collect(Collectors.joining(","));
    remoteDatabase.command("sql", "create index if not exists on `" + typeName +//
        "`(" + propList + ") " +//
        (unique ? "UNIQUE" : "NOTUNIQUE") +//
        " ENGINE " + indexType.name());
    return null;
  }

  @Override
  public DocumentType createDocumentType(final String typeName) {
    final ResultSet result = remoteDatabase.command("sql", "create document type `" + typeName + "`");
    if (result.hasNext())
      return reload().getType(typeName);
    throw new SchemaException("Error on creating document type '" + typeName + "'");
  }

  @Override
  public DocumentType createDocumentType(final String typeName, final int buckets) {
    final ResultSet result = remoteDatabase.command("sql", "create document type `" + typeName + "` buckets " + buckets);
    if (result.hasNext())
      return reload().getType(typeName);
    throw new SchemaException("Error on creating document type '" + typeName + "'");
  }

  @Override
  public DocumentType getOrCreateDocumentType(final String typeName) {
    final ResultSet result = remoteDatabase.command("sql", "create document type `" + typeName + "` if not exists");
    if (result.hasNext())
      return reload().getType(typeName);
    throw new SchemaException("Error on creating document type '" + typeName + "'");
  }

  @Override
  public DocumentType getOrCreateDocumentType(final String typeName, final int buckets) {
    final ResultSet result = remoteDatabase.command("sql",
        "create document type `" + typeName + "` if not exists buckets " + buckets);
    if (result.hasNext())
      return reload().getType(typeName);
    throw new SchemaException("Error on creating document type '" + typeName + "'");
  }

  @Override
  public VertexType createVertexType(final String typeName) {
    final ResultSet result = remoteDatabase.command("sql", "create vertex type `" + typeName + "`");
    if (result.hasNext())
      return (VertexType) reload().getType(typeName);
    throw new SchemaException("Error on creating vertex type '" + typeName + "'");
  }

  @Override
  public VertexType getOrCreateVertexType(String typeName, int buckets) {
    final ResultSet result = remoteDatabase.command("sql",
        "create vertex type `" + typeName + "` if not exists buckets " + buckets);
    if (result.hasNext())
      return (VertexType) reload().getType(typeName);
    throw new SchemaException("Error on creating vertex type '" + typeName + "'");
  }

  @Override
  public VertexType getOrCreateVertexType(final String typeName) {
    final ResultSet result = remoteDatabase.command("sql", "create vertex type `" + typeName + "` if not exists");
    if (result.hasNext())
      return (VertexType) reload().getType(typeName);
    throw new SchemaException("Error on creating vertex type '" + typeName + "'");
  }

  @Override
  public VertexType createVertexType(String typeName, int buckets) {
    final ResultSet result = remoteDatabase.command("sql", "create vertex type `" + typeName + "` buckets " + buckets);
    if (result.hasNext())
      return (VertexType) reload().getType(typeName);
    throw new SchemaException("Error on creating vertex type '" + typeName + "'");
  }

  @Override
  public EdgeType createEdgeType(final String typeName) {
    final ResultSet result = remoteDatabase.command("sql", "create edge type `" + typeName + "`");
    if (result.hasNext())
      return (EdgeType) reload().getType(typeName);
    throw new SchemaException("Error on creating edge type '" + typeName + "'");
  }

  @Override
  public EdgeType getOrCreateEdgeType(String typeName) {
    final ResultSet result = remoteDatabase.command("sql", "create edge type `" + typeName + "` if not exists");
    if (result.hasNext())
      return (EdgeType) reload().getType(typeName);
    throw new SchemaException("Error on creating edge type '" + typeName + "'");
  }

  @Override
  public EdgeType createEdgeType(String typeName, int buckets) {
    final ResultSet result = remoteDatabase.command("sql", "create edge type `" + typeName + "` buckets " + buckets);
    if (result.hasNext())
      return (EdgeType) reload().getType(typeName);
    throw new SchemaException("Error on creating edge type '" + typeName + "'");
  }

  @Override
  public EdgeType getOrCreateEdgeType(final String typeName, final int buckets) {
    final ResultSet result = remoteDatabase.command("sql", "create edge type `" + typeName + "` if not exists buckets " + buckets);
    if (result.hasNext())
      return (EdgeType) reload().getType(typeName);
    throw new SchemaException("Error on creating edge type '" + typeName + "'");
  }

  @Override
  public Collection<? extends DocumentType> getTypes() {
    checkSchemaIsLoaded();
    return types.values();
  }

  @Override
  public DocumentType getType(final String typeName) {
    checkSchemaIsLoaded();

    final RemoteDocumentType t = types.get(typeName);
    if (t == null)
      throw new SchemaException("Type with name '" + typeName + "' was not found");
    return t;
  }

  @Override
  public LocalSchema getEmbedded() {
    return null;
  }

  // UNSUPPORTED METHODS. OPEN A NEW ISSUE TO REQUEST THE SUPPORT OF ADDITIONAL METHODS IN REMOTE
  @Deprecated
  @Override
  public TypeIndexBuilder buildTypeIndex(final String typeName, final String[] propertyNames) {
    throw new UnsupportedOperationException();
  }

  @Override
  @Deprecated
  public BucketIndexBuilder buildBucketIndex(final String typeName, final String bucketName, final String[] propertyNames) {
    throw new UnsupportedOperationException();
  }

  @Deprecated
  @Override
  public ManualIndexBuilder buildManualIndex(final String indexName, final Type[] keyTypes) {
    throw new UnsupportedOperationException();
  }

  @Deprecated
  @Override
  public VectorIndexBuilder buildVectorIndex() {
    throw new UnsupportedOperationException();
  }

  @Deprecated
  @Override
  public TypeBuilder<DocumentType> buildDocumentType() {
    throw new UnsupportedOperationException();
  }

  @Deprecated
  @Override
  public TypeBuilder<VertexType> buildVertexType() {
    throw new UnsupportedOperationException();
  }

  @Deprecated
  @Override
  public TypeBuilder<EdgeType> buildEdgeType() {
    throw new UnsupportedOperationException();
  }

  @Deprecated
  @Override
  public DocumentType createDocumentType(String typeName, List<Bucket> buckets) {
    throw new UnsupportedOperationException();
  }

  @Deprecated
  @Override
  public DocumentType createDocumentType(String typeName, int buckets, int pageSize) {
    throw new UnsupportedOperationException();
  }

  @Deprecated
  @Override
  public DocumentType createDocumentType(String typeName, List<Bucket> buckets, int pageSize) {
    throw new UnsupportedOperationException();
  }

  @Deprecated
  @Override
  public DocumentType getOrCreateDocumentType(String typeName, int buckets, int pageSize) {
    throw new UnsupportedOperationException();
  }

  @Deprecated
  @Override
  public EdgeType createEdgeType(String typeName, List<Bucket> buckets) {
    throw new UnsupportedOperationException();
  }

  @Deprecated
  @Override
  public VertexType createVertexType(String typeName, List<Bucket> buckets) {
    throw new UnsupportedOperationException();
  }

  @Deprecated
  @Override
  public VertexType createVertexType(String typeName, int buckets, int pageSize) {
    throw new UnsupportedOperationException();
  }

  @Deprecated
  @Override
  public VertexType createVertexType(String typeName, List<Bucket> buckets, int pageSize) {
    throw new UnsupportedOperationException();
  }

  @Deprecated
  @Override
  public VertexType getOrCreateVertexType(String typeName, int buckets, int pageSize) {
    throw new UnsupportedOperationException();
  }

  @Deprecated
  @Override
  public EdgeType createEdgeType(String typeName, int buckets, int pageSize) {
    throw new UnsupportedOperationException();
  }

  @Deprecated
  @Override
  public EdgeType createEdgeType(String typeName, List<Bucket> buckets, int pageSize) {
    throw new UnsupportedOperationException();
  }

  @Deprecated
  @Override
  public EdgeType getOrCreateEdgeType(String typeName, int buckets, int pageSize) {
    throw new UnsupportedOperationException();
  }

  @Deprecated
  @Override
  public TimeZone getTimeZone() {
    throw new UnsupportedOperationException();
  }

  @Deprecated
  @Override
  public void setTimeZone(TimeZone timeZone) {
    throw new UnsupportedOperationException();
  }

  @Deprecated
  @Override
  public ZoneId getZoneId() {
    throw new UnsupportedOperationException();
  }

  @Deprecated
  @Override
  public void setZoneId(ZoneId zoneId) {
    throw new UnsupportedOperationException();
  }

  @Deprecated
  @Override
  public String getDateFormat() {
    throw new UnsupportedOperationException();
  }

  @Deprecated
  @Override
  public void setDateFormat(final String dateFormat) {
    throw new UnsupportedOperationException();
  }

  @Deprecated
  @Override
  public String getDateTimeFormat() {
    throw new UnsupportedOperationException();
  }

  @Deprecated
  @Override
  public void setDateTimeFormat(final String dateTimeFormat) {
    throw new UnsupportedOperationException();
  }

  @Deprecated
  @Override
  public String getEncoding() {
    throw new UnsupportedOperationException();
  }

  @Deprecated
  @Override
  public void setEncoding(final String encoding) {
    throw new UnsupportedOperationException();
  }

  @Deprecated
  @Override
  public Schema registerFunctionLibrary(final FunctionLibraryDefinition library) {
    throw new UnsupportedOperationException();
  }

  @Deprecated
  @Override
  public Schema unregisterFunctionLibrary(final String name) {
    throw new UnsupportedOperationException();
  }

  @Deprecated
  @Override
  public Iterable<FunctionLibraryDefinition> getFunctionLibraries() {
    throw new UnsupportedOperationException();
  }

  @Deprecated
  @Override
  public boolean hasFunctionLibrary(final String name) {
    throw new UnsupportedOperationException();
  }

  @Deprecated
  @Override
  public FunctionLibraryDefinition getFunctionLibrary(final String name) throws IllegalArgumentException {
    throw new UnsupportedOperationException();
  }

  @Deprecated
  @Override
  public FunctionDefinition getFunction(final String libraryName, final String functionName) throws IllegalArgumentException {
    throw new UnsupportedOperationException();
  }

  @Deprecated
  @Override
  public Component getFileById(final int id) {
    throw new UnsupportedOperationException();
  }

  @Deprecated
  @Override
  public RemoteBucket getBucketByName(final String name) {
    final RemoteBucket b = buckets.get(name);
    if (b == null)
      throw new SchemaException("Bucket '" + name + "' not found");
    return b;
  }

  @Deprecated
  @Override
  public Component getFileByIdIfExists(final int id) {
    throw new UnsupportedOperationException();
  }

  @Deprecated
  @Override
  public Collection<? extends Bucket> getBuckets() {
    return buckets.values();
  }

  @Deprecated
  @Override
  public LocalBucket getBucketById(final int id) {
    throw new UnsupportedOperationException();
  }

  @Deprecated
  @Override
  public DocumentType copyType(final String typeName, final String newTypeName, final Class<? extends DocumentType> newType,
      final int buckets, final int pageSize, final int transactionBatchSize) {
    throw new UnsupportedOperationException();
  }

  @Deprecated
  @Override
  public Index[] getIndexes() {
    throw new UnsupportedOperationException();
  }

  @Deprecated
  @Override
  public Index getIndexByName(final String indexName) {
    throw new UnsupportedOperationException();
  }

  @Deprecated
  @Override
  public TypeIndex createTypeIndex(final INDEX_TYPE indexType, final boolean unique, final String typeName,
      final String[] propertyNames, final int pageSize) {
    throw new UnsupportedOperationException();
  }

  @Deprecated
  @Override
  public TypeIndex createTypeIndex(final INDEX_TYPE indexType, final boolean unique, final String typeName,
      final String[] propertyNames, final int pageSize, Index.BuildIndexCallback callback) {
    throw new UnsupportedOperationException();
  }

  @Deprecated
  @Override
  public TypeIndex createTypeIndex(final INDEX_TYPE indexType, final boolean unique, final String typeName,
      final String[] propertyNames, final int pageSize, final LSMTreeIndexAbstract.NULL_STRATEGY nullStrategy,
      final Index.BuildIndexCallback callback) {
    throw new UnsupportedOperationException();
  }

  @Deprecated
  @Override
  public TypeIndex getOrCreateTypeIndex(final INDEX_TYPE indexType, final boolean unique, final String typeName,
      final String[] propertyNames, final int pageSize, final Index.BuildIndexCallback callback) {
    throw new UnsupportedOperationException();
  }

  @Deprecated
  @Override
  public TypeIndex getOrCreateTypeIndex(final INDEX_TYPE indexType, final boolean unique, final String typeName,
      final String[] propertyNames, final int pageSize, LSMTreeIndexAbstract.NULL_STRATEGY nullStrategy,
      Index.BuildIndexCallback callback) {
    throw new UnsupportedOperationException();
  }

  @Deprecated
  @Override
  public Index createBucketIndex(final INDEX_TYPE indexType, final boolean unique, final String typeName, final String bucketName,
      final String[] propertyNames, final int pageSize, final LSMTreeIndexAbstract.NULL_STRATEGY nullStrategy,
      final Index.BuildIndexCallback callback) {
    throw new UnsupportedOperationException();
  }

  @Deprecated
  @Override
  public Index createManualIndex(final INDEX_TYPE indexType, final boolean unique, final String indexName, final Type[] keyTypes,
      final int pageSize, final LSMTreeIndexAbstract.NULL_STRATEGY nullStrategy) {
    throw new UnsupportedOperationException();
  }

  @Deprecated
  @Override
  public String getTypeNameByBucketId(final int bucketId) {
    throw new UnsupportedOperationException();
  }

  @Deprecated
  @Override
  public DocumentType getTypeByBucketId(final int bucketId) {
    throw new UnsupportedOperationException();
  }

  @Deprecated
  @Override
  public DocumentType getInvolvedTypeByBucketId(final int bucketId) {
    throw new UnsupportedOperationException();
  }

  @Deprecated
  @Override
  public DocumentType getTypeByBucketName(final String bucketName) {
    ResultSet resultSet = remoteDatabase.command("sql", "select from schema:types where buckets contains `" + bucketName + "`");

    final Result result = resultSet.nextIfAvailable();
    return result != null ? remoteDatabase.getSchema().getType(result.getProperty("name")) : null;  }

  @Deprecated
  @Override
  public Dictionary getDictionary() {
    throw new UnsupportedOperationException();
  }

  @Deprecated
  @Override
  public TypeIndex getOrCreateTypeIndex(final INDEX_TYPE indexType, final boolean unique, final String typeName,
      final String[] propertyNames, int pageSize) {
    throw new UnsupportedOperationException();
  }

  void invalidateSchema() {
    types = null;
  }

  /**
   * Force a reload of the schema from the server.
   */
  public RemoteSchema reload() {
    final ResultSet result = remoteDatabase.command("sql", "select from schema:types");

    final List<Result> cached = new ArrayList<>();
    while (result.hasNext())
      cached.add(result.next());

    if (types == null) {
      types = new HashMap<>();
      buckets = new HashMap<>();
    } else
      buckets.clear();

    for (Result record : cached) {
      final List<String> typeBucketNames = record.getProperty("buckets");
      for (String typeBucketName : typeBucketNames)
        buckets.computeIfAbsent(typeBucketName, (name) -> new RemoteBucket(name));
    }

    for (Result record : cached) {
      final String typeName = record.getProperty("name");

      RemoteDocumentType type = types.get(typeName);
      if (type == null) {
        switch ((String) record.getProperty("type")) {
        case "document":
          type = new RemoteDocumentType(remoteDatabase, record);
          break;
        case "vertex":
          type = new RemoteVertexType(remoteDatabase, record);
          break;
        case "edge":
          type = new RemoteEdgeType(remoteDatabase, record,
              record.hasProperty("bidirectional") ? (Boolean) record.getProperty("bidirectional") : true);
          break;
        default:
          throw new IllegalArgumentException("Unknown record type for " + typeName);
        }
        types.put(typeName, type);
      } else
        type.reload(record);
    }
    return this;
  }

  private void checkSchemaIsLoaded() {
    if (types == null)
      reload();
  }
}
