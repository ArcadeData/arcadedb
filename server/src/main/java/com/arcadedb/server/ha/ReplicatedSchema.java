/*
 * Copyright 2021 Arcade Data Ltd
 *
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package com.arcadedb.server.ha;

import com.arcadedb.database.Database;
import com.arcadedb.engine.Bucket;
import com.arcadedb.engine.Dictionary;
import com.arcadedb.engine.PaginatedComponent;
import com.arcadedb.engine.PaginatedFile;
import com.arcadedb.index.Index;
import com.arcadedb.index.TypeIndex;
import com.arcadedb.index.lsm.LSMTreeIndexAbstract;
import com.arcadedb.schema.DocumentType;
import com.arcadedb.schema.EdgeType;
import com.arcadedb.schema.EmbeddedSchema;
import com.arcadedb.schema.Schema;
import com.arcadedb.schema.VertexType;

import java.io.*;
import java.util.*;

public class ReplicatedSchema implements Schema {
  private final EmbeddedSchema     proxied;
  private final ReplicatedDatabase replicatedDatabase;

  public ReplicatedSchema(final ReplicatedDatabase replicatedDatabase, final EmbeddedSchema proxied) {
    this.replicatedDatabase = replicatedDatabase;
    this.proxied = proxied;
  }

  @Override
  public EmbeddedSchema getEmbedded() {
    return proxied;
  }

  public void create(PaginatedFile.MODE mode) {
    proxied.create(mode);
  }

  public void load(PaginatedFile.MODE mode) throws IOException {
    proxied.load(mode);
  }

  @Override
  public TimeZone getTimeZone() {
    return proxied.getTimeZone();
  }

  @Override
  public void setTimeZone(TimeZone timeZone) {
    proxied.setTimeZone(timeZone);
  }

  @Override
  public String getDateFormat() {
    return proxied.getDateFormat();
  }

  @Override
  public void setDateFormat(String dateFormat) {
    proxied.setDateFormat(dateFormat);
  }

  @Override
  public String getDateTimeFormat() {
    return proxied.getDateTimeFormat();
  }

  @Override
  public void setDateTimeFormat(String dateTimeFormat) {
    proxied.setDateTimeFormat(dateTimeFormat);
  }

  @Override
  public PaginatedComponent getFileById(int id) {
    return proxied.getFileById(id);
  }

  @Override
  public PaginatedComponent getFileByIdIfExists(int id) {
    return proxied.getFileByIdIfExists(id);
  }

  public void removeFile(int fileId) {
    proxied.removeFile(fileId);
  }

  @Override
  public Collection<Bucket> getBuckets() {
    return proxied.getBuckets();
  }

  @Override
  public boolean existsBucket(String bucketName) {
    return proxied.existsBucket(bucketName);
  }

  @Override
  public Bucket getBucketByName(String name) {
    return proxied.getBucketByName(name);
  }

  @Override
  public Bucket getBucketById(int id) {
    return proxied.getBucketById(id);
  }

  @Override
  public Bucket createBucket(String bucketName) {
    return proxied.createBucket(bucketName);
  }

  public Bucket createBucket(String bucketName, int pageSize) {
    return proxied.createBucket(bucketName, pageSize);
  }

  @Override
  public String getEncoding() {
    return proxied.getEncoding();
  }

  @Override
  public DocumentType copyType(String typeName, String newTypeName, Class<? extends DocumentType> newTypeClass, int buckets, int pageSize,
      int transactionBatchSize) {
    return proxied.copyType(typeName, newTypeName, newTypeClass, buckets, pageSize, transactionBatchSize);
  }

  @Override
  public boolean existsIndex(String indexName) {
    return proxied.existsIndex(indexName);
  }

  @Override
  public Index[] getIndexes() {
    return proxied.getIndexes();
  }

  @Override
  public void dropIndex(String indexName) {
    proxied.dropIndex(indexName);
  }

  @Override
  public Index getIndexByName(String indexName) {
    return proxied.getIndexByName(indexName);
  }

  @Override
  public TypeIndex createTypeIndex(EmbeddedSchema.INDEX_TYPE indexType, boolean unique, String typeName, String... propertyNames) {
    return proxied.createTypeIndex(indexType, unique, typeName, propertyNames);
  }

  @Override
  public TypeIndex createTypeIndex(EmbeddedSchema.INDEX_TYPE indexType, boolean unique, String typeName, String[] propertyNames, int pageSize) {
    return proxied.createTypeIndex(indexType, unique, typeName, propertyNames, pageSize);
  }

  @Override
  public TypeIndex createTypeIndex(EmbeddedSchema.INDEX_TYPE indexType, boolean unique, String typeName, String[] propertyNames, int pageSize,
      Index.BuildIndexCallback callback) {
    return proxied.createTypeIndex(indexType, unique, typeName, propertyNames, pageSize, callback);
  }

  @Override
  public TypeIndex createTypeIndex(EmbeddedSchema.INDEX_TYPE indexType, boolean unique, String typeName, String[] propertyNames, int pageSize,
      LSMTreeIndexAbstract.NULL_STRATEGY nullStrategy, Index.BuildIndexCallback callback) {
    return proxied.createTypeIndex(indexType, unique, typeName, propertyNames, pageSize, nullStrategy, callback);
  }

  @Override
  public TypeIndex getOrCreateTypeIndex(EmbeddedSchema.INDEX_TYPE indexType, boolean unique, String typeName, String... propertyNames) {
    return proxied.getOrCreateTypeIndex(indexType, unique, typeName, propertyNames);
  }

  @Override
  public TypeIndex getOrCreateTypeIndex(EmbeddedSchema.INDEX_TYPE indexType, boolean unique, String typeName, String[] propertyNames, int pageSize) {
    return proxied.getOrCreateTypeIndex(indexType, unique, typeName, propertyNames, pageSize);
  }

  @Override
  public TypeIndex getOrCreateTypeIndex(EmbeddedSchema.INDEX_TYPE indexType, boolean unique, String typeName, String[] propertyNames, int pageSize,
      Index.BuildIndexCallback callback) {
    return proxied.getOrCreateTypeIndex(indexType, unique, typeName, propertyNames, pageSize, callback);
  }

  @Override
  public TypeIndex getOrCreateTypeIndex(EmbeddedSchema.INDEX_TYPE indexType, boolean unique, String typeName, String[] propertyNames, int pageSize,
      LSMTreeIndexAbstract.NULL_STRATEGY nullStrategy, Index.BuildIndexCallback callback) {
    return proxied.getOrCreateTypeIndex(indexType, unique, typeName, propertyNames, pageSize, nullStrategy, callback);
  }

  @Override
  public Index createBucketIndex(EmbeddedSchema.INDEX_TYPE indexType, boolean unique, String typeName, String bucketName, String[] propertyNames, int pageSize,
      LSMTreeIndexAbstract.NULL_STRATEGY nullStrategy, Index.BuildIndexCallback callback) {
    return proxied.createBucketIndex(indexType, unique, typeName, bucketName, propertyNames, pageSize, nullStrategy, callback);
  }

  @Override
  public Index createManualIndex(EmbeddedSchema.INDEX_TYPE indexType, boolean unique, String indexName, byte[] keyTypes, int pageSize,
      LSMTreeIndexAbstract.NULL_STRATEGY nullStrategy) {
    return proxied.createManualIndex(indexType, unique, indexName, keyTypes, pageSize, nullStrategy);
  }

  public void close() {
    proxied.close();
  }

  @Override
  public Dictionary getDictionary() {
    return proxied.getDictionary();
  }

  public Database getReplicatedDatabase() {
    return proxied.getDatabase();
  }

  @Override
  public Collection<DocumentType> getTypes() {
    return proxied.getTypes();
  }

  @Override
  public DocumentType getType(String typeName) {
    return proxied.getType(typeName);
  }

  @Override
  public String getTypeNameByBucketId(int bucketId) {
    return proxied.getTypeNameByBucketId(bucketId);
  }

  @Override
  public DocumentType getTypeByBucketId(int bucketId) {
    return proxied.getTypeByBucketId(bucketId);
  }

  @Override
  public boolean existsType(String typeName) {
    return proxied.existsType(typeName);
  }

  @Override
  public void dropType(final String typeName) {
    proxied.dropType(typeName);
  }

  @Override
  public void dropBucket(final String bucketName) {
    proxied.dropBucket(bucketName);
  }

  @Override
  public DocumentType createDocumentType(final String typeName) {
    return proxied.createDocumentType(typeName);
  }

  @Override
  public DocumentType createDocumentType(final String typeName, final int buckets) {
    return proxied.createDocumentType(typeName, buckets);
  }

  @Override
  public DocumentType createDocumentType(final String typeName, final int buckets, final int pageSize) {
    return (DocumentType) replicatedDatabase.recordFileChanges(() -> proxied.createDocumentType(typeName, buckets, pageSize));
  }

  @Override
  public DocumentType getOrCreateDocumentType(String typeName) {
    return proxied.getOrCreateDocumentType(typeName);
  }

  @Override
  public DocumentType getOrCreateDocumentType(String typeName, final int buckets) {
    return proxied.getOrCreateDocumentType(typeName, buckets);
  }

  @Override
  public DocumentType getOrCreateDocumentType(String typeName, final int buckets, final int pageSize) {
    return proxied.getOrCreateDocumentType(typeName, buckets, pageSize);
  }

  @Override
  public VertexType createVertexType(final String typeName) {
    return proxied.createVertexType(typeName);
  }

  @Override
  public VertexType createVertexType(final String typeName, final int buckets) {
    return proxied.createVertexType(typeName, buckets);
  }

  @Override
  public VertexType createVertexType(final String typeName, final int buckets, final int pageSize) {
    return proxied.createVertexType(typeName, buckets, pageSize);
  }

  @Override
  public VertexType getOrCreateVertexType(final String typeName) {
    return proxied.getOrCreateVertexType(typeName);
  }

  @Override
  public VertexType getOrCreateVertexType(final String typeName, final int buckets) {
    return proxied.getOrCreateVertexType(typeName, buckets);
  }

  @Override
  public VertexType getOrCreateVertexType(final String typeName, final int buckets, final int pageSize) {
    return proxied.getOrCreateVertexType(typeName, buckets, pageSize);
  }

  @Override
  public EdgeType createEdgeType(final String typeName) {
    return proxied.createEdgeType(typeName);
  }

  @Override
  public EdgeType createEdgeType(final String typeName, final int buckets) {
    return proxied.createEdgeType(typeName, buckets);
  }

  @Override
  public EdgeType createEdgeType(final String typeName, final int buckets, final int pageSize) {
    return proxied.createEdgeType(typeName, buckets, pageSize);
  }

  @Override
  public EdgeType getOrCreateEdgeType(final String typeName) {
    return proxied.getOrCreateEdgeType(typeName);
  }

  @Override
  public EdgeType getOrCreateEdgeType(final String typeName, int buckets) {
    return proxied.getOrCreateEdgeType(typeName, buckets);
  }

  @Override
  public EdgeType getOrCreateEdgeType(final String typeName, int buckets, int pageSize) {
    return proxied.getOrCreateEdgeType(typeName, buckets, pageSize);
  }

  public void saveConfiguration() {
    proxied.saveConfiguration();
  }

  public void registerFile(final PaginatedComponent file) {
    proxied.registerFile(file);
  }

  public boolean isDirty() {
    return proxied.isDirty();
  }
}
