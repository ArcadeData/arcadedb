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

package com.arcadedb.schema;

import com.arcadedb.Constants;
import com.arcadedb.database.*;
import com.arcadedb.engine.Dictionary;
import com.arcadedb.engine.*;
import com.arcadedb.exception.ConfigurationException;
import com.arcadedb.exception.DatabaseMetadataException;
import com.arcadedb.exception.SchemaException;
import com.arcadedb.index.Index;
import com.arcadedb.index.IndexFactory;
import com.arcadedb.index.IndexInternal;
import com.arcadedb.index.TypeIndex;
import com.arcadedb.index.lsm.*;
import com.arcadedb.log.LogManager;
import com.arcadedb.utility.FileUtils;
import org.json.JSONArray;
import org.json.JSONObject;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileWriter;
import java.io.IOException;
import java.util.*;
import java.util.concurrent.Callable;
import java.util.logging.Level;

public class EmbeddedSchema implements Schema {
  public static final String DEFAULT_DATE_FORMAT     = "yyyy-MM-dd";
  public static final String DEFAULT_DATETIME_FORMAT = "yyyy-MM-dd HH:mm:ss";
  public static final String DEFAULT_ENCODING        = "UTF-8";

  public static final  String                     SCHEMA_FILE_NAME      = "schema.json";
  public static final  String                     SCHEMA_PREV_FILE_NAME = "schema.prev.json";
  private static final int                        EDGE_DEF_PAGE_SIZE    = Bucket.DEF_PAGE_SIZE / 3;
  private final        DatabaseInternal           database;
  private final        List<PaginatedComponent>   files                 = new ArrayList<>();
  private final        Map<String, DocumentType>  types                 = new HashMap<>();
  private final        Map<String, Bucket>        bucketMap             = new HashMap<>();
  protected final      Map<String, IndexInternal> indexMap              = new HashMap<>();
  private final        String                     databasePath;
  private              Dictionary                 dictionary;
  private              String                     dateFormat            = DEFAULT_DATE_FORMAT;
  private              String                     dateTimeFormat        = DEFAULT_DATETIME_FORMAT;
  private              String                     encoding              = DEFAULT_ENCODING;
  private              TimeZone                   timeZone              = TimeZone.getDefault();
  private final        PaginatedComponentFactory  paginatedComponentFactory;
  private final        IndexFactory               indexFactory          = new IndexFactory();
  private              boolean                    readingFromFile       = false;
  private              boolean                    dirtyConfiguration    = false;
  private              boolean                    loadInRamCompleted    = false;
  private              boolean                    multipleUpdate        = false;

  public EmbeddedSchema(final DatabaseInternal database, final String databasePath, final PaginatedFile.MODE mode) {
    this.database = database;
    this.databasePath = databasePath;

    paginatedComponentFactory = new PaginatedComponentFactory(database);
    paginatedComponentFactory.registerComponent(Dictionary.DICT_EXT, new Dictionary.PaginatedComponentFactoryHandler());
    paginatedComponentFactory.registerComponent(Bucket.BUCKET_EXT, new Bucket.PaginatedComponentFactoryHandler());
    paginatedComponentFactory.registerComponent(LSMTreeIndexMutable.UNIQUE_INDEX_EXT, new LSMTreeIndex.PaginatedComponentFactoryHandlerUnique());
    paginatedComponentFactory.registerComponent(LSMTreeIndexMutable.NOTUNIQUE_INDEX_EXT, new LSMTreeIndex.PaginatedComponentFactoryHandlerNotUnique());
    paginatedComponentFactory.registerComponent(LSMTreeIndexCompacted.UNIQUE_INDEX_EXT, new LSMTreeIndex.PaginatedComponentFactoryHandlerUnique());
    paginatedComponentFactory.registerComponent(LSMTreeIndexCompacted.NOTUNIQUE_INDEX_EXT, new LSMTreeIndex.PaginatedComponentFactoryHandlerNotUnique());

    indexFactory.register(INDEX_TYPE.LSM_TREE.name(), new LSMTreeIndex.IndexFactoryHandler());
    indexFactory.register(INDEX_TYPE.FULL_TEXT.name(), new LSMTreeFullTextIndex.IndexFactoryHandler());
  }

  @Override
  public EmbeddedSchema getEmbedded() {
    return this;
  }

  public void create(final PaginatedFile.MODE mode) {
    loadInRamCompleted = true;
    database.begin();
    try {
      dictionary = new Dictionary(database, "dictionary", databasePath + "/dictionary", mode, Dictionary.DEF_PAGE_SIZE);
      files.add(dictionary);

      database.commit();

    } catch (Exception e) {
      LogManager.instance().log(this, Level.SEVERE, "Error on opening dictionary '%s' (error=%s)", e, databasePath, e.toString());
      database.rollback();
      throw new DatabaseMetadataException("Error on loading dictionary (error=" + e.toString() + ")", e);
    }
  }

  public void load(final PaginatedFile.MODE mode) throws IOException {
    final Collection<PaginatedFile> filesToOpen = database.getFileManager().getFiles();

    // REGISTER THE DICTIONARY FIRST
    for (PaginatedFile file : filesToOpen) {
      if (Dictionary.DICT_EXT.equals(file.getFileExtension())) {
        dictionary = (Dictionary) paginatedComponentFactory.createComponent(file, mode);
        registerFile(dictionary);
        break;
      }
    }

    if (dictionary == null)
      throw new ConfigurationException("Dictionary file not found in database directory");

    for (PaginatedFile file : filesToOpen) {
      if (!Dictionary.DICT_EXT.equals(file.getFileExtension())) {
        final PaginatedComponent pf = paginatedComponentFactory.createComponent(file, mode);

        if (pf != null) {
          final Object mainComponent = pf.getMainComponent();

          if (mainComponent instanceof Bucket)
            bucketMap.put(pf.getName(), (Bucket) mainComponent);
          else if (mainComponent instanceof IndexInternal)
            indexMap.put(pf.getName(), (IndexInternal) mainComponent);

          registerFile(pf);
        }
      }
    }

    for (PaginatedComponent f : files)
      if (f != null)
        f.onAfterLoad();

    readConfiguration();
  }

  public void loadChanges() throws IOException {
    final Collection<PaginatedFile> filesToOpen = database.getFileManager().getFiles();

    final PaginatedFile.MODE mode = database.getMode();

    final List<PaginatedComponent> newFilesLoaded = new ArrayList<>();

    for (PaginatedFile file : filesToOpen) {
      if (!Dictionary.DICT_EXT.equals(file.getFileExtension())) {
        final PaginatedComponent pf = paginatedComponentFactory.createComponent(file, mode);

        if (pf != null) {
          if (pf.getId() < files.size() && files.get(pf.getId()) != null)
            // ALREADY LOADED
            continue;

          final Object mainComponent = pf.getMainComponent();

          if (mainComponent instanceof Bucket)
            bucketMap.put(pf.getName(), (Bucket) mainComponent);
          else if (mainComponent instanceof IndexInternal)
            indexMap.put(pf.getName(), (IndexInternal) mainComponent);

          registerFile(pf);

          newFilesLoaded.add(pf);
        }
      }
    }

    for (PaginatedComponent f : newFilesLoaded)
      if (f != null)
        f.onAfterLoad();

    readConfiguration();
  }

  @Override
  public TimeZone getTimeZone() {
    return timeZone;
  }

  @Override
  public void setTimeZone(final TimeZone timeZone) {
    this.timeZone = timeZone;
  }

  @Override
  public String getDateFormat() {
    return dateFormat;
  }

  @Override
  public void setDateFormat(final String dateFormat) {
    this.dateFormat = dateFormat;
  }

  @Override
  public String getDateTimeFormat() {
    return dateTimeFormat;
  }

  @Override
  public void setDateTimeFormat(final String dateTimeFormat) {
    this.dateTimeFormat = dateTimeFormat;
  }

  @Override
  public PaginatedComponent getFileById(final int id) {
    if (id >= files.size())
      throw new SchemaException("File with id '" + id + "' was not found");

    final PaginatedComponent p = files.get(id);
    if (p == null)
      throw new SchemaException("File with id '" + id + "' was not found");
    return p;
  }

  @Override
  public PaginatedComponent getFileByIdIfExists(final int id) {
    if (id >= files.size())
      return null;

    return files.get(id);
  }

  public void removeFile(final int fileId) {
    if (fileId >= files.size())
      return;

    database.getTransaction().removePagesOfFile(fileId);

    files.set(fileId, null);
  }

  @Override
  public Collection<Bucket> getBuckets() {
    return Collections.unmodifiableCollection(bucketMap.values());
  }

  public boolean existsBucket(final String bucketName) {
    return bucketMap.containsKey(bucketName);
  }

  @Override
  public Bucket getBucketByName(final String name) {
    final Bucket p = bucketMap.get(name);
    if (p == null)
      throw new SchemaException("Bucket with name '" + name + "' was not found");
    return p;
  }

  @Override
  public Bucket getBucketById(final int id) {
    if (id < 0 || id >= files.size())
      throw new SchemaException("Bucket with id '" + id + "' was not found");

    final PaginatedComponent p = files.get(id);
    if (p == null || !(p instanceof Bucket))
      throw new SchemaException("Bucket with id '" + id + "' was not found");
    return (Bucket) p;
  }

  @Override
  public Bucket createBucket(final String bucketName) {
    return createBucket(bucketName, Bucket.DEF_PAGE_SIZE);
  }

  public Bucket createBucket(final String bucketName, final int pageSize) {
    return (Bucket) database.executeInWriteLock(new Callable<Object>() {
      @Override
      public Object call() {
        if (bucketMap.containsKey(bucketName))
          throw new SchemaException("Cannot create bucket '" + bucketName + "' because already exists");

        try {
          final Bucket bucket = new Bucket(database, bucketName, databasePath + "/" + bucketName, PaginatedFile.MODE.READ_WRITE, pageSize);
          registerFile(bucket);
          bucketMap.put(bucketName, bucket);

          return bucket;

        } catch (IOException e) {
          throw new SchemaException("Cannot create bucket '" + bucketName + "' (error=" + e + ")", e);
        }
      }
    });
  }

  public String getEncoding() {
    return encoding;
  }

  @Override
  public DocumentType copyType(final String typeName, final String newTypeName, final Class<? extends DocumentType> newTypeClass, final int buckets,
      final int pageSize, final int transactionBatchSize) {
    if (existsType(newTypeName))
      throw new IllegalArgumentException("Type '" + newTypeName + "' already exists");

    final DocumentType oldType = getType(typeName);

    DocumentType newType = null;
    try {
      // CREATE THE NEW TYPE
      if (newTypeClass == VertexType.class)
        newType = createVertexType(newTypeName, buckets, pageSize);
      else if (newTypeClass == EdgeType.class)
        throw new IllegalArgumentException("Type '" + newTypeClass + "' not supported");
      else if (newTypeClass == DocumentType.class)
        newType = createDocumentType(newTypeName, buckets, pageSize);
      else
        throw new IllegalArgumentException("Type '" + newTypeClass + "' not supported");

      // COPY PROPERTIES
      for (String propName : oldType.getPropertyNames()) {
        final Property prop = oldType.getProperty(propName);
        newType.createProperty(propName, prop.getType());
      }

      // COPY ALL THE RECORDS
      long copied = 0;
      database.begin();
      try {
        for (Iterator<Record> iter = database.iterateType(typeName, false); iter.hasNext(); ) {

          Document record = (Document) iter.next();

          final MutableDocument newRecord;
          if (newType instanceof VertexType)
            newRecord = database.newVertex(newTypeName);
          else if (newType instanceof DocumentType)
            newRecord = database.newDocument(newTypeName);
          else
            throw new IllegalArgumentException("Type '" + newType + "' not supported");

          newRecord.fromMap(record.toMap());
          newRecord.save();

          ++copied;

          if (copied > 0 && copied % transactionBatchSize == 0) {
            database.commit();
            database.begin();
          }
        }

        // COPY INDEXES
        for (Index index : oldType.getAllIndexes(false))
          newType.createTypeIndex(index.getType(), index.isUnique(), index.getPropertyNames());

        database.commit();

      } finally {
        if (database.isTransactionActive())
          database.rollback();
      }

    } catch (Exception e) {
      LogManager.instance().log(this, Level.SEVERE, "Error on renaming type '%s' into '%s'", e, typeName, newTypeName);

      if (newType != null)
        try {
          dropType(newTypeName);
        } catch (Exception e2) {
          LogManager.instance()
              .log(this, Level.WARNING, "Error on dropping temporary type '%s' created during copyType() operation from type '%s'", e2, newTypeName, typeName);
        }

      throw e;
    }

    return newType;
  }

  @Override
  public boolean existsIndex(final String indexName) {
    return indexMap.containsKey(indexName);
  }

  @Override
  public Index[] getIndexes() {
    final Index[] indexes = new Index[indexMap.size()];
    int i = 0;
    for (Index index : indexMap.values())
      indexes[i++] = index;
    return indexes;
  }

  @Override
  public void dropIndex(final String indexName) {
    final IndexInternal index = indexMap.remove(indexName);
    if (index == null)
      return;

    index.drop();

    saveConfiguration();
  }

  @Override
  public Index getIndexByName(final String indexName) {
    final Index p = indexMap.get(indexName);
    if (p == null)
      throw new SchemaException("Index with name '" + indexName + "' was not found");
    return p;
  }

  @Override
  public TypeIndex createTypeIndex(final INDEX_TYPE indexType, final boolean unique, final String typeName, final String... propertyNames) {
    return createTypeIndex(indexType, unique, typeName, propertyNames, LSMTreeIndexAbstract.DEF_PAGE_SIZE, null);
  }

  @Override
  public TypeIndex createTypeIndex(final INDEX_TYPE indexType, final boolean unique, final String typeName, final String[] propertyNames, final int pageSize) {
    return createTypeIndex(indexType, unique, typeName, propertyNames, pageSize, LSMTreeIndexAbstract.NULL_STRATEGY.ERROR, null);
  }

  @Override
  public TypeIndex createTypeIndex(final INDEX_TYPE indexType, final boolean unique, final String typeName, final String[] propertyNames, final int pageSize,
      final Index.BuildIndexCallback callback) {
    return createTypeIndex(indexType, unique, typeName, propertyNames, pageSize, LSMTreeIndexAbstract.NULL_STRATEGY.ERROR, callback);
  }

  @Override
  public TypeIndex createTypeIndex(final INDEX_TYPE indexType, final boolean unique, final String typeName, final String[] propertyNames, final int pageSize,
      final LSMTreeIndexAbstract.NULL_STRATEGY nullStrategy, final Index.BuildIndexCallback callback) {
    if (propertyNames.length == 0)
      throw new DatabaseMetadataException("Cannot create index on type '" + typeName + "' because there are no property defined");

    return (TypeIndex) database.executeInWriteLock(new Callable<Object>() {
      @Override
      public Object call() {
        try {
          final DocumentType type = getType(typeName);

          final TypeIndex index = type.getPolymorphicIndexByProperties(propertyNames);
          if (index != null)
            throw new IllegalArgumentException(
                "Found the existent index '" + index.getName() + "' defined on the properties '" + Arrays.asList(propertyNames) + "' for type '" + typeName
                    + "'");

          // CHECK ALL THE PROPERTIES EXIST
          final byte[] keyTypes = new byte[propertyNames.length];
          int i = 0;

          for (String propertyName : propertyNames) {
            final Property property = type.getPolymorphicPropertyIfExists(propertyName);
            if (property == null)
              throw new SchemaException("Cannot create the index on type '" + typeName + "." + propertyName + "' because the property does not exist");

            keyTypes[i++] = property.getType().getBinaryType();
          }

          final List<Bucket> buckets = type.getBuckets(true);

          final Index[] indexes = new Index[buckets.size()];
          for (int idx = 0; idx < buckets.size(); ++idx) {
            final Bucket bucket = buckets.get(idx);
            indexes[idx] = createBucketIndex(type, keyTypes, bucket, typeName, indexType, unique, pageSize, nullStrategy, callback, propertyNames);
          }

          saveConfiguration();

          return type.getPolymorphicIndexByProperties(propertyNames);

        } catch (IOException e) {
          throw new SchemaException("Cannot create index on type '" + typeName + "' (error=" + e + ")", e);
        }
      }
    });
  }

  @Override
  public TypeIndex getOrCreateTypeIndex(final INDEX_TYPE indexType, final boolean unique, final String typeName, final String... propertyNames) {
    return getOrCreateTypeIndex(indexType, unique, typeName, propertyNames, LSMTreeIndexAbstract.DEF_PAGE_SIZE, null);
  }

  @Override
  public TypeIndex getOrCreateTypeIndex(final INDEX_TYPE indexType, final boolean unique, final String typeName, final String[] propertyNames,
      final int pageSize) {
    return getOrCreateTypeIndex(indexType, unique, typeName, propertyNames, pageSize, LSMTreeIndexAbstract.NULL_STRATEGY.ERROR, null);
  }

  @Override
  public TypeIndex getOrCreateTypeIndex(final INDEX_TYPE indexType, final boolean unique, final String typeName, final String[] propertyNames,
      final int pageSize, final Index.BuildIndexCallback callback) {
    return getOrCreateTypeIndex(indexType, unique, typeName, propertyNames, pageSize, LSMTreeIndexAbstract.NULL_STRATEGY.ERROR, callback);
  }

  @Override
  public TypeIndex getOrCreateTypeIndex(final INDEX_TYPE indexType, final boolean unique, final String typeName, final String[] propertyNames,
      final int pageSize, final LSMTreeIndexAbstract.NULL_STRATEGY nullStrategy, final Index.BuildIndexCallback callback) {
    final DocumentType type = getType(typeName);
    final TypeIndex index = type.getPolymorphicIndexByProperties(propertyNames);
    if (index != null) {
      if (index.getNullStrategy() != null && index.getNullStrategy() == null ||//
          index.isUnique() != unique) {
        // DIFFERENT, DROP AND RECREATE IT
        index.drop();
      } else
        return index;
    }

    return createTypeIndex(indexType, unique, typeName, propertyNames, pageSize, nullStrategy, callback);
  }

  @Override
  public Index createBucketIndex(final INDEX_TYPE indexType, final boolean unique, final String typeName, final String bucketName, final String[] propertyNames,
      final int pageSize, final LSMTreeIndexAbstract.NULL_STRATEGY nullStrategy, final Index.BuildIndexCallback callback) {
    if (propertyNames.length == 0)
      throw new DatabaseMetadataException("Cannot create index on type '" + typeName + "' because there are no property defined");

    return (Index) database.executeInWriteLock(new Callable<Object>() {
      @Override
      public Object call() {
        try {
          final DocumentType type = getType(typeName);

          // CHECK ALL THE PROPERTIES EXIST
          final byte[] keyTypes = new byte[propertyNames.length];
          int i = 0;

          for (String propertyName : propertyNames) {
            final Property property = type.getPolymorphicPropertyIfExists(propertyName);
            if (property == null)
              throw new SchemaException("Cannot create the index on type '" + typeName + "." + propertyName + "' because the property does not exist");

            keyTypes[i++] = property.getType().getBinaryType();
          }

          Bucket bucket = null;
          final List<Bucket> buckets = type.getBuckets(false);
          for (Bucket b : buckets) {
            if (bucketName.equals(b.getName())) {
              bucket = b;
              break;
            }
          }

          final Index index = createBucketIndex(type, keyTypes, bucket, typeName, indexType, unique, pageSize, nullStrategy, callback, propertyNames);

          saveConfiguration();

          return index;

        } catch (IOException e) {
          throw new SchemaException("Cannot create index on type '" + typeName + "' (error=" + e + ")", e);
        }
      }
    });
  }

  private Index createBucketIndex(final DocumentType type, final byte[] keyTypes, final Bucket bucket, final String typeName, final INDEX_TYPE indexType,
      final boolean unique, final int pageSize, final LSMTreeIndexAbstract.NULL_STRATEGY nullStrategy, final Index.BuildIndexCallback callback,
      final String[] propertyNames) throws IOException {
    if (bucket == null)
      throw new DatabaseMetadataException(
          "Cannot create index on type '" + typeName + "' because the specified bucket '" + bucket.getName() + "' is not part of the type");

    final String indexName = FileUtils.encode(bucket.getName(), encoding) + "_" + System.nanoTime();

    if (indexMap.containsKey(indexName))
      throw new DatabaseMetadataException("Cannot create index '" + indexName + "' on type '" + typeName + "' because it already exists");

    final IndexInternal index = indexFactory.createIndex(indexType.name(), database, indexName, unique, databasePath + "/" + indexName,
        PaginatedFile.MODE.READ_WRITE, keyTypes, pageSize, nullStrategy, callback);

    registerFile(index.getPaginatedComponent());

    indexMap.put(indexName, index);

    type.addIndexInternal(index, bucket.getId(), propertyNames);
    index.build(callback);

    return index;
  }

  public Index createManualIndex(final INDEX_TYPE indexType, final boolean unique, final String indexName, final byte[] keyTypes, final int pageSize,
      final LSMTreeIndexAbstract.NULL_STRATEGY nullStrategy) {
    return (Index) database.executeInWriteLock(new Callable<Object>() {
      @Override
      public Object call() {
        if (indexMap.containsKey(indexName))
          throw new SchemaException("Cannot create index '" + indexName + "' because already exists");

        try {
          final IndexInternal index = indexFactory.createIndex(indexType.name(), database, FileUtils.encode(indexName, encoding), unique,
              databasePath + "/" + indexName, PaginatedFile.MODE.READ_WRITE, keyTypes, pageSize, nullStrategy, null);

          if (index instanceof PaginatedComponent)
            registerFile((PaginatedComponent) index);

          indexMap.put(indexName, index);

          return index;

        } catch (IOException e) {
          throw new SchemaException("Cannot create index '" + indexName + "' (error=" + e + ")", e);
        }
      }
    });
  }

  public void close() {
    files.clear();
    types.clear();
    bucketMap.clear();
    indexMap.clear();
    dictionary = null;
  }

  public Dictionary getDictionary() {
    return dictionary;
  }

  public Database getDatabase() {
    return database;
  }

  public Collection<DocumentType> getTypes() {
    return Collections.unmodifiableCollection(types.values());
  }

  public DocumentType getType(final String typeName) {
    final DocumentType t = types.get(typeName);
    if (t == null)
      throw new SchemaException("Type with name '" + typeName + "' was not found");
    return t;
  }

  @Override
  public String getTypeNameByBucketId(final int bucketId) {
    for (DocumentType t : types.values()) {
      for (Bucket b : t.getBuckets(false)) {
        if (b.getId() == bucketId)
          return t.getName();
      }
    }

    // NOT FOUND
    return null;
  }

  // TODO: CREATE A MAP FOR THIS
  @Override
  public DocumentType getTypeByBucketId(final int bucketId) {
    for (DocumentType t : types.values()) {
      for (Bucket b : t.getBuckets(false)) {
        if (b.getId() == bucketId)
          return t;
      }
    }

    // NOT FOUND
    return null;
  }

  public boolean existsType(final String typeName) {
    return types.containsKey(typeName);
  }

  public void dropType(final String typeName) {
    database.executeInWriteLock(new Callable<Object>() {
      @Override
      public Object call() {
        multipleUpdate = true;
        try {
          final DocumentType type = database.getSchema().getType(typeName);

          // CHECK INHERITANCE TREE AND ATTACH SUB-TYPES DIRECTLY TO THE PARENT TYPE
          for (DocumentType parent : type.parentTypes)
            parent.subTypes.remove(type);
          for (DocumentType sub : type.subTypes) {
            sub.parentTypes.remove(type);
            for (DocumentType parent : type.parentTypes)
              sub.addParentType(parent);
          }

          final List<Bucket> buckets = type.getBuckets(false);
          final Set<Integer> bucketIds = new HashSet<>(buckets.size());
          for (Bucket b : buckets)
            bucketIds.add(b.getId());

          // DELETE ALL ASSOCIATED INDEXES
          for (Index m : type.getAllIndexes(true))
            dropIndex(m.getName());

          // DELETE ALL ASSOCIATED BUCKETS
          for (Bucket b : buckets)
            dropBucket(b.getName());

          if (type instanceof VertexType)
            database.getGraphEngine().dropVertexType(database, (VertexType) type);

          if (types.remove(typeName) == null)
            throw new SchemaException("Type '" + typeName + "' not found");
        } finally {
          multipleUpdate = false;
          saveConfiguration();
        }
        return null;
      }
    });
  }

  @Override
  public void dropBucket(final String bucketName) {
    database.executeInWriteLock(new Callable<Object>() {
      @Override
      public Object call() {
        final Bucket bucket = getBucketByName(bucketName);

        database.getPageManager().deleteFile(bucket.getId());
        try {
          database.getFileManager().dropFile(bucket.getId());
        } catch (IOException e) {
          LogManager.instance().log(this, Level.SEVERE, "Error on deleting bucket '%s'", e, bucketName);
        }
        removeFile(bucket.getId());

        bucketMap.remove(bucketName);

        for (Index idx : new ArrayList<>(indexMap.values())) {
          if (idx.getAssociatedBucketId() == bucket.getId())
            dropIndex(idx.getName());
        }

        saveConfiguration();
        return null;
      }
    });
  }

  public DocumentType createDocumentType(final String typeName) {
    return createDocumentType(typeName, Runtime.getRuntime().availableProcessors());
  }

  public DocumentType createDocumentType(final String typeName, final int buckets) {
    return createDocumentType(typeName, buckets, Bucket.DEF_PAGE_SIZE);
  }

  public DocumentType createDocumentType(final String typeName, final int buckets, final int pageSize) {
    if (typeName == null || typeName.isEmpty())
      throw new IllegalArgumentException("Missing type");

    if (buckets > 32)
      throw new IllegalArgumentException("Cannot create " + buckets + " buckets: maximum is 32");

    return (DocumentType) database.executeInWriteLock(new Callable<Object>() {
      @Override
      public Object call() {
        if (typeName.indexOf(",") > -1)
          throw new IllegalArgumentException("Type name '" + typeName + "' contains non valid characters");

        if (types.containsKey(typeName))
          throw new SchemaException("Type '" + typeName + "' already exists");

        // CREATE ENTRY IN DICTIONARY IF NEEDED
        dictionary.getIdByName(typeName, true);

        final DocumentType c = new DocumentType(EmbeddedSchema.this, typeName);
        types.put(typeName, c);

        for (int i = 0; i < buckets; ++i) {
          final String bucketName = FileUtils.encode(typeName, encoding) + "_" + i;
          if (existsBucket(bucketName)) {
            LogManager.instance().log(this, Level.WARNING, "Reusing found bucket '%s' for type '%s'", null, bucketName, typeName);
            c.addBucket(getBucketByName(bucketName));
          } else
            // CREATE A NEW ONE
            c.addBucket(createBucket(bucketName, pageSize));
        }

        saveConfiguration();

        return c;
      }
    });
  }

  @Override
  public DocumentType getOrCreateDocumentType(final String typeName) {
    return getOrCreateDocumentType(typeName, Runtime.getRuntime().availableProcessors());
  }

  @Override
  public DocumentType getOrCreateDocumentType(final String typeName, final int buckets) {
    return getOrCreateDocumentType(typeName, buckets, Bucket.DEF_PAGE_SIZE);
  }

  @Override
  public DocumentType getOrCreateDocumentType(String typeName, final int buckets, final int pageSize) {
    final DocumentType t = types.get(typeName);
    if (t != null) {
      if (t.getClass().equals(DocumentType.class))
        return t;
      throw new SchemaException("Type '" + typeName + "' is document not document");
    }
    return createDocumentType(typeName, buckets, pageSize);
  }

  @Override
  public VertexType createVertexType(final String typeName) {
    return createVertexType(typeName, Runtime.getRuntime().availableProcessors());
  }

  @Override
  public VertexType createVertexType(final String typeName, final int buckets) {
    return createVertexType(typeName, buckets, Bucket.DEF_PAGE_SIZE);
  }

  @Override
  public VertexType createVertexType(String typeName, final int buckets, final int pageSize) {
    if (typeName == null || typeName.isEmpty())
      throw new IllegalArgumentException("Missing type");

    if (buckets > 32)
      throw new IllegalArgumentException("Cannot create " + buckets + " buckets: maximum is 32");

    return (VertexType) database.executeInWriteLock(new Callable<Object>() {
      @Override
      public Object call() {
        if (typeName.indexOf(",") > -1)
          throw new IllegalArgumentException("Vertex type name '" + typeName + "' contains non valid characters");

        if (types.containsKey(typeName))
          throw new SchemaException("Vertex type '" + typeName + "' already exists");
        final VertexType c = new VertexType(EmbeddedSchema.this, typeName);
        types.put(typeName, c);

        for (int i = 0; i < buckets; ++i) {
          final String bucketName = FileUtils.encode(typeName, encoding) + "_" + i;
          if (existsBucket(bucketName)) {
            LogManager.instance().log(this, Level.WARNING, "Reusing found bucket '%s' for type '%s'", null, bucketName, typeName);
            c.addBucket(getBucketByName(bucketName));
          } else
            // CREATE A NEW ONE
            c.addBucket(createBucket(bucketName, pageSize));
        }

        database.getGraphEngine().createVertexType(database, c);

        saveConfiguration();

        return c;
      }
    });
  }

  @Override
  public VertexType getOrCreateVertexType(final String typeName) {
    return getOrCreateVertexType(typeName, Runtime.getRuntime().availableProcessors());
  }

  @Override
  public VertexType getOrCreateVertexType(final String typeName, final int buckets) {
    return getOrCreateVertexType(typeName, buckets, Bucket.DEF_PAGE_SIZE);
  }

  @Override
  public VertexType getOrCreateVertexType(String typeName, final int buckets, final int pageSize) {
    final DocumentType t = types.get(typeName);
    if (t != null) {
      if (t.getClass().equals(VertexType.class))
        return (VertexType) t;
      throw new SchemaException("Type '" + typeName + "' is not a vertex type");
    }
    return createVertexType(typeName, buckets, pageSize);
  }

  @Override
  public EdgeType createEdgeType(final String typeName) {
    return createEdgeType(typeName, Runtime.getRuntime().availableProcessors());
  }

  @Override
  public EdgeType createEdgeType(final String typeName, final int buckets) {
    return createEdgeType(typeName, buckets, EDGE_DEF_PAGE_SIZE);
  }

  @Override
  public EdgeType createEdgeType(final String typeName, final int buckets, final int pageSize) {
    if (typeName == null || typeName.isEmpty())
      throw new IllegalArgumentException("Missing type");

    if (buckets > 32)
      throw new IllegalArgumentException("Cannot create " + buckets + " buckets: maximum is 32");

    return (EdgeType) database.executeInWriteLock(new Callable<Object>() {
      @Override
      public Object call() {
        if (typeName.indexOf(",") > -1)
          throw new IllegalArgumentException("Edge type name '" + typeName + "' contains non valid characters");

        if (types.containsKey(typeName))
          throw new SchemaException("Edge type '" + typeName + "' already exists");
        final DocumentType c = new EdgeType(EmbeddedSchema.this, typeName);
        types.put(typeName, c);

        for (int i = 0; i < buckets; ++i) {
          final String bucketName = FileUtils.encode(typeName, encoding) + "_" + i;
          if (existsBucket(bucketName)) {
            LogManager.instance().log(this, Level.WARNING, "Reusing found bucket '%s' for type '%s'", null, bucketName, typeName);
            c.addBucket(getBucketByName(bucketName));
          } else
            // CREATE A NEW ONE
            c.addBucket(createBucket(bucketName, pageSize));
        }

        saveConfiguration();

        return c;
      }
    });
  }

  @Override
  public EdgeType getOrCreateEdgeType(final String typeName) {
    return getOrCreateEdgeType(typeName, Runtime.getRuntime().availableProcessors());
  }

  @Override
  public EdgeType getOrCreateEdgeType(final String typeName, final int buckets) {
    return getOrCreateEdgeType(typeName, buckets, Bucket.DEF_PAGE_SIZE);
  }

  @Override
  public EdgeType getOrCreateEdgeType(String typeName, final int buckets, final int pageSize) {
    final DocumentType t = types.get(typeName);
    if (t != null) {
      if (t.getClass().equals(EdgeType.class))
        return (EdgeType) t;
      throw new SchemaException("Type '" + typeName + "' is not a type of edge");
    }
    return createEdgeType(typeName, buckets, pageSize);
  }

  protected synchronized void readConfiguration() {
    types.clear();

    loadInRamCompleted = false;
    readingFromFile = true;

    boolean saveConfiguration = false;
    try {
      File file = new File(databasePath + "/" + SCHEMA_FILE_NAME);
      if (!file.exists() || file.length() == 0) {
        file = new File(databasePath + "/" + SCHEMA_PREV_FILE_NAME);
        if (!file.exists())
          return;

        LogManager.instance().log(this, Level.WARNING, "Could not find schema file, loading the previous version saved");
      }

      final String fileContent = FileUtils.readStreamAsString(new FileInputStream(file), encoding);

      final JSONObject root = new JSONObject(fileContent);

      if (root.names() == null || root.names().length() == 0)
        // EMPTY SCHEMA
        return;

      final JSONObject settings = root.getJSONObject("settings");

      timeZone = TimeZone.getTimeZone(settings.getString("timeZone"));
      dateFormat = settings.getString("dateFormat");
      dateTimeFormat = settings.getString("dateTimeFormat");

      final JSONObject types = root.getJSONObject("types");

      final Map<String, String[]> parentTypes = new HashMap<>();

      final Map<String, JSONObject> orphanIndexes = new HashMap<>();

      for (String typeName : types.keySet()) {
        final JSONObject schemaType = types.getJSONObject(typeName);

        final DocumentType type;

        final String kind = (String) schemaType.get("type");
        if ("v".equals(kind)) {
          type = new VertexType(this, typeName);
        } else if ("e".equals(kind)) {
          type = new EdgeType(this, typeName);
        } else if ("d".equals(kind)) {
          type = new DocumentType(this, typeName);
        } else
          throw new ConfigurationException("Type '" + kind + "' is not supported");

        this.types.put(typeName, type);

        final JSONArray schemaParent = schemaType.getJSONArray("parents");
        if (schemaParent != null) {
          // SAVE THE PARENT HIERARCHY FOR LATER
          final String[] parents = new String[schemaParent.length()];
          parentTypes.put(typeName, parents);
          for (int i = 0; i < schemaParent.length(); ++i)
            parents[i] = schemaParent.getString(i);
        }

        final JSONArray schemaBucket = schemaType.getJSONArray("buckets");
        if (schemaBucket != null) {
          for (int i = 0; i < schemaBucket.length(); ++i) {
            final PaginatedComponent bucket = bucketMap.get(schemaBucket.getString(i));
            if (bucket == null || !(bucket instanceof Bucket)) {
              LogManager.instance()
                  .log(this, Level.WARNING, "Cannot find bucket %s for type '%s', removing it from type configuration", null, schemaBucket.getString(i), type);

              // GO BACK
              schemaBucket.remove(i);
              --i;

              saveConfiguration = true;
            } else
              type.addBucketInternal((Bucket) bucket);
          }
        }

        if (schemaType.has("properties")) {
          final JSONObject schemaProperties = schemaType.getJSONObject("properties");
          if (schemaProperties != null) {
            for (String propName : schemaProperties.keySet()) {
              final JSONObject prop = schemaProperties.getJSONObject(propName);
              type.createProperty(propName, (String) prop.get("type"));
            }
          }
        }

        final JSONObject schemaIndexes = schemaType.getJSONObject("indexes");
        if (schemaIndexes != null) {
          for (String indexName : schemaIndexes.keySet()) {
            final JSONObject index = schemaIndexes.getJSONObject(indexName);

            final JSONArray schemaIndexProperties = index.getJSONArray("properties");
            final String[] properties = new String[schemaIndexProperties.length()];
            for (int i = 0; i < properties.length; ++i)
              properties[i] = schemaIndexProperties.getString(i);

            final IndexInternal idx = indexMap.get(indexName);
            if (idx != null) {
              final LSMTreeIndexAbstract.NULL_STRATEGY nullStrategy = index.has("nullStrategy") ?
                  LSMTreeIndexAbstract.NULL_STRATEGY.valueOf(index.getString("nullStrategy")) :
                  LSMTreeIndexAbstract.NULL_STRATEGY.ERROR;

              idx.setNullStrategy(nullStrategy);

              type.addIndexInternal(idx, bucketMap.get(index.getString("bucket")).getId(), properties);
            } else {
              orphanIndexes.put(indexName, index);
              index.put("type", typeName);
              LogManager.instance().log(this, Level.WARNING, "Cannot find index '%s' defined in type '%s'. Ignoring it", null, indexName, type);
            }
          }
        }
      }

      // ASSOCIATE ORPHAN INDEXES
      boolean completed = false;
      while (!completed) {
        completed = true;
        for (IndexInternal index : indexMap.values()) {
          if (index.getTypeName() == null) {
            final String indexName = index.getName();

            final int pos = indexName.lastIndexOf("_");
            final String bucketName = indexName.substring(0, pos);
            Bucket bucket = bucketMap.get(bucketName);
            if (bucket != null) {
              for (Map.Entry<String, JSONObject> entry : orphanIndexes.entrySet()) {
                final int pos2 = entry.getKey().lastIndexOf("_");
                final String bucketNameIndex = entry.getKey().substring(0, pos2);

                if (bucketName.equals(bucketNameIndex)) {
                  final DocumentType type = this.types.get(entry.getValue().getString("type"));
                  if (type != null) {
                    final JSONArray schemaIndexProperties = entry.getValue().getJSONArray("properties");

                    final String[] properties = new String[schemaIndexProperties.length()];
                    for (int i = 0; i < properties.length; ++i)
                      properties[i] = schemaIndexProperties.getString(i);

                    final LSMTreeIndexAbstract.NULL_STRATEGY nullStrategy = entry.getValue().has("nullStrategy") ?
                        LSMTreeIndexAbstract.NULL_STRATEGY.valueOf(entry.getValue().getString("nullStrategy")) :
                        LSMTreeIndexAbstract.NULL_STRATEGY.ERROR;

                    index.setNullStrategy(nullStrategy);
                    type.addIndexInternal(index, bucket.getId(), properties);
                    LogManager.instance().log(this, Level.WARNING, "Relinked orphan index '%s' to type '%s'", null, indexName, type.getName());
                    saveConfiguration = true;
                    completed = false;
                    break;
                  }
                }
              }

              if (!completed)
                break;
            }
          }
        }
      }

      if (saveConfiguration)
        saveConfiguration();

      // RESTORE THE INHERITANCE
      for (Map.Entry<String, String[]> entry : parentTypes.entrySet()) {
        final DocumentType type = getType(entry.getKey());
        for (String p : entry.getValue())
          type.addParentType(getType(p));
      }

      loadInRamCompleted = true;

    } catch (Exception e) {
      LogManager.instance().log(this, Level.SEVERE, "Error on loading schema. The schema will be reset", e);
    } finally {
      readingFromFile = false;

      if (dirtyConfiguration)
        saveConfiguration();
    }
  }

  public synchronized void saveConfiguration() {
    if (readingFromFile || !loadInRamCompleted || multipleUpdate || database.isTransactionActive()) {
      // POSTPONE THE SAVING
      dirtyConfiguration = true;
      return;
    }

    try {
      final JSONObject root = serializeConfiguration();

      final File prevFile = new File(databasePath + "/" + SCHEMA_FILE_NAME);
      if (prevFile.exists()) {
        final File copy = new File(databasePath + "/" + SCHEMA_PREV_FILE_NAME);
        if (copy.exists())
          copy.delete();

        prevFile.renameTo(copy);
      }

      final FileWriter file = new FileWriter(databasePath + "/" + SCHEMA_FILE_NAME);
      file.write(root.toString());
      file.close();

      dirtyConfiguration = false;

    } catch (IOException e) {
      LogManager.instance().log(this, Level.SEVERE, "Error on saving schema configuration to file: %s", e, databasePath + "/" + SCHEMA_FILE_NAME);
    }
  }

  public JSONObject serializeConfiguration() {
    final JSONObject root = new JSONObject();
    root.put("version", Constants.getRawVersion());
    root.put("build", Constants.getBuildNumber());

    final JSONObject settings = new JSONObject();
    root.put("settings", settings);

    settings.put("timeZone", timeZone.getDisplayName());
    settings.put("dateFormat", dateFormat);
    settings.put("dateTimeFormat", dateTimeFormat);

    final JSONObject types = new JSONObject();
    root.put("types", types);

    for (DocumentType t : this.types.values()) {
      final JSONObject type = new JSONObject();
      types.put(t.getName(), type);

      final String kind;
      if (t instanceof VertexType)
        kind = "v";
      else if (t instanceof EdgeType)
        kind = "e";
      else
        kind = "d";
      type.put("type", kind);

      final String[] parents = new String[t.getParentTypes().size()];
      for (int i = 0; i < parents.length; ++i)
        parents[i] = t.getParentTypes().get(i).getName();
      type.put("parents", parents);

      final List<Bucket> originalBuckets = t.getBuckets(false);
      final String[] buckets = new String[originalBuckets.size()];
      for (int i = 0; i < buckets.length; ++i)
        buckets[i] = originalBuckets.get(i).getName();

      type.put("buckets", buckets);

      final JSONObject properties = new JSONObject();
      type.put("properties", properties);

      for (String propName : t.getPropertyNames()) {
        final JSONObject prop = new JSONObject();
        properties.put(propName, prop);

        final Property p = t.getProperty(propName);
        prop.put("type", p.getType());
      }

      final JSONObject indexes = new JSONObject();
      type.put("indexes", indexes);

      for (Index i : t.getAllIndexes(false)) {
        for (Index entry : ((TypeIndex) i).getIndexesOnBuckets()) {
          final JSONObject index = new JSONObject();
          indexes.put(entry.getName(), index);

          index.put("bucket", getBucketById(entry.getAssociatedBucketId()).getName());
          index.put("properties", entry.getPropertyNames());
          index.put("nullStrategy", entry.getNullStrategy());
        }
      }
    }
    return root;
  }

  public void registerFile(final PaginatedComponent file) {
    final int fileId = file.getId();

    while (files.size() < fileId + 1)
      files.add(null);

    if (files.get(fileId) != null)
      throw new SchemaException("File with id '" + fileId + "' already exists (previous=" + files.get(fileId) + " new=" + file + ")");

    files.set(fileId, file);
  }

  public boolean isDirty() {
    return dirtyConfiguration;
  }
}
