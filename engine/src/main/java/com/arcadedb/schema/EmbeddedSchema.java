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

import com.arcadedb.Constants;
import com.arcadedb.GlobalConfiguration;
import com.arcadedb.database.Database;
import com.arcadedb.database.DatabaseInternal;
import com.arcadedb.database.Document;
import com.arcadedb.database.MutableDocument;
import com.arcadedb.database.Record;
import com.arcadedb.engine.Bucket;
import com.arcadedb.engine.Dictionary;
import com.arcadedb.engine.PaginatedComponent;
import com.arcadedb.engine.PaginatedComponentFactory;
import com.arcadedb.engine.PaginatedFile;
import com.arcadedb.exception.ConfigurationException;
import com.arcadedb.exception.DatabaseMetadataException;
import com.arcadedb.exception.DatabaseOperationException;
import com.arcadedb.exception.SchemaException;
import com.arcadedb.function.FunctionDefinition;
import com.arcadedb.function.FunctionLibraryDefinition;
import com.arcadedb.index.Index;
import com.arcadedb.index.IndexException;
import com.arcadedb.index.IndexFactory;
import com.arcadedb.index.IndexInternal;
import com.arcadedb.index.TypeIndex;
import com.arcadedb.index.lsm.LSMTreeFullTextIndex;
import com.arcadedb.index.lsm.LSMTreeIndex;
import com.arcadedb.index.lsm.LSMTreeIndexAbstract;
import com.arcadedb.index.lsm.LSMTreeIndexCompacted;
import com.arcadedb.index.lsm.LSMTreeIndexMutable;
import com.arcadedb.log.LogManager;
import com.arcadedb.security.SecurityDatabaseUser;
import com.arcadedb.security.SecurityManager;
import com.arcadedb.utility.FileUtils;
import org.json.JSONArray;
import org.json.JSONObject;

import java.io.*;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.*;
import java.util.logging.*;

public class EmbeddedSchema implements Schema {
  public static final  String                                 DEFAULT_DATE_FORMAT     = "yyyy-MM-dd";
  public static final  String                                 DEFAULT_DATETIME_FORMAT = "yyyy-MM-dd HH:mm:ss";
  public static final  String                                 DEFAULT_ENCODING        = "UTF-8";
  public static final  String                                 SCHEMA_FILE_NAME        = "schema.json";
  public static final  String                                 SCHEMA_PREV_FILE_NAME   = "schema.prev.json";
  private static final String                                 ENCODING                = DEFAULT_ENCODING;
  private static final int                                    EDGE_DEF_PAGE_SIZE      = Bucket.DEF_PAGE_SIZE / 3;
  private final        DatabaseInternal                       database;
  private final        SecurityManager                        security;
  private final        List<PaginatedComponent>               files                   = new ArrayList<>();
  private final        Map<String, DocumentType>              types                   = new HashMap<>();
  private final        Map<String, Bucket>                    bucketMap               = new HashMap<>();
  protected final      Map<String, IndexInternal>             indexMap                = new HashMap<>();
  private final        String                                 databasePath;
  private final        File                                   configurationFile;
  private final        PaginatedComponentFactory              paginatedComponentFactory;
  private final        IndexFactory                           indexFactory            = new IndexFactory();
  private              Dictionary                             dictionary;
  private              String                                 dateFormat              = DEFAULT_DATE_FORMAT;
  private              String                                 dateTimeFormat          = DEFAULT_DATETIME_FORMAT;
  private              TimeZone                               timeZone                = TimeZone.getDefault();
  private              boolean                                readingFromFile         = false;
  private              boolean                                dirtyConfiguration      = false;
  private              boolean                                loadInRamCompleted      = false;
  private              boolean                                multipleUpdate          = false;
  private              AtomicLong                             versionSerial           = new AtomicLong();
  private              Map<String, FunctionLibraryDefinition> functionLibraries       = new ConcurrentHashMap<>();

  public EmbeddedSchema(final DatabaseInternal database, final String databasePath, final SecurityManager security) {
    this.database = database;
    this.databasePath = databasePath;
    this.security = security;

    paginatedComponentFactory = new PaginatedComponentFactory(database);
    paginatedComponentFactory.registerComponent(Dictionary.DICT_EXT, new Dictionary.PaginatedComponentFactoryHandler());
    paginatedComponentFactory.registerComponent(Bucket.BUCKET_EXT, new Bucket.PaginatedComponentFactoryHandler());
    paginatedComponentFactory.registerComponent(LSMTreeIndexMutable.UNIQUE_INDEX_EXT, new LSMTreeIndex.PaginatedComponentFactoryHandlerUnique());
    paginatedComponentFactory.registerComponent(LSMTreeIndexMutable.NOTUNIQUE_INDEX_EXT, new LSMTreeIndex.PaginatedComponentFactoryHandlerNotUnique());
    paginatedComponentFactory.registerComponent(LSMTreeIndexCompacted.UNIQUE_INDEX_EXT, new LSMTreeIndex.PaginatedComponentFactoryHandlerUnique());
    paginatedComponentFactory.registerComponent(LSMTreeIndexCompacted.NOTUNIQUE_INDEX_EXT, new LSMTreeIndex.PaginatedComponentFactoryHandlerNotUnique());

    indexFactory.register(INDEX_TYPE.LSM_TREE.name(), new LSMTreeIndex.IndexFactoryHandler());
    indexFactory.register(INDEX_TYPE.FULL_TEXT.name(), new LSMTreeFullTextIndex.IndexFactoryHandler());
    configurationFile = new File(databasePath + "/" + SCHEMA_FILE_NAME);
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
      throw new DatabaseMetadataException("Error on loading dictionary (error=" + e + ")", e);
    }
  }

  public void load(final PaginatedFile.MODE mode, final boolean initialize) throws IOException {
    files.clear();
    types.clear();
    bucketMap.clear();
    indexMap.clear();
    dictionary = null;

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
      if (file != null && !Dictionary.DICT_EXT.equals(file.getFileExtension())) {
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

    if (initialize)
      initComponents();

    readConfiguration();
    updateSecurity();
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
    if (!(p instanceof Bucket))
      throw new SchemaException("Bucket with id '" + id + "' was not found");
    return (Bucket) p;
  }

  @Override
  public Bucket createBucket(final String bucketName) {
    return createBucket(bucketName, database.getConfiguration().getValueAsInteger(GlobalConfiguration.BUCKET_DEFAULT_PAGE_SIZE));
  }

  public Bucket createBucket(final String bucketName, final int pageSize) {
    database.checkPermissionsOnDatabase(SecurityDatabaseUser.DATABASE_ACCESS.UPDATE_SCHEMA);

    if (bucketMap.containsKey(bucketName))
      throw new SchemaException("Cannot create bucket '" + bucketName + "' because already exists");

    return recordFileChanges(() -> {
      try {
        final Bucket bucket = new Bucket(database, bucketName, databasePath + "/" + bucketName, PaginatedFile.MODE.READ_WRITE, pageSize,
            Bucket.CURRENT_VERSION);
        registerFile(bucket);
        bucketMap.put(bucketName, bucket);

        return bucket;

      } catch (IOException e) {
        throw new SchemaException("Cannot create bucket '" + bucketName + "' (error=" + e + ")", e);
      }
    });
  }

  public String getEncoding() {
    return ENCODING;
  }

  @Override
  public DocumentType copyType(final String typeName, final String newTypeName, final Class<? extends DocumentType> newTypeClass, final int buckets,
      final int pageSize, final int transactionBatchSize) {
    database.checkPermissionsOnDatabase(SecurityDatabaseUser.DATABASE_ACCESS.UPDATE_SCHEMA);

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
          else
            newRecord = database.newDocument(newTypeName);

          newRecord.fromMap(record.toMap());
          newRecord.save();

          ++copied;

          if (copied % transactionBatchSize == 0) {
            database.commit();
            database.begin();
          }
        }

        // COPY INDEXES
        for (Index index : oldType.getAllIndexes(false))
          newType.createTypeIndex(index.getType(), index.isUnique(), index.getPropertyNames().toArray(new String[index.getPropertyNames().size()]));

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
    database.checkPermissionsOnDatabase(SecurityDatabaseUser.DATABASE_ACCESS.UPDATE_SCHEMA);

    recordFileChanges(() -> {
      multipleUpdate = true;

      final IndexInternal index = indexMap.get(indexName);
      if (index == null)
        return null;

      List<Integer> lockedFiles = null;
      try {
        lockedFiles = database.getTransactionManager().tryLockFiles(index.getFileIds(), 5_000);

        if (index.getTypeIndex() != null)
          index.getTypeIndex().removeIndexOnBucket(index);

        indexMap.remove(indexName);
        index.drop();

        if (index.getTypeName() != null) {
          if (index instanceof TypeIndex)
            getType(index.getTypeName()).removeTypeIndexInternal((TypeIndex) index);
          else
            getType(index.getTypeName()).removeBucketIndexInternal(index);
        }

      } catch (Exception e) {
        throw new SchemaException("Cannot drop the index '" + indexName + "' (error=" + e + ")", e);
      } finally {
        if (lockedFiles != null)
          database.getTransactionManager().unlockFilesInOrder(lockedFiles);

        multipleUpdate = false;
      }
      return null;
    });
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
    return createTypeIndex(indexType, unique, typeName, propertyNames, LSMTreeIndexAbstract.DEF_PAGE_SIZE, LSMTreeIndexAbstract.NULL_STRATEGY.SKIP, null);
  }

  @Override
  public TypeIndex createTypeIndex(final INDEX_TYPE indexType, final boolean unique, final String typeName, final String[] propertyNames, final int pageSize) {
    return createTypeIndex(indexType, unique, typeName, propertyNames, pageSize, LSMTreeIndexAbstract.NULL_STRATEGY.SKIP, null);
  }

  @Override
  public TypeIndex createTypeIndex(final INDEX_TYPE indexType, final boolean unique, final String typeName, final String[] propertyNames, final int pageSize,
      final Index.BuildIndexCallback callback) {
    return createTypeIndex(indexType, unique, typeName, propertyNames, pageSize, LSMTreeIndexAbstract.NULL_STRATEGY.SKIP, callback);
  }

  @Override
  public TypeIndex createTypeIndex(final INDEX_TYPE indexType, final boolean unique, final String typeName, final String[] propertyNames, final int pageSize,
      final LSMTreeIndexAbstract.NULL_STRATEGY nullStrategy, final Index.BuildIndexCallback callback) {
    database.checkPermissionsOnDatabase(SecurityDatabaseUser.DATABASE_ACCESS.UPDATE_SCHEMA);
    if (indexType == null)
      throw new DatabaseMetadataException("Cannot create index on type '" + typeName + "' because indexType was not specified");
    if (propertyNames.length == 0)
      throw new DatabaseMetadataException("Cannot create index on type '" + typeName + "' because there are no property defined");

    final DocumentType type = getType(typeName);

    final TypeIndex index = type.getPolymorphicIndexByProperties(propertyNames);
    if (index != null)
      throw new IllegalArgumentException(
          "Found the existent index '" + index.getName() + "' defined on the properties '" + Arrays.asList(propertyNames) + "' for type '" + typeName + "'");

    // CHECK ALL THE PROPERTIES EXIST
    final Type[] keyTypes = new Type[propertyNames.length];
    int i = 0;

    for (String propertyName : propertyNames) {
      if (type instanceof EdgeType && ("@out".equals(propertyName) || "@in".equals(propertyName))) {
        keyTypes[i++] = Type.LINK;
      } else {
        final Property property = type.getPolymorphicPropertyIfExists(propertyName);
        if (property == null)
          throw new SchemaException("Cannot create the index on type '" + typeName + "." + propertyName + "' because the property does not exist");

        keyTypes[i++] = property.getType();
      }
    }

    final List<Bucket> buckets = type.getBuckets(true);
    final Index[] indexes = new Index[buckets.size()];

    try {
      recordFileChanges(() -> {
        database.transaction(() -> {

          for (int idx = 0; idx < buckets.size(); ++idx) {
            final Bucket bucket = buckets.get(idx);
            indexes[idx] = createBucketIndex(type, keyTypes, bucket, typeName, indexType, unique, pageSize, nullStrategy, callback, propertyNames);
          }

          saveConfiguration();

        }, false, 1, null, (error) -> {
          for (int j = 0; j < indexes.length; j++) {
            final IndexInternal indexToRemove = (IndexInternal) indexes[j];
            if (indexToRemove != null)
              indexToRemove.drop();
          }
        });
        return null;
      });

      return type.getPolymorphicIndexByProperties(propertyNames);
    } catch (Throwable e) {
      dropIndex(typeName + Arrays.toString(propertyNames));
      throw new IndexException("Error on creating index on type '" + typeName + "', properties " + Arrays.toString(propertyNames), e);
    }
  }

  @Override
  public TypeIndex getOrCreateTypeIndex(final INDEX_TYPE indexType, final boolean unique, final String typeName, final String... propertyNames) {
    return getOrCreateTypeIndex(indexType, unique, typeName, propertyNames, LSMTreeIndexAbstract.DEF_PAGE_SIZE, null);
  }

  @Override
  public TypeIndex getOrCreateTypeIndex(final INDEX_TYPE indexType, final boolean unique, final String typeName, final String[] propertyNames,
      final int pageSize) {
    return getOrCreateTypeIndex(indexType, unique, typeName, propertyNames, pageSize, LSMTreeIndexAbstract.NULL_STRATEGY.SKIP, null);
  }

  @Override
  public TypeIndex getOrCreateTypeIndex(final INDEX_TYPE indexType, final boolean unique, final String typeName, final String[] propertyNames,
      final int pageSize, final Index.BuildIndexCallback callback) {
    return getOrCreateTypeIndex(indexType, unique, typeName, propertyNames, pageSize, LSMTreeIndexAbstract.NULL_STRATEGY.SKIP, callback);
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
    database.checkPermissionsOnDatabase(SecurityDatabaseUser.DATABASE_ACCESS.UPDATE_SCHEMA);

    if (propertyNames.length == 0)
      throw new DatabaseMetadataException("Cannot create index on type '" + typeName + "' because there are no property defined");

    final DocumentType type = getType(typeName);

    // CHECK ALL THE PROPERTIES EXIST
    final Type[] keyTypes = new Type[propertyNames.length];
    int i = 0;

    for (String propertyName : propertyNames) {
      final Property property = type.getPolymorphicPropertyIfExists(propertyName);
      if (property == null)
        throw new SchemaException("Cannot create the index on type '" + typeName + "." + propertyName + "' because the property does not exist");

      keyTypes[i++] = property.getType();
    }

    return recordFileChanges(() -> {
      final AtomicReference<Index> result = new AtomicReference<>();
      database.transaction(() -> {

        Bucket bucket = null;
        final List<Bucket> buckets = type.getBuckets(false);
        for (Bucket b : buckets) {
          if (bucketName.equals(b.getName())) {
            bucket = b;
            break;
          }
        }

        final Index index = createBucketIndex(type, keyTypes, bucket, typeName, indexType, unique, pageSize, nullStrategy, callback, propertyNames);
        result.set(index);

        saveConfiguration();

      }, false, 1, null, (error) -> {
        final Index indexToRemove = result.get();
        if (indexToRemove != null) {
          ((IndexInternal) indexToRemove).drop();
        }
      });

      return result.get();
    });
  }

  protected Index createBucketIndex(final DocumentType type, final Type[] keyTypes, final Bucket bucket, final String typeName, final INDEX_TYPE indexType,
      final boolean unique, final int pageSize, final LSMTreeIndexAbstract.NULL_STRATEGY nullStrategy, final Index.BuildIndexCallback callback,
      final String[] propertyNames) {
    database.checkPermissionsOnDatabase(SecurityDatabaseUser.DATABASE_ACCESS.UPDATE_SCHEMA);

    if (bucket == null)
      throw new IllegalArgumentException("bucket is null");

    final String indexName = FileUtils.encode(bucket.getName(), ENCODING) + "_" + System.nanoTime();

    if (indexMap.containsKey(indexName))
      throw new DatabaseMetadataException("Cannot create index '" + indexName + "' on type '" + typeName + "' because it already exists");

    final IndexInternal index = indexFactory.createIndex(indexType.name(), database, indexName, unique, databasePath + "/" + indexName,
        PaginatedFile.MODE.READ_WRITE, keyTypes, pageSize, nullStrategy, callback);

    try {
      registerFile(index.getPaginatedComponent());

      indexMap.put(indexName, index);

      type.addIndexInternal(index, bucket.getId(), propertyNames);
      index.build(callback);

      return index;

    } catch (Exception e) {
      dropIndex(indexName);
      throw new IndexException("Error on creating index '" + indexName + "'", e);
    }
  }

  public Index createManualIndex(final INDEX_TYPE indexType, final boolean unique, final String indexName, final Type[] keyTypes, final int pageSize,
      final LSMTreeIndexAbstract.NULL_STRATEGY nullStrategy) {
    database.checkPermissionsOnDatabase(SecurityDatabaseUser.DATABASE_ACCESS.UPDATE_SCHEMA);

    if (indexMap.containsKey(indexName))
      throw new SchemaException("Cannot create index '" + indexName + "' because already exists");

    return recordFileChanges(() -> {
      final AtomicReference<IndexInternal> result = new AtomicReference<>();
      database.transaction(() -> {

        final IndexInternal index = indexFactory.createIndex(indexType.name(), database, FileUtils.encode(indexName, ENCODING), unique,
            databasePath + "/" + indexName, PaginatedFile.MODE.READ_WRITE, keyTypes, pageSize, nullStrategy, null);

        result.set(index);

        if (index instanceof PaginatedComponent)
          registerFile((PaginatedComponent) index);

        indexMap.put(indexName, index);

      }, false, 1, null, (error) -> {
        final IndexInternal indexToRemove = result.get();
        if (indexToRemove != null) {
          indexToRemove.drop();
        }
      });

      return result.get();
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
    database.checkPermissionsOnDatabase(SecurityDatabaseUser.DATABASE_ACCESS.UPDATE_SCHEMA);

    recordFileChanges(() -> {
      multipleUpdate = true;
      try {
        final DocumentType type = database.getSchema().getType(typeName);

        // CHECK INHERITANCE TREE AND ATTACH SUB-TYPES DIRECTLY TO THE PARENT TYPE
        for (DocumentType parent : type.superTypes)
          parent.subTypes.remove(type);
        for (DocumentType sub : type.subTypes) {
          sub.superTypes.remove(type);
          for (DocumentType parent : type.superTypes)
            sub.addSuperType(parent, false);
        }

        // DELETE ALL ASSOCIATED INDEXES
        for (Index m : type.getAllIndexes(true))
          dropIndex(m.getName());

        // DELETE ALL ASSOCIATED BUCKETS
        final List<Bucket> buckets = new ArrayList<>(type.getBuckets(false));
        for (Bucket b : buckets) {
          type.removeBucket(b);
          dropBucket(b.getName());
        }

        if (type instanceof VertexType)
          database.getGraphEngine().dropVertexType((VertexType) type);

        if (types.remove(typeName) == null)
          throw new SchemaException("Type '" + typeName + "' not found");
      } finally {
        multipleUpdate = false;
        saveConfiguration();
        updateSecurity();
      }
      return null;
    });
  }

  @Override
  public void dropBucket(final String bucketName) {
    database.checkPermissionsOnDatabase(SecurityDatabaseUser.DATABASE_ACCESS.UPDATE_SCHEMA);

    final Bucket bucket = getBucketByName(bucketName);

    recordFileChanges(() -> {
      for (DocumentType type : types.values()) {
        if (type.buckets.contains(bucket))
          throw new SchemaException(
              "Error on dropping bucket '" + bucketName + "' because it is assigned to type '" + type.getName() + "'. Remove the association first");
      }

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
    });
  }

  public DocumentType createDocumentType(final String typeName) {
    return createDocumentType(typeName, database.getConfiguration().getValueAsInteger(GlobalConfiguration.TYPE_DEFAULT_BUCKETS), Collections.emptyList(),
        database.getConfiguration().getValueAsInteger(GlobalConfiguration.BUCKET_DEFAULT_PAGE_SIZE));
  }

  public DocumentType createDocumentType(final String typeName, final int buckets) {
    return createDocumentType(typeName, buckets, Collections.emptyList(),
        database.getConfiguration().getValueAsInteger(GlobalConfiguration.BUCKET_DEFAULT_PAGE_SIZE));
  }

  @Override
  public DocumentType createDocumentType(final String typeName, final int buckets, final int pageSize) {
    return createDocumentType(typeName, buckets, Collections.emptyList(), pageSize);
  }

  @Override
  public DocumentType createDocumentType(final String typeName, final List<Bucket> buckets) {
    return createDocumentType(typeName, 0, buckets, database.getConfiguration().getValueAsInteger(GlobalConfiguration.BUCKET_DEFAULT_PAGE_SIZE));
  }

  @Override
  public DocumentType createDocumentType(final String typeName, final List<Bucket> buckets, final int pageSize) {
    return createDocumentType(typeName, 0, buckets, pageSize);
  }

  private DocumentType createDocumentType(final String typeName, final int buckets, final List<Bucket> bucketInstances, final int pageSize) {
    database.checkPermissionsOnDatabase(SecurityDatabaseUser.DATABASE_ACCESS.UPDATE_SCHEMA);

    if (typeName == null || typeName.isEmpty())
      throw new IllegalArgumentException("Missing type");

    if (buckets < 1 && bucketInstances.isEmpty())
      throw new IllegalArgumentException("Invalid number of buckets (" + buckets + "). At least 1 bucket is necessary to create a type");

    if (buckets > 32)
      throw new IllegalArgumentException("Cannot create " + buckets + " buckets: maximum is 32");

    if (typeName.contains(","))
      throw new IllegalArgumentException("Type name '" + typeName + "' contains non valid characters");

    if (types.containsKey(typeName))
      throw new SchemaException("Type '" + typeName + "' already exists");

    return recordFileChanges(() -> {
      // CREATE ENTRY IN DICTIONARY IF NEEDED. THIS IS USED BY EMBEDDED DOCUMENT WHERE THE DICTIONARY ID IS SAVED
      dictionary.getIdByName(typeName, true);

      final DocumentType c = new DocumentType(EmbeddedSchema.this, typeName);
      types.put(typeName, c);

      if (bucketInstances.isEmpty()) {
        for (int i = 0; i < buckets; ++i) {
          final String bucketName = FileUtils.encode(typeName, ENCODING) + "_" + i;
          if (existsBucket(bucketName)) {
            LogManager.instance().log(this, Level.WARNING, "Reusing found bucket '%s' for type '%s'", null, bucketName, typeName);
            c.addBucket(getBucketByName(bucketName));
          } else
            // CREATE A NEW ONE
            c.addBucket(createBucket(bucketName, pageSize));
        }
      } else {
        for (Bucket bucket : bucketInstances)
          c.addBucket(bucket);
      }

      saveConfiguration();
      updateSecurity();

      return c;
    });
  }

  @Override
  public DocumentType getOrCreateDocumentType(final String typeName) {
    return getOrCreateDocumentType(typeName, database.getConfiguration().getValueAsInteger(GlobalConfiguration.TYPE_DEFAULT_BUCKETS));
  }

  @Override
  public DocumentType getOrCreateDocumentType(final String typeName, final int buckets) {
    return getOrCreateDocumentType(typeName, buckets, database.getConfiguration().getValueAsInteger(GlobalConfiguration.BUCKET_DEFAULT_PAGE_SIZE));
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
    return createVertexType(typeName, database.getConfiguration().getValueAsInteger(GlobalConfiguration.TYPE_DEFAULT_BUCKETS), Collections.emptyList(),
        database.getConfiguration().getValueAsInteger(GlobalConfiguration.BUCKET_DEFAULT_PAGE_SIZE));
  }

  @Override
  public VertexType createVertexType(final String typeName, final int buckets) {
    return createVertexType(typeName, buckets, Collections.emptyList(),
        database.getConfiguration().getValueAsInteger(GlobalConfiguration.BUCKET_DEFAULT_PAGE_SIZE));
  }

  @Override
  public VertexType createVertexType(final String typeName, final List<Bucket> bucketInstances) {
    return createVertexType(typeName, 0, bucketInstances, database.getConfiguration().getValueAsInteger(GlobalConfiguration.BUCKET_DEFAULT_PAGE_SIZE));
  }

  @Override
  public VertexType createVertexType(final String typeName, final int buckets, final int pageSize) {
    return createVertexType(typeName, buckets, Collections.emptyList(), pageSize);
  }

  @Override
  public VertexType createVertexType(final String typeName, final List<Bucket> bucketInstances, final int pageSize) {
    return createVertexType(typeName, 0, bucketInstances, pageSize);
  }

  private VertexType createVertexType(final String typeName, final int buckets, final List<Bucket> bucketInstances, final int pageSize) {
    database.checkPermissionsOnDatabase(SecurityDatabaseUser.DATABASE_ACCESS.UPDATE_SCHEMA);

    if (typeName == null || typeName.isEmpty())
      throw new IllegalArgumentException("Missing type");

    if (buckets < 1 && bucketInstances.isEmpty())
      throw new IllegalArgumentException("Invalid number of buckets (" + buckets + "). At least 1 bucket is necessary to create a type");

    if (buckets > 32)
      throw new IllegalArgumentException("Cannot create " + buckets + " buckets: maximum is 32");

    if (typeName.contains(","))
      throw new IllegalArgumentException("Vertex type name '" + typeName + "' contains non valid characters");

    if (types.containsKey(typeName))
      throw new SchemaException("Vertex type '" + typeName + "' already exists");

    return recordFileChanges(() -> {
      final VertexType c = new VertexType(EmbeddedSchema.this, typeName);
      types.put(typeName, c);

      if (bucketInstances.isEmpty()) {
        for (int i = 0; i < buckets; ++i) {
          final String bucketName = FileUtils.encode(typeName, ENCODING) + "_" + i;
          if (existsBucket(bucketName)) {
            LogManager.instance().log(this, Level.WARNING, "Reusing found bucket '%s' for type '%s'", null, bucketName, typeName);
            c.addBucket(getBucketByName(bucketName));
          } else
            // CREATE A NEW ONE
            c.addBucket(createBucket(bucketName, pageSize));
        }
      } else {
        for (Bucket bucket : bucketInstances)
          c.addBucket(bucket);
      }

      database.getGraphEngine().createVertexType(c);

      saveConfiguration();
      updateSecurity();

      return c;
    });
  }

  @Override
  public VertexType getOrCreateVertexType(final String typeName) {
    return getOrCreateVertexType(typeName, database.getConfiguration().getValueAsInteger(GlobalConfiguration.TYPE_DEFAULT_BUCKETS));
  }

  @Override
  public VertexType getOrCreateVertexType(final String typeName, final int buckets) {
    return getOrCreateVertexType(typeName, buckets, database.getConfiguration().getValueAsInteger(GlobalConfiguration.BUCKET_DEFAULT_PAGE_SIZE));
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
    return createEdgeType(typeName, database.getConfiguration().getValueAsInteger(GlobalConfiguration.TYPE_DEFAULT_BUCKETS));
  }

  @Override
  public EdgeType createEdgeType(final String typeName, final int buckets) {
    return createEdgeType(typeName, buckets, Collections.emptyList(), EDGE_DEF_PAGE_SIZE);
  }

  @Override
  public EdgeType createEdgeType(final String typeName, final int buckets, final int pageSize) {
    return createEdgeType(typeName, buckets, Collections.emptyList(), pageSize);
  }

  @Override
  public EdgeType createEdgeType(final String typeName, final List<Bucket> buckets) {
    return createEdgeType(typeName, 0, buckets, EDGE_DEF_PAGE_SIZE);
  }

  @Override
  public EdgeType createEdgeType(final String typeName, final List<Bucket> buckets, final int pageSize) {
    return createEdgeType(typeName, 0, buckets, pageSize);
  }

  private EdgeType createEdgeType(final String typeName, final int buckets, final List<Bucket> bucketInstances, final int pageSize) {
    database.checkPermissionsOnDatabase(SecurityDatabaseUser.DATABASE_ACCESS.UPDATE_SCHEMA);

    if (typeName == null || typeName.isEmpty())
      throw new IllegalArgumentException("Missing type");

    if (buckets < 1 && bucketInstances.isEmpty())
      throw new IllegalArgumentException("Invalid number of buckets (" + buckets + "). At least 1 bucket is necessary to create a type");

    if (buckets > 32)
      throw new IllegalArgumentException("Cannot create " + buckets + " buckets: maximum is 32");

    if (typeName.contains(","))
      throw new IllegalArgumentException("Edge type name '" + typeName + "' contains non valid characters");

    if (types.containsKey(typeName))
      throw new SchemaException("Edge type '" + typeName + "' already exists");

    return recordFileChanges(() -> {
      final DocumentType c = new EdgeType(EmbeddedSchema.this, typeName);
      types.put(typeName, c);

      if (bucketInstances.isEmpty()) {
        for (int i = 0; i < buckets; ++i) {
          final String bucketName = FileUtils.encode(typeName, ENCODING) + "_" + i;
          if (existsBucket(bucketName)) {
            LogManager.instance().log(this, Level.WARNING, "Reusing found bucket '%s' for type '%s'", null, bucketName, typeName);
            c.addBucket(getBucketByName(bucketName));
          } else
            // CREATE A NEW ONE
            c.addBucket(createBucket(bucketName, pageSize));
        }
      } else {
        for (Bucket bucket : bucketInstances)
          c.addBucket(bucket);
      }
      saveConfiguration();
      updateSecurity();

      return c;
    });
  }

  @Override
  public EdgeType getOrCreateEdgeType(final String typeName) {
    return getOrCreateEdgeType(typeName, database.getConfiguration().getValueAsInteger(GlobalConfiguration.TYPE_DEFAULT_BUCKETS));
  }

  @Override
  public EdgeType getOrCreateEdgeType(final String typeName, final int buckets) {
    return getOrCreateEdgeType(typeName, buckets, database.getConfiguration().getValueAsInteger(GlobalConfiguration.BUCKET_DEFAULT_PAGE_SIZE));
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

      final JSONObject root;
      try (FileInputStream fis = new FileInputStream(file)) {
        final String fileContent = FileUtils.readStreamAsString(fis, ENCODING);
        root = new JSONObject(fileContent);
      }

      if (root.names() == null || root.names().length() == 0)
        // EMPTY SCHEMA
        return;

      versionSerial.set(root.has("schemaVersion") ? root.getLong("schemaVersion") : 0L);

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
            if (bucket == null) {
              LogManager.instance()
                  .log(this, Level.WARNING, "Cannot find bucket '%s' for type '%s', removing it from type configuration", null, schemaBucket.getString(i),
                      type);

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
              final Property p = type.createProperty(propName, (String) prop.get("type"));

              if (prop.has("default"))
                p.setDefaultValue(prop.get("default"));

              p.custom.clear();
              if (prop.has("custom"))
                p.custom.putAll(prop.getJSONObject("custom").toMap());
            }
          }
        }

        final JSONObject typeIndexesJSON = schemaType.getJSONObject("indexes");
        if (typeIndexesJSON != null) {

          final List<String> orderedIndexes = new ArrayList<>(typeIndexesJSON.keySet());
          orderedIndexes.sort(Comparator.naturalOrder());

          for (String indexName : orderedIndexes) {
            final JSONObject indexJSON = typeIndexesJSON.getJSONObject(indexName);

            if (!indexName.startsWith(typeName))
              continue;

            final JSONArray schemaIndexProperties = indexJSON.getJSONArray("properties");
            final String[] properties = new String[schemaIndexProperties.length()];
            for (int i = 0; i < properties.length; ++i)
              properties[i] = schemaIndexProperties.getString(i);

            final IndexInternal index = indexMap.get(indexName);
            if (index != null) {
              final LSMTreeIndexAbstract.NULL_STRATEGY nullStrategy = indexJSON.has("nullStrategy") ?
                  LSMTreeIndexAbstract.NULL_STRATEGY.valueOf(indexJSON.getString("nullStrategy")) :
                  LSMTreeIndexAbstract.NULL_STRATEGY.ERROR;

              index.setNullStrategy(nullStrategy);

              final String bucketName = indexJSON.getString("bucket");
              final Bucket bucket = bucketMap.get(bucketName);
              if (bucket == null) {
                orphanIndexes.put(indexName, indexJSON);
                indexJSON.put("type", typeName);
                LogManager.instance().log(this, Level.WARNING, "Cannot find bucket '%s' defined in index '%s'. Ignoring it", null, bucketName, index.getName());
              } else
                type.addIndexInternal(index, bucket.getId(), properties);

            } else {
              orphanIndexes.put(indexName, indexJSON);
              indexJSON.put("type", typeName);
              LogManager.instance().log(this, Level.WARNING, "Cannot find index '%s' defined in type '%s'. Ignoring it", null, indexName, type);
            }
          }
        }

        type.custom.clear();
        if (schemaType.has("custom"))
          type.custom.putAll(schemaType.getJSONObject("custom").toMap());
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
          type.addSuperType(getType(p), false);
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
      versionSerial.incrementAndGet();

      update(toJSON());

      dirtyConfiguration = false;

    } catch (IOException e) {
      LogManager.instance().log(this, Level.SEVERE, "Error on saving schema configuration to file: %s", e, databasePath + "/" + SCHEMA_FILE_NAME);
    }
  }

  public synchronized JSONObject toJSON() {
    final JSONObject root = new JSONObject();
    root.put("schemaVersion", versionSerial.get());
    root.put("dbmsVersion", Constants.getRawVersion());
    root.put("dbmsBuild", Constants.getBuildNumber());

    final JSONObject settings = new JSONObject();
    root.put("settings", settings);

    settings.put("timeZone", timeZone.getID());
    settings.put("dateFormat", dateFormat);
    settings.put("dateTimeFormat", dateTimeFormat);

    final JSONObject types = new JSONObject();
    root.put("types", types);

    for (DocumentType t : this.types.values())
      types.put(t.getName(), t.toJSON());

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

  public void initComponents() {
    for (PaginatedComponent f : files)
      if (f != null)
        f.onAfterLoad();
  }

  @Override
  public Schema registerFunctionLibrary(final FunctionLibraryDefinition library) {
    if (functionLibraries.putIfAbsent(library.getName(), library) != null)
      throw new IllegalArgumentException("Function library '" + library.getName() + "' already registered");
    return this;
  }

  @Override
  public Schema unregisterFunctionLibrary(final String name) {
    functionLibraries.remove(name);
    return this;
  }

  @Override
  public Iterable<FunctionLibraryDefinition> getFunctionLibraries() {
    return functionLibraries.values();
  }

  public FunctionLibraryDefinition getFunctionLibrary(final String name) {
    return functionLibraries.get(name);
  }

  @Override
  public FunctionDefinition getFunction(String libraryName, String functionName) {
    final FunctionLibraryDefinition library = functionLibraries.get(libraryName);
    return library != null ? library.getFunction(functionName) : null;
  }

  public boolean isDirty() {
    return dirtyConfiguration;
  }

  public File getConfigurationFile() {
    return configurationFile;
  }

  public long getVersion() {
    return versionSerial.get();
  }

  public synchronized void update(final JSONObject newSchema) throws IOException {
    if (newSchema.has("schemaVersion"))
      versionSerial.set(newSchema.getLong("schemaVersion"));

    final String latestSchema = newSchema.toString();

    if (configurationFile.exists()) {
      final File copy = new File(databasePath + "/" + SCHEMA_PREV_FILE_NAME);
      if (copy.exists())
        if (!copy.delete())
          LogManager.instance().log(this, Level.WARNING, "Error on deleting previous schema file '%s'", null, copy);

      if (!configurationFile.renameTo(copy))
        LogManager.instance().log(this, Level.WARNING, "Error on renaming previous schema file '%s'", null, copy);
    }

    try (FileWriter file = new FileWriter(databasePath + "/" + SCHEMA_FILE_NAME)) {
      file.write(latestSchema);
    }

    database.getExecutionPlanCache().invalidate();
  }

  private void updateSecurity() {
    if (security != null)
      security.updateSchema(database);
  }

  protected <RET> RET recordFileChanges(final Callable<Object> callback) {
    if (readingFromFile || !loadInRamCompleted) {
      try {
        return (RET) callback.call();
      } catch (Exception e) {
        throw new DatabaseOperationException("Error on updating the schema", e);
      }
    }

    final boolean madeDirty = !dirtyConfiguration;
    if (madeDirty)
      dirtyConfiguration = true;

    boolean executed = false;
    try {
      final RET result = database.getWrappedDatabaseInstance().recordFileChanges(callback);
      executed = true;
      saveConfiguration();
      return result;

    } finally {
      if (!executed && madeDirty)
        // ROLLBACK THE DIRTY STATUS
        dirtyConfiguration = false;
    }
  }
}
