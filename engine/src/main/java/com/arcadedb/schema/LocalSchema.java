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
import com.arcadedb.database.bucketselectionstrategy.BucketSelectionStrategy;
import com.arcadedb.database.bucketselectionstrategy.PartitionedBucketSelectionStrategy;
import com.arcadedb.database.bucketselectionstrategy.RoundRobinBucketSelectionStrategy;
import com.arcadedb.engine.Bucket;
import com.arcadedb.engine.Component;
import com.arcadedb.engine.ComponentFactory;
import com.arcadedb.engine.ComponentFile;
import com.arcadedb.engine.Dictionary;
import com.arcadedb.engine.LocalBucket;
import com.arcadedb.exception.ConfigurationException;
import com.arcadedb.exception.DatabaseMetadataException;
import com.arcadedb.exception.DatabaseOperationException;
import com.arcadedb.exception.NeedRetryException;
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
import com.arcadedb.index.vector.HnswVectorIndex;
import com.arcadedb.log.LogManager;
import com.arcadedb.security.SecurityDatabaseUser;
import com.arcadedb.security.SecurityManager;
import com.arcadedb.serializer.json.JSONArray;
import com.arcadedb.serializer.json.JSONObject;
import com.arcadedb.utility.FileUtils;

import java.io.*;
import java.time.*;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.*;
import java.util.logging.*;

/**
 * Local implementation of the database schema.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
public class LocalSchema implements Schema {
  public static final String                                 DEFAULT_ENCODING              = "UTF-8";
  public static final String                                 SCHEMA_FILE_NAME              = "schema.json";
  public static final String                                 SCHEMA_PREV_FILE_NAME         = "schema.prev.json";
  public static final String                                 CACHED_COUNT_FILE_NAME_LEGACY = "cached-count.json"; // DEPRECATED FROM v25.2.1
  public static final String                                 STATISTICS_FILE_NAME          = "statistics.json";
  public static final int                                    BUILD_TX_BATCH_SIZE           = 100_000;
  final               IndexFactory                           indexFactory                  = new IndexFactory();
  final               Map<String, LocalDocumentType>         types                         = new HashMap<>();
  private             String                                 encoding                      = DEFAULT_ENCODING;
  private final       DatabaseInternal                       database;
  private final       SecurityManager                        security;
  private final       List<Component>                        files                         = new ArrayList<>();
  private final       Map<String, LocalBucket>               bucketMap                     = new HashMap<>();
  private             Map<Integer, LocalDocumentType>        bucketId2TypeMap              = new HashMap<>();
  private             Map<Integer, LocalDocumentType>        bucketId2InvolvedTypeMap      = new HashMap<>();
  protected final     Map<String, IndexInternal>             indexMap                      = new HashMap<>();
  private final       String                                 databasePath;
  private final       File                                   configurationFile;
  private final       ComponentFactory                       componentFactory;
  private             Dictionary                             dictionary;
  private             String                                 dateFormat                    = GlobalConfiguration.DATE_FORMAT.getValueAsString();
  private             String                                 dateTimeFormat                = GlobalConfiguration.DATE_TIME_FORMAT.getValueAsString();
  private             TimeZone                               timeZone                      = TimeZone.getDefault();
  private             ZoneId                                 zoneId                        = ZoneId.systemDefault();
  private             boolean                                readingFromFile               = false;
  private             boolean                                dirtyConfiguration            = false;
  private             boolean                                loadInRamCompleted            = false;
  private             boolean                                multipleUpdate                = false;
  private final       AtomicLong                             versionSerial                 = new AtomicLong();
  private final       Map<String, FunctionLibraryDefinition> functionLibraries             = new ConcurrentHashMap<>();
  private final       Map<Integer, Integer>                  migratedFileIds               = new ConcurrentHashMap<>();

  public LocalSchema(final DatabaseInternal database, final String databasePath, final SecurityManager security) {
    this.database = database;
    this.databasePath = databasePath;
    this.security = security;

    componentFactory = new ComponentFactory(database);
    componentFactory.registerComponent(Dictionary.DICT_EXT, new Dictionary.PaginatedComponentFactoryHandler());
    componentFactory.registerComponent(LocalBucket.BUCKET_EXT, new LocalBucket.PaginatedComponentFactoryHandler());
    componentFactory.registerComponent(LSMTreeIndexMutable.UNIQUE_INDEX_EXT,
        new LSMTreeIndex.PaginatedComponentFactoryHandlerUnique());
    componentFactory.registerComponent(LSMTreeIndexMutable.NOTUNIQUE_INDEX_EXT,
        new LSMTreeIndex.PaginatedComponentFactoryHandlerNotUnique());
    componentFactory.registerComponent(LSMTreeIndexCompacted.UNIQUE_INDEX_EXT,
        new LSMTreeIndex.PaginatedComponentFactoryHandlerUnique());
    componentFactory.registerComponent(LSMTreeIndexCompacted.NOTUNIQUE_INDEX_EXT,
        new LSMTreeIndex.PaginatedComponentFactoryHandlerNotUnique());
    componentFactory.registerComponent(HnswVectorIndex.FILE_EXT, new HnswVectorIndex.PaginatedComponentFactoryHandlerUnique());

    indexFactory.register(INDEX_TYPE.LSM_TREE.name(), new LSMTreeIndex.IndexFactoryHandler());
    indexFactory.register(INDEX_TYPE.FULL_TEXT.name(), new LSMTreeFullTextIndex.IndexFactoryHandler());
    indexFactory.register(INDEX_TYPE.HNSW.name(), new HnswVectorIndex.IndexFactoryHandler());
    configurationFile = new File(databasePath + File.separator + SCHEMA_FILE_NAME);
  }

  @Override
  public LocalSchema getEmbedded() {
    return this;
  }

  public void create(final ComponentFile.MODE mode) {
    loadInRamCompleted = true;
    database.begin();
    try {
      dictionary = new Dictionary(database, "dictionary", databasePath + "/dictionary", mode, Dictionary.DEF_PAGE_SIZE);
      files.add(dictionary);

      database.commit();

    } catch (final Exception e) {
      LogManager.instance().log(this, Level.SEVERE, "Error on opening dictionary '%s' (error=%s)", e, databasePath, e.toString());
      database.rollback();
      throw new DatabaseMetadataException("Error on loading dictionary (error=" + e + ")", e);
    }
  }

  public void load(final ComponentFile.MODE mode, final boolean initialize) throws IOException {
    files.clear();
    types.clear();
    bucketMap.clear();
    indexMap.clear();
    dictionary = null;

    final Collection<ComponentFile> filesToOpen = database.getFileManager().getFiles();

    // REGISTER THE DICTIONARY FIRST
    for (final ComponentFile file : filesToOpen) {
      if (file != null)
        if (Dictionary.DICT_EXT.equals(file.getFileExtension())) {
          dictionary = (Dictionary) componentFactory.createComponent(file, mode);
          registerFile(dictionary);
          break;
        }
    }

    if (dictionary == null)
      throw new ConfigurationException("Dictionary file not found in database directory");

    for (final ComponentFile file : filesToOpen) {
      if (file != null && !Dictionary.DICT_EXT.equals(file.getFileExtension())) {
        final Component pf = componentFactory.createComponent(file, mode);

        if (pf != null) {
          final Object mainComponent = pf.getMainComponent();

          if (mainComponent instanceof LocalBucket bucket)
            bucketMap.put(pf.getName(), bucket);
          else if (mainComponent instanceof IndexInternal internal)
            indexMap.put(pf.getName(), internal);

          registerFile(pf);
        }
      }
    }

    if (initialize)
      initComponents();

    readConfiguration();

    for (final Component f : new ArrayList<>(files))
      if (f != null)
        f.onAfterSchemaLoad();

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
  public ZoneId getZoneId() {
    return zoneId;
  }

  public void setZoneId(final ZoneId zoneId) {
    this.zoneId = zoneId;
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
  public Component getFileById(final int id) {
    if (id >= files.size())
      throw new SchemaException("File with id '" + id + "' was not found");

    final Component p = files.get(id);
    if (p == null)
      throw new SchemaException("File with id '" + id + "' was not found");
    return p;
  }

  @Override
  public Component getFileByIdIfExists(final int id) {
    if (id >= files.size())
      return null;

    return files.get(id);
  }

  public void removeFile(final int fileId) {
    if (fileId >= files.size())
      return;

    database.getTransaction().removeFile(fileId);

    files.set(fileId, null);
  }

  @Override
  public Collection<? extends Bucket> getBuckets() {
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
  public LocalBucket getBucketById(final int id) {
    return getBucketById(id, true);
  }

  public LocalBucket getBucketById(final int id, final boolean throwExceptionIfNotFound) {
    if (id < 0 || id >= files.size())
      if (throwExceptionIfNotFound)
        throw new SchemaException("Bucket with id '" + id + "' was not found");
      else
        return null;

    final Component p = files.get(id);
    if (!(p instanceof LocalBucket)) {
      if (throwExceptionIfNotFound)
        throw new SchemaException("Bucket with id '" + id + "' was not found");
      else
        return null;
    }
    return (LocalBucket) p;
  }

  @Override
  public LocalBucket createBucket(final String bucketName) {
    return createBucket(bucketName, database.getConfiguration().getValueAsInteger(GlobalConfiguration.BUCKET_DEFAULT_PAGE_SIZE));
  }

  public LocalBucket createBucket(final String bucketName, final int pageSize) {
    database.checkPermissionsOnDatabase(SecurityDatabaseUser.DATABASE_ACCESS.UPDATE_SCHEMA);

    if (bucketMap.containsKey(bucketName))
      throw new SchemaException("Cannot create bucket '" + bucketName + "' because already exists");

    return recordFileChanges(() -> {
      try {
        final LocalBucket bucket = new LocalBucket(database, bucketName, databasePath + File.separator + bucketName,
            ComponentFile.MODE.READ_WRITE, pageSize, LocalBucket.CURRENT_VERSION);
        registerFile((Component) bucket);
        bucketMap.put(bucketName, bucket);

        return bucket;

      } catch (final IOException e) {
        throw new SchemaException("Cannot create bucket '" + bucketName + "' (error=" + e + ")", e);
      }
    });
  }

  public String getEncoding() {
    return encoding;
  }

  @Override
  public void setEncoding(final String encoding) {
    this.encoding = encoding;
  }

  @Override
  public DocumentType copyType(final String typeName, final String newTypeName, final Class<? extends DocumentType> newTypeClass,
      final int buckets, final int pageSize, final int transactionBatchSize) {
    database.checkPermissionsOnDatabase(SecurityDatabaseUser.DATABASE_ACCESS.UPDATE_SCHEMA);

    if (existsType(newTypeName))
      throw new IllegalArgumentException("Type '" + newTypeName + "' already exists");

    final DocumentType oldType = getType(typeName);

    DocumentType newType = null;
    try {
      // CREATE THE NEW TYPE
      if (newTypeClass == LocalVertexType.class)
        newType = buildVertexType().withName(newTypeName).withTotalBuckets(buckets).withPageSize(pageSize).create();
      else if (newTypeClass == LocalEdgeType.class)
        throw new IllegalArgumentException("Type '" + newTypeClass + "' not supported");
      else if (newTypeClass == LocalDocumentType.class)
        newType = buildDocumentType().withName(newTypeName).withTotalBuckets(buckets).withPageSize(pageSize).create();
      else
        throw new IllegalArgumentException("Type '" + newTypeClass + "' not supported");

      // COPY PROPERTIES
      for (final String propName : oldType.getPropertyNames()) {
        final Property prop = oldType.getProperty(propName);
        newType.createProperty(propName, prop.getType(), prop.getOfType());
      }

      // COPY ALL THE RECORDS
      long copied = 0;
      database.begin();
      try {
        for (final Iterator<Record> iter = database.iterateType(typeName, false); iter.hasNext(); ) {

          final Document record = (Document) iter.next();

          final MutableDocument newRecord;
          if (newType instanceof LocalVertexType)
            newRecord = database.newVertex(newTypeName);
          else
            newRecord = database.newDocument(newTypeName);

          newRecord.fromMap(record.propertiesAsMap());
          newRecord.save();

          ++copied;

          if (transactionBatchSize > 0 && copied % transactionBatchSize == 0) {
            database.commit();
            database.begin();
          }
        }

        // COPY INDEXES
        for (final Index index : oldType.getAllIndexes(false))
          newType.createTypeIndex(index.getType(), index.isUnique(),
              index.getPropertyNames().toArray(new String[index.getPropertyNames().size()]));

        database.commit();

      } finally {
        if (database.isTransactionActive())
          database.rollback();
      }

    } catch (final Exception e) {
      LogManager.instance().log(this, Level.SEVERE, "Error on renaming type '%s' into '%s'", e, typeName, newTypeName);

      if (newType != null)
        try {
          dropType(newTypeName);
        } catch (final Exception e2) {
          LogManager.instance()
              .log(this, Level.WARNING, "Error on dropping temporary type '%s' created during copyType() operation from type '%s'",
                  e2, newTypeName, typeName);
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
    for (final Index index : indexMap.values())
      indexes[i++] = index;
    return indexes;
  }

  @Override
  public void dropIndex(final String indexName) {
    database.checkPermissionsOnDatabase(SecurityDatabaseUser.DATABASE_ACCESS.UPDATE_SCHEMA);

    recordFileChanges(() -> {
      boolean setMultipleUpdate = !multipleUpdate;
      if (!multipleUpdate)
        multipleUpdate = true;

      final IndexInternal index = indexMap.get(indexName);
      if (index == null)
        return null;

      if (index.getTypeName() != null && existsType(index.getTypeName())) {
        final DocumentType type = getType(index.getTypeName());
        final BucketSelectionStrategy strategy = type.getBucketSelectionStrategy();
        if (strategy instanceof PartitionedBucketSelectionStrategy selectionStrategy) {
          if (List.of(selectionStrategy.getProperties()).equals(index.getPropertyNames()))
            // CURRENT INDEX WAS USED FOR PARTITION, SETTING DEFAULT STRATEGY
            type.setBucketSelectionStrategy(new RoundRobinBucketSelectionStrategy());
        }
      }

      try {
        database.executeLockingFiles(index.getFileIds(), () -> {
          if (index.getTypeIndex() != null)
            index.getTypeIndex().removeIndexOnBucket(index);

          index.drop();
          indexMap.remove(indexName);

          if (index.getTypeName() != null) {
            final LocalDocumentType type = getType(index.getTypeName());
            if (index instanceof TypeIndex typeIndex)
              type.removeTypeIndexInternal(typeIndex);
            else
              type.removeBucketIndexInternal(index);
          }
          return null;
        });

      } catch (final NeedRetryException e) {
        throw e;
      } catch (final Exception e) {
        throw new SchemaException("Cannot drop the index '" + indexName + "' (error=" + e + ")", e);
      } finally {
        if (setMultipleUpdate)
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
  public TypeIndexBuilder buildTypeIndex(final String typeName, final String[] propertyNames) {
    return new TypeIndexBuilder(database, typeName, propertyNames);
  }

  @Override
  public BucketIndexBuilder buildBucketIndex(final String typeName, final String bucketName, final String[] propertyNames) {
    return new BucketIndexBuilder(database, typeName, bucketName, propertyNames);
  }

  @Override
  public ManualIndexBuilder buildManualIndex(final String indexName, final Type[] keyTypes) {
    return new ManualIndexBuilder(database, indexName, keyTypes);
  }

  @Override
  public VectorIndexBuilder buildVectorIndex() {
    return new VectorIndexBuilder(database);
  }

  @Override
  public TypeIndex createTypeIndex(final INDEX_TYPE indexType, final boolean unique, final String typeName,
      final String... propertyNames) {
    return buildTypeIndex(typeName, propertyNames).withType(indexType).withUnique(unique).create();
  }

  @Override
  @Deprecated
  public TypeIndex createTypeIndex(final INDEX_TYPE indexType, final boolean unique, final String typeName,
      final String[] propertyNames, final int pageSize) {
    return buildTypeIndex(typeName, propertyNames).withType(indexType).withUnique(unique).withPageSize(pageSize).create();
  }

  @Override
  @Deprecated
  public TypeIndex createTypeIndex(final INDEX_TYPE indexType, final boolean unique, final String typeName,
      final String[] propertyNames, final int pageSize, final Index.BuildIndexCallback callback) {
    return buildTypeIndex(typeName, propertyNames).withType(indexType).withUnique(unique).withPageSize(pageSize)
        .withCallback(callback).create();
  }

  @Override
  @Deprecated
  public TypeIndex createTypeIndex(final INDEX_TYPE indexType, final boolean unique, final String typeName,
      final String[] propertyNames, final int pageSize, final LSMTreeIndexAbstract.NULL_STRATEGY nullStrategy,
      final Index.BuildIndexCallback callback) {
    return buildTypeIndex(typeName, propertyNames).withType(indexType).withUnique(unique).withPageSize(pageSize)
        .withCallback(callback).withNullStrategy(nullStrategy).create();
  }

  @Override
  public TypeIndex getOrCreateTypeIndex(final INDEX_TYPE indexType, final boolean unique, final String typeName,
      final String... propertyNames) {
    return buildTypeIndex(typeName, propertyNames).withType(indexType).withUnique(unique).withIgnoreIfExists(true).create();
  }

  @Override
  @Deprecated
  public TypeIndex getOrCreateTypeIndex(final INDEX_TYPE indexType, final boolean unique, final String typeName,
      final String[] propertyNames, final int pageSize) {
    return buildTypeIndex(typeName, propertyNames).withType(indexType).withUnique(unique).withPageSize(pageSize)
        .withIgnoreIfExists(true).create();
  }

  @Override
  @Deprecated
  public TypeIndex getOrCreateTypeIndex(final INDEX_TYPE indexType, final boolean unique, final String typeName,
      final String[] propertyNames, final int pageSize, final Index.BuildIndexCallback callback) {
    return buildTypeIndex(typeName, propertyNames).withType(indexType).withUnique(unique).withPageSize(pageSize)
        .withCallback(callback).withIgnoreIfExists(true).create();
  }

  @Override
  @Deprecated
  public TypeIndex getOrCreateTypeIndex(final INDEX_TYPE indexType, final boolean unique, final String typeName,
      final String[] propertyNames, final int pageSize, final LSMTreeIndexAbstract.NULL_STRATEGY nullStrategy,
      final Index.BuildIndexCallback callback) {
    return buildTypeIndex(typeName, propertyNames).withType(indexType).withUnique(unique).withPageSize(pageSize)
        .withNullStrategy(nullStrategy).withCallback(callback).withIgnoreIfExists(true).create();
  }

  @Override
  @Deprecated
  public Index createBucketIndex(final INDEX_TYPE indexType, final boolean unique, final String typeName, final String bucketName,
      final String[] propertyNames, final int pageSize, final LSMTreeIndexAbstract.NULL_STRATEGY nullStrategy,
      final Index.BuildIndexCallback callback) {
    return buildBucketIndex(typeName, bucketName, propertyNames).withType(indexType).withUnique(unique).withPageSize(pageSize)
        .withNullStrategy(nullStrategy).withCallback(callback).create();
  }

  @Override
  @Deprecated
  public Index createManualIndex(final INDEX_TYPE indexType, final boolean unique, final String indexName, final Type[] keyTypes,
      final int pageSize, final LSMTreeIndexAbstract.NULL_STRATEGY nullStrategy) {
    return buildManualIndex(indexName, keyTypes).withUnique(unique).withPageSize(pageSize).withNullStrategy(nullStrategy).create();
  }

  public void close() {
    writeStatisticsFile();
    files.clear();
    types.clear();
    bucketMap.clear();
    indexMap.clear();
    dictionary = null;
    bucketId2TypeMap.clear();
  }

  private void readStatisticsFile() {
    try {
      boolean legacyFile = false;
      File file = new File(databasePath + File.separator + STATISTICS_FILE_NAME);
      if (!file.exists() || file.length() == 0) {
        // TRY LEGACY FILE (<v25.2.1)
        file = new File(databasePath + File.separator + CACHED_COUNT_FILE_NAME_LEGACY);
        if (!file.exists() || file.length() == 0)
          return;

        legacyFile = true;
      }

      final JSONObject json;
      try (final FileInputStream fis = new FileInputStream(file)) {
        final String fileContent = FileUtils.readStreamAsString(fis, encoding);
        json = new JSONObject(fileContent);
      }

      for (String key : json.keySet()) {
        final LocalBucket bucket = bucketMap.get(key);
        if (bucket != null) {
          if (legacyFile) {
            bucket.setCachedRecordCount(json.getLong(key));
          } else {
            final JSONObject obj = json.getJSONObject(key);
            if (!obj.isNull("count"))
              bucket.setCachedRecordCount(obj.getLong("count"));
            if (!obj.isNull("pages"))
              bucket.setPageStatistics(obj.getJSONArray("pages"));
          }
        }
      }

    } catch (Throwable e) {
      LogManager.instance().log(this, Level.WARNING, "Error on reading cached count file", e);
    }
  }

  private void writeStatisticsFile() {
    final File directory = new File(databasePath);
    if (!directory.exists())
      // DATABASE DIRECTORY WAS DELETED
      return;

    try {
      final JSONObject json = new JSONObject();
      for (Map.Entry<String, LocalBucket> b : bucketMap.entrySet())
        json.put(b.getKey(), b.getValue().getStatistics());

      try (final FileWriter file = new FileWriter(new File(directory, STATISTICS_FILE_NAME))) {
        file.write(json.toString());
      }
    } catch (Throwable e) {
      LogManager.instance().log(this, Level.WARNING, "Error on saving statistics file", e);
    }
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

  public LocalDocumentType getType(final String typeName) {
    final LocalDocumentType t = types.get(typeName);
    if (t == null)
      throw new SchemaException("Type with name '" + typeName + "' was not found");
    return t;
  }

  @Override
  public String getTypeNameByBucketId(final int bucketId) {
    final DocumentType type = getTypeByBucketId(bucketId);
    return type != null ? type.getName() : null;
  }

  @Override
  public DocumentType getTypeByBucketId(final int bucketId) {
    return bucketId2TypeMap.get(bucketId);
  }

  @Override
  public DocumentType getInvolvedTypeByBucketId(final int bucketId) {
    return bucketId2InvolvedTypeMap.get(bucketId);
  }

  @Override
  public DocumentType getTypeByBucketName(final String bucketName) {
    return bucketId2TypeMap.get(getBucketByName(bucketName).getFileId());
  }

  public boolean existsType(final String typeName) {
    return types.containsKey(typeName);
  }

  public void dropType(final String typeName) {
    database.checkPermissionsOnDatabase(SecurityDatabaseUser.DATABASE_ACCESS.UPDATE_SCHEMA);

    recordFileChanges(() -> {
      multipleUpdate = true;
      try {
        final LocalDocumentType type = (LocalDocumentType) database.getSchema().getType(typeName);

        // CHECK INHERITANCE TREE AND ATTACH SUB-TYPES DIRECTLY TO THE PARENT TYPE
        final List<LocalDocumentType> superTypes = new ArrayList<>(type.superTypes);
        for (final LocalDocumentType parent : superTypes)
          type.removeSuperType(parent);

        for (final LocalDocumentType sub : type.subTypes) {
          sub.superTypes.remove(type);
          for (final LocalDocumentType parent : superTypes)
            sub.addSuperType(parent, false);
        }

        // DELETE ALL ASSOCIATED INDEXES
        for (final Index m : new ArrayList<>(type.getAllIndexes(true)))
          dropIndex(m.getName());

        if (type instanceof LocalVertexType vertexType)
          // DELETE IN/OUT EDGE FILES
          database.getGraphEngine().dropVertexType(vertexType);

        // DELETE ALL ASSOCIATED BUCKETS
        final List<Bucket> buckets = new ArrayList<>(type.getBuckets(false));
        for (final Bucket b : buckets) {
          type.removeBucket(b);
          dropBucket(b.getName());
        }

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
      for (final LocalDocumentType type : types.values()) {
        if (type.buckets.contains(bucket))
          throw new SchemaException(
              "Error on dropping bucket '" + bucketName + "' because it is assigned to type '" + type.getName()
                  + "'. Remove the association first");
      }

      database.getPageManager().deleteFile(database, bucket.getFileId());
      try {
        database.getFileManager().dropFile(bucket.getFileId());
      } catch (final IOException e) {
        LogManager.instance().log(this, Level.SEVERE, "Error on deleting bucket '%s'", e, bucketName);
      }
      removeFile(bucket.getFileId());

      bucketMap.remove(bucketName);

      for (final Index idx : new ArrayList<>(indexMap.values())) {
        if (idx.getAssociatedBucketId() == bucket.getFileId())
          dropIndex(idx.getName());
      }

      saveConfiguration();
      return null;
    });
  }

  @Override
  public DocumentType createDocumentType(final String typeName) {
    return buildDocumentType().withName(typeName).create();
  }

  public DocumentType createDocumentType(final String typeName, final int buckets) {
    return buildDocumentType().withName(typeName).withTotalBuckets(buckets).create();
  }

  @Override
  public DocumentType createDocumentType(final String typeName, final int buckets, final int pageSize) {
    return buildDocumentType().withName(typeName).withTotalBuckets(buckets).withPageSize(pageSize).create();
  }

  @Override
  public DocumentType createDocumentType(final String typeName, final List<Bucket> buckets) {
    return buildDocumentType().withName(typeName).withBuckets(buckets).create();
  }

  @Override
  public DocumentType createDocumentType(final String typeName, final List<Bucket> buckets, final int pageSize) {
    return buildDocumentType().withName(typeName).withBuckets(buckets).withPageSize(pageSize).create();
  }

  @Override
  public DocumentType getOrCreateDocumentType(final String typeName) {
    return buildDocumentType().withName(typeName).withIgnoreIfExists(true).create();
  }

  @Override
  public DocumentType getOrCreateDocumentType(final String typeName, final int buckets) {
    return buildDocumentType().withName(typeName).withTotalBuckets(buckets).withIgnoreIfExists(true).create();
  }

  @Override
  public DocumentType getOrCreateDocumentType(final String typeName, final int buckets, final int pageSize) {
    return buildDocumentType().withName(typeName).withTotalBuckets(buckets).withPageSize(pageSize).withIgnoreIfExists(true)
        .create();
  }

  @Override
  public TypeBuilder<LocalDocumentType> buildDocumentType() {
    return new TypeBuilder<>(database, LocalDocumentType.class);
  }

  @Override
  public VertexType createVertexType(final String typeName) {
    return buildVertexType().withName(typeName).create();
  }

  @Override
  public VertexType createVertexType(final String typeName, final int buckets) {
    return buildVertexType().withName(typeName).withTotalBuckets(buckets).create();
  }

  @Override
  public VertexType createVertexType(final String typeName, final List<Bucket> bucketInstances) {
    return buildVertexType().withName(typeName).withBuckets(bucketInstances).create();
  }

  @Override
  public VertexType createVertexType(final String typeName, final int buckets, final int pageSize) {
    return buildVertexType().withName(typeName).withTotalBuckets(buckets).withPageSize(pageSize).create();
  }

  @Override
  public VertexType createVertexType(final String typeName, final List<Bucket> bucketInstances, final int pageSize) {
    return buildVertexType().withName(typeName).withBuckets(bucketInstances).withPageSize(pageSize).create();
  }

  @Override
  public VertexType getOrCreateVertexType(final String typeName) {
    return buildVertexType().withName(typeName).withIgnoreIfExists(true).create();
  }

  @Override
  public VertexType getOrCreateVertexType(final String typeName, final int buckets) {
    return buildVertexType().withName(typeName).withTotalBuckets(buckets).withIgnoreIfExists(true).create();
  }

  @Override
  public VertexType getOrCreateVertexType(final String typeName, final int buckets, final int pageSize) {
    return buildVertexType().withName(typeName).withTotalBuckets(buckets).withPageSize(pageSize).withIgnoreIfExists(true).create();
  }

  @Override
  public TypeBuilder<VertexType> buildVertexType() {
    return new TypeBuilder<>(database, VertexType.class);
  }

  @Override
  public EdgeType createEdgeType(final String typeName) {
    return buildEdgeType().withName(typeName).create();
  }

  @Override
  public EdgeType createEdgeType(final String typeName, final int buckets) {
    return buildEdgeType().withName(typeName).withTotalBuckets(buckets).create();
  }

  @Override
  public EdgeType createEdgeType(final String typeName, final int buckets, final int pageSize) {
    return buildEdgeType().withName(typeName).withTotalBuckets(buckets).withPageSize(pageSize).create();
  }

  @Override
  public EdgeType createEdgeType(final String typeName, final List<Bucket> buckets) {
    return buildEdgeType().withName(typeName).withBuckets(buckets).create();
  }

  @Override
  public EdgeType createEdgeType(final String typeName, final List<Bucket> buckets, final int pageSize) {
    return buildEdgeType().withName(typeName).withBuckets(buckets).withPageSize(pageSize).create();
  }

  @Override
  public EdgeType getOrCreateEdgeType(final String typeName) {
    return buildEdgeType().withName(typeName).withIgnoreIfExists(true).create();
  }

  @Override
  public EdgeType getOrCreateEdgeType(final String typeName, final int buckets) {
    return buildEdgeType().withName(typeName).withTotalBuckets(buckets).withIgnoreIfExists(true).create();
  }

  @Override
  public EdgeType getOrCreateEdgeType(final String typeName, final int buckets, final int pageSize) {
    return buildEdgeType().withName(typeName).withTotalBuckets(buckets).withPageSize(pageSize).withIgnoreIfExists(true).create();
  }

  @Override
  public TypeBuilder<EdgeType> buildEdgeType() {
    return new TypeBuilder<>(database, EdgeType.class);
  }

  protected synchronized void readConfiguration() {
    types.clear();

    loadInRamCompleted = false;
    readingFromFile = true;

    boolean saveConfiguration = false;
    try {
      File file = new File(databasePath + File.separator + SCHEMA_FILE_NAME);
      if (!file.exists() || file.length() == 0) {
        file = new File(databasePath + File.separator + SCHEMA_PREV_FILE_NAME);
        if (!file.exists())
          return;

        LogManager.instance().log(this, Level.WARNING, "Could not find schema file, loading the previous version saved");
      }

      final JSONObject root;
      try (final FileInputStream fis = new FileInputStream(file)) {
        final String fileContent = FileUtils.readStreamAsString(fis, encoding);
        root = new JSONObject(fileContent);
      }

      if (root.names() == null || root.names().isEmpty())
        // EMPTY SCHEMA
        return;

      versionSerial.set(root.has("schemaVersion") ? root.getLong("schemaVersion") : 0L);

      final JSONObject settings = root.getJSONObject("settings");

      if (settings.has("timeZone")) {
        timeZone = TimeZone.getTimeZone(settings.getString("timeZone"));
        zoneId = timeZone.toZoneId();
      } else if (settings.has("zoneId")) {
        zoneId = ZoneId.of(settings.getString("zoneId"));
        timeZone = TimeZone.getTimeZone(zoneId);
      }

      dateFormat = settings.getString("dateFormat");
      dateTimeFormat = settings.getString("dateTimeFormat");

      final JSONObject types = root.getJSONObject("types");

      final Map<String, String[]> parentTypes = new HashMap<>();

      final Map<String, JSONObject> orphanIndexes = new HashMap<>();

      for (final String typeName : types.keySet()) {
        final JSONObject schemaType = types.getJSONObject(typeName);

        final LocalDocumentType type;

        final String kind = (String) schemaType.get("type");
        type = switch (kind) {
          case "v" -> new LocalVertexType(this, typeName);
          case "e" -> new LocalEdgeType(this, typeName,
              !schemaType.has("bidirectional") || schemaType.getBoolean("bidirectional"));
          case "d" -> new LocalDocumentType(this, typeName);
          case null, default -> throw new ConfigurationException("Type '" + kind + "' is not supported");
        };

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
            final Bucket bucket = bucketMap.get(schemaBucket.getString(i));
            if (bucket == null) {
              LogManager.instance()
                  .log(this, Level.WARNING, "Cannot find bucket '%s' for type '%s', removing it from type configuration", null,
                      schemaBucket.getString(i), type);

              // GO BACK
              schemaBucket.remove(i);
              --i;

              saveConfiguration = true;
            } else
              type.addBucketInternal(bucket);
          }
        }

        type.custom.clear();
        if (schemaType.has("custom"))
          type.custom.putAll(schemaType.getJSONObject("custom").toMap());
      }

      // CREATE THE PROPERTIES AFTER ALL THE TYPES HAVE BEEN CREATED TO FIND ALL THE REFERENCES LINKED WITH `TO`
      for (final String typeName : types.keySet()) {
        final JSONObject schemaType = types.getJSONObject(typeName);
        final LocalDocumentType type = getType(typeName);
        if (schemaType.has("properties")) {
          final JSONObject schemaProperties = schemaType.getJSONObject("properties");
          if (schemaProperties != null) {
            for (final String propName : schemaProperties.keySet()) {
              final JSONObject prop = schemaProperties.getJSONObject(propName);
              type.createProperty(propName, prop);
            }
          }
        }
      }

      // RESTORE THE INHERITANCE
      for (final Map.Entry<String, String[]> entry : parentTypes.entrySet()) {
        final LocalDocumentType type = getType(entry.getKey());
        for (final String p : entry.getValue())
          type.addSuperType(getType(p), false);
      }

      // PARSE INDEXES
      for (final String typeName : types.keySet()) {
        final JSONObject schemaType = types.getJSONObject(typeName);
        final JSONObject typeIndexesJSON = schemaType.getJSONObject("indexes");
        if (typeIndexesJSON != null) {
          final LocalDocumentType type = getType(typeName);

          final List<String> orderedIndexes = new ArrayList<>(typeIndexesJSON.keySet());
          orderedIndexes.sort(Comparator.naturalOrder());

          for (final String indexName : orderedIndexes) {
            final JSONObject indexJSON = typeIndexesJSON.getJSONObject(indexName);

            final JSONArray schemaIndexProperties = indexJSON.getJSONArray("properties");
            final String[] properties = new String[schemaIndexProperties.length()];
            for (int i = 0; i < properties.length; ++i)
              properties[i] = schemaIndexProperties.getString(i);

            IndexInternal index = indexMap.get(indexName);
            if (index != null) {
              final LSMTreeIndexAbstract.NULL_STRATEGY nullStrategy = indexJSON.has("nullStrategy") ?
                  LSMTreeIndexAbstract.NULL_STRATEGY.valueOf(indexJSON.getString("nullStrategy")) :
                  LSMTreeIndexAbstract.NULL_STRATEGY.ERROR;

              index.setNullStrategy(nullStrategy);

              if (indexJSON.has("type")) {
                final String configuredIndexType = indexJSON.getString("type");

                if (!index.getType().toString().equals(configuredIndexType)) {
                  if (configuredIndexType.equalsIgnoreCase(Schema.INDEX_TYPE.FULL_TEXT.toString())) {
                    index = new LSMTreeFullTextIndex((LSMTreeIndex) index);
                    indexMap.put(indexName, index);
                  } else {
                    orphanIndexes.put(indexName, indexJSON);
                    indexJSON.put("type", typeName);
                    LogManager.instance()
                        .log(this, Level.WARNING, "Index '%s' of type %s is different from definition %s. Ignoring it",//
                            index.getName(), index.getType(), configuredIndexType);
                    continue;
                  }
                }
              }

              final String bucketName = indexJSON.getString("bucket");
              final Bucket bucket = bucketMap.get(bucketName);
              if (bucket == null) {
                orphanIndexes.put(indexName, indexJSON);
                indexJSON.put("type", typeName);
                LogManager.instance()
                    .log(this, Level.WARNING, "Cannot find bucket '%s' defined in index '%s'. Ignoring it", null, bucketName,
                        index.getName());
              } else
                type.addIndexInternal(index, bucket.getFileId(), properties, null);

            } else {
              orphanIndexes.put(indexName, indexJSON);
              indexJSON.put("type", typeName);
              LogManager.instance()
                  .log(this, Level.WARNING, "Cannot find index '%s' defined in type '%s'. Ignoring it", null, indexName, type);
            }
          }
        }
      }

      // ASSOCIATE ORPHAN INDEXES
      boolean completed = false;
      while (!completed) {
        completed = true;
        for (final IndexInternal index : indexMap.values()) {
          if (index.getTypeName() == null) {
            final String indexName = index.getName();

            final int pos = indexName.lastIndexOf("_");
            final String bucketName = indexName.substring(0, pos);
            final Bucket bucket = bucketMap.get(bucketName);
            if (bucket != null) {
              for (final Map.Entry<String, JSONObject> entry : orphanIndexes.entrySet()) {
                final int pos2 = entry.getKey().lastIndexOf("_");
                final String bucketNameIndex = entry.getKey().substring(0, pos2);

                if (bucketName.equals(bucketNameIndex)) {
                  final LocalDocumentType type = this.types.get(entry.getValue().getString("type"));
                  if (type != null) {
                    final JSONArray schemaIndexProperties = entry.getValue().getJSONArray("properties");

                    final String[] properties = new String[schemaIndexProperties.length()];
                    for (int i = 0; i < properties.length; ++i)
                      properties[i] = schemaIndexProperties.getString(i);

                    final LSMTreeIndexAbstract.NULL_STRATEGY nullStrategy = entry.getValue().has("nullStrategy") ?
                        LSMTreeIndexAbstract.NULL_STRATEGY.valueOf(entry.getValue().getString("nullStrategy")) :
                        LSMTreeIndexAbstract.NULL_STRATEGY.ERROR;

                    index.setNullStrategy(nullStrategy);
                    type.addIndexInternal(index, bucket.getFileId(), properties, null);
                    LogManager.instance()
                        .log(this, Level.WARNING, "Relinked orphan index '%s' to type '%s'", null, indexName, type.getName());
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

      // SET THE BUCKET STRATEGY AFTER THE INDEXES BECAUSE SOME OF THEM REQUIRE INDEXES (LIKE THE PARTITIONED)
      for (final String typeName : types.keySet()) {
        final JSONObject schemaType = types.getJSONObject(typeName);
        if (schemaType.has("bucketSelectionStrategy")) {
          final JSONObject bucketSelectionStrategy = schemaType.getJSONObject("bucketSelectionStrategy");

          final Object[] properties = bucketSelectionStrategy.has("properties") ?
              bucketSelectionStrategy.getJSONArray("properties").toList().toArray() :
              new Object[0];

          final DocumentType type = getType(typeName);
          type.setBucketSelectionStrategy(bucketSelectionStrategy.getString("name"), properties);
        }
      }

      if (saveConfiguration)
        saveConfiguration();

    } catch (final Exception e) {
      LogManager.instance().log(this, Level.SEVERE, "Error on loading schema. The schema will be reset", e);
    } finally {
      readingFromFile = false;
      loadInRamCompleted = true;

      if (dirtyConfiguration)
        saveConfiguration();

      rebuildBucketTypeMap();
      readStatisticsFile();
    }
  }

  public synchronized void saveConfiguration() {
    rebuildBucketTypeMap();

    if (readingFromFile || !loadInRamCompleted || multipleUpdate || database.isTransactionActive()) {
      // POSTPONE THE SAVING
      dirtyConfiguration = true;
      return;
    }

    try {
      versionSerial.incrementAndGet();

      update(toJSON());

      dirtyConfiguration = false;

    } catch (final IOException e) {
      LogManager.instance().log(this, Level.SEVERE, "Error on saving schema configuration to file: %s", e,
          databasePath + File.separator + SCHEMA_FILE_NAME);
    }
  }

  public synchronized JSONObject toJSON() {
    final JSONObject root = new JSONObject();
    root.put("schemaVersion", versionSerial.get());
    root.put("dbmsVersion", Constants.getRawVersion());
    root.put("dbmsBuild", Constants.getBuildNumber());

    final JSONObject settings = new JSONObject();
    root.put("settings", settings);

    settings.put("zoneId", zoneId.getId());
    settings.put("dateFormat", dateFormat);
    settings.put("dateTimeFormat", dateTimeFormat);

    final JSONObject types = new JSONObject();
    root.put("types", types);

    for (final DocumentType t : this.types.values())
      types.put(t.getName(), t.toJSON());

    return root;
  }

  public void registerFile(final Component file) {
    final int fileId = file.getFileId();

    while (files.size() < fileId + 1)
      files.add(null);

    if (files.get(fileId) != null)
      throw new SchemaException(
          "File with id '" + fileId + "' already exists (previous=" + files.get(fileId) + " new=" + file + ")");

    files.set(fileId, file);
  }

  public void initComponents() {
    for (final Component f : files)
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

  @Override
  public boolean hasFunctionLibrary(final String name) {
    return functionLibraries.containsKey(name);
  }

  public FunctionLibraryDefinition getFunctionLibrary(final String name) {
    final FunctionLibraryDefinition flib = functionLibraries.get(name);
    if (flib == null)
      throw new IllegalArgumentException("Function library '" + name + "' not defined");
    return flib;
  }

  @Override
  public FunctionDefinition getFunction(final String libraryName, final String functionName) throws IllegalArgumentException {
    return getFunctionLibrary(libraryName).getFunction(functionName);
  }

  public void setMigratedFileId(final int oldFileId, final int newFileId) {
    LogManager.instance().log(this, Level.FINE, "Migrating file id %d to %d", null, oldFileId, newFileId);
    migratedFileIds.put(oldFileId, newFileId);
  }

  public Integer getMigratedFileId(final int oldFileId) {
    return migratedFileIds.get(oldFileId);
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
      final File copy = new File(databasePath + File.separator + SCHEMA_PREV_FILE_NAME);
      if (copy.exists())
        if (!copy.delete())
          LogManager.instance().log(this, Level.WARNING, "Error on deleting previous schema file '%s'", null, copy);

      if (!configurationFile.renameTo(copy))
        LogManager.instance().log(this, Level.WARNING, "Error on renaming previous schema file '%s'", null, copy);
    }

    try (final FileWriter file = new FileWriter(databasePath + File.separator + SCHEMA_FILE_NAME)) {
      file.write(latestSchema);
    }

    database.getExecutionPlanCache().invalidate();
  }

  protected <RET> RET recordFileChanges(final Callable<Object> callback) {
    if (readingFromFile || !loadInRamCompleted) {
      try {
        return (RET) callback.call();
      } catch (final Exception e) {
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

      // INVALIDATE EXECUTION PLAN IN CASE TYPE OR INDEX CONCUR IN THE GENERATED PLANS
      database.getExecutionPlanCache().invalidate();

      return result;

    } finally {
      if (!executed && madeDirty)
        // ROLLBACK THE DIRTY STATUS
        dirtyConfiguration = false;
    }
  }

  protected Index createBucketIndex(final LocalDocumentType type, final Type[] keyTypes, final Bucket bucket, final String typeName,
      final INDEX_TYPE indexType, final boolean unique, final int pageSize, final LSMTreeIndexAbstract.NULL_STRATEGY nullStrategy,
      final Index.BuildIndexCallback callback, final String[] propertyNames, final TypeIndex propIndex, final int batchSize) {
    database.checkPermissionsOnDatabase(SecurityDatabaseUser.DATABASE_ACCESS.UPDATE_SCHEMA);

    if (bucket == null)
      throw new IllegalArgumentException("bucket is null");

    final String indexName = bucket.getName() + "_" + System.nanoTime();

    if (indexMap.containsKey(indexName))
      throw new DatabaseMetadataException(
          "Cannot create index '" + indexName + "' on type '" + typeName + "' because it already exists");

    final IndexBuilder<Index> builder = buildBucketIndex(typeName, bucket.getName(), propertyNames).withUnique(unique)
        .withType(indexType).withFilePath(databasePath + File.separator + indexName).withKeyTypes(keyTypes).withPageSize(pageSize)
        .withNullStrategy(nullStrategy).withCallback(callback).withIndexName(indexName);

    final IndexInternal index = indexFactory.createIndex(builder);

    try {
      registerFile(index.getComponent());

      indexMap.put(indexName, index);

      type.addIndexInternal(index, bucket.getFileId(), propertyNames, propIndex);
      index.build(batchSize, callback);

      return index;

    } catch (final NeedRetryException e) {
      dropIndex(indexName);
      throw e;
    } catch (final Exception e) {
      dropIndex(indexName);
      throw new IndexException("Error on creating index '" + indexName + "'", e);
    }
  }

  protected boolean isSchemaLoaded() {
    return loadInRamCompleted;
  }

  protected void updateSecurity() {
    if (security != null)
      security.updateSchema(database);
  }

  /**
   * Replaces the map to allow concurrent usage while rebuilding the map.
   */
  private void rebuildBucketTypeMap() {
    final Map<Integer, LocalDocumentType> newBucketId2TypeMap = new HashMap<>();
    for (final LocalDocumentType t : types.values()) {
      for (final Bucket b : t.getBuckets(false))
        newBucketId2TypeMap.put(b.getFileId(), t);
    }
    bucketId2TypeMap = newBucketId2TypeMap;

    // COMPUTE INVOLVED BUCKETS FOR SECURITY
    final Map<Integer, LocalDocumentType> newBucketId2InvolvedTypeMap = new HashMap<>();
    for (final LocalDocumentType t : types.values()) {
      for (final Bucket b : t.getInvolvedBuckets())
        newBucketId2InvolvedTypeMap.put(b.getFileId(), t);
    }
    bucketId2InvolvedTypeMap = newBucketId2InvolvedTypeMap;
  }
}
