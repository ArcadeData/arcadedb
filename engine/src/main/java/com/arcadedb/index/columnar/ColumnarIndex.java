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
package com.arcadedb.index.columnar;

import com.arcadedb.database.DatabaseInternal;
import com.arcadedb.database.Identifiable;
import com.arcadedb.database.LocalDatabase;
import com.arcadedb.database.RID;
import com.arcadedb.engine.ComponentFile;
import com.arcadedb.engine.PaginatedComponent;
import com.arcadedb.index.IndexCursor;
import com.arcadedb.index.IndexException;
import com.arcadedb.index.IndexInternal;
import com.arcadedb.index.TypeIndex;
import com.arcadedb.index.lsm.LSMTreeIndex;
import com.arcadedb.index.lsm.LSMTreeIndexAbstract;
import com.arcadedb.schema.ColumnarIndexMetadata;
import com.arcadedb.schema.IndexMetadata;
import com.arcadedb.schema.Schema;
import com.arcadedb.schema.Type;
import com.arcadedb.serializer.json.JSONObject;
import com.arcadedb.utility.DateUtils;

import java.time.temporal.ChronoUnit;
import java.util.List;
import java.util.Map;

/**
 * The schema face of one bucket's column of a COLUMNAR index. It holds no entries: an empty LSM-Tree shell gives it a file id, a
 * name and the {@link IndexInternal} scaffolding, while the values live in the bucket's columnar projection, which the database's
 * {@link ColumnarIndexManager} fills after each commit. That is why no write reaches {@link #put} or {@link #remove} (the indexer
 * and the graph batch skip a type that is not {@link Schema.INDEX_TYPE#isMaintainedInTransaction() maintained in transaction}), why
 * a key lookup is refused, and why {@link #close()} never touches the projection.
 */
public final class ColumnarIndex implements IndexInternal {
  /** The modifier a property name carries when a list is indexed per item. */
  public static final String BY_ITEM_SUFFIX = " by item";

  private final    LSMTreeIndex          shell;
  private volatile ColumnarIndexMetadata metadata;
  private volatile ColumnDefinition      column;
  private          TypeIndex             typeIndex;

  /**
   * Load time: wraps the shell the schema loaded from its file, with the settings read back from the schema. The caller registers
   * the index with the {@link ColumnarIndexManager}.
   */
  public ColumnarIndex(final LSMTreeIndex shell, final ColumnarIndexMetadata metadata) {
    this.shell = shell;
    this.metadata = metadata;
  }

  /** Creation time: allocates the shell, as the sparse vector index does. The factory registers the index with the manager. */
  ColumnarIndex(final DatabaseInternal database, final String name, final String filePath, final ComponentFile.MODE mode,
      final int pageSize, final Type[] keyTypes, final ColumnarIndexMetadata metadata) {
    this.shell = new LSMTreeIndex(database, name, false, filePath, mode, keyTypes, pageSize, LSMTreeIndexAbstract.NULL_STRATEGY.SKIP);
    this.metadata = metadata;
  }

  /**
   * The column this index projects, keyed by this index's file id. A list indexed BY ITEM is a LIST column (the BY ITEM modifier is
   * accepted on a LIST only); any other column has the type of its key.
   */
  public ColumnDefinition columnDefinition() {
    ColumnDefinition current = column;
    if (current == null) {
      final String indexProperty = getPropertyNames().getFirst();
      final boolean byItem = indexProperty.endsWith(BY_ITEM_SUFFIX);
      final String property = byItem ? indexProperty.substring(0, indexProperty.length() - BY_ITEM_SUFFIX.length()) : indexProperty;
      current = defineColumn(getFileId(), property, byItem ? Type.LIST : shell.getKeyTypes()[0], byItem);
      column = current;
    }
    return current;
  }

  /**
   * The column of a property of the given type.
   *
   * @throws IndexException when the projection cannot hold the type, see {@link ColumnKind#of(Type, boolean)}
   */
  public static ColumnDefinition defineColumn(final int indexFileId, final String property, final Type propertyType,
      final boolean byItem) {
    final ColumnKind kind = ColumnKind.of(propertyType, byItem);
    final ChronoUnit precision = switch (propertyType) {
      case DATE -> ChronoUnit.DAYS;
      case DATETIME, DATETIME_SECOND, DATETIME_MICROS, DATETIME_NANOS -> DateUtils.getPrecisionFromType(propertyType);
      default -> null;
    };
    return new ColumnDefinition(indexFileId, property, propertyType, kind, byItem, precision);
  }

  public ColumnarIndexMetadata getColumnarMetadata() {
    return metadata;
  }

  @Override
  public Schema.INDEX_TYPE getType() {
    return Schema.INDEX_TYPE.COLUMNAR;
  }

  @Override
  public IndexCursor get(final Object[] keys) {
    return get(keys, -1);
  }

  @Override
  public IndexCursor get(final Object[] keys, final int limit) {
    throw new IndexException("COLUMNAR indexes answer facets, not key lookups (index '" + getName() + "')");
  }

  /** No-op (counted in {@link ColumnarTestHooks#INDEX_WRITES}): the projection is filled after commit, never by a write. */
  @Override
  public void put(final Object[] keys, final RID[] rid) {
    ColumnarTestHooks.INDEX_WRITES.incrementAndGet();
  }

  /** No-op, see {@link #put}. */
  @Override
  public void remove(final Object[] keys) {
    ColumnarTestHooks.INDEX_WRITES.incrementAndGet();
  }

  /** No-op, see {@link #put}. */
  @Override
  public void remove(final Object[] keys, final Identifiable rid) {
    ColumnarTestHooks.INDEX_WRITES.incrementAndGet();
  }

  /** No-op: a replayed transaction staged nothing for this index. */
  @Override
  public void putReplay(final Object[] keys, final RID[] rids) {
    ColumnarTestHooks.INDEX_WRITES.incrementAndGet();
  }

  /** No-op, see {@link #putReplay}. */
  @Override
  public void removeReplay(final Object[] keys, final Identifiable rid) {
    ColumnarTestHooks.INDEX_WRITES.incrementAndGet();
  }

  /**
   * Scans nothing here, except an empty bucket's (no record, but a bucket emptied by deletes still has pages to walk): the projection
   * builds the column in the background, or publishes it at once on an empty bucket whose usable projection is free
   * ({@link ColumnarRebuilder#createInlineIfEmpty}). The schema has
   * bound this index to its bucket by now, so this opens the projection the factory could not open yet and schedules its
   * {@code create} rebuild (R17, coalesced with the other indexes created meanwhile) unless the projection already published this
   * column in a valid state (an empty bucket is published with its columns at open, V-B1), both under the manager's registry lock
   * ({@link ColumnarIndexManager#buildRegistered}), asks the manager to reconcile its columns, and indexes 0 records.
   */
  @Override
  public long build(final int buildIndexBatchSize, final boolean sharesCallerTransaction, final BuildIndexCallback callback) {
    manager().buildRegistered(this);
    return 0;
  }

  @Override
  public void drop() {
    final ColumnarIndexManager manager = manager();
    shell.drop();
    manager.unregister(this);
    manager.requestReconcile();
  }

  /** Closes the shell only: the projection outlives a schema reload and is closed with the database. */
  @Override
  public void close() {
    shell.close();
  }

  /** Shell only, see {@link #close()}. */
  @Override
  public void onSuperseded() {
    shell.onSuperseded();
  }

  /** Whether the projection of this index holds a valid state to answer facets from, see {@link ColumnarIndexManager#isUsable}. */
  @Override
  public boolean isUsable() {
    return manager().isUsable(this);
  }

  @Override
  public long countEntries() {
    return shell.countEntries();
  }

  @Override
  public boolean compact() {
    return false;
  }

  @Override
  public boolean isCompacting() {
    return false;
  }

  @Override
  public boolean scheduleCompaction() {
    return false;
  }

  @Override
  public IndexMetadata getMetadata() {
    return shell.getMetadata();
  }

  /** The columnar settings live here, not on the shell, so a new file is created from this definition (issue #5723). */
  @Override
  public IndexMetadata getMetadataForNewFile() {
    return metadata;
  }

  @Override
  public void setMetadata(final IndexMetadata metadata) {
    shell.setMetadata(metadata);
  }

  @Override
  public void setMetadata(final JSONObject indexJSON) {
    shell.setMetadata(indexJSON);
    final IndexMetadata base = shell.getMetadata();
    final ColumnarIndexMetadata loaded = new ColumnarIndexMetadata(base.typeName, base.propertyNames.toArray(new String[0]),
        base.associatedBucketId);
    loaded.fromJSON(indexJSON);
    loaded.inheritCommonSettingsFrom(base);
    this.metadata = loaded;
  }

  @Override
  public boolean setStatus(final INDEX_STATUS[] expectedStatuses, final INDEX_STATUS newStatus) {
    return shell.setStatus(expectedStatuses, newStatus);
  }

  @Override
  public String getMostRecentFileName() {
    return shell.getMostRecentFileName();
  }

  @Override
  public String getTypeName() {
    return shell.getTypeName();
  }

  @Override
  public List<String> getPropertyNames() {
    return shell.getPropertyNames();
  }

  @Override
  public String getName() {
    return shell.getName();
  }

  @Override
  public Map<String, Long> getStats() {
    return shell.getStats();
  }

  @Override
  public LSMTreeIndexAbstract.NULL_STRATEGY getNullStrategy() {
    return shell.getNullStrategy();
  }

  @Override
  public void setNullStrategy(final LSMTreeIndexAbstract.NULL_STRATEGY nullStrategy) {
    shell.setNullStrategy(nullStrategy);
  }

  @Override
  public int getFileId() {
    return shell.getFileId();
  }

  @Override
  public boolean isUnique() {
    return false;
  }

  @Override
  public PaginatedComponent getComponent() {
    return shell.getComponent();
  }

  @Override
  public Type[] getKeyTypes() {
    return shell.getKeyTypes();
  }

  @Override
  public byte[] getBinaryKeyTypes() {
    return shell.getBinaryKeyTypes();
  }

  @Override
  public List<Integer> getFileIds() {
    return shell.getFileIds();
  }

  @Override
  public int getAssociatedBucketId() {
    final int bucketId = shell.getAssociatedBucketId();
    // With no bucket associated, the index's own file id, so the transaction locking never locks file -1 (as LSMTreeGeoIndex)
    return bucketId >= 0 ? bucketId : shell.getFileId();
  }

  @Override
  public boolean supportsOrderedIterations() {
    return false;
  }

  @Override
  public boolean isAutomatic() {
    return shell.isAutomatic();
  }

  @Override
  public int getPageSize() {
    return shell.getPageSize();
  }

  @Override
  public void setTypeIndex(final TypeIndex typeIndex) {
    this.typeIndex = typeIndex;
  }

  @Override
  public TypeIndex getTypeIndex() {
    return typeIndex;
  }

  @Override
  public boolean isValid() {
    return shell.isValid();
  }

  @Override
  public IndexInternal getAssociatedIndex() {
    return null;
  }

  @Override
  public void updateTypeName(final String newTypeName) {
    shell.updateTypeName(newTypeName);
  }

  /** The shell's definition (bucket, properties, null strategy, collations) with the columnar type and settings. */
  @Override
  public JSONObject toJSON() {
    final JSONObject json = shell.toJSON();
    json.put("type", getType());
    json.put("unique", false);
    metadata.toJSON(json);
    return json;
  }

  @Override
  public String toString() {
    return getName();
  }

  /** The manager of the database this index belongs to. */
  ColumnarIndexManager manager() {
    return ((LocalDatabase) shell.getComponent().getDatabase().getEmbedded()).getColumnarIndexManager();
  }
}
