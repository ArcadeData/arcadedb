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
package com.arcadedb.index;

import com.arcadedb.database.Document;
import com.arcadedb.database.Identifiable;
import com.arcadedb.database.RID;
import com.arcadedb.index.lsm.LSMTreeIndexAbstract;
import com.arcadedb.schema.Schema;
import com.arcadedb.utility.ExcludeFromJacocoGeneratedReport;

import java.util.List;

/**
 * Basic Index interface.
 */
@ExcludeFromJacocoGeneratedReport
public interface Index {
  interface BuildIndexCallback {
    void onDocumentIndexed(Document document, long totalIndexed);
  }

  /**
   * Retrieves the set of RIDs associated to a key.
   */
  IndexCursor get(Object[] keys);

  /**
   * Retrieves the set of RIDs associated to a key with a limit for the result.
   */
  IndexCursor get(Object[] keys, int limit);

  /**
   * Add multiple values for one key in the index.
   *
   * @param keys
   * @param rid  as an array of RIDs
   */
  void put(Object[] keys, RID[] rid);

  /**
   * Removes the keys from the index.
   *
   * @param keys
   */
  void remove(Object[] keys);

  /**
   * Removes an entry keys/record entry from the index.
   */
  void remove(Object[] keys, Identifiable rid);

  /**
   * Returns the number of LIVE entries in the index: keys deleted but not yet purged by a compaction (tombstones) are
   * NOT counted, so on an LSM index the value drops as records are removed instead of settling on a residual (#5601).
   * <p>
   * "Entry" is the index's own unit, which is not always one per record: a full-text index counts one entry per
   * analyzed token, a sparse vector index one per posting, and a geospatial index one per covering cell of a shape.
   * <p>
   * The cost is index-dependent and only {@code HASH} answers in constant time - every LSM-based implementation walks
   * the whole structure. Never call it on a query path.
   */
  long countEntries();

  Schema.INDEX_TYPE getType();

  String getTypeName();

  /**
   * The property names this index is defined on, in index order. A name can carry the modifier that says HOW the property is
   * indexed - {@code "tags by item"}, {@code "map by key"}, {@code "map by value"} - which is part of the stored name, not of
   * the document property: use {@link #basePropertyName} to get back the property a query names.
   */
  List<String> getPropertyNames();

  /**
   * Strips the {@code by key} / {@code by value} / {@code by item} modifier an index property name can carry, leaving the
   * document property it indexes. {@code "obj.hd by item"} answers {@code "obj.hd"}; a name with no modifier answers itself.
   * <p>
   * The distinction matters wherever an index property name meets a name the user wrote: a query says {@code obj.hd}, the
   * index calls the same thing {@code obj.hd by item}, and comparing the two spellings directly silently matches nothing.
   */
  static String basePropertyName(final String indexProperty) {
    if (indexProperty == null)
      return null;
    if (indexProperty.endsWith(" by key"))
      return indexProperty.substring(0, indexProperty.length() - 7);
    if (indexProperty.endsWith(" by value"))
      return indexProperty.substring(0, indexProperty.length() - 9);
    if (indexProperty.endsWith(" by item"))
      return indexProperty.substring(0, indexProperty.length() - 8);
    return indexProperty;
  }

  String getName();

  LSMTreeIndexAbstract.NULL_STRATEGY getNullStrategy();

  void setNullStrategy(LSMTreeIndexAbstract.NULL_STRATEGY nullStrategy);

  boolean isUnique();

  int getAssociatedBucketId();

  boolean supportsOrderedIterations();

  /**
   * Whether something populates this index: it is bound to a type and to the properties of it whose values become its
   * keys, so the engine maintains it on every write and a rebuild can regenerate it from the records.
   * <p>
   * False only for a MANUAL index, which is bound to nothing: its entries are whatever the caller put in it and there
   * is no record to derive them from, which is why {@code REBUILD INDEX} and {@code COMPACT INDEX} refuse a named one
   * and skip it in their {@code *} sweep (issue #5780).
   */
  boolean isAutomatic();
}
