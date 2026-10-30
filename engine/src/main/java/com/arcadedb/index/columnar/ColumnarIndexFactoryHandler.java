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

import com.arcadedb.database.Database;
import com.arcadedb.database.Record;
import com.arcadedb.engine.ComponentFile;
import com.arcadedb.index.Index;
import com.arcadedb.index.IndexException;
import com.arcadedb.index.IndexFactoryHandler;
import com.arcadedb.index.IndexInternal;
import com.arcadedb.schema.ColumnarIndexMetadata;
import com.arcadedb.schema.IndexBuilder;
import com.arcadedb.schema.IndexMetadata;
import com.arcadedb.schema.Type;

import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Set;

/** Creates the {@link ColumnarIndex} of one bucket, refusing a definition the columnar projection cannot hold. */
public final class ColumnarIndexFactoryHandler implements IndexFactoryHandler {

  /** Stops the scan of {@link #checkCardinality} once the limit is crossed. */
  private static final class DistinctValues implements ValueSink {
    private final Set<Object> values = new HashSet<>();

    @Override
    public void ordinal(final int o) {
      // the dictionary of the check is empty: every value arrives as unknownValue
    }

    @Override
    public void ordinals(final int[] sorted, final int count) {
      // as ordinal()
    }

    @Override
    public void longValue(final long v) {
      // only dictionary-encoded columns are checked
    }

    @Override
    public void doubleValue(final double v) {
      // only dictionary-encoded columns are checked
    }

    @Override
    public void missing() {
      // a missing value is not a distinct value
    }

    @Override
    public void unknownValue(final Object normalized) {
      values.add(normalized);
    }
  }

  @Override
  public IndexInternal create(final IndexBuilder<? extends Index> builder) {
    final IndexMetadata given = builder.getMetadata();
    // A COLUMNAR index projects a property of a type: a manual index (no type, no property) has nothing to project
    if (given == null || given.typeName == null || !given.hasIndexedProperties())
      throw new IndexException("A COLUMNAR index is defined on a property of a type, it cannot be a manual index");
    checkDefinition(builder.isUnique(), given.propertyNames);
    final Type[] keyTypes = builder.getKeyTypes();
    if (keyTypes == null || keyTypes.length != 1)
      throw new IndexException("A COLUMNAR index is defined on exactly one property (one index per property)");

    final ColumnarIndexMetadata metadata;
    if (given instanceof ColumnarIndexMetadata columnar)
      metadata = columnar;
    else {
      // a builder that carries no columnar settings (a bucket index builder): the defaults
      metadata = new ColumnarIndexMetadata(given.typeName, given.propertyNames.toArray(new String[0]), given.associatedBucketId);
      metadata.inheritCommonSettingsFrom(given);
    }

    // The key type of a property indexed BY ITEM is the item type, and BY ITEM is accepted on a LIST only
    final boolean byItem = metadata.propertyNames.getFirst().endsWith(ColumnarIndex.BY_ITEM_SUFFIX);
    ColumnKind.of(byItem ? Type.LIST : keyTypes[0], byItem);

    final ColumnarIndex index = new ColumnarIndex(builder.getDatabase(), builder.getIndexName(), builder.getFilePath(),
        ComponentFile.MODE.READ_WRITE, builder.getPageSize(), keyTypes, metadata);
    index.manager().register(index);
    return index;
  }

  /**
   * The checks of a COLUMNAR definition that need no data: not unique, exactly one property, no BY KEY or BY VALUE.
   *
   * @throws IndexException when the definition is refused
   */
  public static void checkDefinition(final boolean unique, final List<String> propertyNames) {
    if (unique)
      throw new IndexException("A COLUMNAR index cannot be unique");
    if (propertyNames.size() != 1)
      throw new IndexException("A COLUMNAR index is defined on exactly one property (one index per property), got " + propertyNames);
    final String indexProperty = propertyNames.getFirst();
    if (!indexProperty.equals(Index.basePropertyName(indexProperty)) && !indexProperty.endsWith(ColumnarIndex.BY_ITEM_SUFFIX))
      throw new IndexException("A COLUMNAR index does not support BY KEY or BY VALUE, got '" + indexProperty + "'");
  }

  /**
   * Refuses a dictionary-encoded column whose type (subtypes included) already holds more than {@code limit} distinct values (spec
   * 5.1). The scan stops at the first value over the limit. A value the column cannot hold is not counted, as the projection
   * would count it missing.
   *
   * @throws IndexException when there are more than {@code limit} distinct values
   */
  public static void checkCardinality(final Database db, final String type, final ColumnDefinition column, final int limit) {
    if (column.kind() != ColumnKind.ORDINAL && column.kind() != ColumnKind.ORDINAL_LIST)
      return;

    final GlobalDictionary empty = GlobalDictionary.createInMemory();
    final ColumnarValueExtractor extractor = new ColumnarValueExtractor(type, new ColumnDefinition[] { column },
        (bucketId, property) -> empty, new ColumnarStats());
    final DistinctValues distinct = new DistinctValues();
    final Iterator<Record> records = db.iterateType(type, true);
    while (records.hasNext()) {
      extractor.extract(records.next().asDocument(), column.property(), empty, distinct);
      if (distinct.values.size() > limit)
        throw new IndexException("Property '" + column.property() + "' of type '" + type + "' has more than " + limit
            + " distinct values, the limit of a COLUMNAR index (set METADATA {\"maxCardinality\": n} or "
            + "arcadedb.columnar.maxCardinality to raise it)");
    }
  }
}
