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
package com.arcadedb.database.bucketselectionstrategy;

import com.arcadedb.database.Document;
import com.arcadedb.index.TypeIndex;
import com.arcadedb.schema.DocumentType;

import java.util.Arrays;

/**
 * Select the bucket using a partition algorithm computed as the hashed value of the properties values. This allows to predetermine in which bucket is contained
 * a key(s) and therefore a document. There are some limitations on using this implementation: (1) field identified as partition key cannot be modified. (This
 * could be solved in the future by removing and recreating the document in a different bucket. If the record is part of a graph, then the edges will be updated
 * accordingly.)
 *
 * @author Luca Garulli
 */
public class PartitionedBucketSelectionStrategy extends RoundRobinBucketSelectionStrategy {
  private       DocumentType type;
  private final String[]     propertyNames;

  public PartitionedBucketSelectionStrategy(final String[] propertyNames) {
    this.propertyNames = propertyNames;
  }

  @Override
  public void setType(final DocumentType type) {
    super.setType(type);
    this.type = type;

    final TypeIndex index = type.getPolymorphicIndexByProperties(propertyNames);
    if (index == null || !index.isAutomatic() || !index.isUnique())
      throw new IllegalArgumentException("Cannot find an index on properties " + Arrays.toString(propertyNames));
  }

  @Override
  public int getBucketIdByRecord(final Document record, final boolean async) {
    if (propertyNames != null) {
      final DocumentType documentType = record.getType();
      if (!this.type.equals(documentType))
        throw new IllegalArgumentException(
            "Record of type '" + documentType.getName() + "' is not supported by partitioned bucket selection strategy built on type '" + type.getName() + "'");

      int hash = 0;
      for (int i = 0; i < propertyNames.length; i++) {
        Object value = record.get(propertyNames[i]);
        if (value != null)
          hash += value.hashCode();
      }
      return (hash & 0x7fffffff) % total;
    }

    return super.getBucketIdByRecord(record, async);
  }

  @Override
  public int getBucketIdByKeys(final Object[] keyValues, final boolean async) {
    if (propertyNames != null) {
      int hash = 0;
      for (int i = 0; i < keyValues.length; i++) {
        Object value = keyValues[i];
        if (value != null)
          hash += value.hashCode();
      }
      return (hash & 0x7fffffff) % total;
    }

    return super.getBucketIdByKeys(keyValues, async);
  }

  @Override
  public String getName() {
    return "partitioned";
  }
}
