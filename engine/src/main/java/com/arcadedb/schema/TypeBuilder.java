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

import com.arcadedb.GlobalConfiguration;
import com.arcadedb.database.Database;
import com.arcadedb.engine.Bucket;

import java.util.*;

/**
 * Builder class for schema types.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
public class TypeBuilder<T> {
  final Database database;
  final Class<T> type;
  boolean      ignoreIfExists  = false;
  String       typeName;
  List<Bucket> bucketInstances = Collections.emptyList();
  int          buckets;
  int          pageSize;

  protected TypeBuilder(final Database database, final Class<T> type) {
    this.database = database;
    this.type = type;
    this.buckets = database.getConfiguration().getValueAsInteger(GlobalConfiguration.TYPE_DEFAULT_BUCKETS);
    this.pageSize = database.getConfiguration().getValueAsInteger(GlobalConfiguration.BUCKET_DEFAULT_PAGE_SIZE);
  }

  public T create() {
    return (T) database.getSchema().getEmbedded().createType(this);
  }

  public TypeBuilder<T> withName(final String typeName) {
    this.typeName = typeName;
    return this;
  }

  public TypeBuilder<T> withIgnoreIfExists(final boolean ignoreIfExists) {
    this.ignoreIfExists = ignoreIfExists;
    return this;
  }

  public TypeBuilder<T> withTotalBuckets(final int buckets) {
    this.buckets = buckets;
    return this;
  }

  public TypeBuilder<T> withBuckets(final List<Bucket> bucketInstances) {
    this.bucketInstances = bucketInstances;
    this.buckets = 0;
    return this;
  }

  public TypeBuilder<T> withPageSize(final int pageSize) {
    this.pageSize = pageSize;
    return this;
  }
}
