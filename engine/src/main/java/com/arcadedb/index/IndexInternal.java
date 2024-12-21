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

import com.arcadedb.engine.Component;
import com.arcadedb.schema.Type;
import com.arcadedb.serializer.json.JSONObject;
import com.arcadedb.utility.ExcludeFromJacocoGeneratedReport;

import java.io.*;
import java.util.*;

/**
 * Internal Index interface.
 */
@ExcludeFromJacocoGeneratedReport
public interface IndexInternal extends Index {
  public enum INDEX_STATUS {UNAVAILABLE, AVAILABLE, COMPACTION_SCHEDULED, COMPACTION_IN_PROGRESS}

  long build(int buildIndexBatchSize, BuildIndexCallback callback);

  boolean compact() throws IOException, InterruptedException;

  void setMetadata(String name, String[] propertyNames, int associatedBucketId);

  boolean setStatus(INDEX_STATUS[] expectedStatuses, INDEX_STATUS newStatus);

  void close();

  void drop();

  Map<String, Long> getStats();

  int getFileId();

  Component getComponent();

  Type[] getKeyTypes();

  byte[] getBinaryKeyTypes();

  List<Integer> getFileIds();

  void setTypeIndex(TypeIndex typeIndex);

  TypeIndex getTypeIndex();

  int getPageSize();

  boolean isCompacting();

  boolean isValid();

  boolean scheduleCompaction();

  String getMostRecentFileName();

  JSONObject toJSON();

  IndexInternal getAssociatedIndex();
}
