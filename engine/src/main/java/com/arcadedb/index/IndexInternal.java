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

package com.arcadedb.index;

import com.arcadedb.database.Document;
import com.arcadedb.database.Identifiable;
import com.arcadedb.database.RID;
import com.arcadedb.engine.PaginatedComponent;
import com.arcadedb.index.lsm.LSMTreeIndexAbstract;
import com.arcadedb.schema.SchemaImpl;

import java.io.IOException;
import java.util.Map;

/**
 * Internal Index interface.
 */
public interface IndexInternal extends Index {
  boolean compact() throws IOException, InterruptedException;

  void setMetadata(String name, String[] propertyNames, int associatedBucketId);

  void close();

  void drop();

  String getName();

  Map<String, Long> getStats();

  int getFileId();

  PaginatedComponent getPaginatedComponent();
}
