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

package com.arcadedb.importer;

import com.arcadedb.database.DatabaseInternal;

import java.io.IOException;

public class JSONImporter implements ContentImporter {
  @Override
  public void load(SourceSchema sourceSchema, AnalyzedEntity.ENTITY_TYPE entityType, final Parser parser, final DatabaseInternal database,
      final ImporterContext context, final ImporterSettings settings) throws IOException {
  }

  @Override
  public SourceSchema analyze(AnalyzedEntity.ENTITY_TYPE entityType, final Parser parser, final ImporterSettings settings,
      AnalyzedSchema analyzedSchema) {
    return new SourceSchema(this, parser.getSource(), null);
  }

  @Override
  public String getFormat() {
    return "JSON";
  }
}
