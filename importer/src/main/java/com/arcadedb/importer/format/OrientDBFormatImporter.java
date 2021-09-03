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

package com.arcadedb.importer.format;

import com.arcadedb.importer.OrientDBImporter;
import com.arcadedb.database.DatabaseInternal;
import com.arcadedb.importer.*;

import java.io.IOException;
import java.util.zip.GZIPInputStream;

public class OrientDBFormatImporter extends AbstractFormatImporter {
  @Override
  public void load(final SourceSchema sourceSchema, final AnalyzedEntity.ENTITY_TYPE entityType, final Parser parser, final DatabaseInternal database,
      final ImporterContext context, final ImporterSettings settings) throws ImportException {

    context.parsed.set(0);

    try {
      new OrientDBImporter(database) {
        @Override
        public GZIPInputStream openInputStream() throws IOException {
          sourceSchema.getSource().reset();
          return (GZIPInputStream) sourceSchema.getSource().inputStream;
        }
      }.run();
    } catch (IOException e) {
      throw new ImportException("Error on importing OrientDB database", e);
    }
  }

  @Override
  public SourceSchema analyze(final AnalyzedEntity.ENTITY_TYPE entityType, final Parser parser, final ImporterSettings settings,
      final AnalyzedSchema analyzedSchema) throws IOException {
    return new SourceSchema(this, parser.getSource(), analyzedSchema);
  }

  @Override
  public String getFormat() {
    return "OrientDB";
  }
}
