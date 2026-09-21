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
package com.arcadedb.gremlin.integration.importer.format;

import com.arcadedb.database.DatabaseInternal;
import com.arcadedb.gremlin.ArcadeGraph;
import com.arcadedb.integration.importer.AnalyzedEntity;
import com.arcadedb.integration.importer.AnalyzedSchema;
import com.arcadedb.integration.importer.ImportException;
import com.arcadedb.integration.importer.ImporterContext;
import com.arcadedb.integration.importer.ImporterSettings;
import com.arcadedb.integration.importer.Parser;
import com.arcadedb.integration.importer.SourceSchema;
import com.arcadedb.integration.importer.format.CSVImporterFormat;
import com.arcadedb.schema.EdgeType;
import com.arcadedb.schema.VertexType;
import org.apache.tinkerpop.gremlin.structure.io.IoCore;

import java.io.IOException;
import java.io.InputStream;

public class GraphMLImporterFormat extends CSVImporterFormat {
  @Override
  public void load(final SourceSchema sourceSchema, final AnalyzedEntity.EntityType entityType, final Parser parser, final DatabaseInternal database,
      final ImporterContext context, final ImporterSettings settings) throws ImportException {

    final ArcadeGraph graph = ArcadeGraph.open(database);

    // TinkerPop's own GraphML reader reports no per-record progress, so the statistics this format owes the caller
    // (context.parsed/createdVertices/createdEdges - see ImporterContext#toMap()) are taken as a before/after delta
    // across the whole schema instead of counted as the reader writes (issue #8054).
    final long verticesBefore = countRecordsOfKind(database, VertexType.class);
    final long edgesBefore = countRecordsOfKind(database, EdgeType.class);

    try (final InputStream is = parser.getInputStream()) {
      graph.io(IoCore.graphml()).reader().create().readGraph(is, graph);
    } catch (final IOException e) {
      throw new ImportException("Error on importing GraphML", e);
    }

    final long createdVertices = countRecordsOfKind(database, VertexType.class) - verticesBefore;
    final long createdEdges = countRecordsOfKind(database, EdgeType.class) - edgesBefore;

    context.createdVertices.addAndGet(createdVertices);
    context.createdEdges.addAndGet(createdEdges);
    context.parsed.addAndGet(createdVertices + createdEdges);
  }

  @Override
  public SourceSchema analyze(final AnalyzedEntity.EntityType entityType, final Parser parser, final ImporterSettings settings, final AnalyzedSchema analyzedSchema)
      throws IOException {
    return new SourceSchema(this, parser.getSource(), analyzedSchema);
  }

  @Override
  public String getFormat() {
    return "graphml";
  }
}
