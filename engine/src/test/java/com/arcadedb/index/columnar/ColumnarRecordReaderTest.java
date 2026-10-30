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

import com.arcadedb.TestHelper;
import com.arcadedb.database.Binary;
import com.arcadedb.database.Document;
import com.arcadedb.database.DocumentIndexer;
import com.arcadedb.database.ImmutableDocument;
import com.arcadedb.database.MutableDocument;
import com.arcadedb.database.RID;
import com.arcadedb.graph.MutableVertex;
import com.arcadedb.schema.Type;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * The one-walk reader of the apply pass and the rebuild (final review C, ingest cost) answers for every column exactly what the
 * per-column read ({@link DocumentIndexer#getPropertyValue}) answers, and leaves the record's content where it found it.
 */
class ColumnarRecordReaderTest extends TestHelper {
  private static final ColumnDefinition[] COLUMNS = {
      ColumnarIndex.defineColumn(1, "brand", Type.STRING, false),
      ColumnarIndex.defineColumn(2, "tags", Type.LIST, true),
      ColumnarIndex.defineColumn(3, "size", Type.LONG, false),
      ColumnarIndex.defineColumn(4, "author", Type.STRING, false),
      ColumnarIndex.defineColumn(5, "meta.k", Type.STRING, false),
      ColumnarIndex.defineColumn(6, "late", Type.STRING, false),
      ColumnarIndex.defineColumn(7, "absent", Type.STRING, false) };

  @Test
  void aDocumentReadsAsThePerColumnRead() {
    database.command("sql", "CREATE DOCUMENT TYPE Asset");
    final RID full = save(database.newDocument("Asset"));
    final ColumnarValueExtractor.RecordReader reader = new ColumnarValueExtractor.RecordReader(COLUMNS);
    reader.prepare(database.getSchema().getDictionary());
    // a name first written after prepare: read on its own
    final RID[] late = new RID[1];
    database.transaction(() -> late[0] = database.newDocument("Asset").set("brand", "b").set("late", "x").save().getIdentity());

    assertReadsAsPerColumn(reader, full);
    assertReadsAsPerColumn(reader, late[0]);
  }

  @Test
  void aVertexReadsAsThePerColumnRead() {
    // a vertex keeps its edge pointers before its properties
    database.command("sql", "CREATE VERTEX TYPE Node");
    database.command("sql", "CREATE EDGE TYPE Link");
    final RID[] vertex = new RID[1];
    database.transaction(() -> {
      final MutableVertex v = database.newVertex("Node");
      fill(v);
      v.save();
      final MutableVertex other = database.newVertex("Node").set("brand", "o").save();
      v.newEdge("Link", other);
      vertex[0] = v.getIdentity();
    });
    final ColumnarValueExtractor.RecordReader reader = new ColumnarValueExtractor.RecordReader(COLUMNS);
    reader.prepare(database.getSchema().getDictionary());

    assertReadsAsPerColumn(reader, vertex[0]);
  }

  private RID save(final MutableDocument document) {
    database.transaction(() -> {
      fill(document);
      document.save();
    });
    return document.getIdentity();
  }

  private static void fill(final MutableDocument document) {
    document.set("brand", "acme").set("tags", List.of("t1", "t2")).set("size", 42L).set("author", null)
        .set("meta", Map.of("k", "v")).set("body", "x".repeat(100));
  }

  private void assertReadsAsPerColumn(final ColumnarValueExtractor.RecordReader reader, final RID rid) {
    final ImmutableDocument walked = (ImmutableDocument) database.lookupByRID(rid, true);
    final Document perColumn = (Document) database.lookupByRID(rid, true);
    final Binary content = walked.getBuffer();
    final int before = content.position();

    reader.begin(walked);
    try {
      for (int k = 0; k < COLUMNS.length; k++)
        assertThat(reader.raw(k)).as(COLUMNS[k].property() + " of " + rid).isEqualTo(DocumentIndexer.getPropertyValue(perColumn,
            COLUMNS[k].property()));
    } finally {
      reader.end();
    }

    assertThat(content.position()).isEqualTo(before);
    // the record still reads after the walk, as the external-value collection does next
    assertThat(walked.get("brand")).isEqualTo(perColumn.get("brand"));
    assertThat(walked.toMap(false)).isEqualTo(perColumn.toMap(false));
  }
}
