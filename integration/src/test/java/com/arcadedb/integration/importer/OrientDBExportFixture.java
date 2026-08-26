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
package com.arcadedb.integration.importer;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.io.Writer;
import java.nio.charset.StandardCharsets;
import java.util.zip.GZIPOutputStream;

/**
 * Writes a minimal, hand-crafted OrientDB export archive. The archives bundled as test resources cover only the
 * shapes an export of a trivial database happens to produce; a regression test for a specific shape (a wide LONG, a
 * composite index, a `@fieldTypes` hint) has to be able to spell that shape out.
 * <p>
 * The layout mirrors the one produced by OrientDB's `ODatabaseExport`: an `info` header, the cluster list, the schema
 * and then the records, where the record at `#0:1` carries the schema and the one at `#0:2` the index manager.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
final class OrientDBExportFixture {

  private OrientDBExportFixture() {
  }

  /**
   * @param file        destination of the gzipped export
   * @param databaseName name written in the `info` header
   * @param classes     the JSON body of the schema `classes` array, without the enclosing brackets
   * @param indexes     the JSON body of the index manager `indexes` array, without the enclosing brackets
   * @param records     the JSON body of the data records, without the enclosing brackets and without a leading comma
   */
  static void write(final File file, final String databaseName, final String classes, final String indexes,
      final String records) throws IOException {
    final StringBuilder buffer = new StringBuilder(1024);
    buffer.append("{\"info\":{\"name\":\"").append(databaseName)
        .append("\",\"default-cluster-id\":3,\"exporter-version\":12,\"engine-version\":\"3.1.13\",")
        .append("\"engine-build\":\"test\",\"storage-config-version\":23,\"schema-version\":4,")
        .append("\"schemaRecordId\":\"#0:1\",\"indexMgrRecordId\":\"#0:2\"},");
    buffer.append("\"clusters\":[{\"name\":\"internal\",\"id\":0},{\"name\":\"index\",\"id\":1},")
        .append("{\"name\":\"manindex\",\"id\":2},{\"name\":\"default\",\"id\":3}],");
    buffer.append("\"schema\":{\"version\":1,\"blob-clusters\":[],\"classes\":[").append(classes).append("]},");
    buffer.append("\"records\":[");
    buffer.append("{\"@type\":\"d\",\"@rid\":\"#0:1\",\"@version\":1},");
    buffer.append("{\"@type\":\"d\",\"@rid\":\"#0:2\",\"@version\":1,\"indexes\":[").append(indexes).append("]}");
    if (records != null && !records.isEmpty())
      buffer.append(",").append(records);
    buffer.append("]}");

    try (final Writer writer = new OutputStreamWriter(new GZIPOutputStream(new FileOutputStream(file)),
        StandardCharsets.UTF_8)) {
      writer.write(buffer.toString());
    }
  }
}
