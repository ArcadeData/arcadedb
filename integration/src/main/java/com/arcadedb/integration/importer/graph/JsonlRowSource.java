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
package com.arcadedb.integration.importer.graph;

import com.arcadedb.serializer.json.JSONObject;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;

/**
 * {@link GraphImporter.RecordSource} that reads newline-delimited JSON (JSONL / NDJSON).
 * Each line is a JSON object whose keys are the attribute names.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
public class JsonlRowSource implements GraphImporter.RecordSource {

  private final String filePath;

  public JsonlRowSource(final String filePath) {
    this.filePath = filePath;
  }

  public static JsonlRowSource from(final String dir, final String fileName) {
    return new JsonlRowSource(new File(dir, fileName).getPath());
  }

  @Override
  public void forEach(final GraphImporter.RecordVisitor visitor) throws Exception {
    try (final BufferedReader br = new BufferedReader(
        new InputStreamReader(new FileInputStream(filePath), StandardCharsets.UTF_8), 1 << 16)) {
      final JsonlRecordReader rec = new JsonlRecordReader();
      String line;
      while ((line = br.readLine()) != null) {
        line = line.trim();
        if (line.isEmpty() || line.charAt(0) != '{')
          continue;
        rec.json = new JSONObject(line);
        visitor.visit(rec);
      }
    }
  }

  private static class JsonlRecordReader implements GraphImporter.RecordReader {
    JSONObject json;

    @Override
    public String get(final String attribute) {
      return json.has(attribute) ? json.getString(attribute) : null;
    }

    @Override
    public int getInt(final String attribute) {
      return json.has(attribute) ? json.getInt(attribute) : 0;
    }
  }
}
