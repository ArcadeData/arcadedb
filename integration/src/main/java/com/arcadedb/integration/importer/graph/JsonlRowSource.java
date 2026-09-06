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

import com.arcadedb.serializer.json.JSONArray;
import com.arcadedb.serializer.json.JSONObject;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.util.List;

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

    /**
     * A JSON array or nested object has no scalar form, but aborting the whole import on one is
     * worse than handing back its raw JSON text: the text is what a caller mapping an embedding
     * with {@code property(...)} rather than {@code floatArrayProperty(...)} expects, and
     * {@code VectorUtils.toFloatArray()} parses it (#7185).
     */
    @Override
    public String get(final String attribute) {
      // opt() is null for both an absent attribute and an explicit JSON null, so one lookup covers
      // the "not set" case that getString() would otherwise throw on
      final Object value = json.opt(attribute);
      if (value == null)
        return null;
      if (value instanceof JSONArray || value instanceof JSONObject)
        return value.toString();
      // scalar: go through getString() so numbers keep their exact source text
      return json.getString(attribute);
    }

    @Override
    public int getInt(final String attribute) {
      return json.isNull(attribute) ? 0 : json.getInt(attribute);
    }

    @Override
    public long getLong(final String attribute) {
      return json.isNull(attribute) ? 0L : json.getLong(attribute);
    }

    @Override
    public double getDouble(final String attribute) {
      return json.isNull(attribute) ? 0.0 : json.getDouble(attribute);
    }

    /**
     * Reads the JSON array natively into a {@code float[]}: no intermediate string, no boxed
     * element, one allocation of exactly the vector's size.
     */
    @Override
    public float[] getFloatArray(final String attribute) {
      if (json.isNull(attribute))
        return null;
      final JSONArray array = json.getJSONArray(attribute);
      final int length = array.length();
      final float[] result = new float[length];
      for (int i = 0; i < length; i++)
        result[i] = array.getFloat(i);
      return result;
    }

    @Override
    public List<Object> getList(final String attribute) {
      return json.isNull(attribute) ? null : json.getJSONArray(attribute).toList();
    }
  }
}
