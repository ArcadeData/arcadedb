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
package com.arcadedb.server.http.handler.batch;

import com.arcadedb.serializer.json.JSONArray;
import com.arcadedb.serializer.json.JSONObject;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.util.Set;

/**
 * Parses JSONL (newline-delimited JSON) into BatchRecord instances.
 * <p>
 * Expected format per line:
 * <pre>
 * {"@type":"vertex","@class":"Person","@id":"t1","name":"Alice","age":30}
 * {"@type":"edge","@class":"KNOWS","@from":"t1","@to":"t2","since":2020}
 * </pre>
 * Blank lines are skipped. The record object is reused across calls.
 * <p>
 * Parsing errors are surfaced as {@link IllegalArgumentException} so the HTTP layer maps them
 * to a 400 Bad Request with a clear message instead of a generic 500.
 */
public class JsonlBatchRecordStream implements BatchRecordStream {

  private static final Set<String> META_KEYS = Set.of("@type", "@class", "@id", "@from", "@to");

  private final BufferedReader reader;
  private final BatchRecord    record;
  private       int            lineNumber;
  private       boolean        ready;

  public JsonlBatchRecordStream(final InputStream input) {
    this.reader = new BufferedReader(new InputStreamReader(input, StandardCharsets.UTF_8), 65536);
    this.record = new BatchRecord();
  }

  @Override
  public boolean hasNext() throws IOException {
    if (ready)
      return true;

    while (true) {
      final String line = reader.readLine();
      if (line == null)
        return false;

      lineNumber++;

      // Skip blank lines
      if (line.isBlank())
        continue;

      parseLine(line);
      ready = true;
      return true;
    }
  }

  @Override
  public BatchRecord next() {
    ready = false;
    return record;
  }

  @Override
  public int getLineNumber() {
    return lineNumber;
  }

  @Override
  public void close() throws IOException {
    reader.close();
  }

  private void parseLine(final String line) {
    record.reset();

    // A JSON array is a common mistake (the format used by INSERT INTO ... CONTENT [...]).
    // Detect it early and surface the JSONL requirement instead of a confusing parse error.
    final int start = firstNonWhitespace(line);
    final char first = line.charAt(start);
    if (first == '[')
      throw new IllegalArgumentException("Malformed JSONL at line " + lineNumber
          + ": expected one JSON object per line but got a JSON array. "
          + "The /api/v1/batch endpoint requires the JSONL format (newline-delimited JSON objects)");
    if (first != '{')
      throw new IllegalArgumentException("Malformed JSONL at line " + lineNumber
          + ": expected a JSON object starting with '{'");

    final JSONObject json;
    try {
      json = new JSONObject(line);
    } catch (final RuntimeException e) {
      throw new IllegalArgumentException("Malformed JSON at line " + lineNumber + ": " + e.getMessage(), e);
    }

    final String type = json.getString("@type", null);
    if (type == null)
      throw new IllegalArgumentException("Missing @type at line " + lineNumber + ". Expected 'vertex' or 'edge'");

    if ("vertex".equals(type) || "v".equals(type)) {
      record.kind = BatchRecord.Kind.VERTEX;
    } else if ("edge".equals(type) || "e".equals(type)) {
      record.kind = BatchRecord.Kind.EDGE;
    } else
      throw new IllegalArgumentException("Unknown @type '" + type + "' at line " + lineNumber + ". Expected 'vertex' or 'edge'");

    record.typeName = json.getString("@class", null);
    if (record.typeName == null || record.typeName.isEmpty())
      throw new IllegalArgumentException("Missing @class at line " + lineNumber);

    if (record.kind == BatchRecord.Kind.VERTEX) {
      record.tempId = json.getString("@id", null);
    } else {
      record.fromRef = json.getString("@from", null);
      record.toRef = json.getString("@to", null);
      if (record.fromRef == null || record.toRef == null)
        throw new IllegalArgumentException("Edge missing @from or @to at line " + lineNumber);
    }

    // Extract all non-meta keys as properties.
    // JSON arrays/objects are unwrapped to java.util.List / java.util.Map so downstream
    // schema validation and Type.convert (which only recognise Collection/Map) accept
    // them - issue #4069.
    for (final String key : json.keySet()) {
      if (META_KEYS.contains(key))
        continue;
      record.addProperty(key, unwrap(json.get(key)));
    }
  }

  private static Object unwrap(final Object value) {
    if (value instanceof JSONArray array)
      return array.toList();
    if (value instanceof JSONObject object)
      return object.toMap();
    return value;
  }

  private static int firstNonWhitespace(final String s) {
    final int len = s.length();
    int i = 0;
    while (i < len && Character.isWhitespace(s.charAt(i)))
      i++;
    return i < len ? i : 0;
  }
}
