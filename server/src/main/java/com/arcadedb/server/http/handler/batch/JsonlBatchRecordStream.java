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
import java.util.Map;
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
 * Properties sit FLAT beside the control keys; they are not nested under a {@code properties} object. Control and
 * data therefore share one namespace, and the {@code @} prefix is what separates them: a key outside
 * {@link #META_KEYS} that begins with {@code @} is refused rather than stored as a property with that name, and so is
 * a {@code properties} key carrying an object, which is the nested-form misreading the gRPC sibling's
 * {@code GraphBatchRecord.properties} map invites. Both used to be accepted and turned into a property, so a payload
 * built on the obvious guess loaded with the right counters and the wrong data (issue #7570).
 * <p>
 * Parsing errors are surfaced as {@link IllegalArgumentException} so the HTTP layer maps them
 * to a 400 Bad Request with a clear message instead of a generic 500. A line that did not form a well-formed JSON
 * object at all uses the {@link MalformedBatchRecordException} subclass, because a truncated upload produces exactly
 * that and the client must not be sent hunting for a bad line in a file that is fine - see that class.
 */
public class JsonlBatchRecordStream implements BatchRecordStream {

  private static final Set<String> META_KEYS = Set.of("@type", "@class", "@id", "@from", "@to");

  /** Named in the refusal messages so the client is told what the five understood control keys are. */
  private static final String META_KEY_LIST = "@type, @class, @id, @from and @to";

  /**
   * The one non-{@code @} key that cannot be taken at face value: {@code GraphBatchRecord} carries a
   * {@code properties} map, so a reader of the gRPC contract nests the properties under this name. Only refused when
   * its value is an object - {@code {"properties":"public"}} is ordinary data and a domain is allowed a field with
   * this name.
   */
  private static final String NESTED_PROPERTIES_KEY = "properties";

  private final BufferedReader reader;
  private final BatchRecord    record;
  private       int            lineNumber;
  private       long           linesSkipped;
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
      if (line.isBlank()) {
        linesSkipped++;
        continue;
      }

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
  public long getLinesRead() {
    return lineNumber;
  }

  @Override
  public long getLinesSkipped() {
    return linesSkipped;
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
      throw new MalformedBatchRecordException("Malformed JSONL at line " + lineNumber
          + ": expected one JSON object per line but got a JSON array. "
          + "The /api/v1/batch endpoint requires the JSONL format (newline-delimited JSON objects)");
    if (first != '{')
      throw new MalformedBatchRecordException("Malformed JSONL at line " + lineNumber
          + ": expected a JSON object starting with '{'");

    final JSONObject json;
    try {
      json = new JSONObject(line);
    } catch (final RuntimeException e) {
      throw new MalformedBatchRecordException("Malformed JSON at line " + lineNumber + ": " + e.getMessage(), e);
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
      final Object value = unwrap(json.get(key));
      rejectReservedKey(key, value);
      record.addProperty(key, value);
    }
  }

  /**
   * Refuses the two keys that cannot be what they look like. Both used to fall through into the property list, where
   * they produced a successful load holding data the client never meant to send (issue #7570).
   */
  private void rejectReservedKey(final String key, final Object value) {
    if (!key.isEmpty() && key.charAt(0) == '@')
      throw new IllegalArgumentException("Unknown control key '" + key + "' at line " + lineNumber
          + ": the '@' prefix is reserved by the batch encoding and only " + META_KEY_LIST + " are understood. "
          + "A property name cannot start with '@': rename the key, or drop it");

    if (NESTED_PROPERTIES_KEY.equals(key) && value instanceof Map)
      throw new IllegalArgumentException("Reserved key 'properties' at line " + lineNumber
          + ": a batch line carries its properties flat, beside the '@' control keys, not nested under a "
          + "'properties' object. Send {\"@type\":\"vertex\",\"@class\":\"Person\",\"name\":\"Alice\"} rather than "
          + "{\"@type\":\"vertex\",\"@class\":\"Person\",\"properties\":{\"name\":\"Alice\"}}. Nested here, the object "
          + "would be stored as a single property literally named 'properties' and nothing would fail until "
          + "something queried for a field that was never written");
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
