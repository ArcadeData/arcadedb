package com.arcadedb.schema;/*
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

import com.arcadedb.index.Index;
import com.arcadedb.serializer.json.JSONObject;
import com.arcadedb.utility.ExcludeFromJacocoGeneratedReport;

import java.util.Set;

/**
 * Schema Property.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
@ExcludeFromJacocoGeneratedReport
public interface Property {

  String RID_PROPERTY            = "@rid";
  String TYPE_PROPERTY           = "@type";
  String IN_PROPERTY             = "@in";
  String OUT_PROPERTY            = "@out";
  String CAT_PROPERTY            = "@cat";
  String PROPERTY_TYPES_PROPERTY = "@props";
  String THIS_PROPERTY           = "@this";

  Set<String> METADATA_PROPERTIES = Set.of(RID_PROPERTY, TYPE_PROPERTY, IN_PROPERTY, OUT_PROPERTY, CAT_PROPERTY,
      PROPERTY_TYPES_PROPERTY);

  Index createIndex(Schema.INDEX_TYPE type, boolean unique);

  Index getOrCreateIndex(Schema.INDEX_TYPE type, boolean unique);

  String getName();

  Type getType();

  int getId();

  Object getDefaultValue();

  Property setDefaultValue(Object defaultValue);

  String getOfType();

  Property setOfType(String ofType);

  Property setReadonly(boolean readonly);

  boolean isReadonly();

  Property setMandatory(boolean mandatory);

  boolean isMandatory();

  Property setNotNull(boolean notNull);

  boolean isNotNull();

  Property setHidden(boolean hidden);

  boolean isHidden();

  /**
   * When true, the property's value is stored in a paired external bucket instead of inline in the primary
   * record. The main record carries only a small TYPE_EXTERNAL pointer, so traversal-only queries that don't
   * project this property never load its bytes. Best for vector embeddings, large strings, embedded JSON, and
   * full-text payloads.
   * <p>
   * <b>Null semantics.</b> {@code set("field", null)} on an EXTERNAL property does NOT consume external bucket
   * space: the serializer writes an inline TYPE_NULL byte and releases any pre-existing paired blob via the
   * orphan-cleanup pass. {@code set(field, null)} and {@code remove(field)} are therefore equivalent in terms
   * of paired-bucket storage; they differ only in whether the property header slot is retained (set-null
   * keeps it, remove drops it). Reads return null in both cases.
   */
  Property setExternal(boolean external);

  boolean isExternal();

  /**
   * Compression policy for an EXTERNAL property's value:
   * <ul>
   *   <li>{@code none} (default) - store raw.</li>
   *   <li>{@code fast} - LZ4 fast encoder. ~1.2-1.5x faster compress than Snappy on text, identical decompress
   *       speed regardless of tier. Best default when writes are frequent.</li>
   *   <li>{@code max} - LZ4 HC encoder. ~10pp smaller output than {@code fast}, 8-20x slower compress;
   *       decompression speed is the same as {@code fast}. Best for write-once / read-many payloads.</li>
   *   <li>{@code auto} - try {@code fast}; keep compressed only when it saves more than 10% of bytes.
   *       <b>Cost:</b> on no-win records (e.g. dense float32 embeddings) the work is "compress, measure, throw
   *       it away, fall back to raw". You pay one wasted LZ4 compress + one extra byte-array copy per record
   *       compared to {@code none}. Use {@code none} explicitly when the workload tips no-win consistently.</li>
   * </ul>
   * The legacy alias {@code lz4} is accepted and stored as {@code fast}. Ignored for non-EXTERNAL properties.
   */
  Property setCompression(String compression);

  String getCompression();

  Property setMax(String max);

  String getMax();

  Property setMin(String min);

  String getMin();

  Property setRegexp(String regexp);

  String getRegexp();

  Set<String> getCustomKeys();

  Object getCustomValue(String key);

  JSONObject toJSON();

  Object setCustomValue(String key, Object value);
}
