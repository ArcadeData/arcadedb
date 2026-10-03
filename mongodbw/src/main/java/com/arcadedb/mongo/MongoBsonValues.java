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
package com.arcadedb.mongo;

import com.arcadedb.serializer.BinaryTypes;
import de.bwaldvogel.mongo.bson.BinData;
import de.bwaldvogel.mongo.bson.BsonJavaScript;
import de.bwaldvogel.mongo.bson.BsonRegularExpression;
import de.bwaldvogel.mongo.bson.BsonTimestamp;
import de.bwaldvogel.mongo.bson.Decimal128;
import de.bwaldvogel.mongo.bson.Document;
import de.bwaldvogel.mongo.bson.LegacyUUID;
import de.bwaldvogel.mongo.bson.MaxKey;
import de.bwaldvogel.mongo.bson.MinKey;
import de.bwaldvogel.mongo.bson.ObjectId;

import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;

/**
 * Lossless mapping between the BSON values the MongoDB wire library hands over and what the engine can store.
 * <p>
 * The engine has no counterpart for a binary blob, a regular expression, a timestamp, MinKey, MaxKey or JavaScript code, and
 * its serializer drops a property whose value type it does not know. Such a value is therefore stored as an embedded map
 * tagged with {@link #TAG}, and turned back into the BSON object on the way out. A {@link Decimal128} is stored as a
 * {@link BigDecimal} (a DECIMAL) and read back as a Decimal128. A value the engine cannot hold at all is refused with an
 * error instead of being silently dropped.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
final class MongoBsonValues {
  static final String TAG = "$bson";

  private static final String BIN_DATA       = "binData";
  private static final String REGEX          = "regex";
  private static final String TIMESTAMP      = "timestamp";
  private static final String MIN_KEY        = "minKey";
  private static final String MAX_KEY        = "maxKey";
  private static final String JAVASCRIPT     = "javascript";
  private static final String LEGACY_UUID    = "legacyUuid";

  private MongoBsonValues() {
  }

  /**
   * Converts a value received from a client into the form stored in the database. An {@link ObjectId} becomes its hex string.
   *
   * @throws IllegalArgumentException if the value (or an element nested in it) has a type that cannot be stored
   */
  @SuppressWarnings("unchecked")
  static Object toStored(final Object value) {
    if (value == null)
      return null;
    if (value instanceof ObjectId id)
      return id.getHexData();
    if (value instanceof Decimal128 decimal)
      return decimal.toBigDecimal();
    if (value instanceof BinData bin)
      return tagged(BIN_DATA, "data", bin.getData());
    if (value instanceof BsonRegularExpression regex)
      return tagged(REGEX, "pattern", regex.getPattern(), "options", regex.getOptions() != null ? regex.getOptions() : "");
    if (value instanceof BsonTimestamp timestamp)
      return tagged(TIMESTAMP, "value", timestamp.getValue());
    if (value instanceof MinKey)
      return tagged(MIN_KEY);
    if (value instanceof MaxKey)
      return tagged(MAX_KEY);
    if (value instanceof BsonJavaScript script)
      return tagged(JAVASCRIPT, "code", script.getCode());
    if (value instanceof LegacyUUID uuid)
      return tagged(LEGACY_UUID, "uuid", uuid.getUuid().toString());
    if (value instanceof Map<?, ?> map) {
      final Map<String, Object> converted = LinkedHashMap.newLinkedHashMap(map.size());
      for (final Map.Entry<String, Object> entry : ((Map<String, Object>) map).entrySet())
        converted.put(entry.getKey(), toStored(entry.getValue()));
      return converted;
    }
    if (value instanceof List<?> list) {
      final List<Object> converted = new ArrayList<>(list.size());
      for (final Object item : list)
        converted.add(toStored(item));
      return converted;
    }
    if (BinaryTypes.getTypeFromValue(value, null) == -1)
      throw new IllegalArgumentException("The BSON type " + value.getClass().getSimpleName() + " is not supported");
    return value;
  }

  /**
   * Turns a stored tagged map or DECIMAL back into the BSON object it was written from; any other value is returned as is.
   */
  static Object toBson(final Object value) {
    if (value instanceof BigDecimal decimal)
      return new Decimal128(decimal);
    if (value instanceof Map<?, ?> map && map.get(TAG) instanceof String kind)
      return fromTagged(kind, map);
    return value;
  }

  static boolean isTagged(final Object value) {
    return value instanceof Map<?, ?> map && map.get(TAG) instanceof String;
  }

  private static Object fromTagged(final String kind, final Map<?, ?> map) {
    switch (kind) {
    case BIN_DATA:
      return new BinData((byte[]) map.get("data"));
    case REGEX:
      return new BsonRegularExpression((String) map.get("pattern"), (String) map.get("options"));
    case TIMESTAMP:
      return new BsonTimestamp(((Number) map.get("value")).longValue());
    case MIN_KEY:
      return MinKey.getInstance();
    case MAX_KEY:
      return MaxKey.getInstance();
    case JAVASCRIPT:
      return new BsonJavaScript((String) map.get("code"));
    case LEGACY_UUID:
      return new LegacyUUID(UUID.fromString((String) map.get("uuid")));
    default:
      return map;
    }
  }

  private static Map<String, Object> tagged(final String kind, final Object... keyValues) {
    final Map<String, Object> map = LinkedHashMap.newLinkedHashMap(keyValues.length / 2 + 1);
    map.put(TAG, kind);
    for (int i = 0; i < keyValues.length; i += 2)
      map.put((String) keyValues[i], keyValues[i + 1]);
    return map;
  }
}
