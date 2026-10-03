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

import com.arcadedb.log.LogManager;
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
import de.bwaldvogel.mongo.exception.ErrorCode;
import de.bwaldvogel.mongo.exception.MongoServerError;

import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.Base64;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.logging.Level;

/**
 * Lossless mapping between the BSON values the MongoDB wire library hands over and what the engine can store.
 * <p>
 * The engine has no counterpart for a binary blob, a regular expression, a timestamp, MinKey, MaxKey or JavaScript code, and
 * its serializer drops a property whose value type it does not know. Such a value is therefore stored as an embedded map
 * tagged with {@link #TAG}, and turned back into the BSON object on the way out. A {@link Decimal128} is stored as a
 * {@link BigDecimal} (a DECIMAL) and read back as a Decimal128. A value the engine cannot hold at all is refused with an
 * error instead of being silently dropped.
 * <p>
 * A tagged value is a map to the engine: equality and {@code $in} work, but a range operator or a sort on a timestamp, binary,
 * MinKey, MaxKey, regular expression or JavaScript field compares maps and does not follow BSON ordering. A DECIMAL beyond
 * Decimal128 (more than 34 digits) is read back as a double rather than failing the read.
 * <p>
 * The field name {@code $bson} is reserved for the tag: a map with a string {@code $bson} key written through SQL or another
 * protocol is read back by the MongoDB plugin as the BSON value it names (a malformed or unknown tag stays a plain map).
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
final class MongoBsonValues {
  static final String TAG = "$bson";

  private static final AtomicBoolean WIDE_DECIMAL_WARNED = new AtomicBoolean();

  private static final String OBJECT_ID = "objectId";
  // binary is held as Base64 text, not byte[]: a byte[] inside a map compares by identity, which would break eq and $in filters
  private static final String BIN_DATA = "binData";
  private static final String REGEX = "regex";
  private static final String TIMESTAMP = "timestamp";
  private static final String MIN_KEY = "minKey";
  private static final String MAX_KEY = "maxKey";
  private static final String JAVASCRIPT = "javascript";
  private static final String LEGACY_UUID = "legacyUuid";

  private MongoBsonValues() {
  }

  /**
   * Whether {@link #toStored} may change this value, that is whether it is anything but a plain string, number or boolean.
   */
  static boolean needsConversion(final Object value) {
    return !(value == null || value instanceof String || value instanceof Boolean || value instanceof Number && !(value instanceof Decimal128));
  }

  /**
   * Converts a value used in a filter into the form it is stored in. A regular expression is left alone: as a filter value it
   * is a pattern to match, not a value to compare. A Decimal128 NaN or Infinity is refused, as it is on insert.
   */
  static Object toBound(final Object value) {
    return needsConversion(value) && !(value instanceof BsonRegularExpression) ? toStored(value) : value;
  }

  /**
   * Converts a value received from a client into the form stored in the database. An {@link ObjectId} becomes a tagged map; use {@link #toStored(String, Object)} for an {@code _id}.
   * A map or list is copied only when an element changes.
   *
   * @throws MongoServerError if the value (or an element nested in it) has a type that cannot be stored
   */
  @SuppressWarnings("unchecked")
  static Object toStored(final Object value) {
    if (!needsConversion(value))
      return value;
    if (value instanceof ObjectId id)
      return tagged(OBJECT_ID, "hex", id.getHexData());
    if (value instanceof Decimal128 decimal)
      return toBigDecimal(decimal);
    if (value instanceof BinData bin)
      return tagged(BIN_DATA, "data", Base64.getEncoder().encodeToString(bin.getData()));
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
    if (value instanceof Map<?, ?> map)
      return mapToStored((Map<String, Object>) map);
    if (value instanceof List<?> list)
      return listToStored(list);
    if (BinaryTypes.getTypeFromValue(value, null) == -1)
      throw new MongoServerError(ErrorCode.BadValue, "The BSON type " + value.getClass().getSimpleName() + " is not supported");
    return value;
  }

  /**
   * Converts a value for the named top-level field. An {@code _id} keeps an {@link ObjectId} as its plain hex string, the form
   * the primary key lookups and indexes match on; every other field stores it as a tagged map that keeps its type.
   */
  static Object toStored(final String field, final Object value) {
    return "_id".equals(field) ? idToStored(value) : toStored(value);
  }

  /**
   * Replaces every {@link ObjectId} in a filter on {@code _id} (a plain value, an operator document or a list) with its hex
   * string, so that it matches the stored {@code _id}.
   */
  @SuppressWarnings("unchecked")
  static Object idFilter(final Object value) {
    if (value instanceof ObjectId id)
      return id.getHexData();
    if (value instanceof Document document) {
      final Document converted = new Document();
      for (final Map.Entry<String, Object> entry : document.entrySet())
        converted.put(entry.getKey(), idFilter(entry.getValue()));
      return converted;
    }
    if (value instanceof List<?> list) {
      final List<Object> converted = new ArrayList<>(list.size());
      for (final Object item : list)
        converted.add(idFilter(item));
      return converted;
    }
    return value;
  }

  private static Object idToStored(final Object value) {
    return value instanceof ObjectId id ? id.getHexData() : toStored(value);
  }

  private static byte[] hexToBytes(final String hex) {
    if (hex == null || hex.length() != 24)
      throw new IllegalArgumentException("Not an ObjectId hex string");
    final byte[] bytes = new byte[12];
    for (int i = 0; i < 24; i += 2) {
      final int high = Character.digit(hex.charAt(i), 16);
      final int low = Character.digit(hex.charAt(i + 1), 16);
      if (high < 0 || low < 0)
        throw new IllegalArgumentException("Not an ObjectId hex string");
      bytes[i / 2] = (byte) ((high << 4) + low);
    }
    return bytes;
  }

  /**
   * Refuses a document that uses the reserved tag as a field name, whatever the type of its value.
   */
  static void checkNotReserved(final Map<?, ?> map) {
    if (map.containsKey(TAG))
      throw new MongoServerError(ErrorCode.BadValue, "The field name '" + TAG + "' is reserved");
  }

  private static Object mapToStored(final Map<String, Object> map) {
    checkNotReserved(map);
    Map<String, Object> converted = null;
    int i = 0;
    for (final Map.Entry<String, Object> entry : map.entrySet()) {
      final Object original = entry.getValue();
      final Object stored = toStored(original);
      if (stored != original && converted == null) {
        // first change: copy what was seen unchanged so far
        converted = LinkedHashMap.newLinkedHashMap(map.size());
        int j = 0;
        for (final Map.Entry<String, Object> previous : map.entrySet()) {
          if (j++ >= i)
            break;
          converted.put(previous.getKey(), previous.getValue());
        }
      }
      if (converted != null)
        converted.put(entry.getKey(), stored);
      ++i;
    }
    return converted != null ? converted : map;
  }

  private static Object listToStored(final List<?> list) {
    List<Object> converted = null;
    for (int i = 0; i < list.size(); i++) {
      final Object original = list.get(i);
      final Object stored = toStored(original);
      if (stored != original && converted == null) {
        converted = new ArrayList<>(list.size());
        for (int j = 0; j < i; j++)
          converted.add(list.get(j));
      }
      if (converted != null)
        converted.add(stored);
    }
    return converted != null ? converted : list;
  }

  /**
   * Turns a stored tagged map or DECIMAL back into the BSON object it was written from; any other value is returned as is.
   */
  static Object toBson(final Object value) {
    if (value instanceof BigDecimal decimal) {
      try {
        return new Decimal128(decimal);
      } catch (final ArithmeticException | IllegalArgumentException e) {
        // more than 34 significant digits or an exponent out of range: keep the response readable
        // warn once: a large result set would otherwise flood the log
        LogManager.instance().log(MongoBsonValues.class, WIDE_DECIMAL_WARNED.compareAndSet(false, true) ? Level.WARNING : Level.FINE,
            "A DECIMAL beyond Decimal128 is returned as a double (value %s)", decimal);
        return decimal.doubleValue();
      }
    }
    if (value instanceof Map<?, ?> map && map.get(TAG) instanceof String kind)
      return fromTagged(kind, map);
    return value;
  }

  static boolean isTagged(final Object value) {
    return value instanceof Map<?, ?> map && map.get(TAG) instanceof String;
  }

  /**
   * Exact numeric coercion for arithmetic on a DECIMAL: integers and Decimal128 keep every digit.
   *
   * @throws MongoServerError for a NaN or infinite floating point value, which has no decimal form
   */
  static BigDecimal toBigDecimal(final Number number) {
    if (number instanceof BigDecimal decimal)
      return decimal;
    if (number instanceof Decimal128 decimal)
      return toBigDecimal(decimal);
    if (number instanceof Long || number instanceof Integer || number instanceof Short || number instanceof Byte)
      return BigDecimal.valueOf(number.longValue());
    final double value = number.doubleValue();
    if (!Double.isFinite(value))
      throw new MongoServerError(ErrorCode.BadValue, "The value " + value + " (NaN or Infinity) cannot be combined with a decimal");
    return BigDecimal.valueOf(value);
  }

  private static BigDecimal toBigDecimal(final Decimal128 decimal) {
    try {
      return decimal.toBigDecimal();
    } catch (final ArithmeticException e) {
      // BigDecimal has no negative zero: store it as zero, which compares equal
      if (decimal.doubleValue() == 0)
        return BigDecimal.ZERO;
      throw new MongoServerError(ErrorCode.BadValue, "The Decimal128 value " + decimal + " (NaN or Infinity) cannot be stored");
    }
  }

  /**
   * A map that is tagged but does not have the shape written by {@link #toStored} (data written through SQL or another
   * protocol) is returned as a plain map rather than failing the whole read.
   */
  private static Object fromTagged(final String kind, final Map<?, ?> map) {
    try {
      return decode(kind, map);
    } catch (final ClassCastException | NullPointerException | IllegalArgumentException e) {
      return map;
    }
  }

  private static Object decode(final String kind, final Map<?, ?> map) {
    return switch (kind) {
      case OBJECT_ID -> new ObjectId(hexToBytes((String) map.get("hex")));
      case BIN_DATA -> new BinData(Base64.getDecoder().decode((String) map.get("data")));
      case REGEX -> new BsonRegularExpression((String) map.get("pattern"), (String) map.get("options"));
      case TIMESTAMP -> new BsonTimestamp(((Number) map.get("value")).longValue());
      case MIN_KEY -> MinKey.getInstance();
      case MAX_KEY -> MaxKey.getInstance();
      case JAVASCRIPT -> new BsonJavaScript((String) map.get("code"));
      case LEGACY_UUID -> new LegacyUUID(UUID.fromString((String) map.get("uuid")));
      default -> map;
    };
  }

  private static Map<String, Object> tagged(final String kind, final Object... keyValues) {
    final Map<String, Object> map = LinkedHashMap.newLinkedHashMap(keyValues.length / 2 + 1);
    map.put(TAG, kind);
    for (int i = 0; i < keyValues.length; i += 2)
      map.put((String) keyValues[i], keyValues[i + 1]);
    return map;
  }
}
