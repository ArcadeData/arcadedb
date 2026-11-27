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
 * {@link BigDecimal} (a DECIMAL) and read back as a Decimal128, but for the ones a BigDecimal has no form for (a negative zero,
 * NaN and the infinities), which are stored tagged with their 128 bits (issue #12073). A value the engine cannot hold at all is
 * refused with an error instead of being silently dropped.
 * <p>
 * An {@link ObjectId} {@code _id} is stored as its bare hex string, the form the {@code _id} has always had, and a String {@code _id}
 * that reads as such a hex string is stored escaped, so that the two are two values to the unique index, to the filters and to the
 * reads (issue #12073). A bare hex {@code _id} stored before that reads back as an ObjectId, as it always did, and is that
 * ObjectId to the filters and to the unique index: the bare form cannot tell whether it was written as a String, so it keeps the
 * meaning every read has given it. A String filter (or insert) with that hex is therefore another value, as an ObjectId and a
 * String are in {@code mongod}.
 * <p>
 * A tagged value is a map to the engine, so SQL compares maps: the plugin's own filters and sorts compare the BSON value it is
 * read back as ({@link MongoQueryMatcher}, {@link MongoSortOrder}), a statement written in SQL does not. A DECIMAL beyond
 * Decimal128 (more than 34 digits, an exponent out of its range, or an unscaled value of more than 12321 digits) is read back as a
 * double rather than failing the read.
 * <p>
 * The field name {@code $bson} is reserved for the tag: a map with a string {@code $bson} key written through SQL or another
 * protocol is read back by the MongoDB plugin as the BSON value it names (a malformed or unknown tag stays a plain map).
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
final class MongoBsonValues {
  static final String TAG = "$bson";

  private static final AtomicBoolean WIDE_DECIMAL_WARNED = new AtomicBoolean();

  // the widest unscaled value, in bits, that is handed to the Decimal128 conversion: for a scale inside the exponent range of a
  // Decimal128, 34 digits of coefficient and an exponent from -6176 to 6111 make 12321 digits at most, which is under 2^40930. A wider
  // unscaled value is returned as a double without converting its digits: it can only be a Decimal128 when it is padded with thousands
  // of zeros beyond its scale, and finding that out takes a conversion of every digit
  private static final int MAX_DECIMAL128_BITS = 40_930;

  // an ObjectId outside _id is kept as a prefixed string, not a map, so that it stays comparable and usable as an index key
  static final String OBJECT_ID_PREFIX = "$oid:";
  private static final String ESCAPED_PREFIX = "$str:";

  // binary is held as Base64 text, not byte[]: a byte[] inside a map compares by identity, which would break eq and $in filters
  private static final String BIN_DATA = "binData";
  private static final String REGEX = "regex";
  private static final String TIMESTAMP = "timestamp";
  private static final String MIN_KEY = "minKey";
  private static final String MAX_KEY = "maxKey";
  private static final String JAVASCRIPT = "javascript";
  private static final String LEGACY_UUID = "legacyUuid";
  private static final String DECIMAL128  = "decimal128";

  // the encoding of the special values of a Decimal128, read from its high 64 bits
  private static final long DECIMAL_SPECIAL  = 0x7C00000000000000L;
  private static final long DECIMAL_INFINITY = 0x7800000000000000L;

  /**
   * A value already in the form it is stored in, which the binding of a filter must not convert again: the bound of a filter on the
   * {@code _id}, whose stored form (see {@link #idToStored}) is not the one of any other field.
   */
  record Stored(Object value) {
  }

  private MongoBsonValues() {
  }

  /**
   * Whether {@link #toStored} may change this value, that is whether it is anything but a plain string, number or boolean.
   */
  static boolean needsConversion(final Object value) {
    if (value instanceof String string)
      return !string.isEmpty() && string.charAt(0) == '$';
    return !(value == null || value instanceof Boolean || value instanceof Number && !(value instanceof Decimal128));
  }

  /**
   * Converts a value used in a filter into the form it is stored in. A regular expression is left alone: only the matcher evaluates
   * one, as a pattern or, under {@code $eq}, as a value compared with a stored regular expression, and it never narrows the SQL
   * candidates. A Decimal128 NaN or Infinity is refused, as it is on insert.
   */
  static Object toBound(final Object value) {
    if (value instanceof Stored stored)
      return stored.value();
    return needsConversion(value) && !(value instanceof BsonRegularExpression) ? toStored(value) : value;
  }

  /**
   * Converts a value received from a client into the form stored in the database. An {@link ObjectId} becomes the string
   * {@code $oid:<hex>}; use {@link #toStored(String, Object)} for an {@code _id}.
   * A map or list is copied only when an element changes.
   *
   * @throws MongoServerError if the value (or an element nested in it) has a type that cannot be stored
   */
  @SuppressWarnings("unchecked")
  static Object toStored(final Object value) {
    if (!needsConversion(value))
      return value;
    if (value instanceof String string)
      // a client string that looks like an encoded ObjectId is escaped so that it reads back as the same string
      return string.startsWith(OBJECT_ID_PREFIX) || string.startsWith(ESCAPED_PREFIX) ? ESCAPED_PREFIX + string : string;
    if (value instanceof ObjectId id)
      return OBJECT_ID_PREFIX + id.getHexData();
    if (value instanceof Decimal128 decimal)
      return decimalToStored(decimal);
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
   * the primary key lookups and indexes match on, and escapes a String that reads as one (see {@link #idToStored}); every other
   * field stores an ObjectId as a prefixed string that keeps its type.
   */
  static Object toStored(final String field, final Object value) {
    return "_id".equals(field) ? idToStored(value) : toStored(value);
  }

  /**
   * Replaces every value of a filter on {@code _id} (a plain value, an operator document or a list) with its stored form (see
   * {@link #idToStored}), marked {@link Stored} so that the binding does not convert it again: the SQL then compares it with the
   * stored {@code _id}. A regular expression and a null are left as they are: a pattern, and the absence of a value.
   */
  static Object idFilter(final Object value) {
    if (value == null || value instanceof BsonRegularExpression)
      return value;
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
    return new Stored(idToStored(value));
  }

  /**
   * The stored form of an {@code _id}: an ObjectId is its bare hex string, the form the {@code _id} has always had. A String that
   * reads as such a hex string is escaped, so that it is not the same key as the ObjectId and reads back as a String (issue
   * #12073); any other string is escaped like a string of any field, only when it looks like an encoded value.
   */
  private static Object idToStored(final Object value) {
    if (value instanceof ObjectId id)
      return id.getHexData();
    if (value instanceof String string && MongoDBToSqlTranslator.isObjectIdHex(string))
      return ESCAPED_PREFIX + string;
    return toStored(value);
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
    if (value instanceof BigDecimal decimal)
      return decimalToBson(decimal);
    if (value instanceof String string && string.startsWith(ESCAPED_PREFIX))
      return string.substring(ESCAPED_PREFIX.length());
    if (value instanceof String string && isStoredObjectId(string)) {
      try {
        return MongoDBToSqlTranslator.getObjectId(string.substring(OBJECT_ID_PREFIX.length()));
      } catch (final IllegalArgumentException e) {
        return value;
      }
    }
    if (value instanceof Map<?, ?> map && map.get(TAG) instanceof String kind)
      return fromTagged(kind, map);
    return value;
  }

  /**
   * A DECIMAL as a Decimal128, or as a double when it has no Decimal128 form (more than 34 significant digits, an exponent out of
   * range, or an unscaled value wider than {@code MAX_DECIMAL128_BITS}), so that the response stays readable. The size of the value
   * is checked first, which costs nothing, so that a value that wide is not converted digit by digit by the library to find out.
   */
  private static Object decimalToBson(final BigDecimal decimal) {
    if (decimal.unscaledValue().bitLength() <= MAX_DECIMAL128_BITS)
      try {
        return new Decimal128(decimal);
      } catch (final ArithmeticException | IllegalArgumentException | AssertionError e) {
        // the library reports an exponent out of range as an assertion error, a coefficient it cannot round exactly as an exception
      }
    // warn once: a large result set would otherwise flood the log
    LogManager.instance().log(MongoBsonValues.class, WIDE_DECIMAL_WARNED.compareAndSet(false, true) ? Level.WARNING : Level.FINE,
        "A DECIMAL beyond Decimal128 is returned as a double (scale %d)", decimal.scale());
    return decimal.doubleValue();
  }

  static boolean isStoredObjectId(final String string) {
    return string.startsWith(OBJECT_ID_PREFIX);
  }

  /**
   * Whether a stored string is one of the encoded forms {@link #toBson} decodes (an ObjectId or an escaped string).
   */
  static boolean isEncodedString(final String string) {
    return string.startsWith(OBJECT_ID_PREFIX) || string.startsWith(ESCAPED_PREFIX);
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
    if (isSpecial(decimal))
      throw new MongoServerError(ErrorCode.BadValue, "The Decimal128 value " + decimal + " (NaN or Infinity) cannot be combined with a decimal");
    try {
      return decimal.toBigDecimal();
    } catch (final ArithmeticException e) {
      // BigDecimal has no negative zero: for arithmetic it is zero, which compares equal
      return BigDecimal.ZERO;
    }
  }

  /**
   * A Decimal128 as a DECIMAL, the form SQL compares as a number; tagged with its 128 bits when a BigDecimal has no form for it (a
   * negative zero of any scale, NaN, an infinity), so that it reads back as it was written (issue #12073).
   */
  private static Object decimalToStored(final Decimal128 decimal) {
    if (!isSpecial(decimal))
      try {
        final BigDecimal value = decimal.toBigDecimal();
        // a negative zero converts to a zero without the sign (the sign bit is the top bit of the high word)
        if (value.signum() != 0 || decimal.getHigh() >= 0)
          return value;
      } catch (final ArithmeticException e) {
        // a negative zero the conversion refuses: tagged below
      }
    return tagged(DECIMAL128, "high", decimal.getHigh(), "low", decimal.getLow());
  }

  /** NaN or an infinity, read from the encoding of a Decimal128. */
  private static boolean isSpecial(final Decimal128 decimal) {
    final long special = decimal.getHigh() & DECIMAL_SPECIAL;
    return special == DECIMAL_SPECIAL || special == DECIMAL_INFINITY;
  }

  /**
   * The sum of an {@code $inc} with a decimal operand. NaN and the infinities follow the IEEE rules (NaN wins, opposite infinities
   * make NaN), a finite sum is exact.
   */
  static Number addDecimal(final Number a, final Number b) {
    final double specialA = specialValue(a);
    final double specialB = specialValue(b);
    if (specialA == 0D && specialB == 0D)
      return toBigDecimal(a).add(toBigDecimal(b));
    final double sum = specialA + specialB;
    return Double.isNaN(sum) ? Decimal128.NaN : sum > 0 ? Decimal128.POSITIVE_INFINITY : Decimal128.NEGATIVE_INFINITY;
  }

  /** NaN or the infinity a number is, 0 for a finite number (which does not change a sum with one). */
  private static double specialValue(final Number number) {
    if (number instanceof Decimal128 decimal) {
      final long special = decimal.getHigh() & DECIMAL_SPECIAL;
      if (special == DECIMAL_SPECIAL)
        return Double.NaN;
      if (special == DECIMAL_INFINITY)
        return decimal.getHigh() < 0 ? Double.NEGATIVE_INFINITY : Double.POSITIVE_INFINITY;
      return 0D;
    }
    if (number instanceof Double || number instanceof Float) {
      final double value = number.doubleValue();
      return Double.isFinite(value) ? 0D : value;
    }
    return 0D;
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
      case BIN_DATA -> new BinData(Base64.getDecoder().decode((String) required(map, "data")));
      case REGEX -> new BsonRegularExpression((String) required(map, "pattern"), options(map));
      case TIMESTAMP -> new BsonTimestamp(((Number) required(map, "value")).longValue());
      case MIN_KEY -> MinKey.getInstance();
      case MAX_KEY -> MaxKey.getInstance();
      case JAVASCRIPT -> new BsonJavaScript((String) required(map, "code"));
      case LEGACY_UUID -> new LegacyUUID(UUID.fromString((String) required(map, "uuid")));
      // the constructor takes the low 64 bits first
      case DECIMAL128 -> new Decimal128(((Number) required(map, "low")).longValue(), ((Number) required(map, "high")).longValue());
      default -> map;
    };
  }

  /**
   * A required field of a tagged map; a missing one makes the map malformed, so the caller falls back to the plain map instead of
   * handing a null to the BSON library, where the failure would only show up when the response is encoded.
   */
  private static Object required(final Map<?, ?> map, final String field) {
    final Object value = map.get(field);
    if (value == null)
      throw new IllegalArgumentException("Missing field " + field);
    return value;
  }

  private static String options(final Map<?, ?> map) {
    final Object options = map.get("options");
    return options != null ? (String) options : "";
  }

  private static Map<String, Object> tagged(final String kind, final Object... keyValues) {
    final Map<String, Object> map = LinkedHashMap.newLinkedHashMap(keyValues.length / 2 + 1);
    map.put(TAG, kind);
    for (int i = 0; i < keyValues.length; i += 2)
      map.put((String) keyValues[i], keyValues[i + 1]);
    return map;
  }
}
