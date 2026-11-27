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
package com.arcadedb.postgres;

import com.arcadedb.database.Binary;
import com.arcadedb.database.Database;
import com.arcadedb.database.DatabaseFactory;
import com.arcadedb.database.Document;
import com.arcadedb.database.EmbeddedDocument;
import com.arcadedb.database.Record;
import com.arcadedb.query.opencypher.temporal.CypherDuration;
import com.arcadedb.query.opencypher.temporal.CypherTemporalValue;
import com.arcadedb.query.opencypher.traversal.TraversalPath;
import com.arcadedb.query.sql.executor.Result;
import com.arcadedb.schema.Type;
import com.arcadedb.serializer.JsonSerializer;
import com.arcadedb.serializer.json.JSONArray;
import com.arcadedb.serializer.json.JSONObject;
import com.google.gson.JsonArray;
import com.google.gson.JsonElement;
import com.google.gson.JsonParser;
import com.google.gson.Strictness;
import com.google.gson.stream.JsonReader;
import com.google.gson.stream.JsonToken;

import java.io.IOException;
import java.io.StringReader;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.math.RoundingMode;
import java.nio.ByteBuffer;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.OffsetDateTime;
import java.time.OffsetTime;
import java.time.ZoneOffset;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeFormatterBuilder;
import java.time.format.DateTimeParseException;
import java.time.format.ResolverStyle;
import java.time.temporal.ChronoField;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Date;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.UUID;
import java.util.function.Function;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

/**
 * Represents PostgreSQL data types and provides serialization/deserialization functionality.
 */
public enum PostgresType {
  // The three catalog columns after the OID are pg_type's own: typname, typelem (the element type of an
  // array, 0 for a scalar) and typarray (the array type built on a scalar, 0 for an array). They are the
  // answer this protocol gives a client that enumerates or looks up pg_type (issue #5290), so they hold
  // PostgreSQL's real names and OIDs rather than anything ArcadeDB-specific: a client resolves the OID it
  // was handed in RowDescription against them, and a name that is not Postgres' name resolves to nothing.
  SMALLINT(21, "int2", 0, 1005, Short.class, 2, value -> Short.parseShort(value)),
  INTEGER(23, "int4", 0, 1007, Integer.class, 4, value -> Integer.parseInt(value)),
  LONG(20, "int8", 0, 1016, Long.class, 8, value -> Long.parseLong(value)),
  REAL(700, "float4", 0, 1021, Float.class, 4, value -> Float.parseFloat(value)),
  DOUBLE(701, "float8", 0, 1022, Double.class, 8, value -> Double.parseDouble(value)),
  CHAR(18, "char", 0, 1002, Character.class, 1, value -> value.charAt(0)),
  BOOLEAN(16, "bool", 0, 1000, Boolean.class, 1, value -> parseBooleanText(value)),
  DATE(1082, "date", 0, 1182, LocalDate.class, 4, value -> parseDateText(value)),
  TIMESTAMP(1114, "timestamp", 0, 1115, LocalDateTime.class, 8, value -> parseTimestampText(value)),
  VARCHAR(1043, "varchar", 0, 1015, String.class, -1, value -> value),
  TEXT(25, "text", 0, 1009, String.class, -1, value -> value),
  BPCHAR(1042, "bpchar", 0, 1014, String.class, -1, value -> value),
  JSON(114, "json", 0, 199, JSONObject.class, -1, PostgresType::parseJsonText),
  // The types a driver binds a zoned datetime, a time of day, a duration or a Jsonb wrapper as (issue #11960): each one used to
  // fail the Bind with "not supported for deserializing". They are read as parameters only, into the engine's ZONED_DATETIME,
  // LOCAL_TIME, OFFSET_TIME, DURATION and the json value; this protocol never announces a column under them. typarray names the
  // array type PostgreSQL builds on each, which is a row of pg_type too (issue #12449).
  TIMESTAMPTZ(1184, "timestamptz", 0, 1185, ZonedDateTime.class, 8, value -> parseTimestampTzText(value)),
  TIME(1083, "time", 0, 1183, LocalTime.class, 8, value -> parseTimeText(value)),
  TIMETZ(1266, "timetz", 0, 1270, OffsetTime.class, 12, value -> parseTimeTzText(value)),
  INTERVAL(1186, "interval", 0, 1187, CypherDuration.class, 16, value -> parseIntervalText(value)),
  JSONB(3802, "jsonb", 0, 3807, JSONObject.class, -1, PostgresType::parseJsonText),
  // The type PostgreSQL has for a blob, and the only honest answer for a byte[] (issue #6411). A byte[] used
  // to be typed as "char"[] (OID 1002) by getTypeForValue and as varchar (OID 1043) by getTypeFromArcade, so
  // a BINARY column's OID depended on whether the result set happened to be empty - and neither answer was
  // right: an array of one-byte characters makes the client decode arbitrary bytes as text, which for any
  // non-UTF-8 payload loses data rather than merely looking odd.
  BYTEA(17, "bytea", 0, 1001, byte[].class, -1, PostgresType::parseByteaText),
  // PostgreSQL's arbitrary-precision decimal (issue #6447). DECIMAL used to be typed as DOUBLE by the schema
  // path (lossy for a BigDecimal outside float64's range/precision) and as VARCHAR by the value path (forces a
  // client to treat the column as text) - neither was right, and the two disagreeing meant a column's OID also
  // depended on whether the result set was empty. NUMERIC is the honest answer PostgreSQL already has for this.
  NUMERIC(1700, "numeric", 0, 1231, BigDecimal.class, -1, BigDecimal::new),
  // Adding array types with PostgreSQL array type codes
  ARRAY_INT(1007, "_int4", 23, 0, Collection.class, -1, value -> parseArrayFromString(value, Integer::parseInt)),
  // 1002 is "char"[], the array of the single-byte CHAR (OID 18) this enum pairs it with. It used to be
  // declared as 1003, which in PostgreSQL is name[] - an unrelated type - so a client resolving the OID
  // this protocol announced for a list of characters was told a type ArcadeDB never produces (issue #5290).
  ARRAY_CHAR(1002, "_char", 18, 0, Collection.class, -1, value -> parseArrayFromString(value, s -> s.charAt(0))),
  ARRAY_LONG(1016, "_int8", 20, 0, Collection.class, -1, value -> parseArrayFromString(value, Long::parseLong)),
  ARRAY_REAL(1021, "_float4", 700, 0, Collection.class, -1, value -> parseArrayFromString(value, Float::parseFloat)),
  ARRAY_DOUBLE(1022, "_float8", 701, 0, Collection.class, -1, value -> parseArrayFromString(value, Double::parseDouble)),
  ARRAY_TEXT(1009, "_text", 25, 0, Collection.class, -1, value -> parseArrayFromString(value, s -> s)),
  ARRAY_JSON(199, "_json", 114, 0, Collection.class, -1, value -> parseArrayFromString(value, s -> s)),
  ARRAY_BOOLEAN(1000, "_bool", 16, 0, Collection.class, -1, value -> parseArrayFromString(value, PostgresType::parseBooleanText)),
  // numeric[]: the one array type that holds every integer, long, float, double and BigDecimal exactly (issue #9008)
  ARRAY_NUMERIC(1231, "_numeric", 1700, 0, Collection.class, -1, value -> parseArrayFromString(value, BigDecimal::new)),
  // The array types of the scalars above that had none (issue #10592): each scalar's typarray named one of these OIDs, and a client
  // resolving it - pgjdbc's createArrayOf("varchar", ...) binds a parameter of type 1015 - found no type behind it. This protocol
  // never announces them for a column, it reads them as parameters and lists them in pg_type.
  ARRAY_SMALLINT(1005, "_int2", 21, 0, Collection.class, -1, value -> parseArrayFromString(value, Short::parseShort)),
  ARRAY_BYTEA(1001, "_bytea", 17, 0, Collection.class, -1, value -> parseArrayFromString(value, PostgresType::parseByteaText)),
  ARRAY_BPCHAR(1014, "_bpchar", 1042, 0, Collection.class, -1, value -> parseArrayFromString(value, s -> s)),
  ARRAY_VARCHAR(1015, "_varchar", 1043, 0, Collection.class, -1, value -> parseArrayFromString(value, s -> s)),
  ARRAY_DATE(1182, "_date", 1082, 0, Collection.class, -1, value -> parseArrayFromString(value, PostgresType::parseDateText)),
  ARRAY_TIMESTAMP(1115, "_timestamp", 1114, 0, Collection.class, -1,
      value -> parseArrayFromString(value, PostgresType::parseTimestampText)),
  // The types a client looks up in pg_type by name while it sets up a connection or reads metadata, though this protocol never announces
  // a column under them (issue #11027): every identifier column of the real catalog is name, and pgjdbc answers getMax*NameLength()
  // and getClientInfoProperties() from the typlen of the row called "name"; the announced OID of an identifier column stays varchar.
  // name and int2vector carry the typelem PostgreSQL gives them, "char" and int2 (issue #12449), without being arrays of this protocol:
  // name is a fixed-length string, int2vector a vector with a text format of its own, so neither is read or written as one.
  NAME(19, "name", 18, 1003, String.class, 64, value -> value, false),
  OID(26, "oid", 0, 1028, Long.class, 4, value -> Long.parseLong(value)),
  INT2VECTOR(22, "int2vector", 21, 1006, String.class, -1, value -> value, false),
  // The array types of the scalars above that had none (issue #12449), each named by its scalar's typarray. As the ones of #10592,
  // this protocol never announces them for a column: it reads them as parameters and lists them in pg_type.
  ARRAY_TIMESTAMPTZ(1185, "_timestamptz", 1184, 0, Collection.class, -1,
      value -> parseArrayFromString(value, PostgresType::parseTimestampTzText)),
  ARRAY_TIME(1183, "_time", 1083, 0, Collection.class, -1, value -> parseArrayFromString(value, PostgresType::parseTimeText)),
  ARRAY_TIMETZ(1270, "_timetz", 1266, 0, Collection.class, -1, value -> parseArrayFromString(value, PostgresType::parseTimeTzText)),
  ARRAY_INTERVAL(1187, "_interval", 1186, 0, Collection.class, -1, value -> parseArrayFromString(value, PostgresType::parseIntervalText)),
  ARRAY_JSONB(3807, "_jsonb", 3802, 0, Collection.class, -1, value -> parseArrayFromString(value, s -> s)),
  ARRAY_NAME(1003, "_name", 19, 0, Collection.class, -1, value -> parseArrayFromString(value, s -> s)),
  ARRAY_OID(1028, "_oid", 26, 0, Collection.class, -1, value -> parseArrayFromString(value, Long::parseLong)),
  ARRAY_INT2VECTOR(1006, "_int2vector", 22, 0, Collection.class, -1, value -> parseArrayFromString(value, s -> s));

  private static final Map<Integer, PostgresType> CODE_MAP = Arrays.stream(values())
      .collect(Collectors.toMap(type -> type.code, type -> type));

  /**
   * Every spelling a client may name a type by: its own {@code typname} plus the SQL-standard names
   * PostgreSQL treats as aliases of it. Built once, because a by-name lookup is a per-query cost.
   */
  private static final Map<String, PostgresType> NAME_MAP = buildNameMap();

  private static Map<String, PostgresType> buildNameMap() {
    final Map<String, PostgresType> names = new HashMap<>();
    for (final PostgresType type : values())
      names.put(type.typeName, type);

    // Only the spellings the loop above did not already install: every entry here is a SQL-standard name
    // that is not any type's own typname.
    names.put("smallint", SMALLINT);
    names.put("integer", INTEGER);
    names.put("int", INTEGER);
    names.put("bigint", LONG);
    names.put("real", REAL);
    names.put("double precision", DOUBLE);
    names.put("boolean", BOOLEAN);
    names.put("character varying", VARCHAR);
    names.put("character", BPCHAR);
    names.put("decimal", NUMERIC);
    names.put("timestamp without time zone", TIMESTAMP);
    names.put("timestamp with time zone", TIMESTAMPTZ);
    names.put("time without time zone", TIME);
    names.put("time with time zone", TIMETZ);

    return Map.copyOf(names);
  }

  // PostgreSQL-compatible datetime format (ISO 8601 without 'T' separator)
  private static final String            POSTGRES_TIMESTAMP_FORMAT   = "yyyy-MM-dd HH:mm:ss.SSSSSS";
  private static final DateTimeFormatter POSTGRES_DATETIME_FORMATTER = DateTimeFormatter.ofPattern(POSTGRES_TIMESTAMP_FORMAT);
  // PostgreSQL caps array dimensions at 6 (MAXDIM in pg_config_manual.h).
  private static final int               MAX_ARRAY_DIMENSIONS        = 6;
  // PostgreSQL's epoch for DATE/TIMESTAMP binary formats is 2000-01-01T00:00:00 UTC.
  private static final long              POSTGRES_EPOCH_SECONDS      = 946684800L;
  private static final long              POSTGRES_EPOCH_DAYS         = 10957L; // LocalDate.of(2000, 1, 1).toEpochDay()
  // An offset as a client writes it after a time: +02, +02:00, +02:00:00 or Z (lenient parsing takes all of them); the compact
  // +0200 and +020000 are given their colons first by withOffsetColons.
  private static final DateTimeFormatter OFFSET_FORMATTER          = new DateTimeFormatterBuilder().parseLenient()
      .appendOffset("+HH:MM:ss", "Z").toFormatter();
  private static final Pattern           COMPACT_OFFSET            = Pattern.compile("([+-]\\d{2})(\\d{2})(\\d{2})?$");
  // STRICT: a date the calendar does not have (2024-02-30) is refused, not moved to the last day of the month
  private static final DateTimeFormatter TIMESTAMPTZ_FORMATTER     = new DateTimeFormatterBuilder()
      .append(DateTimeFormatter.ISO_LOCAL_DATE).appendLiteral('T').append(DateTimeFormatter.ISO_LOCAL_TIME)
      .optionalStart().append(OFFSET_FORMATTER).optionalEnd().toFormatter().withResolverStyle(ResolverStyle.STRICT);
  private static final DateTimeFormatter TIMETZ_FORMATTER          = new DateTimeFormatterBuilder()
      .append(DateTimeFormatter.ISO_LOCAL_TIME).append(OFFSET_FORMATTER).toFormatter();
  // What PostgreSQL writes for a time, a timetz and a timestamptz (issue #12449): the seconds always, a fraction of at most six digits
  // without trailing zeros, and an offset of hours, with the minutes only when there are some (+02, +05:30)
  private static final DateTimeFormatter POSTGRES_TIME_OUTPUT        = new DateTimeFormatterBuilder().appendPattern("HH:mm:ss")
      .appendFraction(ChronoField.NANO_OF_SECOND, 0, 6, true).toFormatter();
  private static final DateTimeFormatter POSTGRES_TIMETZ_OUTPUT      = new DateTimeFormatterBuilder().append(POSTGRES_TIME_OUTPUT)
      .appendOffset("+HH:mm", "+00").toFormatter();
  private static final DateTimeFormatter POSTGRES_TIMESTAMPTZ_OUTPUT = new DateTimeFormatterBuilder().appendPattern("uuuu-MM-dd ")
      .append(POSTGRES_TIMETZ_OUTPUT).toFormatter();
  // PostgreSQL's uuid, which only a bound parameter carries here: no column is announced under it (issue #11838)
  private static final int               UUID_CODE                   = 2950;

  /**
   * Renders a record, a row, a map or a path of a json column as the HTTP API does (issue #11958), vertex edge counts left out as
   * {@code toJSON(true)} left them out. One instance serves every connection: it is configured here and never again, and a
   * serialization keeps no state in the serializer (the date patterns and type hints it needs live in locals of each call).
   */
  private static final JsonSerializer JSON_DOCUMENT_SERIALIZER = JsonSerializer.createJsonSerializer()
      .setIncludeVertexEdges(false)
      .setUseCollectionSize(false)
      .setUseCollectionSizeForEdges(false);

  /**
   * A timestamptz parameter (issue #11960): {@code 2024-02-29 13:14:15.123456+02} as PostgreSQL and pgjdbc write it, or the ISO
   * form with a {@code T}. The offset is kept; a text without one is read as UTC, the zone this protocol reports for the session.
   */
  private static ZonedDateTime parseTimestampTzText(final String value) {
    final String text = value.trim();
    final String iso = text.length() > 10 && text.charAt(10) == ' ' ? text.substring(0, 10) + 'T' + text.substring(11) : text;
    try {
      final var parsed = TIMESTAMPTZ_FORMATTER.parseBest(withOffsetColons(iso.replace(" ", "")), OffsetDateTime::from,
          LocalDateTime::from);
      return parsed instanceof OffsetDateTime offset ? offset.toZonedDateTime() : ((LocalDateTime) parsed).atZone(ZoneOffset.UTC);
    } catch (final DateTimeParseException e) {
      throw unparsableTemporal("TIMESTAMPTZ", value, e);
    }
  }

  private static LocalTime parseTimeText(final String value) {
    try {
      return LocalTime.parse(value.trim());
    } catch (final DateTimeParseException e) {
      throw unparsableTemporal("TIME", value, e);
    }
  }

  private static OffsetTime parseTimeTzText(final String value) {
    try {
      return OffsetTime.parse(withOffsetColons(value.trim().replace(" ", "")), TIMETZ_FORMATTER);
    } catch (final DateTimeParseException e) {
      throw unparsableTemporal("TIMETZ", value, e);
    }
  }

  /** {@code +0200} as {@code +02:00} and {@code +020000} as {@code +02:00:00}, the forms the lenient offset parser reads. */
  private static String withOffsetColons(final String text) {
    final Matcher matcher = COMPACT_OFFSET.matcher(text);
    if (!matcher.find())
      return text;
    final String colons = matcher.group(1) + ':' + matcher.group(2) + (matcher.group(3) != null ? ':' + matcher.group(3) : "");
    return text.substring(0, matcher.start()) + colons;
  }

  private static final BigDecimal MICROS_PER_SECOND = BigDecimal.valueOf(1_000_000L);
  private static final BigDecimal MICROS_PER_DAY    = BigDecimal.valueOf(86_400_000_000L);
  // The digits an interval amount may have on either side of the point. Far more than any interval holds (PostgreSQL counts months
  // and days in an int32 and the rest in int64 microseconds), and few enough that no scaling of an amount costs anything: an
  // exponent such as 1e100000000 would otherwise make setScale build a number of a hundred million digits.
  private static final int        MAX_AMOUNT_DIGITS = 18;
  private static final String     ISO_AMOUNT        = "([-+]?(?:\\d+(?:\\.\\d*)?|\\.\\d+))";
  private static final Pattern    ISO_INTERVAL      = Pattern.compile(
      "([-+])?P(?:" + ISO_AMOUNT + "Y)?(?:" + ISO_AMOUNT + "M)?(?:" + ISO_AMOUNT + "W)?(?:" + ISO_AMOUNT + "D)?(?:T(?:" + ISO_AMOUNT
          + "H)?(?:" + ISO_AMOUNT + "M)?(?:" + ISO_AMOUNT + "S)?)?", Pattern.CASE_INSENSITIVE);
  private static final String[]   ISO_UNITS         = { "years", "months", "weeks", "days", "hours", "minutes", "seconds" };
  // the year-month field of the SQL-standard form, 1-2 for one year and two months
  private static final Pattern    YEAR_MONTH_FIELD  = Pattern.compile("([-+]?)(\\d+)-(\\d+)");

  /**
   * An interval parameter (issue #11960) in the forms PostgreSQL reads: ISO 8601 ({@code P1DT5S}, and the alternative
   * {@code P0001-02-03T04:05:06}), the postgres style psycopg sends ({@code 1 day 0:00:05}, {@code -1 day 23:59:55}), the verbose one
   * pgjdbc sends ({@code 0 years 0 mons 1 days 0 hours 0 mins 5.0 secs}, {@code @ 1 day ago}), the signed sql_standard one
   * ({@code +1 day +5 second +0 microsecond}) and the SQL-standard fields ({@code 1-2} for one year two months, {@code 3 4:05:06}
   * for three days and a time, a lone number for seconds). A fraction of a month spills into days of 30 and a fraction of a day into
   * hours, as in PostgreSQL, in the ISO form too. The result is the engine's DURATION value.
   */
  static CypherDuration parseIntervalText(final String value) {
    final String text = value.trim();
    final long[] monthsAndDays = new long[2];
    BigDecimal micros = BigDecimal.ZERO;
    boolean negate = false;
    try {
      if (text.startsWith("P") || text.startsWith("-P") || text.startsWith("+P") || text.startsWith("p")) {
        final Matcher iso = ISO_INTERVAL.matcher(text);
        if (!iso.matches())
          // the alternative format, P0001-02-03T04:05:06, whose fields are whole numbers
          try {
            return CypherDuration.parse(text);
          } catch (final RuntimeException e) {
            throw new IllegalArgumentException("Cannot parse INTERVAL text value: " + value, e);
          }
        boolean any = false;
        for (int i = 0; i < ISO_UNITS.length; i++) {
          final String amount = iso.group(i + 2);
          if (amount != null) {
            micros = micros.add(addAmount(amount(amount), ISO_UNITS[i], monthsAndDays, value));
            any = true;
          }
        }
        if (!any)
          throw new IllegalArgumentException("Cannot parse INTERVAL text value: " + value);
        negate = "-".equals(iso.group(1));
      } else {
        final String[] tokens = (text.startsWith("@") ? text.substring(1) : text).trim().toLowerCase(Locale.ENGLISH).split("\\s+");
        int count = tokens.length;
        negate = count > 0 && tokens[count - 1].equals("ago");
        if (negate)
          count--;
        if (count == 0 || tokens[0].isEmpty())
          throw new IllegalArgumentException("Cannot parse INTERVAL text value: " + value);

        for (int i = 0; i < count; i++) {
          final String token = tokens[i];
          final Matcher yearMonth = YEAR_MONTH_FIELD.matcher(token);
          if (yearMonth.matches()) {
            final BigDecimal months = amount(yearMonth.group(2)).multiply(BigDecimal.valueOf(12)).add(amount(yearMonth.group(3)));
            micros = micros.add(addMonths("-".equals(yearMonth.group(1)) ? months.negate() : months, monthsAndDays));
          } else if (token.indexOf(':') >= 0)
            micros = micros.add(timeOfIntervalInMicros(token));
          else if (i + 1 >= count)
            // a lone trailing number is seconds, as PostgreSQL reads '5'
            micros = micros.add(amount(token).multiply(MICROS_PER_SECOND));
          else if (tokens[i + 1].indexOf(':') >= 0)
            // a number before the time field is the day field: 3 4:05:06
            micros = micros.add(addDays(amount(token), monthsAndDays));
          else {
            final BigDecimal amount = amount(token);
            micros = micros.add(addAmount(amount, tokens[++i], monthsAndDays, value));
          }
        }
      }
      final long totalMicros = micros.setScale(0, RoundingMode.HALF_EVEN).longValueExact();
      final int sign = negate ? -1 : 1;
      return new CypherDuration(sign * monthsAndDays[0], sign * monthsAndDays[1], sign * Math.floorDiv(totalMicros, 1_000_000L),
          sign * Math.floorMod(totalMicros, 1_000_000L) * 1_000L);
    } catch (final NumberFormatException | ArithmeticException e) {
      throw new IllegalArgumentException("Cannot parse INTERVAL text value: " + value, e);
    }
  }

  /** Adds {@code amount} of {@code unit}: whole months and days into {@code monthsAndDays}, the rest answered in microseconds. */
  private static BigDecimal addAmount(final BigDecimal amount, final String unit, final long[] monthsAndDays, final String value) {
    return switch (unit) {
      // a fraction of a year is rounded to whole months, as PostgreSQL does (1.1 years is 13 months), not spilled into days
      case "year", "years", "y", "yr", "yrs" ->
          addMonths(amount.multiply(BigDecimal.valueOf(12)).setScale(0, RoundingMode.HALF_EVEN), monthsAndDays);
      case "mon", "mons", "month", "months" -> addMonths(amount, monthsAndDays);
      case "week", "weeks", "w" -> addDays(amount.multiply(BigDecimal.valueOf(7)), monthsAndDays);
      case "day", "days", "d" -> addDays(amount, monthsAndDays);
      case "hour", "hours", "h", "hr", "hrs" -> amount.multiply(BigDecimal.valueOf(3_600_000_000L));
      case "minute", "minutes", "min", "mins", "m" -> amount.multiply(BigDecimal.valueOf(60_000_000L));
      case "second", "seconds", "sec", "secs", "s" -> amount.multiply(MICROS_PER_SECOND);
      case "millisecond", "milliseconds", "msec", "msecs", "ms" -> amount.multiply(BigDecimal.valueOf(1_000L));
      case "microsecond", "microseconds", "usec", "usecs", "us" -> amount;
      default -> throw new IllegalArgumentException("Cannot parse INTERVAL text value: " + value + " (unknown unit '" + unit + "')");
    };
  }

  /** One number of an interval, refused before any arithmetic when it has more digits than {@link #MAX_AMOUNT_DIGITS} allows. */
  private static BigDecimal amount(final String token) {
    final BigDecimal amount = new BigDecimal(token);
    if (amount.scale() > MAX_AMOUNT_DIGITS || amount.precision() - amount.scale() > MAX_AMOUNT_DIGITS)
      throw new IllegalArgumentException("INTERVAL amount out of range: " + (token.length() > 40 ? token.substring(0, 40) + "..." : token));
    return amount;
  }

  /** Adds the whole months to {@code monthsAndDays[0]} and answers the microseconds of what a fraction spills into days of 30. */
  private static BigDecimal addMonths(final BigDecimal months, final long[] monthsAndDays) {
    final BigDecimal whole = months.setScale(0, RoundingMode.DOWN);
    monthsAndDays[0] += whole.longValueExact();
    return addDays(months.subtract(whole).multiply(BigDecimal.valueOf(30)), monthsAndDays);
  }

  /** Adds the whole days to {@code monthsAndDays[1]} and answers the fraction of a day in microseconds. */
  private static BigDecimal addDays(final BigDecimal days, final long[] monthsAndDays) {
    final BigDecimal whole = days.setScale(0, RoundingMode.DOWN);
    monthsAndDays[1] += whole.longValueExact();
    return days.subtract(whole).multiply(MICROS_PER_DAY);
  }

  /** The {@code [-]h:mm[:ss[.ffffff]]} part of an interval in microseconds, signed as written. */
  private static BigDecimal timeOfIntervalInMicros(final String token) {
    final boolean negative = token.startsWith("-");
    final String unsigned = negative || token.startsWith("+") ? token.substring(1) : token;
    final String[] parts = unsigned.split(":");
    if (parts.length < 2 || parts.length > 3)
      throw new IllegalArgumentException("Cannot parse INTERVAL time '" + token + "'");
    BigDecimal micros = amount(parts[0]).multiply(BigDecimal.valueOf(3_600_000_000L))
        .add(amount(parts[1]).multiply(BigDecimal.valueOf(60_000_000L)));
    if (parts.length == 3)
      micros = micros.add(amount(parts[2]).multiply(MICROS_PER_SECOND));
    return negative ? micros.negate() : micros;
  }

  /**
   * Parses an inbound json or jsonb value. A list of documents is sent to the client as a json array (issue #5366), so
   * the same payload must be accepted back: an array yields a List, an object a {@link JSONObject}, and a scalar document (a
   * number, a string, {@code true}, {@code false} or {@code null}, which PostgreSQL accepts as json too) its value.
   */
  private static Object parseJsonText(final String value) {
    if (value == null)
      return null;
    final String trimmed = value.trim();
    if (!trimmed.isEmpty() && trimmed.charAt(0) == '[')
      return new JSONArray(trimmed).toList();
    if (!trimmed.isEmpty() && trimmed.charAt(0) == '{')
      return new JSONObject(value);
    return parseJsonScalar(trimmed);
  }

  /**
   * A scalar json document, read strictly: the whole text must be one JSON value, so {@code not_json} (an unquoted string a
   * lenient reader would take as one) is refused, and so is an empty document, which PostgreSQL refuses too and which the parser
   * would otherwise answer as a JSON null. Converted to its Java value by the same reader that converts a JSON array's elements, so
   * a number has the type it would have inside an array.
   */
  private static Object parseJsonScalar(final String text) {
    if (text.isEmpty())
      throw new IllegalArgumentException("Invalid input syntax for type json: the document is empty");
    try {
      final JsonReader reader = new JsonReader(new StringReader(text));
      reader.setStrictness(Strictness.STRICT);
      final JsonElement element = JsonParser.parseReader(reader);
      if (reader.peek() != JsonToken.END_DOCUMENT)
        throw new IllegalArgumentException("Invalid input syntax for type json: '" + abbreviate(text) + "'");
      final JsonArray holder = new JsonArray(1);
      holder.add(element);
      return new JSONArray(holder).toList().getFirst();
    } catch (final IllegalArgumentException e) {
      throw e;
    } catch (final IOException | RuntimeException e) {
      throw new IllegalArgumentException("Invalid input syntax for type json: '" + abbreviate(text) + "'", e);
    }
  }

  private static String abbreviate(final String text) {
    return text.length() > 40 ? text.substring(0, 40) + "..." : text;
  }

  /**
   * Parses the text representation of a bytea. PostgreSQL emits the hex format {@code \x<hexdigits>} for any
   * server version from 9.0 on - which is what this protocol announces - but still accepts the older escape
   * format on input, and some clients still send it, so both are read here.
   */
  private static byte[] parseByteaText(final String value) {
    if (value == null)
      throw new PostgresProtocolException("Cannot parse null BYTEA text value");

    if (value.length() >= 2 && value.charAt(0) == '\\' && (value.charAt(1) == 'x' || value.charAt(1) == 'X')) {
      final int digits = value.length() - 2;
      if (digits % 2 != 0)
        throw new PostgresProtocolException("Invalid hex BYTEA text value: odd number of digits");

      final byte[] bytes = new byte[digits / 2];
      for (int i = 0; i < bytes.length; i++) {
        final int high = Character.digit(value.charAt(2 + i * 2), 16);
        final int low = Character.digit(value.charAt(3 + i * 2), 16);
        if (high < 0 || low < 0)
          throw new PostgresProtocolException("Invalid hex BYTEA text value: not a hex digit");
        bytes[i] = (byte) ((high << 4) | low);
      }
      return bytes;
    }

    return parseByteaEscapeText(value);
  }

  /**
   * Decodes the pre-9.0 bytea escape format: a backslash introduces either another backslash or a three-digit
   * octal byte, and everything else stands for itself.
   */
  private static byte[] parseByteaEscapeText(final String value) {
    final byte[] bytes = new byte[value.length()];
    int len = 0;
    for (int i = 0; i < value.length(); i++) {
      final char c = value.charAt(i);
      if (c != '\\') {
        bytes[len++] = (byte) c;
        continue;
      }

      if (i + 1 >= value.length())
        throw new PostgresProtocolException("Invalid escaped BYTEA text value: trailing backslash");

      final char next = value.charAt(++i);
      if (next == '\\') {
        bytes[len++] = (byte) '\\';
        continue;
      }

      if (i + 2 >= value.length())
        throw new PostgresProtocolException("Invalid escaped BYTEA text value: truncated octal escape");

      int octal = 0;
      for (int digit = 0; digit < 3; digit++) {
        final int parsed = Character.digit(value.charAt(i + digit), 8);
        if (parsed < 0)
          throw new PostgresProtocolException("Invalid escaped BYTEA text value: not an octal digit");
        octal = octal * 8 + parsed;
      }
      i += 2;
      bytes[len++] = (byte) octal;
    }
    return Arrays.copyOf(bytes, len);
  }

  /** Renders bytes in the {@code \x<hexdigits>} form PostgreSQL 9.0 and later emit. */
  private static String toByteaText(final byte[] bytes) {
    final StringBuilder buffer = new StringBuilder(2 + bytes.length * 2);
    buffer.append("\\x");
    for (final byte b : bytes) {
      buffer.append(Character.forDigit((b >> 4) & 0xF, 16));
      buffer.append(Character.forDigit(b & 0xF, 16));
    }
    return buffer.toString();
  }

  /** The bytes behind a value announced as bytea, or null when the value is not one. */
  private static byte[] byteaValueOf(final Object value) {
    if (value instanceof byte[] bytes)
      return bytes;
    if (value instanceof Binary binary)
      return binary.toByteArray();
    return null;
  }

  private static Boolean parseBooleanText(final String value) {
    if (value == null)
      throw new PostgresProtocolException("Cannot parse null BOOLEAN text value");
    return switch (value.toLowerCase(Locale.ROOT)) {
      case "t", "true", "1", "y", "yes", "on" -> Boolean.TRUE;
      case "f", "false", "0", "n", "no", "off" -> Boolean.FALSE;
      default -> throw new PostgresProtocolException("Cannot parse BOOLEAN text value: " + value);
    };
  }

  /**
   * A DATE parameter as the {@link LocalDate} it names, the engine's default DATE value (arcadedb.dateImplementation) and what
   * a Bolt date decodes to. A {@link Date} at UTC midnight was a datetime to the engine: a Cypher equality on a DATE property
   * never matched it and an undeclared property stored it as a DATETIME (issue #11956).
   */
  private static LocalDate parseDateText(final String value) {
    if (value == null)
      throw new PostgresProtocolException("Cannot parse null DATE text value");
    try {
      return LocalDate.parse(value);
    } catch (final DateTimeParseException e) {
      throw unparsableTemporal("DATE", value, e);
    }
  }

  private static LocalDateTime parseTimestampText(final String value) {
    if (value == null)
      throw new PostgresProtocolException("Cannot parse null TIMESTAMP text value");
    final String iso = value.replace(' ', 'T');
    try {
      return LocalDateTime.parse(iso);
    } catch (final DateTimeParseException e) {
      try {
        return OffsetDateTime.parse(iso).toLocalDateTime();
      } catch (final DateTimeParseException offsetRefused) {
        throw unparsableTemporal("TIMESTAMP", value, offsetRefused);
      }
    }
  }

  /**
   * A parameter whose text is no date: the client's value, refused as an {@link IllegalArgumentException} carrying the parse
   * failure, which the wire answers {@code 22007} invalid_datetime_format like the same text written into a statement. Raised
   * raw, the {@link DateTimeParseException} was classified as a server fault and answered {@code XX000} (issue #11314).
   */
  private static IllegalArgumentException unparsableTemporal(final String type, final String value, final DateTimeParseException cause) {
    return new IllegalArgumentException("Cannot parse " + type + " text value: " + value, cause);
  }

  private static Number toNumber(final Object value) {
    if (value instanceof Number n)
      return n;
    if (value instanceof Boolean b)
      return b ? 1 : 0;
    if (value instanceof Character c)
      return (int) c;
    return new BigDecimal(value.toString());
  }

  private static BigDecimal toBigDecimalValue(final Object value) {
    if (value instanceof BigDecimal bd)
      return bd;
    if (value instanceof Number n)
      return new BigDecimal(n.toString());
    return new BigDecimal(value.toString());
  }

  // NUMERIC's binary wire format (PostgreSQL src/backend/utils/adt/numeric.c) is a header of four int16s -
  // ndigits, weight, sign, dscale - followed by ndigits base-10000 digits, most significant first. weight is
  // the base-10000 exponent of the first digit, so the decimal point always falls on a 4-decimal-digit
  // boundary; leading and trailing all-zero digit groups are dropped (weight/dscale carry that information
  // instead), which is why a value like 0.5 encodes as a single digit [5000] at weight -1 rather than padding
  // out to whatever precision produced it.
  private static final int NUMERIC_DIGIT_WIDTH = 4;
  private static final int NUMERIC_BASE        = 10000;
  private static final int NUMERIC_POS_SIGN    = 0x0000;
  private static final int NUMERIC_NEG_SIGN    = 0x4000;
  private static final int NUMERIC_NAN_SIGN    = 0xC000;
  // PostgreSQL's own cap (NUMERIC_MAX_RESULT_SCALE-derived) on digit groups in one value; a declared count
  // above this cannot be a value this codec produced, only a malformed or adversarial payload.
  private static final int NUMERIC_MAX_DIGITS  = 4000;
  // The largest bit length a BigInteger with NUMERIC_MAX_DIGITS * NUMERIC_DIGIT_WIDTH (16000) decimal digits
  // can have: ceil(16000 * log2(10)) = ceil(16000 * 3.321928094887362) = 53151. Any BigInteger past this bound
  // has more than 16000 decimal digits, guaranteed - checking bitLength() (already computed, effectively O(1))
  // catches that before putNumericBinary calls toString() on it, which is not.
  private static final int NUMERIC_MAX_UNSCALED_BITS = 53151;

  /**
   * Encodes a BigDecimal in PostgreSQL's NUMERIC binary wire format. Unlike {@link #parseNumericBinary}, which
   * only has to reject bytes an attacker sent, this also has to reject a BigDecimal ArcadeDB itself produced:
   * a DECIMAL property has no configured precision/scale limit, so a value with a large enough scale would
   * otherwise silently wrap through the header fields' {@code (short)} casts below and write a self-
   * inconsistent payload - the {@code putInt} length prefix would stay correct, but a real client decoding the
   * wrapped {@code ndigits}/{@code weight}/{@code dscale} out of that payload would not agree with it.
   */
  private static void putNumericBinary(final Binary typeBuffer, final BigDecimal decimalValue) {
    // Checked on the RAW scale, before any normalization. BigDecimal(String) parsing scientific notation (e.g.
    // a plain SQL literal like '1E2000000000' - DECIMAL has no configured precision/scale limit) is itself O(1):
    // it just stores unscaledValue=1, scale=-2000000000. setScale(0) below is not - going from a very negative
    // scale up to 0 has to actually materialize unscaledValue * 10^|scale| as a real BigInteger. Rejecting the
    // raw scale first means that materialization, and the multi-GB allocation it would otherwise trigger, never
    // starts. -(long) avoids the int overflow of negating Integer.MIN_VALUE.
    if (decimalValue.scale() < 0 && -(long) decimalValue.scale() > NUMERIC_MAX_DIGITS * NUMERIC_DIGIT_WIDTH)
      throw new PostgresProtocolException("NUMERIC scale exceeds the supported maximum: " + decimalValue.scale());

    final BigDecimal normalized = decimalValue.scale() < 0 ? decimalValue.setScale(0) : decimalValue;
    final int dscale = normalized.scale();
    // Checked before any padding so a value with an absurd scale fails fast instead of first allocating a
    // proportionally huge digit string.
    if (dscale < 0 || dscale > NUMERIC_MAX_DIGITS * NUMERIC_DIGIT_WIDTH)
      throw new PostgresProtocolException("NUMERIC scale exceeds the supported maximum: " + dscale);

    final boolean negative = normalized.signum() < 0;
    final BigInteger unscaled = normalized.unscaledValue().abs();
    // Bounds the MAGNITUDE the same way the checks above bound the SCALE: a short scientific-notation literal
    // like '1E2000000000' is already rejected by the scale check (it always implies a very negative scale),
    // but a value that is simply a great many literal digits at a small/zero scale - reachable from a compact
    // SQL expression rather than a giant literal - would otherwise sail past both scale checks and reach the
    // O(n) toString()/padding work below before the digit-count check further down ever gets a chance to
    // reject it.
    if (unscaled.bitLength() > NUMERIC_MAX_UNSCALED_BITS)
      throw new PostgresProtocolException("NUMERIC precision exceeds the supported maximum: " + unscaled.bitLength() + " bits");

    if (unscaled.equals(BigInteger.ZERO)) {
      typeBuffer.putInt(8);
      typeBuffer.putShort((short) 0);
      typeBuffer.putShort((short) 0);
      typeBuffer.putShort((short) NUMERIC_POS_SIGN);
      typeBuffer.putShort((short) dscale);
      return;
    }

    // Position the digits so index 0 is the most significant decimal digit and the decimal point sits
    // `dscale` digits from the right, padding with leading/trailing zeros so both the integer part and the
    // fractional part are a whole number of 4-digit groups.
    String digits = unscaled.toString();
    if (dscale > digits.length())
      digits = "0".repeat(dscale - digits.length()) + digits;
    int integerDigitCount = digits.length() - dscale;

    final int fractionPad = (NUMERIC_DIGIT_WIDTH - dscale % NUMERIC_DIGIT_WIDTH) % NUMERIC_DIGIT_WIDTH;
    digits = digits + "0".repeat(fractionPad);

    final int leadPad = (NUMERIC_DIGIT_WIDTH - integerDigitCount % NUMERIC_DIGIT_WIDTH) % NUMERIC_DIGIT_WIDTH;
    digits = "0".repeat(leadPad) + digits;
    integerDigitCount += leadPad;

    final int groupCount = digits.length() / NUMERIC_DIGIT_WIDTH;
    final int integerGroups = integerDigitCount / NUMERIC_DIGIT_WIDTH;

    final int[] groups = new int[groupCount];
    for (int i = 0; i < groupCount; i++)
      groups[i] = Integer.parseInt(digits.substring(i * NUMERIC_DIGIT_WIDTH, (i + 1) * NUMERIC_DIGIT_WIDTH));

    int start = 0;
    while (start < groupCount - 1 && groups[start] == 0)
      start++;
    int end = groupCount - 1;
    while (end > start && groups[end] == 0)
      end--;

    final int ndigits = end - start + 1;
    final int weight = integerGroups - 1 - start;

    // Both fields are about to go through a (short) cast: reject rather than let either wrap silently into a
    // header that no longer matches the digits actually written.
    if (ndigits > NUMERIC_MAX_DIGITS)
      throw new PostgresProtocolException("NUMERIC digit count exceeds the supported maximum: " + ndigits);
    if (weight < Short.MIN_VALUE || weight > Short.MAX_VALUE)
      throw new PostgresProtocolException("NUMERIC weight exceeds the supported range: " + weight);

    typeBuffer.putInt(8 + ndigits * 2);
    typeBuffer.putShort((short) ndigits);
    typeBuffer.putShort((short) weight);
    typeBuffer.putShort((short) (negative ? NUMERIC_NEG_SIGN : NUMERIC_POS_SIGN));
    typeBuffer.putShort((short) dscale);
    for (int i = 0; i < ndigits; i++)
      typeBuffer.putShort((short) groups[start + i]);
  }

  /** Decodes PostgreSQL's NUMERIC binary wire format, the inverse of {@link #putNumericBinary}. */
  private static BigDecimal parseNumericBinary(final ByteBuffer buffer) {
    final int ndigits = buffer.getShort() & 0xFFFF;
    final int weight = buffer.getShort();
    final int sign = buffer.getShort() & 0xFFFF;
    final int dscale = buffer.getShort() & 0xFFFF;

    if (sign == NUMERIC_NAN_SIGN)
      throw new PostgresProtocolException("NaN NUMERIC values are not supported");
    if (sign != NUMERIC_POS_SIGN && sign != NUMERIC_NEG_SIGN)
      throw new PostgresProtocolException("Invalid NUMERIC sign: " + sign);
    if (ndigits > NUMERIC_MAX_DIGITS)
      throw new PostgresProtocolException("NUMERIC digit count exceeds the supported maximum: " + ndigits);
    if (ndigits * 2 > buffer.remaining())
      throw new PostgresProtocolException("NUMERIC digit count exceeds remaining buffer bytes");
    // dscale is an unsigned 16-bit field, so this cannot runaway unbounded, but a declared scale wildly beyond
    // what ndigits could ever back is still a malformed payload rather than one this codec produced - and
    // setScale below would otherwise pad a BigInteger out to it. Same bound the encode side rejects at.
    if (dscale > NUMERIC_MAX_DIGITS * NUMERIC_DIGIT_WIDTH)
      throw new PostgresProtocolException("NUMERIC scale exceeds the supported maximum: " + dscale);
    // weight is otherwise unbounded within its short range: a payload with an extreme weight combined with a
    // small ndigits and the max-allowed dscale derives a `scale` far apart from dscale, and setScale below
    // would materialize a BigInteger to bridge that gap - the same class of unbounded-materialization risk
    // putNumericBinary guards against on the encode side, just with a smaller blast radius here because weight
    // is capped by its own wire width rather than an arbitrary int.
    if (weight < -NUMERIC_MAX_DIGITS || weight > NUMERIC_MAX_DIGITS)
      throw new PostgresProtocolException("NUMERIC weight exceeds the supported range: " + weight);

    if (ndigits == 0)
      return BigDecimal.ZERO.setScale(dscale);

    BigInteger magnitude = BigInteger.ZERO;
    for (int i = 0; i < ndigits; i++) {
      final int digit = buffer.getShort() & 0xFFFF;
      if (digit >= NUMERIC_BASE)
        throw new PostgresProtocolException("Invalid NUMERIC digit: " + digit);
      magnitude = magnitude.multiply(BigInteger.valueOf(NUMERIC_BASE)).add(BigInteger.valueOf(digit));
    }

    // The digit sequence runs from base-10000 exponent `weight` down to `weight - ndigits + 1`; each step is
    // NUMERIC_DIGIT_WIDTH decimal digits, so the whole magnitude sits at decimal scale -4*(weight-ndigits+1).
    final int scale = NUMERIC_DIGIT_WIDTH * (ndigits - 1 - weight);
    BigDecimal result = new BigDecimal(magnitude, scale);
    if (sign == NUMERIC_NEG_SIGN)
      result = result.negate();

    // Trailing zero digit groups are dropped by the encoder, so the scale derived from `weight`/`ndigits` can
    // be smaller than dscale; padding back up to it is always exact (the dropped digits were zero) - unless the
    // payload is malformed and dscale does not actually match the precision ndigits/weight encode, in which
    // case this is a value this codec could never have produced rather than a genuine JVM arithmetic error.
    try {
      return result.setScale(dscale, RoundingMode.UNNECESSARY);
    } catch (final ArithmeticException e) {
      throw new PostgresProtocolException("NUMERIC dscale does not match the encoded precision: " + dscale);
    }
  }

  private static boolean toBooleanValue(final Object value) {
    if (value instanceof Boolean b)
      return b;
    if (value instanceof Number n)
      return n.intValue() != 0;
    return parseBooleanText(value.toString());
  }

  private static LocalDate toLocalDateValue(final Object value) {
    if (value instanceof LocalDate ld)
      return ld;
    if (value instanceof LocalDateTime ldt)
      return ldt.toLocalDate();
    if (value instanceof Date d)
      return d.toInstant().atZone(ZoneOffset.UTC).toLocalDate();
    if (value instanceof String s)
      return LocalDate.parse(s);
    throw new PostgresProtocolException("Unsupported DATE binary value type: " + value.getClass());
  }

  private static LocalDateTime toLocalDateTimeValue(final Object value) {
    if (value instanceof LocalDateTime l)
      return l;
    if (value instanceof Date d)
      return LocalDateTime.ofInstant(d.toInstant(), ZoneOffset.UTC);
    if (value instanceof LocalDate ld)
      return ld.atStartOfDay();
    if (value instanceof String s)
      return parseTimestampText(s);
    throw new PostgresProtocolException("Unsupported TIMESTAMP binary value type: " + value.getClass());
  }

  public final  int                      code;
  /** pg_type.typname, the name PostgreSQL itself gives {@link #code}. */
  public final  String                   typeName;
  /**
   * pg_type.typelem: the OID of the element type for an array type, 0 for a scalar except {@link #NAME} and {@link #INT2VECTOR}, which
   * PostgreSQL also gives one. {@link #isArrayType()} says which types are arrays of this protocol.
   */
  public final  int                      elementCode;
  /** pg_type.typarray: the OID of the array type built on a scalar, 0 for an array type. */
  public final  int                      arrayCode;
  public final  Class<?>                 cls;
  public final  int                      size;
  private final Function<String, Object> textParser;
  private final boolean                  array;

  /** A type that is an array exactly when it names an element type, which is every type but {@link #NAME} and {@link #INT2VECTOR}. */
  PostgresType(final int code, final String typeName, final int elementCode, final int arrayCode, final Class<?> cls,
      final int size, Function<String, Object> textParser) {
    this(code, typeName, elementCode, arrayCode, cls, size, textParser, elementCode != 0);
  }

  PostgresType(final int code, final String typeName, final int elementCode, final int arrayCode, final Class<?> cls,
      final int size, Function<String, Object> textParser, final boolean array) {
    this.code = code;
    this.typeName = typeName;
    this.elementCode = elementCode;
    this.arrayCode = arrayCode;
    this.cls = cls;
    this.size = size;
    this.textParser = textParser;
    this.array = array;
  }

  /**
   * The SQL name PostgreSQL prints this type under - {@code integer}, not {@code int4} - which is what {@code format_type()}, a
   * {@code regtype} cast and {@code information_schema.columns.data_type} answer (issues #10228, #10370). {@link #typeName} is the
   * other spelling, {@code pg_type.typname} and {@code information_schema.columns.udt_name}. An array is its element's SQL name
   * and {@code []}. The one place a catalog column that carries the SQL name reads it from, so a new constant declares both
   * spellings here and no writer can reach for {@link #typeName} by accident.
   */
  public String sqlName() {
    if (isArrayType()) {
      final PostgresType element = byCode(elementCode);
      return element == null ? typeName : element.sqlName() + "[]";
    }
    return switch (this) {
      case SMALLINT -> "smallint";
      case INTEGER -> "integer";
      case LONG -> "bigint";
      case REAL -> "real";
      case DOUBLE -> "double precision";
      case CHAR -> "\"char\"";
      case BOOLEAN -> "boolean";
      case TIMESTAMP -> "timestamp without time zone";
      case TIMESTAMPTZ -> "timestamp with time zone";
      case TIME -> "time without time zone";
      case TIMETZ -> "time with time zone";
      case VARCHAR -> "character varying";
      case BPCHAR -> "character";
      default -> typeName;
    };
  }

  /**
   * Returns the type carrying the given OID, or null when this protocol has no such type. Unlike
   * {@code deserializeAsText}/{@code deserializeAsBinary}, which reject an unknown OID, a catalog lookup
   * for a type ArcadeDB cannot produce is a legitimate answer of "no row" rather than an error.
   */
  public static PostgresType byCode(final int code) {
    return CODE_MAP.get(code);
  }

  /**
   * Returns the type carrying the given {@code typname}, or null when this protocol has no such type.
   * <p>
   * PostgreSQL accepts a type under two spellings - its catalog {@code typname} and, for the types the SQL
   * standard names, that standard name - and a client writing {@code 'integer'::regtype} means the same type
   * as one writing {@code 'int4'::regtype}. Both are answered here, so a lookup by name does not depend on
   * which of the two the client happened to write.
   */
  public static PostgresType byName(final String name) {
    return name == null ? null : NAME_MAP.get(name.trim().toLowerCase(Locale.ENGLISH));
  }

  /** Parses an array literal in text format: see {@link PostgresArrayLiteral} for the grammar (issue #10870). */
  private static ArrayList<Object> parseArrayFromString(final String arrayStr, final Function<String, ?> elementParser) {
    return PostgresArrayLiteral.parse(arrayStr, elementParser);
  }

  public static PostgresType getTypeForValue(final Object value) {
    // a Cypher date, time or datetime is typed as the java.time value it wraps, as SQL reads the same property (issue #11955)
    final Object val = CypherTemporalValue.toJavaTime(value);
    if (val == null) {
      return PostgresType.VARCHAR;
    } else if (val instanceof Float) {
      return PostgresType.REAL;
    } else if (val instanceof Double) {
      return PostgresType.DOUBLE;
    } else if (val instanceof Integer) {
      return PostgresType.INTEGER;
    } else if (val instanceof Short || val instanceof Byte) {
      // Matches the schema path (Type.SHORT/Type.BYTE -> SMALLINT, issue #6447): widening to INTEGER here made
      // a column's OID depend on whether the result set happened to be empty.
      return PostgresType.SMALLINT;
    } else if (val instanceof Long) {
      return PostgresType.LONG;
    } else if (val instanceof Boolean) {
      return PostgresType.BOOLEAN;
    } else if (val instanceof String) {
      return PostgresType.VARCHAR;
    } else if (val instanceof Character) {
      return PostgresType.CHAR;
    } else if (val instanceof BigDecimal) {
      return PostgresType.NUMERIC;
    } else if (val instanceof JSONObject) {
      return PostgresType.JSON;
    } else if (val instanceof Result) {
      return PostgresType.JSON;
    } else if (val instanceof EmbeddedDocument) {
      return PostgresType.JSON;
    } else if (val instanceof Map || val instanceof TraversalPath) {
      // a Cypher path travels as its {_type, nodes, relationships, length} map (issue #9913)
      return PostgresType.JSON;
    } else if (val instanceof Record) {
      return PostgresType.JSON;
    } else if (val instanceof Collection<?> collection) {
      return getArrayTypeForElements(collection.iterator());
    } else if (val instanceof Iterable<?> iterable) {
      return getArrayTypeForElements(iterable.iterator());

    } else if (val instanceof Iterator<?> iterator) {
      while (iterator.hasNext()) {
        Object next = iterator.next();
        if (next != null) {
          return getArrayTypeForElementType(next);
        }
      }
      return PostgresType.ARRAY_TEXT;
    } else if (val instanceof byte[] || val instanceof Binary) {
      return PostgresType.BYTEA;
    } else if (val.getClass().isArray()) {
      // Handle Java arrays
      return switch (val) {
        // Shorts and boxed bytes widen to int4[]: getArrayTypeForElementType answers ARRAY_INT for a Short or a
        // Byte element, and there is no int2[] entry to pair with a narrower answer. Only the primitive byte[]
        // means BINARY (handled above); a Byte[] is an array of small integers.
        case short[] shorts -> PostgresType.ARRAY_INT;
        case Short[] shorts -> PostgresType.ARRAY_INT;
        case Byte[] bytes -> PostgresType.ARRAY_INT;
        case int[] ints -> PostgresType.ARRAY_INT;
        case Integer[] ints -> PostgresType.ARRAY_INT;
        case long[] longs -> PostgresType.ARRAY_LONG;
        case Long[] longs -> PostgresType.ARRAY_LONG;
        case double[] doubles -> PostgresType.ARRAY_DOUBLE;
        case Double[] doubles -> PostgresType.ARRAY_DOUBLE;
        case float[] floats -> PostgresType.ARRAY_REAL;
        case Float[] floats -> PostgresType.ARRAY_REAL;
        case boolean[] booleans -> PostgresType.ARRAY_BOOLEAN;
        case Boolean[] booleans -> PostgresType.ARRAY_BOOLEAN;
        case char[] chars -> PostgresType.ARRAY_CHAR;
        case Character[] chars -> PostgresType.ARRAY_CHAR;
        case String[] strings -> PostgresType.ARRAY_TEXT;
        default -> throw new IllegalStateException("Unexpected value: " + val);
      };
    } else if (val instanceof Date) {
      return PostgresType.DATE;
    } else if (val instanceof LocalDate) {
      // Type.DATE's runtime representation is configurable per database (GlobalConfiguration.DATE_IMPLEMENTATION)
      // and defaults to LocalDate, not Date - so a sampled value from a default-configured database never hit
      // the Date branch above and fell all the way through to VARCHAR, disagreeing with the schema path's DATE.
      return PostgresType.DATE;
    } else if (val instanceof LocalDateTime) {
      return PostgresType.TIMESTAMP;
    }

    return PostgresType.VARCHAR;

  }

  /**
   * A lazy multi-value as the list of its elements, any other value as it is (issue #11957): the {@link Iterable} that
   * {@code out()}, {@code in()}, {@code outE()} and {@code inE()} answer, or the {@link Iterator} of a nested result set. A
   * column holding one is then typed and serialized like any other collection (a json array of the records, a text[] of
   * scalars), the way the gRPC encoder sends it as a list since #11044, where the serializer, which has no arm for a
   * non-collection iterable, used to send its identity string ({@code com.arcadedb.graph.GraphEngine$10@72d8f43f}) in a
   * column announced as json. An {@link Iterator} is walked once: the caller keeps the list rather than the iterator.
   */
  public static Object materializeLazyMultiValue(final Object value) {
    final Iterator<?> iterator;
    if (value instanceof Collection<?>)
      return value;
    else if (value instanceof Iterable<?> iterable)
      iterator = iterable.iterator();
    else if (value instanceof Iterator<?> it)
      iterator = it;
    else
      return value;

    final List<Object> list = new ArrayList<>();
    while (iterator.hasNext())
      list.add(iterator.next());
    return list;
  }

  /**
   * Maps an ArcadeDB schema Type to a PostgreSQL type, resolving the element type of a LIST from the
   * property's declared "OF" clause.
   *
   * @param arcadeType The ArcadeDB schema type
   * @param ofType     The declared element type name of a LIST property, or null when undeclared. Ignored for
   *                   any other type.
   *
   * @return The corresponding PostgreSQL type
   */
  public static PostgresType getTypeFromArcade(final Type arcadeType, final String ofType) {
    if (arcadeType == Type.LIST)
      return getArrayTypeForOfType(ofType);
    return getTypeFromArcade(arcadeType);
  }

  /**
   * Resolves the array type of a "LIST OF &lt;ofType&gt;" property. An ofType that does not name a scalar
   * {@link Type} refers to an embedded document type, so the list is advertised as a single json document
   * holding a JSON array; this mirrors the convention used by Type.coerceCollectionOfType. An undeclared
   * ofType stays text[].
   */
  private static PostgresType getArrayTypeForOfType(final String ofType) {
    if (ofType == null || ofType.isBlank())
      return ARRAY_TEXT;

    final Type elementType = Type.getTypeByName(ofType);
    if (elementType == null)
      // Not a scalar: the list holds embedded documents of a schema type.
      return JSON;

    // Every branch must agree with getArrayTypeForElementType, which types each element of a populated list: a
    // mismatch would make a column's OID depend on whether the list is empty.
    return switch (elementType) {
      case BOOLEAN -> ARRAY_BOOLEAN;
      case INTEGER, SHORT, BYTE -> ARRAY_INT;
      case LONG -> ARRAY_LONG;
      case FLOAT -> ARRAY_REAL;
      case DOUBLE -> ARRAY_DOUBLE;
      case DECIMAL -> ARRAY_NUMERIC;
      // A BINARY is a bytea, so a list of them is a bytea[] (issue #10927)
      case BINARY -> ARRAY_BYTEA;
      // Nested collections (issue #5365) join maps and embedded documents in being carried as a JSON document:
      // a Postgres array is rectangular and homogeneous, an ArcadeDB nested list is neither.
      case MAP, EMBEDDED, LIST, ARRAY_OF_SHORTS, ARRAY_OF_INTEGERS, ARRAY_OF_LONGS, ARRAY_OF_FLOATS, ARRAY_OF_DOUBLES -> JSON;
      default -> ARRAY_TEXT;
    };
  }

  /**
   * Maps an ArcadeDB schema Type to a PostgreSQL type.
   *
   * @param arcadeType The ArcadeDB schema type
   *
   * @return The corresponding PostgreSQL type
   */
  public static PostgresType getTypeFromArcade(Type arcadeType) {
    if (arcadeType == null) {
      return PostgresType.VARCHAR;
    }

    // Every branch must agree with getTypeForValue, which types a column from a sample row: a mismatch would make
    // the column's OID depend on whether the result set happens to be empty.
    return switch (arcadeType) {
      case BOOLEAN -> PostgresType.BOOLEAN;
      case INTEGER -> PostgresType.INTEGER;
      case SHORT -> PostgresType.SMALLINT;
      case LONG -> PostgresType.LONG;
      case FLOAT -> PostgresType.REAL;
      case DOUBLE -> PostgresType.DOUBLE;
      case BYTE -> PostgresType.SMALLINT;
      case STRING -> PostgresType.VARCHAR;
      case DATETIME, DATETIME_MICROS, DATETIME_NANOS, DATETIME_SECOND -> PostgresType.TIMESTAMP;
      case DATE -> PostgresType.DATE;
      case BINARY -> PostgresType.BYTEA;
      case LIST -> PostgresType.ARRAY_TEXT;
      case ARRAY_OF_SHORTS, ARRAY_OF_INTEGERS -> PostgresType.ARRAY_INT;
      case ARRAY_OF_LONGS -> PostgresType.ARRAY_LONG;
      case ARRAY_OF_FLOATS -> PostgresType.ARRAY_REAL;
      case ARRAY_OF_DOUBLES -> PostgresType.ARRAY_DOUBLE;
      case MAP, EMBEDDED -> PostgresType.JSON;
      case LINK -> PostgresType.VARCHAR;
      case DECIMAL -> PostgresType.NUMERIC;
      default -> PostgresType.VARCHAR;
    };
  }

  /**
   * Serializes a value as text format into the provided Binary buffer.
   *
   * @param pgType     The PostgreSQL type
   * @param typeBuffer The buffer to write to
   * @param value      The value to serialize
   */
  public void serializeAsText(final PostgresType pgType, final Binary typeBuffer, final Object value) {
    writeString(typeBuffer, toText(pgType, value));
  }

  /**
   * The text-format representation of a value under a Postgres type - what a {@code DataRow} carries for the
   * column in text format, and what {@code COPY ... TO STDOUT} in text or CSV format escapes and delimits
   * (issue #7188). Null for a SQL NULL.
   */
  public String toText(final PostgresType pgType, final Object rawValue) {
    final Object value = CypherTemporalValue.toJavaTime(rawValue);
    String serializedValue = null;
    final byte[] byteaValue = pgType == BYTEA ? byteaValueOf(value) : null;
    final String temporalText = temporalText(pgType, value);
    if (temporalText != null) {
      serializedValue = temporalText;
    } else if (pgType == JSON && value != null && (value instanceof Collection<?> || value.getClass().isArray())) {
      // The column was announced as a json document holding an array (issue #5366): emit a real JSON array
      // instead of a Postgres array literal, so the payload matches the announced OID.
      serializedValue = serializeCollectionAsJson(
          value instanceof Collection<?> collection ? collection : convertPrimitiveArrayToCollection(value));
    } else if (byteaValue != null) {
      // A blob announced as bytea travels as \x<hex> (issue #6411), not as the {1,2,3} array literal the
      // generic byte[] handling below would produce for it.
      serializedValue = toByteaText(byteaValue);
    } else if (value instanceof Collection<?> collection) {
      // Handle array serialization
      serializedValue = serializeArrayToString(collection, pgType);
    } else if (value != null && value.getClass().isArray()) {
      // Handle primitive arrays by converting them to Collections
      Collection<?> collection = convertPrimitiveArrayToCollection(value);
      serializedValue = serializeArrayToString(collection, pgType);
    } else if (value instanceof Boolean b) {
      // PostgreSQL BOOL text format is "t"/"f"
      serializedValue = b ? "t" : "f";
    } else if (value instanceof BigDecimal bd) {
      // BigDecimal.toString() switches to scientific notation outside a small exponent range (e.g. "1E+10"),
      // which is not a NUMERIC text value PostgreSQL itself would ever emit or a client would expect to parse.
      serializedValue = bd.toPlainString();
    } else if (value instanceof Date date && pgType == DATE) {
      // DATE (OID 1082) expects "YYYY-MM-DD" in text format
      serializedValue = date.toInstant().atZone(ZoneOffset.UTC).format(DateTimeFormatter.ISO_LOCAL_DATE);
    } else if (value instanceof Date date) {
      // TIMESTAMP (or any other non-DATE column) backed by a java.util.Date: keep the time-of-day
      // instead of truncating to a date-only value (issue #6675)
      serializedValue = LocalDateTime.ofInstant(date.toInstant(), ZoneOffset.UTC).format(POSTGRES_DATETIME_FORMATTER);
    } else if (value instanceof LocalDateTime ldt) {
      // TIMESTAMP (OID 1114) expects "yyyy-MM-dd HH:mm:ss.SSSSSS" in text format
      serializedValue = ldt.format(POSTGRES_DATETIME_FORMATTER);
    } else if (value instanceof JSONObject json) {
      serializedValue = json.toString();
    } else if (isJsonDocument(value)) {
      serializedValue = toJsonDocument(value).toString();
    } else if (value != null) {
      serializedValue = value.toString();
    }
    return serializedValue;
  }

  /**
   * The text PostgreSQL writes for a value of a time, timetz, timestamptz or interval type (issue #12449), or null when {@code type}
   * is none of them or the value is not one it can be read as, so that every other value keeps the text it had.
   */
  private static String temporalText(final PostgresType type, final Object value) {
    if (value == null || type == null)
      return null;
    return switch (type) {
      case TIME -> value instanceof LocalTime time ? time.format(POSTGRES_TIME_OUTPUT) : null;
      case TIMETZ -> value instanceof OffsetTime time ? time.format(POSTGRES_TIMETZ_OUTPUT) : null;
      case TIMESTAMPTZ -> value instanceof ZonedDateTime zoned ? zoned.format(POSTGRES_TIMESTAMPTZ_OUTPUT)
          : value instanceof OffsetDateTime offset ? offset.format(POSTGRES_TIMESTAMPTZ_OUTPUT) : null;
      case INTERVAL -> value instanceof CypherDuration duration ? intervalText(duration) : null;
      default -> null;
    };
  }

  /**
   * An interval in PostgreSQL's default output style ({@code IntervalStyle = postgres}): {@code 1 year 2 mons 3 days 04:05:06.5}, each
   * field only when it is not zero, {@code 00:00:00} for the empty interval, a unit in the plural unless its amount is exactly 1, and
   * each field carrying its own sign, which is what {@link #parseIntervalText} and every driver read back.
   */
  static String intervalText(final CypherDuration duration) {
    final StringBuilder text = new StringBuilder(32);
    appendIntervalField(text, duration.getMonths() / 12, "year");
    appendIntervalField(text, duration.getMonths() % 12, "mon");
    appendIntervalField(text, duration.getDays(), "day");
    final long micros = intervalMicros(duration);
    if (micros != 0 || text.isEmpty()) {
      if (!text.isEmpty())
        text.append(' ');
      if (micros < 0)
        text.append('-');
      final long magnitude = Math.abs(micros);
      final long hours = magnitude / 3_600_000_000L;
      final long minutes = magnitude / 60_000_000L % 60;
      final long seconds = magnitude / 1_000_000L % 60;
      final long fraction = magnitude % 1_000_000L;
      if (hours < 10)
        text.append('0');
      text.append(hours).append(':');
      if (minutes < 10)
        text.append('0');
      text.append(minutes).append(':');
      if (seconds < 10)
        text.append('0');
      text.append(seconds);
      if (fraction != 0) {
        final String digits = Long.toString(1_000_000L + fraction).substring(1);
        int end = digits.length();
        while (digits.charAt(end - 1) == '0')
          --end;
        text.append('.').append(digits, 0, end);
      }
    }
    return text.toString();
  }

  private static void appendIntervalField(final StringBuilder text, final long amount, final String unit) {
    if (amount == 0)
      return;
    if (!text.isEmpty())
      text.append(' ');
    text.append(amount).append(' ').append(unit);
    if (amount != 1)
      text.append('s');
  }

  /** The part of an interval below a day, in microseconds, as PostgreSQL stores it (the nanoseconds past a microsecond are dropped). */
  private static long intervalMicros(final CypherDuration duration) {
    return duration.getSeconds() * 1_000_000L + Math.floorDiv(duration.getNanosAdjustment(), 1_000);
  }

  /**
   * Serializes a value as PostgreSQL binary format into the provided Binary buffer.
   * Used when a client requests binary results via Bind message format codes. Types without a
   * binary mapping fall back to {@link #serializeAsText}.
   */
  public void serializeAsBinary(final PostgresType pgType, final Binary typeBuffer, final Object rawValue) {
    final Object value = CypherTemporalValue.toJavaTime(rawValue);
    if (value == null) {
      typeBuffer.putInt(-1);
      return;
    }
    switch (pgType) {
    case SMALLINT -> {
      typeBuffer.putInt(2);
      typeBuffer.putShort(toNumber(value).shortValue());
    }
    case INTEGER -> {
      typeBuffer.putInt(4);
      typeBuffer.putInt(toNumber(value).intValue());
    }
    case LONG -> {
      typeBuffer.putInt(8);
      typeBuffer.putLong(toNumber(value).longValue());
    }
    case REAL -> {
      typeBuffer.putInt(4);
      typeBuffer.putInt(Float.floatToRawIntBits(toNumber(value).floatValue()));
    }
    case DOUBLE -> {
      typeBuffer.putInt(8);
      typeBuffer.putLong(Double.doubleToRawLongBits(toNumber(value).doubleValue()));
    }
    case BOOLEAN -> {
      typeBuffer.putInt(1);
      typeBuffer.putByte((byte) (toBooleanValue(value) ? 1 : 0));
    }
    case BYTEA -> {
      // bytea binary format is the raw bytes, with no framing of its own beyond the length prefix.
      final byte[] bytes = byteaValueOf(value);
      if (bytes == null) {
        serializeAsText(pgType, typeBuffer, value);
        return;
      }
      typeBuffer.putInt(bytes.length);
      typeBuffer.putByteArray(bytes);
    }
    case NUMERIC -> putNumericBinary(typeBuffer, toBigDecimalValue(value));
    case CHAR -> {
      // Postgres "char" (OID 18) is a single byte on the wire.
      typeBuffer.putInt(1);
      final char c = value instanceof Character ch ? ch : value.toString().charAt(0);
      typeBuffer.putByte((byte) c);
    }
    case DATE -> {
      typeBuffer.putInt(4);
      typeBuffer.putInt((int) (toLocalDateValue(value).toEpochDay() - POSTGRES_EPOCH_DAYS));
    }
    case TIMESTAMP -> {
      typeBuffer.putInt(8);
      final LocalDateTime ldt = toLocalDateTimeValue(value);
      final long secsFromPgEpoch = ldt.toEpochSecond(ZoneOffset.UTC) - POSTGRES_EPOCH_SECONDS;
      typeBuffer.putLong(secsFromPgEpoch * 1_000_000L + ldt.getNano() / 1000L);
    }
    // the binary formats of the types whose array types #12449 adds, which a binary array of them is made of: the reverse of
    // deserializeBinary
    case TIME -> {
      typeBuffer.putInt(8);
      typeBuffer.putLong(toLocalTimeValue(value).toNanoOfDay() / 1_000L);
    }
    case TIMETZ -> {
      final OffsetTime time = toOffsetTimeValue(value);
      typeBuffer.putInt(12);
      typeBuffer.putLong(time.toLocalTime().toNanoOfDay() / 1_000L);
      // seconds WEST of UTC, the sign PostgreSQL stores
      typeBuffer.putInt(-time.getOffset().getTotalSeconds());
    }
    case TIMESTAMPTZ -> {
      final OffsetDateTime instant = toOffsetDateTimeValue(value);
      typeBuffer.putInt(8);
      typeBuffer.putLong((instant.toEpochSecond() - POSTGRES_EPOCH_SECONDS) * 1_000_000L + instant.getNano() / 1_000L);
    }
    case INTERVAL -> {
      final CypherDuration duration = value instanceof CypherDuration d ? d : parseIntervalText(value.toString());
      typeBuffer.putInt(16);
      typeBuffer.putLong(intervalMicros(duration));
      typeBuffer.putInt((int) duration.getDays());
      typeBuffer.putInt((int) duration.getMonths());
    }
    case JSONB -> {
      // jsonb_send: a version byte (1) ahead of the json text
      final byte[] json = (value instanceof String text ? text : toText(JSON, value)).getBytes(DatabaseFactory.getDefaultCharset());
      typeBuffer.putInt(json.length + 1);
      typeBuffer.putByte((byte) 1);
      typeBuffer.putByteArray(json);
    }
    case OID -> {
      // an unsigned 32-bit number: 4294967295 travels as the bits of -1
      typeBuffer.putInt(4);
      typeBuffer.putInt((int) toNumber(value).longValue());
    }
    case INT2VECTOR -> putInt2VectorBinary(typeBuffer, value);
    case ARRAY_INT, ARRAY_CHAR, ARRAY_LONG, ARRAY_REAL, ARRAY_DOUBLE, ARRAY_TEXT, ARRAY_JSON, ARRAY_BOOLEAN, ARRAY_NUMERIC, ARRAY_SMALLINT,
         ARRAY_BYTEA, ARRAY_BPCHAR, ARRAY_VARCHAR, ARRAY_DATE, ARRAY_TIMESTAMP, ARRAY_TIMESTAMPTZ, ARRAY_TIME, ARRAY_TIMETZ, ARRAY_INTERVAL,
         ARRAY_JSONB, ARRAY_NAME, ARRAY_OID, ARRAY_INT2VECTOR -> {
      final Collection<?> elements = value instanceof Collection<?> collection ? collection
          : value.getClass().isArray() ? convertPrimitiveArrayToCollection(value) : null;
      if (elements == null)
        // the OID announced an array and a binary client decodes the array layout: a text literal would be read as garbage
        throw new PostgresProtocolException("Cannot send a " + value.getClass().getSimpleName() + " value as a binary " + pgType.typeName
            + " array: a collection or a Java array is expected");
      putArrayBinary(pgType, typeBuffer, elements);
    }
    // Strings and JSON do not have a separate binary representation: their wire format is identical to text
    // for our purposes (length-prefixed UTF-8 bytes).
    default -> serializeAsText(pgType, typeBuffer, value);
    }
  }

  /**
   * Writes a one-dimensional array in PostgreSQL's binary array format (array_send): the number of dimensions, a flag telling
   * whether a NULL element is present, the element type OID, then for the dimension its length and lower bound (1), then every
   * element as a length-prefixed binary value (-1 for NULL). An empty array has no dimensions at all. A client that asks for the
   * binary format of an array column (pgjdbc does once a statement is server-prepared) decodes exactly this layout, and fails on
   * the text literal (issue #9007). A nested list or a document is one JSON-text element of the one-dimensional array, as in the text
   * format; an element of a numeric array is converted to the element type, so a Long inside an int4[] is narrowed.
   */
  private void putArrayBinary(final PostgresType arrayType, final Binary typeBuffer, final Collection<?> elements) {
    final PostgresType elementType = elementTypeOf(arrayType);
    final int lengthPosition = typeBuffer.position();
    typeBuffer.putInt(0);  // the value length, patched once the elements are written

    boolean hasNull = false;
    for (final Object element : elements)
      if (element == null) {
        hasNull = true;
        break;
      }

    typeBuffer.putInt(elements.isEmpty() ? 0 : 1);
    typeBuffer.putInt(hasNull ? 1 : 0);
    typeBuffer.putInt(elementType.code);
    if (!elements.isEmpty()) {
      typeBuffer.putInt(elements.size());
      typeBuffer.putInt(1);
      for (final Object element : elements) {
        if (element == null)
          typeBuffer.putInt(-1);
        else if (elementType == TEXT || elementType == JSON || elementType == VARCHAR || elementType == BPCHAR)
          writeString(typeBuffer, arrayElementToText(element));
        else
          try {
            serializeAsBinary(elementType, typeBuffer, element);
          } catch (final RuntimeException e) {
            throw new PostgresProtocolException("Cannot send a " + element.getClass().getSimpleName() + " element in a binary "
                + arrayType.typeName + " array (the column was announced as that array type): " + e.getMessage(), e);
          }
      }
    }
    typeBuffer.putInt(lengthPosition, typeBuffer.position() - lengthPosition - 4);
  }

  /**
   * An int2vector in binary, which is what int2vectorsend writes: the array format of its int2 elements, with a lower bound of 0. The
   * value is the {@code 1 3} text this protocol reads one as, or a collection of numbers.
   */
  private void putInt2VectorBinary(final Binary typeBuffer, final Object value) {
    final List<Short> elements = new ArrayList<>();
    if (value instanceof Collection<?> collection)
      for (final Object element : collection)
        elements.add(toNumber(element).shortValue());
    else
      for (final String element : value.toString().trim().split("\\s+"))
        if (!element.isEmpty())
          elements.add(Short.parseShort(element));

    typeBuffer.putInt(20 + elements.size() * 6 - (elements.isEmpty() ? 8 : 0));
    typeBuffer.putInt(elements.isEmpty() ? 0 : 1);
    typeBuffer.putInt(0);
    typeBuffer.putInt(SMALLINT.code);
    if (!elements.isEmpty()) {
      typeBuffer.putInt(elements.size());
      typeBuffer.putInt(0);
      for (final Short element : elements) {
        typeBuffer.putInt(2);
        typeBuffer.putShort(element);
      }
    }
  }

  private static LocalTime toLocalTimeValue(final Object value) {
    return value instanceof LocalTime time ? time : parseTimeText(value.toString());
  }

  private static OffsetTime toOffsetTimeValue(final Object value) {
    return value instanceof OffsetTime time ? time : parseTimeTzText(value.toString());
  }

  private static OffsetDateTime toOffsetDateTimeValue(final Object value) {
    return switch (value) {
      case OffsetDateTime offset -> offset;
      case ZonedDateTime zoned -> zoned.toOffsetDateTime();
      case Instant instant -> instant.atOffset(ZoneOffset.UTC);
      case Date date -> date.toInstant().atOffset(ZoneOffset.UTC);
      // a timestamp without a zone is the UTC wall time this protocol reports for the session
      case LocalDateTime local -> local.atOffset(ZoneOffset.UTC);
      default -> parseTimestampTzText(value.toString()).toOffsetDateTime();
    };
  }

  /** The element type of an array type: its {@code typelem}, the one place the pairing is declared. */
  private static PostgresType elementTypeOf(final PostgresType arrayType) {
    final PostgresType element = arrayType.isArrayType() ? byCode(arrayType.elementCode) : null;
    if (element == null)
      throw new IllegalArgumentException("Not an array type: " + arrayType);
    return element;
  }

  /** The text of one element of a text[] or json[] array: what {@link #serializeArrayToString} writes, without the literal quoting. */
  private String arrayElementToText(final Object rawElement) {
    // a Cypher datetime in a list is written as the java.time value it wraps, as SQL writes it (issue #11955)
    final Object element = CypherTemporalValue.toJavaTime(rawElement);
    return switch (element) {
      case String str -> str;
      case Float f -> String.valueOf(f.floatValue());
      case Double d -> String.valueOf(d.doubleValue());
      case Date date -> LocalDateTime.ofInstant(date.toInstant(), ZoneOffset.UTC).format(POSTGRES_DATETIME_FORMATTER);
      case LocalDateTime ldt -> ldt.format(POSTGRES_DATETIME_FORMATTER);
      case Binary binary -> binary.getString();
      case Collection<?> nested -> serializeNestedAsJsonArray(nested).toString();
      case Result result -> toJsonDocument(result).toString();
      case JSONObject json -> json.toString();
      case Map<?, ?> map -> toJsonDocument(map).toString();
      // a Cypher path as the same JSON object the text array carries, not its toString() (issue #9913)
      case TraversalPath path -> toJsonDocument(path).toString();
      case Record record -> toJsonDocument(record).toString();
      default -> element.getClass().isArray() ? serializeNestedAsJsonArray(convertPrimitiveArrayToCollection(element)).toString()
          : element.toString();
    };
  }

  private void writeString(final Binary typeBuffer, final String value) {
    if (value == null) {
      typeBuffer.putInt(-1);
      return;
    }

    final byte[] str = value.getBytes(DatabaseFactory.getDefaultCharset());
    typeBuffer.putInt(str.length);
    typeBuffer.putByteArray(str);
  }

  /**
   * Serializes a Collection into a PostgreSQL array string format.
   */
  private String serializeArrayToString(Collection<?> collection, PostgresType pgType) {
    if (collection.isEmpty())
      return "{}";

    StringBuilder sb = new StringBuilder("{");
    // the element of a time[], timetz[], timestamptz[] or interval[] is written as the scalar is (issue #12449)
    final PostgresType elementType = pgType != null && pgType.isArrayType() ? byCode(pgType.elementCode) : null;
    boolean first = true;
    for (final Object rawElement : collection) {
      if (!first) {
        sb.append(",");
      }
      first = false;
      // a Cypher datetime in a list is written as the java.time value it wraps, as SQL writes it (issue #11955)
      final Object element = CypherTemporalValue.toJavaTime(rawElement);
      if (element == null) {
        sb.append("NULL");
        continue;
      }
      if (element instanceof Float) {
        sb.append(((Number) element).floatValue());
      } else if (element instanceof Double) {
        sb.append(((Number) element).doubleValue());
      } else if (element instanceof BigDecimal bd) {
        // as the scalar is written: toString() switches to scientific notation, which a numeric[] element is never written in
        sb.append(bd.toPlainString());
      } else if (element instanceof Number || element instanceof Boolean) {
        sb.append(element);
      } else if (pgType == ARRAY_BYTEA && byteaValueOf(element) != null) {
        // a bytea[] element is the \x<hex> text the scalar has (issue #10870), not the JSON array of its bytes
        appendQuoted(sb, toByteaText(byteaValueOf(element)));
      } else if (pgType == ARRAY_DATE && element instanceof Date date) {
        // a date[] element is the date alone, as the scalar DATE is written: the time of day is not part of the value
        appendQuoted(sb, date.toInstant().atZone(ZoneOffset.UTC).format(DateTimeFormatter.ISO_LOCAL_DATE));
      } else if (elementType != null && temporalText(elementType, element) instanceof String temporal) {
        appendQuoted(sb, temporal);
      } else if (element instanceof Character) {
        // A Postgres array literal quotes an element with the DOUBLE quote and nothing else: a single quote is
        // an ordinary data character there, so wrapping the character in single quotes (issue #7801) shipped
        // {'a','b'}, which every decoder - this enum's own parseArrayFromString included - reads back as the
        // three-character strings 'a' and 'b'. Quoted like every other textual element, {"a","b"} decodes to a
        // and b, and a character that would otherwise break the literal - a comma, a brace, a quote, a
        // backslash - is escaped.
        appendQuoted(sb, element.toString());
      } else if (element instanceof Date date) {
        // Format Date as PostgreSQL-compatible timestamp in arrays
        LocalDateTime ldt = LocalDateTime.ofInstant(date.toInstant(), ZoneOffset.UTC);
        sb.append("\"").append(ldt.format(POSTGRES_DATETIME_FORMATTER)).append("\"");
      } else if (element instanceof LocalDateTime ldt) {
        // Format LocalDateTime as PostgreSQL-compatible timestamp in arrays
        sb.append("\"").append(ldt.format(POSTGRES_DATETIME_FORMATTER)).append("\"");
      } else if (element instanceof Binary binary) {
        appendQuoted(sb, binary.getString());
      } else if (element instanceof Collection<?> subCollection) {
        // A nested list is carried as a JSON document (issue #5365): the column is advertised as json[], so the
        // element must be a quoted JSON array. Emitting a nested "{...}" literal instead made the announced OID
        // and the payload disagree, and clients re-parsed the inner braces as a second array dimension. A record or a map inside
        // it is rendered as the scalar json column renders it (issue #11958).
        appendQuoted(sb, serializeNestedAsJsonArray(subCollection).toString());
      } else if (element.getClass().isArray()) {
        appendQuoted(sb, serializeNestedAsJsonArray(convertPrimitiveArrayToCollection(element)).toString());
      } else if (element instanceof JSONObject json) {
        appendQuoted(sb, json.toString());
      } else if (isJsonDocument(element)) {
        appendQuoted(sb, toJsonDocument(element).toString());
      } else if (element instanceof String str) {
        appendQuoted(sb, str);
      } else {
        sb.append(element.toString());
      }
    }
    sb.append("}");
    return sb.toString();
  }

  /**
   * Appends a value as a double-quoted element of a Postgres array literal. Inside the quotes both the
   * backslash and the double quote must be escaped, in that order (issue #5366): escaping only the quote left a
   * backslash in the data acting as an escape character, so "C:\temp" reached the client as "C:temp" and a
   * nested JSON document could be truncated in the middle.
   */
  private static void appendQuoted(final StringBuilder sb, final String value) {
    sb.append('"').append(value.replace("\\", "\\\\").replace("\"", "\\\"")).append('"');
  }

  /**
   * Serializes a collection of documents, maps or nested collections as a single JSON array (issue #5366).
   * The column is advertised as json, so the value is plain JSON: no Postgres array literal, no quoting and no
   * escaping of the elements, which is what made JDBC clients display the documents as escaped text.
   */
  private String serializeCollectionAsJson(final Collection<?> collection) {
    return serializeNestedAsJsonArray(collection).toString();
  }

  /**
   * Converts a single collection element into a value the JSON serializer renders natively. Documents keep
   * their "@type" attribute, temporal values use the Postgres text format, and nested collections recurse.
   */
  private Object toJsonElement(final Object rawElement) {
    // a Cypher datetime in a list is written as the java.time value it wraps, as SQL writes it (issue #11955)
    final Object element = CypherTemporalValue.toJavaTime(rawElement);
    return switch (element) {
      case null -> null;
      case JSONObject json -> json;
      case JSONArray json -> json;
      case Result result -> toJsonDocument(result);
      case Record record -> toJsonDocument(record);
      case Map<?, ?> map -> toJsonDocument(map);
      case TraversalPath path -> toJsonDocument(path);
      case Collection<?> nested -> serializeNestedAsJsonArray(nested);
      case Binary binary -> binary.getString();
      case Date date -> LocalDateTime.ofInstant(date.toInstant(), ZoneOffset.UTC).format(POSTGRES_DATETIME_FORMATTER);
      case LocalDateTime ldt -> ldt.format(POSTGRES_DATETIME_FORMATTER);
      default -> element.getClass().isArray() ? serializeNestedAsJsonArray(convertPrimitiveArrayToCollection(element)) : element;
    };
  }

  /** A record, a row, a map or a path: the values a json column carries as one JSON document. */
  private static boolean isJsonDocument(final Object value) {
    return value instanceof Map || value instanceof TraversalPath || value instanceof Record || value instanceof Result;
  }

  /**
   * A record, a row, a map or a path as the JSON the HTTP API sends for it (issue #11958). {@code toJSON()} and {@code new
   * JSONObject(map)} wrote a temporal as an epoch number whose unit depended on where it sat (a DATE was days in a record and
   * milliseconds in a map) and dropped the zone of a ZONED_DATETIME and the sub-millisecond digits of a DATETIME_NANOS inside a map.
   * The date patterns are those of the database the value comes from (the record's, or the row's when it knows one; each node and
   * relationship of a path uses its own); a value no database owns (a map literal, a row built without one) gets the defaults.
   */
  private static Object toJsonDocument(final Object value) {
    // A record that is not a document (an edge segment, a stripe directory, the payload of an EXTERNAL property) is engine
    // infrastructure the serializer has no form for: it would hand the record back and the column would carry its toString().
    // Its own toJSON(true) stays the rendering, as before.
    if (value instanceof Record record && !(record instanceof Document))
      return record.toJSON(true);
    final Database database = switch (value) {
      case Document document -> document.getDatabase();
      case Result result -> result.isElement() ? result.toElement().getDatabase() : result.getDatabase();
      default -> null;
    };
    return JSON_DOCUMENT_SERIALIZER.serializeValue(database, value);
  }

  private JSONArray serializeNestedAsJsonArray(final Collection<?> collection) {
    final JSONArray array = new JSONArray();
    for (final Object element : collection)
      array.put(toJsonElement(element));
    return array;
  }

  /**
   * Converts a primitive array to a Collection for serialization.
   * Handles all primitive array types: int[], long[], float[], double[], short[], boolean[], char[], byte[]
   * and object arrays like String[].
   */
  private Collection<?> convertPrimitiveArrayToCollection(Object array) {
    if (array instanceof int[] intArray) {
      List<Integer> list = new ArrayList<>(intArray.length);
      for (int val : intArray) {
        list.add(val);
      }
      return list;
    } else if (array instanceof long[] longArray) {
      List<Long> list = new ArrayList<>(longArray.length);
      for (long val : longArray) {
        list.add(val);
      }
      return list;
    } else if (array instanceof float[] floatArray) {
      List<Float> list = new ArrayList<>(floatArray.length);
      for (float val : floatArray) {
        list.add(val);
      }
      return list;
    } else if (array instanceof double[] doubleArray) {
      List<Double> list = new ArrayList<>(doubleArray.length);
      for (double val : doubleArray) {
        list.add(val);
      }
      return list;
    } else if (array instanceof short[] shortArray) {
      List<Short> list = new ArrayList<>(shortArray.length);
      for (short val : shortArray) {
        list.add(val);
      }
      return list;
    } else if (array instanceof boolean[] booleanArray) {
      List<Boolean> list = new ArrayList<>(booleanArray.length);
      for (boolean val : booleanArray) {
        list.add(val);
      }
      return list;
    } else if (array instanceof char[] charArray) {
      List<Character> list = new ArrayList<>(charArray.length);
      for (char val : charArray) {
        list.add(val);
      }
      return list;
    } else if (array instanceof byte[] byteArray) {
      List<Byte> list = new ArrayList<>(byteArray.length);
      for (byte val : byteArray) {
        list.add(val);
      }
      return list;
    } else if (array instanceof Object[] objectArray) {
      // Handle object arrays like String[]
      return Arrays.asList(objectArray);
    } else {
      // Fallback: should not happen, but return empty list
      return new ArrayList<>();
    }
  }

  /**
   * The array type that can hold EVERY non-null element, not just the first one (issue #9008): a list loaded from
   * JSON can mix integers and doubles, or numbers and text, and an {@code int4[]} announced from the first element
   * made a client read {@code [1, 2.5]} as {@code [1, 2]} or fail on the rest.
   */
  private static PostgresType getArrayTypeForElements(final Iterator<?> elements) {
    PostgresType type = null;
    Class<?> lastClass = null;
    while (elements.hasNext()) {
      final Object element = elements.next();
      if (element == null)
        continue;
      // an element of the class just seen cannot change the answer: spares the type chain on a long homogeneous list
      if (element.getClass() == lastClass)
        continue;
      lastClass = element.getClass();
      final PostgresType elementType = getArrayTypeForElementType(element);
      type = type == null ? elementType : mergeTypes(type, elementType, true);
      if (type == ARRAY_TEXT)
        break; // text[] holds anything: nothing left to widen
    }
    return type != null ? type : ARRAY_TEXT;
  }

  private static int numericRank(final PostgresType type) {
    return switch (type) {
      case SMALLINT -> 0;
      case INTEGER, ARRAY_INT -> 1;
      case LONG, ARRAY_LONG -> 2;
      case REAL, ARRAY_REAL -> 3;
      case DOUBLE, ARRAY_DOUBLE -> 4;
      case NUMERIC, ARRAY_NUMERIC -> 5;
      default -> -1;
    };
  }

  /**
   * The narrowest type that holds every value of both {@code a} and {@code b}, used to type a column over all the rows
   * of a result and a list over all its elements (issue #9008, #9009). Integers widen to long, integers and floats to
   * double (a long beyond 2^53 is then rounded, which numeric could avoid only by failing on NaN and infinity), and
   * anything meeting a BigDecimal to numeric. Whatever else differs falls back to the one type every value can be written under: varchar for a column, text[] for a list.
   *
   * <p>A column mixing a BigDecimal row with a NaN or infinite double row is announced numeric and cannot carry the
   * double; that combination is accepted as too rare to give up numeric for.
   *
   * @param listElements true when merging the element types of ONE list (so json, which stands for a list of
   *                     documents there, merges into text[]); false when merging the types of one column across rows
   */
  public static PostgresType mergeTypes(final PostgresType a, final PostgresType b, final boolean listElements) {
    if (a == b)
      return a;

    final int rankA = numericRank(a);
    final int rankB = numericRank(b);
    if (rankA >= 0 && rankB >= 0 && a.isArrayType() == b.isArrayType()) {
      final int low = Math.min(rankA, rankB);
      final int high = Math.max(rankA, rankB);
      final int rank;
      if (high == 5)
        rank = 5;
      else if (high <= 2 || low >= 3)
        rank = high;
      else
        rank = 4; // an integer, smallint or long with a float is a double: numeric could not carry a NaN or an infinity
      final boolean array = a.isArrayType();
      return switch (rank) {
        case 0, 1 -> array ? ARRAY_INT : INTEGER;
        case 2 -> array ? ARRAY_LONG : LONG;
        case 3 -> array ? ARRAY_REAL : REAL;
        case 4 -> array ? ARRAY_DOUBLE : DOUBLE;
        default -> array ? ARRAY_NUMERIC : NUMERIC;
      };
    }

    if (listElements || a.isArrayType() && b.isArrayType())
      return ARRAY_TEXT;
    return VARCHAR;
  }

  /**
   * The type to announce for a property that the schema does not declare, when the announcement has to stay true for
   * rows that are not the ones inspected - the statement-level layout a client keeps and reuses (issue #9009). A
   * schemaless property can hold an integer in one record and a long, a double or text in the next, so only text can
   * be promised for it: every scalar becomes varchar and every list text[], while json and bytea, which carry their
   * own framing, are left as they are.
   */
  public static PostgresType stableTypeForUndeclared(final PostgresType type) {
    if (type == JSON || type == BYTEA || type == VARCHAR)
      return type;
    if (type.isArrayType())
      return ARRAY_TEXT;
    return type.isNativeScalarType() ? VARCHAR : type;
  }

  /**
   * Determines the appropriate array type based on the element type.
   */
  public static PostgresType getArrayTypeForElementType(Object element) {
    if (element instanceof Integer ||
        element instanceof Short ||
        element instanceof Byte)
      return ARRAY_INT;
    if (element instanceof Float ||
        element.getClass() == float.class)
      return ARRAY_REAL;
    if (element instanceof Double ||
        element.getClass() == double.class)
      return ARRAY_DOUBLE;
    if (element instanceof Long)
      return ARRAY_LONG;
    if (element instanceof Boolean)
      return ARRAY_BOOLEAN;
    if (element instanceof BigDecimal)
      return ARRAY_NUMERIC;
    if (element instanceof String)
      return ARRAY_TEXT;
    // A BINARY element is a bytea, as the scalar is, and not the JSON array of its bytes a primitive array is (issue #10927)
    if (element instanceof byte[] || element instanceof Binary)
      return ARRAY_BYTEA;
    // A list of documents is advertised as a single json value holding a JSON array, not as json[] (issue
    // #5366). A json[] forces every element to travel as a quoted string inside a Postgres array literal, and
    // JDBC-based clients (DBeaver, DataGrip/PhpStorm, DbVisualizer) then show the escaped text instead of the
    // documents. One json document renders as the plain [{...},{...}] the data actually is, everywhere.
    if (element instanceof JSONObject ||
        element instanceof Map ||
        element instanceof TraversalPath ||
        element instanceof Result ||
        element instanceof EmbeddedDocument ||
        element instanceof Record)
      return JSON;
    // A nested collection or array (issue #5365) cannot be announced as a flat array of scalars either: a
    // Postgres array is rectangular and homogeneous while an ArcadeDB nested list can be ragged and mixed.
    if (element instanceof Iterable ||
        element.getClass().isArray())
      return JSON;
    // Default to text array for all other types
    return ARRAY_TEXT;
  }

  /**
   * Deserializes a value based on the PostgreSQL type code and format code.
   *
   * @param code         The PostgreSQL type code
   * @param formatCode   The format code (0 for text, 1 for binary)
   * @param valueAsBytes The raw byte array to deserialize
   *
   * @return The deserialized object
   *
   * @throws PostgresProtocolException if the type or format is not supported
   */
  public static Object deserialize(final long code, final int formatCode, final byte[] valueAsBytes) {
    return switch (formatCode) {
      case 0 -> deserializeText(code, valueAsBytes);
      case 1 -> deserializeBinary(code, valueAsBytes);
      default -> throw new PostgresProtocolException("Invalid format code " + formatCode);
    };
  }

  private static Object deserializeText(final long code, final byte[] valueAsBytes) {
    String str = new String(valueAsBytes, DatabaseFactory.getDefaultCharset());
    if (code == 0) { // UNSPECIFIED
      // A client that leaves the type unspecified may still send a list as an array literal (psycopg does, issue #2559). It is
      // read as one only when it is a well-formed literal: a JSON document such as {"k": 1} merely starts and ends with a brace,
      // and stays the string it was sent as (issue #10870).
      if (str.startsWith("{") && str.endsWith("}")) {
        final List<Object> array = PostgresArrayLiteral.tryParse(str);
        if (array != null)
          return array;
      }
      return str;
    }

    if (code == UUID_CODE)
      return parseUuidText(str);

    PostgresType type = CODE_MAP.get((int) code);
    if (type == null) {
      throw new PostgresProtocolException("Type with code " + code + " not supported for deserializing");
    }

    return type.textParser.apply(str);
  }

  /**
   * A uuid parameter (issue #11838), which psycopg binds for a {@code uuid.UUID} and pgjdbc for a {@code java.util.UUID}. ArcadeDB
   * has no uuid type and stores one as its text, so the parameter is read as the canonical text PostgreSQL itself prints, lower
   * case with hyphens, from exactly the spellings PostgreSQL's uuid input accepts (string_to_uuid() in
   * src/backend/utils/adt/uuid.c, documented in "UUID Type"): 32 hex digits of either case, an optional hyphen after any group of
   * four digits but the last ({@code a0ee-bc99-9c0b-4ef8-bb6d-6bb9-bd38-0a11} is valid), the whole optionally between a MATCHED
   * pair of braces, and nothing else, no surrounding blank either. Parsed in place, without a copy of the text.
   */
  static String parseUuidText(final String value) {
    final int length = value.length();
    final boolean braces = length > 0 && value.charAt(0) == '{';
    int position = braces ? 1 : 0;
    long mostSignificant = 0;
    long leastSignificant = 0;
    for (int digit = 0; digit < 32; digit++) {
      final int nibble = position < length ? hexDigit(value.charAt(position++)) : -1;
      if (nibble < 0)
        throw invalidUuid(value);
      if (digit < 16)
        mostSignificant = mostSignificant << 4 | nibble;
      else
        leastSignificant = leastSignificant << 4 | nibble;
      if ((digit & 3) == 3 && digit < 31 && position < length && value.charAt(position) == '-')
        position++;
    }
    if (braces) {
      if (position >= length || value.charAt(position) != '}')
        throw invalidUuid(value);
      position++;
    }
    if (position != length)
      throw invalidUuid(value);
    return new UUID(mostSignificant, leastSignificant).toString();
  }

  /** An ASCII hex digit's value, or -1: PostgreSQL reads uuid digits with isxdigit(), so no other script's digits count. */
  private static int hexDigit(final char c) {
    if (c >= '0' && c <= '9')
      return c - '0';
    if (c >= 'a' && c <= 'f')
      return c - 'a' + 10;
    if (c >= 'A' && c <= 'F')
      return c - 'A' + 10;
    return -1;
  }

  private static PostgresProtocolException invalidUuid(final String value) {
    return new PostgresProtocolException("Invalid input syntax for type uuid: '" + value + "'");
  }

  private static Object deserializeBinary(final long code, final byte[] valueAsBytes) {
    ByteBuffer buffer = ByteBuffer.wrap(valueAsBytes);
    if (code == UUID_CODE) {
      // the 16 bytes of the uuid, most significant first
      if (valueAsBytes.length != 16)
        throw new PostgresProtocolException("Invalid binary uuid of " + valueAsBytes.length + " bytes");
      return new UUID(buffer.getLong(), buffer.getLong()).toString();
    }

    PostgresType type = CODE_MAP.get((int) code);

    if (type == null) {
      throw new PostgresProtocolException("Type with code " + code + " not supported for deserializing");
    }

    return switch (type) {
      case VARCHAR, TEXT, BPCHAR -> {
        // In PostgreSQL binary format, VARCHAR/TEXT/BPCHAR is just the raw bytes
        // The length is already provided in the Bind message's parameter size
        yield new String(valueAsBytes, DatabaseFactory.getDefaultCharset());
      }
      case SMALLINT -> buffer.getShort();
      case INTEGER -> buffer.getInt();
      case LONG -> buffer.getLong();
      case REAL -> buffer.getFloat();
      case DOUBLE -> buffer.getDouble();
      case DATE -> {
        // PostgreSQL binary DATE: int32 days since 2000-01-01
        final int days = buffer.getInt();
        yield LocalDate.ofEpochDay(POSTGRES_EPOCH_DAYS + days);
      }
      // PostgreSQL binary TIMESTAMP: int64 microseconds since 2000-01-01T00:00:00Z
      case TIMESTAMP -> fromPostgresEpochMicros(buffer.getLong());
      // timestamptz is the same count of an instant, with no zone on the wire: read at UTC (issue #11960)
      case TIMESTAMPTZ -> fromPostgresEpochMicros(buffer.getLong()).atZone(ZoneOffset.UTC);
      // time: int64 microseconds since midnight; timetz adds an int32 zone offset in seconds WEST of UTC (the sign PostgreSQL stores)
      case TIME -> LocalTime.ofNanoOfDay(buffer.getLong() * 1_000L);
      case TIMETZ -> {
        final LocalTime time = LocalTime.ofNanoOfDay(buffer.getLong() * 1_000L);
        yield OffsetTime.of(time, ZoneOffset.ofTotalSeconds(-buffer.getInt()));
      }
      // interval: int64 microseconds, int32 days, int32 months
      case INTERVAL -> {
        final long microseconds = buffer.getLong();
        final int days = buffer.getInt();
        final int months = buffer.getInt();
        yield new CypherDuration(months, days, Math.floorDiv(microseconds, 1_000_000L), Math.floorMod(microseconds, 1_000_000L) * 1_000L);
      }
      case BYTEA -> valueAsBytes.clone();
      case NUMERIC -> parseNumericBinary(buffer);
      // "char" is one UNSIGNED byte: a bare (char) cast sign-extends, and 'é' (0xE9) came back as '￩'
      case CHAR -> (char) (buffer.get() & 0xFF);
      case BOOLEAN -> buffer.get() == 1;
      case JSON -> {
        // In PostgreSQL binary format, json is the raw UTF-8 text with no inner length prefix
        // (json_send emits it verbatim); the length is already provided by the Bind message.
        yield parseJsonText(new String(valueAsBytes, DatabaseFactory.getDefaultCharset()));
      }
      case JSONB -> {
        // jsonb_send writes a version byte (1) ahead of the json text
        if (valueAsBytes.length == 0 || valueAsBytes[0] != 1)
          throw new PostgresProtocolException("Unsupported binary jsonb version " + (valueAsBytes.length == 0 ? "(empty)" : valueAsBytes[0]));
        yield parseJsonText(new String(valueAsBytes, 1, valueAsBytes.length - 1, DatabaseFactory.getDefaultCharset()));
      }
      case ARRAY_INT, ARRAY_LONG, ARRAY_DOUBLE, ARRAY_REAL, ARRAY_TEXT, ARRAY_BOOLEAN, ARRAY_CHAR, ARRAY_JSON, ARRAY_NUMERIC,
           ARRAY_SMALLINT, ARRAY_BYTEA, ARRAY_BPCHAR, ARRAY_VARCHAR, ARRAY_DATE, ARRAY_TIMESTAMP, ARRAY_TIMESTAMPTZ, ARRAY_TIME,
           ARRAY_TIMETZ, ARRAY_INTERVAL, ARRAY_JSONB, ARRAY_NAME, ARRAY_OID, ARRAY_INT2VECTOR -> deserializeBinaryArray(buffer);
      // namesend writes the bytes of the name, as text does
      case NAME -> new String(valueAsBytes, DatabaseFactory.getDefaultCharset());
      // an oid is an UNSIGNED 32-bit number: read into a long, as the text format reads it, so 4294967295 is not -1
      case OID -> Integer.toUnsignedLong(buffer.getInt());
      // int2vectorsend writes the array format of int2[], and the text format reads it as its "1 2 3" text: read the same way
      case INT2VECTOR -> deserializeBinaryArray(buffer).stream().map(String::valueOf).collect(Collectors.joining(" "));
    };
  }

  /** A PostgreSQL binary timestamp, microseconds since 2000-01-01T00:00:00 UTC, as the UTC wall time it names. */
  private static LocalDateTime fromPostgresEpochMicros(final long microseconds) {
    return LocalDateTime.ofEpochSecond(POSTGRES_EPOCH_SECONDS + Math.floorDiv(microseconds, 1_000_000L),
        (int) Math.floorMod(microseconds, 1_000_000L) * 1000, ZoneOffset.UTC);
  }

  private static List<Object> deserializeBinaryArray(ByteBuffer buffer) {
    final int ndim = buffer.getInt();    // number of dimensions
    buffer.getInt();                      // hasnull flag (unused)
    final int elemOid = buffer.getInt(); // element type OID

    if (ndim == 0)
      return new ArrayList<>();

    if (ndim < 0 || ndim > MAX_ARRAY_DIMENSIONS)
      throw new PostgresProtocolException("Invalid array dimension count: " + ndim);

    long totalElements = 1L;
    for (int d = 0; d < ndim; d++) {
      final int dimSize = buffer.getInt();
      buffer.getInt();                   // lower bound (unused)
      if (dimSize < 0)
        throw new PostgresProtocolException("Negative array dimension size: " + dimSize);
      totalElements *= dimSize;
      if (totalElements > Integer.MAX_VALUE)
        throw new PostgresProtocolException("Array element count exceeds Integer.MAX_VALUE");
    }

    // Each element occupies at least 4 bytes (the length prefix), so an element count
    // exceeding remaining/4 cannot fit and signals a malformed or malicious payload.
    if (totalElements > buffer.remaining() / 4L)
      throw new PostgresProtocolException("Array element count exceeds remaining buffer bytes");

    final int count = (int) totalElements;
    final ArrayList<Object> result = new ArrayList<>(count);
    for (int i = 0; i < count; i++) {
      final int elemLen = buffer.getInt();
      if (elemLen == -1) {
        result.add(null);
      } else {
        // The element is read out of the bytes this value holds: a length past them, or below the null marker, is no element
        if (elemLen < -1 || elemLen > buffer.remaining())
          throw new PostgresProtocolException("Invalid array element length: " + elemLen);
        final byte[] elemBytes = new byte[elemLen];
        buffer.get(elemBytes);
        result.add(deserializeBinaryElement(elemOid, elemBytes));
      }
    }
    return result;
  }

  private static Object deserializeBinaryElement(final int elemOid, final byte[] bytes) {
    final ByteBuffer buf = ByteBuffer.wrap(bytes);
    if (elemOid == BOOLEAN.code)
      return buf.get() != 0;
    if (elemOid == LONG.code)
      return buf.getLong();
    // the elements of int2[] (OID 1005, ARRAY_SMALLINT)
    if (elemOid == SMALLINT.code)
      return buf.getShort();
    if (elemOid == INTEGER.code)
      return buf.getInt();
    if (elemOid == REAL.code)
      return buf.getFloat();
    if (elemOid == DOUBLE.code)
      return buf.getDouble();
    if (elemOid == NUMERIC.code)
      return parseNumericBinary(buf);
    // a "char"[] element is the single byte the scalar is, read as the Character the text format reads (issue #10870)
    if (elemOid == CHAR.code)
      return (char) (buf.get() & 0xFF);
    // the elements of the date[], timestamp[] and bytea[] a client binds (issue #10592), and of the temporal, oid and int2vector
    // arrays (issue #12449): read as the scalar is, not as its bytes' text
    if (elemOid == DATE.code || elemOid == TIMESTAMP.code || elemOid == BYTEA.code || elemOid == TIMESTAMPTZ.code || elemOid == TIME.code
        || elemOid == TIMETZ.code || elemOid == INTERVAL.code || elemOid == OID.code || elemOid == INT2VECTOR.code)
      return deserializeBinary(elemOid, bytes);
    // a jsonb[] element is its json text, as a json[] element and the text format of both read it, after jsonb_send's version byte
    if (elemOid == JSONB.code) {
      if (bytes.length == 0 || bytes[0] != 1)
        throw new PostgresProtocolException("Unsupported binary jsonb version " + (bytes.length == 0 ? "(empty)" : bytes[0]));
      return new String(bytes, 1, bytes.length - 1, DatabaseFactory.getDefaultCharset());
    }
    // text/varchar/bpchar/json/unknown - raw bytes are already the UTF-8 string content.
    return new String(bytes, DatabaseFactory.getDefaultCharset());
  }

  /**
   * Checks if this type is an array type.
   */
  public boolean isArrayType() {
    // an array type is one that names an element type, read off the pairing every constant declares so that a new array constant
    // cannot be left out of a list of them (issue #10592); name and int2vector name one without being arrays (issue #12449)
    return array;
  }

  /**
   * Returns true for scalar types that should be advertised with their native Postgres OID rather
   * than collapsing to VARCHAR. Clients use the announced OID to choose a deserializer, so
   * advertising VARCHAR for numeric/boolean/temporal columns causes values to round-trip as
   * strings and breaks typed parameter comparisons.
   */
  public boolean isNativeScalarType() {
    return this == BYTEA ||
        this == SMALLINT ||
        this == INTEGER ||
        this == LONG ||
        this == REAL ||
        this == DOUBLE ||
        this == NUMERIC ||
        this == CHAR ||
        this == BOOLEAN ||
        this == DATE ||
        this == TIMESTAMP;
  }
}
