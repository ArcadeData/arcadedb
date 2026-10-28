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
package com.arcadedb.index.columnar;

import com.arcadedb.TestHelper;
import com.arcadedb.database.Document;
import com.arcadedb.database.MutableDocument;
import com.arcadedb.database.Record;
import com.arcadedb.index.IndexException;
import com.arcadedb.query.sql.executor.Result;
import com.arcadedb.query.sql.executor.ResultSet;
import com.arcadedb.schema.DocumentType;
import com.arcadedb.schema.Type;
import com.arcadedb.serializer.json.JSONObject;
import com.arcadedb.utility.DateUtils;
import org.junit.jupiter.api.Test;

import java.math.BigDecimal;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.temporal.ChronoUnit;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class ColumnarValueExtractorTest extends TestHelper {
  private final ColumnarStats                     stats        = new ColumnarStats();
  private final Map<Integer, GlobalDictionary>    dictionaries = new HashMap<>();
  private       ColumnarValueExtractor            extractor;

  /** Records every sink call as text, so a test reads what the extractor said. */
  private static final class RecordingSink implements ValueSink {
    final List<String> events = new ArrayList<>();
    int   ordinal = -1;
    int[] ordinals;
    long  longValue;
    double doubleValue;

    @Override
    public void ordinal(final int o) {
      ordinal = o;
      events.add("ordinal");
    }

    @Override
    public void ordinals(final int[] sorted, final int count) {
      ordinals = Arrays.copyOf(sorted, count);
      events.add("ordinals");
    }

    @Override
    public void longValue(final long v) {
      longValue = v;
      events.add("long");
    }

    @Override
    public void doubleValue(final double v) {
      doubleValue = v;
      events.add("double");
    }

    @Override
    public void missing() {
      events.add("missing");
    }

    @Override
    public void unknownValue(final Object normalized) {
      events.add("unknown:" + normalized);
    }
  }

  private static ColumnDefinition define(final String property, final Type type, final boolean byItem) {
    final ChronoUnit precision = type == Type.DATE ? ChronoUnit.DAYS
        : type == Type.DATETIME || type == Type.DATETIME_SECOND || type == Type.DATETIME_MICROS || type == Type.DATETIME_NANOS
            ? DateUtils.getPrecisionFromType(type)
            : null;
    return new ColumnDefinition(1, property, type, ColumnKind.of(type, byItem), byItem, precision);
  }

  private GlobalDictionary dictionaryOf(final int bucketId, final String property) {
    return dictionaries.computeIfAbsent(bucketId * 31 + property.hashCode(), k -> GlobalDictionary.createInMemory());
  }

  private ColumnarValueExtractor extractorFor(final ColumnDefinition... columns) {
    return new ColumnarValueExtractor("Asset", columns, this::dictionaryOf, stats);
  }

  private DocumentType assetType() {
    return database.getSchema().createDocumentType("Asset");
  }

  private ColumnDefinition[] allColumns() {
    return new ColumnDefinition[] { define("brand", Type.STRING, false), define("tags", Type.LIST, true),
        define("fileSize", Type.LONG, false), define("price", Type.DOUBLE, false), define("publishDate", Type.DATETIME_MICROS, false),
        define("day", Type.DATE, false), define("modified", Type.DATETIME, false),
        define("mixed", Type.STRING, false) };
  }

  private void declareAllProperties(final DocumentType type) {
    type.createProperty("brand", Type.STRING);
    type.createProperty("tags", Type.LIST);
    type.createProperty("fileSize", Type.LONG);
    type.createProperty("price", Type.DOUBLE);
    type.createProperty("publishDate", Type.DATETIME_MICROS);
    type.createProperty("day", Type.DATE);
    type.createProperty("modified", Type.DATETIME);
  }

  private Document save(final MutableDocument document) {
    final Document[] saved = new Document[1];
    database.transaction(() -> {
      document.save();
      saved[0] = document;
    });
    return saved[0].getIdentity().asDocument();
  }

  private RecordingSink extract(final Document record, final String property) {
    final RecordingSink sink = new RecordingSink();
    extractor.extract(record, property, sink);
    return sink;
  }

  /** Extraction as the apply thread does it: grows the dictionary of the record's bucket and counts failures. */
  private RecordingSink applyExtract(final Document record, final int columnIndex) {
    final ColumnDefinition column = allColumns()[columnIndex];
    final RecordingSink sink = new RecordingSink();
    extractor.extractForApply(record, column, dictionaryOf(record.getIdentity().getBucketId(), column.property()), sink);
    return sink;
  }

  @Test
  void stringIsOrdinalOfNormalizedValue() {
    declareAllProperties(assetType());
    extractor = extractorFor(allColumns());
    final Document first = save(database.newDocument("Asset").set("brand", "Nike"));
    final Document second = save(database.newDocument("Asset").set("brand", "Nike"));

    final RecordingSink sink = new RecordingSink();
    final GlobalDictionary dictionary = dictionaryOf(first.getIdentity().getBucketId(), "brand");
    extractor.extractForApply(first, allColumns()[0], dictionary, sink);
    assertThat(sink.events).containsExactly("ordinal");
    assertThat(sink.ordinal).isZero();

    final RecordingSink again = new RecordingSink();
    extractor.extractForApply(second, allColumns()[0], dictionary, again);
    assertThat(again.ordinal).isZero();
    assertThat(dictionary.size()).isEqualTo(1);
  }

  @Test
  void integralNumbersShareOneLongKey() {
    declareAllProperties(assetType());
    extractor = extractorFor(allColumns());
    final Document fromInteger = save(database.newDocument("Asset").set("fileSize", 12));
    final Document fromLong = save(database.newDocument("Asset").set("fileSize", 12L));

    assertThat(extract(fromInteger, "fileSize").longValue).isEqualTo(12L);
    assertThat(extract(fromLong, "fileSize").longValue).isEqualTo(12L);
    assertThat(extract(fromInteger, "fileSize").events).containsExactly("long");
  }

  @Test
  void dateTimeUsesThePropertyPrecision() {
    declareAllProperties(assetType());
    extractor = extractorFor(allColumns());
    final LocalDateTime when = LocalDateTime.of(2024, 5, 17, 13, 45, 12, 123_456_000);
    final Document record = save(database.newDocument("Asset").set("publishDate", when).set("modified", when));

    final RecordingSink micros = extract(record, "publishDate");
    assertThat(micros.longValue).isEqualTo(DateUtils.dateTimeToTimestamp(when, ChronoUnit.MICROS));
    final RecordingSink millis = extract(record, "modified");
    assertThat(millis.longValue).isEqualTo(DateUtils.dateTimeToTimestamp(when, ChronoUnit.MILLIS));
    assertThat(micros.longValue).isNotEqualTo(millis.longValue);
  }

  @Test
  void dateColumnIsEpochDays() {
    declareAllProperties(assetType());
    extractor = extractorFor(allColumns());
    final LocalDate day = LocalDate.of(1969, 12, 31);
    final Document record = save(database.newDocument("Asset").set("day", day));

    assertThat(extract(record, "day").longValue).isEqualTo(-1L);
  }

  @Test
  void floatingPointColumnWidensLikeTheDecimalForm() {
    final DocumentType type = assetType();
    declareAllProperties(type);
    extractor = extractorFor(allColumns());
    final Document record = save(database.newDocument("Asset").set("price", 0.05f));

    // Type.widenFloat reads 0.05f as 0.05, not as 0.05000000074505806
    assertThat(extract(record, "price").doubleValue).isEqualTo(0.05d);
  }

  @Test
  void listByItemIsSortedAndDeduplicated() {
    declareAllProperties(assetType());
    extractor = extractorFor(allColumns());
    final Document record = save(database.newDocument("Asset").set("tags", List.of("b", "a", "b")));

    final GlobalDictionary dictionary = dictionaryOf(record.getIdentity().getBucketId(), "tags");
    dictionary.getOrAdd("z");
    final RecordingSink sink = new RecordingSink();
    extractor.extractForApply(record, allColumns()[1], dictionary, sink);

    assertThat(sink.events).containsExactly("ordinals");
    assertThat(sink.ordinals).containsExactly(dictionary.ordinalOf("b"), dictionary.ordinalOf("a"));
    assertThat(sink.ordinals[0]).isLessThan(sink.ordinals[1]);
    assertThat(dictionary.ordinalOf("b")).isEqualTo(1);
  }

  @Test
  void emptyAndAllFailingListsAreMissing() {
    declareAllProperties(assetType());
    extractor = extractorFor(allColumns());
    final Document empty = save(database.newDocument("Asset").set("tags", List.of()));
    final Document nested = save(database.newDocument("Asset").set("tags", List.of(Map.of("k", "v"))));

    assertThat(extract(empty, "tags").events).containsExactly("missing");
    assertThat(stats.conversionFailures("Asset", "tags")).isZero();
    assertThat(applyExtract(nested, 1).events).containsExactly("missing");
    assertThat(stats.conversionFailures("Asset", "tags")).isEqualTo(1);
  }

  @Test
  void nullAndAbsentPropertiesAreMissingWithoutFailure() {
    declareAllProperties(assetType());
    extractor = extractorFor(allColumns());
    final Document record = save(database.newDocument("Asset"));

    for (final String property : List.of("brand", "tags", "fileSize", "price", "publishDate", "day"))
      assertThat(extract(record, property).events).as(property).containsExactly("missing");
    assertThat(stats.conversionFailures("Asset", "brand")).isZero();
    assertThat(stats.conversionFailures("Asset", "fileSize")).isZero();
  }

  @Test
  void unconvertibleValueIsMissingAndCounted() {
    final DocumentType type = assetType();
    // plant raw values while the properties are not in the schema: they stay as they were stored
    final Document sized = save(database.newDocument("Asset").set("fileSize", new BigDecimal("1.5")));
    final Document tagged = save(database.newDocument("Asset").set("tags", List.of("a", Map.of("k", "v"), "b")));
    final Document priced = save(database.newDocument("Asset").set("price", "cheap"));
    declareAllProperties(type);
    extractor = extractorFor(allColumns());

    assertThat(applyExtract(sized, 2).events).containsExactly("missing");
    assertThat(stats.conversionFailures("Asset", "fileSize")).isEqualTo(1);

    final RecordingSink tags = new RecordingSink();
    extractor.extractForApply(tagged, allColumns()[1], dictionaryOf(tagged.getIdentity().getBucketId(), "tags"), tags);
    assertThat(tags.events).containsExactly("ordinals");
    assertThat(tags.ordinals).hasSize(2);
    assertThat(stats.conversionFailures("Asset", "tags")).isEqualTo(1);

    assertThat(applyExtract(priced, 3).events).containsExactly("missing");
    assertThat(stats.conversionFailures("Asset", "price")).isEqualTo(1);
    assertThat(stats.conversionFailures("Asset", "brand")).isZero();
  }

  @Test
  void unreadableDateValuesAreMissingAndCountedWithoutThrowing() {
    final DocumentType type = assetType();
    // raw values the date converters refuse: unparseable strings, a boolean, a map, an out-of-range number
    final List<Object> bad = List.of("not a date", "2024-13-45", true, Map.of("k", "v"), new BigDecimal("1e400"));
    final List<Document> records = new ArrayList<>();
    for (final Object value : bad)
      records.add(save(database.newDocument("Asset").set("day", value).set("modified", value).set("publishDate", value)));
    declareAllProperties(type);
    extractor = extractorFor(allColumns());

    int expected = 0;
    for (final Document record : records) {
      expected++;
      for (final int column : new int[] { 4, 5, 6 })
        assertThat(applyExtract(record, column).events).containsExactly("missing");
      assertThat(stats.conversionFailures("Asset", "day")).isEqualTo(expected);
      assertThat(stats.conversionFailures("Asset", "modified")).isEqualTo(expected);
      assertThat(stats.conversionFailures("Asset", "publishDate")).isEqualTo(expected);
    }
  }

  @Test
  void readOnlyExtractionDoesNotCountFailures() {
    final DocumentType type = assetType();
    final Document sized = save(database.newDocument("Asset").set("fileSize", new BigDecimal("1.5")));
    final Document tagged = save(database.newDocument("Asset").set("tags", List.of("a", Map.of("k", "v"))));
    declareAllProperties(type);
    extractor = extractorFor(allColumns());

    assertThat(extract(sized, "fileSize").events).containsExactly("missing");
    extract(tagged, "tags");
    extractor.extract(tagged, "tags", GlobalDictionary.createInMemory(), new RecordingSink());

    assertThat(stats.conversionFailures("Asset", "fileSize")).isZero();
    assertThat(stats.conversionFailures("Asset", "tags")).isZero();
  }

  @Test
  void unknownListItemsAreReportedOncePerDistinctValue() {
    declareAllProperties(assetType());
    extractor = extractorFor(allColumns());
    final Document repeated = save(database.newDocument("Asset").set("tags", List.of("x", "x", "known")));
    final Document numbers = save(database.newDocument("Asset").set("tags", List.of(5, 5L, 6)));
    final GlobalDictionary tags = dictionaryOf(repeated.getIdentity().getBucketId(), "tags");
    final int known = tags.getOrAdd("known");

    final RecordingSink first = extract(repeated, "tags");
    assertThat(first.events).containsExactly("unknown:x", "ordinals");
    assertThat(first.ordinals).containsExactly(known);

    final RecordingSink second = extract(numbers, "tags");
    assertThat(second.events).containsExactly("unknown:5", "unknown:6");
    // the buffer of the previous call leaves nothing behind
    assertThat(extract(repeated, "tags").events).containsExactly("unknown:x", "ordinals");
  }

  @Test
  void negativeZeroIsStoredAsZero() {
    declareAllProperties(assetType());
    extractor = extractorFor(allColumns());
    final Document negative = save(database.newDocument("Asset").set("price", -0.0d));

    final double stored = extract(negative, "price").doubleValue;
    assertThat(Double.doubleToRawLongBits(stored)).isEqualTo(Double.doubleToRawLongBits(0.0d));
  }

  @Test
  void hugeListDoesNotLeaveALargeBufferBehindAndStillWorks() {
    declareAllProperties(assetType());
    extractor = extractorFor(allColumns());
    final List<String> many = new ArrayList<>();
    for (int i = 0; i < 10_000; i++)
      many.add("t" + i);
    final Document big = save(database.newDocument("Asset").set("tags", many));
    final Document small = save(database.newDocument("Asset").set("tags", List.of("t3")));
    final GlobalDictionary tags = dictionaryOf(big.getIdentity().getBucketId(), "tags");
    for (final String tag : many)
      tags.getOrAdd(tag);

    assertThat(extract(big, "tags").ordinals).hasSize(10_000);
    assertThat(extract(small, "tags").ordinals).containsExactly(tags.ordinalOf("t3"));
  }

  @Test
  void stringPropertyHoldingANumberKeysLikeGroupBy() {
    final DocumentType type = assetType();
    final Document record = save(database.newDocument("Asset").set("brand", 5));
    declareAllProperties(type);
    extractor = extractorFor(allColumns());

    final GlobalDictionary dictionary = dictionaryOf(record.getIdentity().getBucketId(), "brand");
    final RecordingSink sink = new RecordingSink();
    extractor.extractForApply(record, allColumns()[0], dictionary, sink);

    assertThat(sink.events).containsExactly("ordinal");
    assertThat(dictionary.valueOf(sink.ordinal)).isEqualTo(5L);
    assertThat(stats.conversionFailures("Asset", "brand")).isZero();

    try (final ResultSet result = database.query("sql", "SELECT brand AS v FROM Asset GROUP BY brand")) {
      // the result carries the raw value, the group key is its normalization
      assertThat(Type.normalizeNumberForKey(result.next().<Object>getProperty("v"))).isEqualTo(5L);
      assertThat(result.hasNext()).isFalse();
    }
  }

  @Test
  void readOnlyExtractionNeverGrowsTheDictionary() {
    declareAllProperties(assetType());
    extractor = extractorFor(allColumns());
    final Document record = save(database.newDocument("Asset").set("brand", "new").set("tags", List.of("x", "known")));
    final GlobalDictionary tags = dictionaryOf(record.getIdentity().getBucketId(), "tags");
    final int known = tags.getOrAdd("known");

    final RecordingSink brand = extract(record, "brand");
    assertThat(brand.events).containsExactly("unknown:new");
    assertThat(dictionaryOf(record.getIdentity().getBucketId(), "brand").size()).isZero();

    final RecordingSink list = extract(record, "tags");
    assertThat(list.events).containsExactly("unknown:x", "ordinals");
    assertThat(list.ordinals).containsExactly(known);
    assertThat(tags.size()).isEqualTo(1);
  }

  @Test
  void dictionaryIsResolvedPerBucket() {
    final DocumentType type = database.getSchema().buildDocumentType().withName("Asset").withTotalBuckets(2).create();
    declareAllProperties(type);
    extractor = extractorFor(allColumns());
    final String bucketA = type.getBuckets(false).get(0).getName();
    final String bucketB = type.getBuckets(false).get(1).getName();

    final MutableDocument nikeA = database.newDocument("Asset").set("brand", "Nike");
    final MutableDocument adidasB = database.newDocument("Asset").set("brand", "Adidas");
    final MutableDocument nikeB = database.newDocument("Asset").set("brand", "Nike");
    database.transaction(() -> {
      nikeA.save(bucketA);
      adidasB.save(bucketB);
      nikeB.save(bucketB);
    });
    final int idA = nikeA.getIdentity().getBucketId();
    final int idB = nikeB.getIdentity().getBucketId();
    assertThat(idA).isNotEqualTo(idB);

    final ColumnDefinition brand = allColumns()[0];
    for (final MutableDocument document : List.of(nikeA, adidasB, nikeB))
      extractor.extractForApply(document, brand, dictionaryOf(document.getIdentity().getBucketId(), "brand"), new RecordingSink());

    assertThat(dictionaryOf(idA, "brand").size()).isEqualTo(1);
    assertThat(dictionaryOf(idB, "brand").size()).isEqualTo(2);
    assertThat(extract(nikeA, "brand").ordinal).isEqualTo(dictionaryOf(idA, "brand").ordinalOf("Nike")).isZero();
    assertThat(extract(adidasB, "brand").ordinal).isEqualTo(dictionaryOf(idB, "brand").ordinalOf("Adidas")).isZero();
    assertThat(extract(nikeB, "brand").ordinal).isEqualTo(dictionaryOf(idB, "brand").ordinalOf("Nike")).isEqualTo(1);
  }

  @Test
  void explicitDictionaryOverloadUsesIt() {
    declareAllProperties(assetType());
    extractor = extractorFor(allColumns());
    final Document record = save(database.newDocument("Asset").set("brand", "Nike"));
    dictionaryOf(record.getIdentity().getBucketId(), "brand").getOrAdd("Nike");

    final GlobalDictionary otherGeneration = GlobalDictionary.createInMemory();
    otherGeneration.getOrAdd("Puma");
    otherGeneration.getOrAdd("Reebok");
    otherGeneration.getOrAdd("Nike");

    final RecordingSink viaResolver = extract(record, "brand");
    assertThat(viaResolver.ordinal).isZero();
    final RecordingSink explicit = new RecordingSink();
    extractor.extract(record, "brand", otherGeneration, explicit);
    assertThat(explicit.ordinal).isEqualTo(2);
  }

  @Test
  void matchesGroupByKeysOnRandomData() {
    declareAllProperties(assetType());
    extractor = extractorFor(allColumns());
    final Random random = new Random(42);
    final String[] brands = new String[40];
    for (int i = 0; i < brands.length; i++)
      brands[i] = "brand" + i;

    database.transaction(() -> {
      for (int i = 0; i < 2_000; i++) {
        final MutableDocument document = database.newDocument("Asset");
        if (random.nextInt(10) > 0)
          document.set("brand", brands[random.nextInt(brands.length)]);
        final int tagCount = random.nextInt(5);
        if (tagCount > 0) {
          final List<String> tags = new ArrayList<>();
          for (int t = 0; t < tagCount; t++)
            tags.add("tag" + random.nextInt(15));
          document.set("tags", tags);
        }
        final int mixed = random.nextInt(8);
        switch (mixed) {
        case 0 -> document.set("mixed", 5);
        case 1 -> document.set("mixed", 5L);
        case 2 -> document.set("mixed", 5.0d);
        case 3 -> document.set("mixed", new BigDecimal("5.00"));
        case 4 -> document.set("mixed", 1.5d);
        case 5 -> document.set("mixed", "five");
        default -> {
        }
        }
        document.save();
      }
    });

    final Map<Object, Long> extractedBrands = new HashMap<>();
    final Map<Object, Long> extractedTags = new HashMap<>();
    final Map<Object, Long> extractedMixed = new HashMap<>();
    database.transaction(() -> {
      final Iterator<Record> records = database.iterateType("Asset", true);
      while (records.hasNext()) {
        final Document record = (Document) records.next();
        final GlobalDictionary brandDictionary = dictionaryOf(record.getIdentity().getBucketId(), "brand");
        final GlobalDictionary tagDictionary = dictionaryOf(record.getIdentity().getBucketId(), "tags");
        final RecordingSink brand = new RecordingSink();
        extractor.extractForApply(record, allColumns()[0], brandDictionary, brand);
        if (brand.ordinal >= 0 && brand.events.contains("ordinal"))
          extractedBrands.merge(brandDictionary.valueOf(brand.ordinal), 1L, Long::sum);
        final GlobalDictionary mixedDictionary = dictionaryOf(record.getIdentity().getBucketId(), "mixed");
        final RecordingSink mixed = new RecordingSink();
        extractor.extractForApply(record, allColumns()[7], mixedDictionary, mixed);
        if (mixed.events.contains("ordinal"))
          extractedMixed.merge(mixedDictionary.valueOf(mixed.ordinal), 1L, Long::sum);
        final RecordingSink tags = new RecordingSink();
        extractor.extractForApply(record, allColumns()[1], tagDictionary, tags);
        if (tags.ordinals != null)
          for (final int ordinal : tags.ordinals)
            extractedTags.merge(tagDictionary.valueOf(ordinal), 1L, Long::sum);
      }
    });

    assertThat(extractedBrands).isNotEmpty().isEqualTo(groupBy("SELECT brand AS v, count(*) AS c FROM Asset GROUP BY brand"));
    assertThat(extractedTags).isNotEmpty()
        .isEqualTo(groupBy("SELECT v, count(*) AS c FROM (SELECT tags.asSet() AS v FROM Asset UNWIND v) GROUP BY v"));
    assertThat(extractedMixed).containsKeys(5L, new BigDecimal("1.5"), "five")
        .isEqualTo(groupBy("SELECT mixed AS v, count(*) AS c FROM Asset GROUP BY mixed"));
    assertThat(stats.conversionFailures("Asset", "brand")).isZero();
    assertThat(stats.conversionFailures("Asset", "tags")).isZero();
    assertThat(stats.conversionFailures("Asset", "mixed")).isZero();
  }

  private Map<Object, Long> groupBy(final String sql) {
    final Map<Object, Long> counts = new HashMap<>();
    try (final ResultSet result = database.query("sql", sql)) {
      while (result.hasNext()) {
        final Result row = result.next();
        final Object key = row.getProperty("v");
        if (key != null)
          // the row carries the raw value, the group key is its normalization
          counts.merge(Type.normalizeNumberForKey(key), ((Number) row.getProperty("c")).longValue(), Long::sum);
      }
    }
    return counts;
  }

  @Test
  void toColumnLongMatchesExtraction() {
    declareAllProperties(assetType());
    extractor = extractorFor(allColumns());
    final LocalDateTime when = LocalDateTime.of(2024, 5, 17, 13, 45, 12, 123_456_000);
    final LocalDate day = LocalDate.of(2024, 5, 17);
    final Document record = save(database.newDocument("Asset").set("fileSize", 12).set("publishDate", when).set("day", day));

    assertThat(extractor.toColumnLong("fileSize", 12)).isEqualTo(extract(record, "fileSize").longValue).isEqualTo(12L);
    assertThat(extractor.toColumnLong("fileSize", 12.0d)).isEqualTo(12L);
    assertThat(extractor.toColumnLong("publishDate", when)).isEqualTo(extract(record, "publishDate").longValue);
    assertThat(extractor.toColumnLong("day", day)).isEqualTo(extract(record, "day").longValue).isEqualTo(day.toEpochDay());
  }

  @Test
  void toColumnLongRefusesUnconvertible() {
    declareAllProperties(assetType());
    extractor = extractorFor(allColumns());

    assertThatThrownBy(() -> extractor.toColumnLong("fileSize", "abc")).isInstanceOf(IllegalArgumentException.class);
    assertThatThrownBy(() -> extractor.toColumnLong("fileSize", new BigDecimal("1.5"))).isInstanceOf(IllegalArgumentException.class);
    assertThatThrownBy(() -> extractor.toColumnLong("fileSize", null)).isInstanceOf(IllegalArgumentException.class);
    assertThatThrownBy(() -> extractor.toColumnLong("brand", 1)).isInstanceOf(IllegalArgumentException.class);
    assertThatThrownBy(() -> extractor.toColumnLong("price", 1)).isInstanceOf(IllegalArgumentException.class);
    assertThatThrownBy(() -> extractor.toColumnLong("nope", 1)).isInstanceOf(IllegalArgumentException.class);
  }

  @Test
  void dateUnitPerPropertyType() {
    extractor = extractorFor(allColumns());

    assertThat(extractor.dateUnit("day")).isEqualTo(TimeUnit.DAYS);
    assertThat(extractor.dateUnit("modified")).isEqualTo(TimeUnit.MILLISECONDS);
    assertThat(extractor.dateUnit("publishDate")).isEqualTo(TimeUnit.MICROSECONDS);
    assertThat(extractor.dateUnit("fileSize")).isNull();
    assertThat(extractor.dateUnit("brand")).isNull();
  }

  @Test
  void unsupportedPropertyTypesAreRefused() {
    for (final Type type : List.of(Type.DECIMAL, Type.MAP, Type.LIST, Type.EMBEDDED, Type.BINARY, Type.ARRAY_OF_FLOATS))
      assertThatThrownBy(() -> ColumnKind.of(type, false)).as(type.name()).isInstanceOf(IndexException.class);
    assertThatThrownBy(() -> ColumnKind.of(Type.STRING, true)).isInstanceOf(IndexException.class);

    assertThat(ColumnKind.of(Type.LIST, true)).isEqualTo(ColumnKind.ORDINAL_LIST);
    assertThat(ColumnKind.of(Type.ARRAY_OF_INTEGERS, true)).isEqualTo(ColumnKind.ORDINAL_LIST);
    assertThat(ColumnKind.of(Type.LINK, false)).isEqualTo(ColumnKind.ORDINAL);
    assertThat(ColumnKind.of(Type.DATETIME_NANOS, false)).isEqualTo(ColumnKind.LONG);
    assertThat(ColumnKind.of(Type.FLOAT, false)).isEqualTo(ColumnKind.DOUBLE);
  }

  @Test
  void columnDefinitionRoundTripsThroughJson() {
    for (final ColumnDefinition column : allColumns()) {
      final JSONObject json = column.toJSON();
      assertThat(ColumnDefinition.fromJSON(new JSONObject(json.toString()))).isEqualTo(column);
    }
  }
}
