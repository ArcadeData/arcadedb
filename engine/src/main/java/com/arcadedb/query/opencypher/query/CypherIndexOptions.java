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
package com.arcadedb.query.opencypher.query;

import com.arcadedb.exception.CommandParsingException;
import com.arcadedb.index.IndexException;
import com.arcadedb.index.fulltext.StandardEnglishStopWordsAnalyzer;
import com.arcadedb.schema.FullTextIndexMetadata;
import com.arcadedb.schema.IndexMetadata;
import com.arcadedb.schema.LSMVectorIndexMetadata;
import com.arcadedb.schema.Schema;
import com.arcadedb.serializer.json.JSONObject;
import org.apache.lucene.analysis.Analyzer;

import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.Locale;
import java.util.Map;
import java.util.StringJoiner;
import java.util.function.IntSupplier;

/**
 * Translates the {@code OPTIONS} of Neo4j's {@code CREATE VECTOR INDEX} and {@code CREATE FULLTEXT INDEX} into the
 * {@code METADATA} an ArcadeDB LSM_VECTOR or FULL_TEXT index is built from (issue #10678). A key this class does not know is
 * refused rather than dropped, as the SQL {@code METADATA} clause does (#5639): an index built without a setting its creator
 * asked for answers differently with no sign of why.
 */
final class CypherIndexOptions {
  static final String INDEX_CONFIG                   = "indexConfig";
  static final String INDEX_PROVIDER                 = "indexProvider";
  static final String VECTOR_DIMENSIONS              = "vector.dimensions";
  static final String VECTOR_SIMILARITY              = "vector.similarity_function";
  static final String VECTOR_HNSW_M                  = "vector.hnsw.m";
  static final String VECTOR_HNSW_EF                 = "vector.hnsw.ef_construction";
  static final String VECTOR_QUANTIZED               = "vector.quantization.enabled";
  static final String VECTOR_QUANTIZATION_TYPE       = "vector.quantization.type";
  static final String VECTOR_SEARCH_EXPANSION        = "vector.default_search_expansion_factor";
  static final String FULLTEXT_ANALYZER              = "fulltext.analyzer";
  static final String FULLTEXT_EVENTUALLY_CONSISTENT = "fulltext.eventually_consistent";

  /**
   * The Neo4j analyzer names and the class of the analyzer a FULL_TEXT index builds for each, with a constructor that takes no
   * arguments. {@code standard-no-stop-words}, Neo4j's default, is the analyzer a FULL_TEXT index uses when none is named;
   * {@code standard}, the same analyzer with the English stop words, is {@link StandardEnglishStopWordsAnalyzer}, since
   * Lucene's {@code StandardAnalyzer()} has none (issue #10863).
   */
  private static final Map<String, String> ANALYZERS = Map.ofEntries(//
      Map.entry("standard-no-stop-words", "org.apache.lucene.analysis.standard.StandardAnalyzer"),//
      Map.entry("standard", StandardEnglishStopWordsAnalyzer.class.getName()),//
      Map.entry("simple", "org.apache.lucene.analysis.core.SimpleAnalyzer"),//
      Map.entry("whitespace", "org.apache.lucene.analysis.core.WhitespaceAnalyzer"),//
      Map.entry("keyword", "org.apache.lucene.analysis.core.KeywordAnalyzer"),//
      Map.entry("english", "org.apache.lucene.analysis.en.EnglishAnalyzer"),//
      Map.entry("french", "org.apache.lucene.analysis.fr.FrenchAnalyzer"),//
      Map.entry("german", "org.apache.lucene.analysis.de.GermanAnalyzer"),//
      Map.entry("italian", "org.apache.lucene.analysis.it.ItalianAnalyzer"),//
      Map.entry("spanish", "org.apache.lucene.analysis.es.SpanishAnalyzer"),//
      Map.entry("portuguese", "org.apache.lucene.analysis.pt.PortugueseAnalyzer"),//
      Map.entry("dutch", "org.apache.lucene.analysis.nl.DutchAnalyzer"),//
      Map.entry("swedish", "org.apache.lucene.analysis.sv.SwedishAnalyzer"),//
      Map.entry("danish", "org.apache.lucene.analysis.da.DanishAnalyzer"),//
      Map.entry("finnish", "org.apache.lucene.analysis.fi.FinnishAnalyzer"),//
      Map.entry("norwegian", "org.apache.lucene.analysis.no.NorwegianAnalyzer"),//
      Map.entry("russian", "org.apache.lucene.analysis.ru.RussianAnalyzer"),//
      Map.entry("cjk", "org.apache.lucene.analysis.cjk.CJKAnalyzer"));

  private CypherIndexOptions() {
  }

  /**
   * The LSM_VECTOR metadata of a {@code CREATE VECTOR INDEX}. The Neo4j settings are {@code vector.dimensions} (ArcadeDB sizes
   * the index on it: when the statement does not give it, it is taken from {@code existingDimensions}, the length of a vector
   * the indexed property already holds, as newer Neo4j versions infer it from the data, issue #10863, or, when the property
   * holds none yet, from the first vector the index receives, issue #10915),
   * {@code vector.similarity_function} ({@code cosine}, the default, or {@code euclidean}),
   * {@code vector.hnsw.m} (the graph degree, doubled into {@code maxConnections} since JVector does not double it at the base
   * layer as Lucene's HNSW does), {@code vector.hnsw.ef_construction} ({@code beamWidth}), {@code vector.quantization.type}
   * ({@code none}, {@code scalar} for {@code INT8}, {@code binary}) and the deprecated {@code vector.quantization.enabled}
   * ({@code INT8} when true), which must agree with the type when both are given.
   * <p>
   * With no quantization setting the index keeps the exact vectors ({@code NONE}), the LSM_VECTOR default, where Neo4j
   * quantizes ({@code binary} since 2026.08, {@code scalar} before): a quantized index answers with a lower recall, which a
   * script that did not ask for it must not get behind its back. SHOW INDEXES names the quantization of every vector index,
   * so the statement it lists means the same on both (discussion #10893).
   * <p>
   * Every other LSM_VECTOR setting is given under its own name, as in the {@code METADATA} of SQL {@code CREATE INDEX}
   * ({@code efSearch}, {@code quantization: 'PRODUCT'}, ...), and is read and checked by the same reader. Those names have no
   * dot, so they cannot be taken for a Neo4j setting; a setting named both ways is refused rather than one silently winning.
   */
  static JSONObject vectorMetadata(final Object options, final String typeName, final String[] propertyNames,
      final IntSupplier existingDimensions) {
    final Map<String, Object> config = indexConfig(options, "CREATE VECTOR INDEX");
    final JSONObject metadata = new JSONObject();
    // the Neo4j setting each metadata key was translated from, to refuse the same metadata key named natively too
    final Map<String, String> translatedFrom = new HashMap<>();

    String quantizationType = null;
    Boolean quantizationEnabled = null;
    for (final Map.Entry<String, Object> entry : config.entrySet()) {
      final String key = entry.getKey();
      final Object value = entry.getValue();
      switch (key) {
      case VECTOR_DIMENSIONS -> translate(metadata, translatedFrom, key, "dimensions", positiveInt(value, key));
      case VECTOR_SIMILARITY -> {
        final String function = value instanceof String s ? s.toLowerCase(Locale.ROOT) : null;
        // dot_product is ArcadeDB's own, which Neo4j does not have: accepted so the statement SHOW INDEXES lists for such an
        // index runs as it is
        if (!"cosine".equals(function) && !"euclidean".equals(function) && !"dot_product".equals(function))
          throw new CommandParsingException(
              "Unsupported `" + VECTOR_SIMILARITY + "` '" + value + "': use 'cosine', 'euclidean' or 'dot_product'");
        translate(metadata, translatedFrom, key, "similarity", function.toUpperCase(Locale.ROOT));
      }
      case VECTOR_HNSW_M -> {
        final int m = positiveInt(value, key);
        if (m > Integer.MAX_VALUE / 2)
          throw new CommandParsingException("`" + key + "` is too large: " + m);
        translate(metadata, translatedFrom, key, "maxConnections", 2 * m);
      }
      case VECTOR_HNSW_EF -> translate(metadata, translatedFrom, key, "beamWidth", positiveInt(value, key));
      case VECTOR_QUANTIZATION_TYPE -> {
        quantizationType = value instanceof String s ? s.toLowerCase(Locale.ROOT) : null;
        if (!"none".equals(quantizationType) && !"scalar".equals(quantizationType) && !"binary".equals(quantizationType))
          throw new CommandParsingException("Unsupported `" + key + "` '" + value + "': use 'none', 'scalar' or 'binary' (for "
              + "ArcadeDB's product quantization: quantization: 'PRODUCT')");
      }
      case VECTOR_QUANTIZED -> {
        if (!(value instanceof Boolean enabled))
          throw new CommandParsingException("`" + key + "` must be true or false, found " + value);
        quantizationEnabled = enabled;
      }
      case VECTOR_SEARCH_EXPANSION -> throw new CommandParsingException("`" + key + "` is not supported: an LSM_VECTOR index "
          + "has no per-index expansion of the candidates a search asks for, its search beam is set with efSearch");
      default -> {
        if (key.indexOf('.') >= 0)
          throw new CommandParsingException(
              "Unsupported vector index setting `" + key + "`: supported are " + VECTOR_DIMENSIONS + ", " + VECTOR_SIMILARITY
                  + ", " + VECTOR_QUANTIZATION_TYPE + ", " + VECTOR_QUANTIZED + ", " + VECTOR_HNSW_M + " and " + VECTOR_HNSW_EF
                  + ", and the LSM_VECTOR settings under their own names (efSearch, quantization, ...)");
      }
      }
    }

    // enabled with 'none', or disabled with 'scalar' / 'binary', asks for two different indexes
    if (quantizationEnabled != null && quantizationType != null && quantizationEnabled == "none".equals(quantizationType))
      throw new CommandParsingException(
          "`" + VECTOR_QUANTIZED + "`: " + quantizationEnabled + " contradicts `" + VECTOR_QUANTIZATION_TYPE + "`: '"
              + quantizationType + "': give only `" + VECTOR_QUANTIZATION_TYPE + "`, which replaces the deprecated flag");
    if (quantizationType != null)
      translate(metadata, translatedFrom, VECTOR_QUANTIZATION_TYPE, "quantization", switch (quantizationType) {
        case "scalar" -> "INT8";
        case "binary" -> "BINARY";
        default -> "NONE";
      });
    else if (quantizationEnabled != null)
      translate(metadata, translatedFrom, VECTOR_QUANTIZED, "quantization", quantizationEnabled ? "INT8" : "NONE");

    copyNativeSettings(config, metadata, translatedFrom);

    if (!metadata.has("dimensions")) {
      // asked only now, so a statement that names the dimensions never reads the data
      final int inferred = existingDimensions.getAsInt();
      if (inferred > 0)
        metadata.put("dimensions", inferred);
      else
        // no vector yet to take them from, the order of a script that creates the schema before the data: the index takes them
        // from the first vector it receives (issue #10915)
        metadata.put("sizeOnFirstVector", true);
    }
    if (!metadata.has("similarity"))
      metadata.put("similarity", "COSINE");
    return checked(metadata, new LSMVectorIndexMetadata(typeName, propertyNames, -1), Schema.INDEX_TYPE.LSM_VECTOR);
  }

  /**
   * The FULL_TEXT metadata of a {@code CREATE FULLTEXT INDEX}: {@code fulltext.analyzer}, a Neo4j analyzer name or the class
   * name of a Lucene analyzer, and {@code fulltext.eventually_consistent}, accepted with either value since a FULL_TEXT index
   * is updated by the transaction that changes the record, which is what Neo4j's default ({@code false}) promises. Every other
   * FULL_TEXT setting is given under its own name, as for a vector index ({@link #vectorMetadata(Object, String, String[],
   * IntSupplier)}).
   */
  static JSONObject fullTextMetadata(final Object options, final String typeName, final String[] propertyNames) {
    final Map<String, Object> config = indexConfig(options, "CREATE FULLTEXT INDEX");
    final JSONObject metadata = new JSONObject();
    final Map<String, String> translatedFrom = new HashMap<>();
    for (final Map.Entry<String, Object> entry : config.entrySet()) {
      final String key = entry.getKey();
      final Object value = entry.getValue();
      switch (key) {
      case FULLTEXT_ANALYZER -> translate(metadata, translatedFrom, key, "analyzer", analyzerClass(value));
      case FULLTEXT_EVENTUALLY_CONSISTENT -> {
        if (!(value instanceof Boolean))
          throw new CommandParsingException("`" + key + "` must be true or false, found " + value);
      }
      default -> {
        if (key.indexOf('.') >= 0)
          throw new CommandParsingException(
              "Unsupported full-text index setting `" + key + "`: supported are " + FULLTEXT_ANALYZER + " and "
                  + FULLTEXT_EVENTUALLY_CONSISTENT + ", and the FULL_TEXT settings under their own names (defaultOperator, "
                  + "bm25_k1, ...)");
      }
      }
    }
    copyNativeSettings(config, metadata, translatedFrom);
    return checked(metadata, new FullTextIndexMetadata(typeName, propertyNames, -1), Schema.INDEX_TYPE.FULL_TEXT);
  }

  /**
   * The metadata, once the reader of the index type has read it onto a throwaway definition: a setting it refuses (a native key
   * misspelled, a value out of range) is reported by the statement before any type or property is created for the index.
   */
  private static JSONObject checked(final JSONObject metadata, final IndexMetadata definition, final Schema.INDEX_TYPE indexType) {
    try {
      definition.fromUserMetadata(metadata, indexType);
    } catch (final IndexException | IllegalArgumentException e) {
      throw new CommandParsingException("Invalid " + (indexType == Schema.INDEX_TYPE.LSM_VECTOR ? "vector" : "full-text")
          + " index OPTIONS: " + e.getMessage(), e);
    }
    return metadata;
  }

  /**
   * The {@code indexConfig} that creates the vector index {@code metadata} describes again, the one SHOW INDEXES lists: the
   * Neo4j settings where Neo4j has a spelling for the value, then every other setting that differs from the LSM_VECTOR default
   * under its own name. The quantization is always named, since Neo4j's default for it is not ArcadeDB's.
   */
  static Map<String, Object> vectorIndexConfig(final LSMVectorIndexMetadata metadata) {
    final Map<String, Object> settings = metadata.getUserSettingsDifferingFrom(
        new LSMVectorIndexMetadata(metadata.typeName, null, -1));
    final Map<String, Object> config = new LinkedHashMap<>();
    // an index still waiting for its first vector has none to list: its settings carry sizeOnFirstVector instead
    if (metadata.dimensions > 0)
      config.put(VECTOR_DIMENSIONS, metadata.dimensions);
    config.put(VECTOR_SIMILARITY, metadata.similarityFunction.name().toLowerCase(Locale.ROOT));
    settings.remove("dimensions");
    settings.remove("similarity");

    settings.remove("quantization");
    switch (metadata.quantizationType) {
    case NONE -> config.put(VECTOR_QUANTIZATION_TYPE, "none");
    case INT8 -> config.put(VECTOR_QUANTIZATION_TYPE, "scalar");
    case BINARY -> config.put(VECTOR_QUANTIZATION_TYPE, "binary");
    default -> config.put("quantization", metadata.quantizationType.name());
    }

    // vector.hnsw.m is doubled into maxConnections, so an odd degree has no Neo4j spelling
    if (settings.containsKey("maxConnections") && metadata.maxConnections % 2 == 0) {
      settings.remove("maxConnections");
      config.put(VECTOR_HNSW_M, metadata.maxConnections / 2);
    }
    if (settings.remove("beamWidth") != null)
      config.put(VECTOR_HNSW_EF, metadata.beamWidth);

    // an efSearch that was asked for is kept even at its default value, which a search reads differently from none (#6494)
    if (metadata.efSearchConfigured)
      settings.put("efSearch", metadata.efSearch);
    // refused at creation since #5559, so a value an older version persisted is not a setting a statement can ask for
    settings.remove("locationCacheSize");

    for (final Map.Entry<String, Object> entry : settings.entrySet())
      config.put(entry.getKey(), entry.getValue() instanceof Enum<?> e ? e.name() : entry.getValue());
    return config;
  }

  /**
   * The {@code indexConfig} that creates the full-text index {@code metadata} describes again: {@code fulltext.analyzer} when
   * the analyzer is not the default one, then every other setting that differs from the FULL_TEXT default under its own name.
   */
  static Map<String, Object> fullTextIndexConfig(final FullTextIndexMetadata metadata) {
    final Map<String, Object> settings = metadata.getUserSettingsDifferingFrom(
        new FullTextIndexMetadata(metadata.typeName, null, -1));
    final Map<String, Object> config = new LinkedHashMap<>();
    settings.remove("analyzer");
    final String analyzer = analyzerName(metadata.getAnalyzerClass());
    if (analyzer != null)
      config.put(FULLTEXT_ANALYZER, analyzer);
    config.putAll(settings);
    return config;
  }

  /**
   * {@code indexConfig} written as the Cypher map literal of an {@code OPTIONS}, every key quoted: a per-field key of a
   * full-text index ({@code <field>_boost}) carries a property name, which may hold a backtick.
   */
  static String indexConfigLiteral(final Map<String, Object> config) {
    final StringJoiner entries = new StringJoiner(", ", "{indexConfig: {", "}}");
    for (final Map.Entry<String, Object> entry : config.entrySet()) {
      final Object value = entry.getValue();
      entries.add(CypherSchemaCommands.quote(entry.getKey()) + ": " + (value instanceof String s ?
          "'" + s.replace("\\", "\\\\").replace("'", "\\'") + "'" :
          String.valueOf(value)));
    }
    return entries.toString();
  }

  /** Records the metadata key a Neo4j setting is translated into, refusing one that another Neo4j setting already set. */
  private static void translate(final JSONObject metadata, final Map<String, String> translatedFrom, final String neo4jKey,
      final String metadataKey, final Object value) {
    final String previous = translatedFrom.putIfAbsent(metadataKey, neo4jKey);
    if (previous != null && !previous.equals(neo4jKey))
      throw new CommandParsingException("`" + previous + "` and `" + neo4jKey + "` both set " + metadataKey + ": give one");
    metadata.put(metadataKey, value);
  }

  /**
   * Copies the settings named natively (no dot) into the metadata as they are, for the index's own reader to check, refusing
   * one a Neo4j setting of the same statement already set.
   */
  private static void copyNativeSettings(final Map<String, Object> config, final JSONObject metadata,
      final Map<String, String> translatedFrom) {
    for (final Map.Entry<String, Object> entry : config.entrySet()) {
      final String key = entry.getKey();
      if (key.indexOf('.') >= 0)
        continue;
      final String neo4jKey = translatedFrom.get(key);
      if (neo4jKey != null)
        throw new CommandParsingException("`" + neo4jKey + "` and `" + key + "` both set " + key + ": give one");
      metadata.put(key, entry.getValue());
    }
  }

  /**
   * The {@code indexConfig} map of the options, empty when there is none. {@code indexProvider} is accepted and ignored: it names
   * the Neo4j implementation, and ArcadeDB has one of each kind.
   */
  private static Map<String, Object> indexConfig(final Object options, final String statement) {
    if (options == null)
      return Map.of();
    if (!(options instanceof Map<?, ?> map))
      throw new CommandParsingException(statement + " OPTIONS must be a map, found " + options);

    Map<String, Object> config = Map.of();
    for (final Map.Entry<?, ?> entry : map.entrySet()) {
      final String key = String.valueOf(entry.getKey());
      if (INDEX_CONFIG.equals(key)) {
        if (entry.getValue() == null)
          continue;
        if (!(entry.getValue() instanceof Map<?, ?> configMap))
          throw new CommandParsingException(statement + " `" + INDEX_CONFIG + "` must be a map, found " + entry.getValue());
        @SuppressWarnings("unchecked")
        final Map<String, Object> typed = (Map<String, Object>) configMap;
        config = typed;
      } else if (!INDEX_PROVIDER.equals(key))
        throw new CommandParsingException(
            "Unsupported " + statement + " option `" + key + "`: supported are " + INDEX_CONFIG + " and " + INDEX_PROVIDER);
    }
    return config;
  }

  /**
   * The {@code fulltext.analyzer} that names {@code analyzerClass}: its Neo4j name when it has one, the class name otherwise,
   * null for the default analyzer (which a statement needs not name).
   */
  static String analyzerName(final String analyzerClass) {
    if (analyzerClass == null || analyzerClass.equals(ANALYZERS.get("standard-no-stop-words")))
      return null;
    for (final Map.Entry<String, String> entry : ANALYZERS.entrySet())
      if (entry.getValue().equals(analyzerClass))
        return entry.getKey();
    return analyzerClass;
  }

  private static int positiveInt(final Object value, final String key) {
    if (value instanceof Number number && number.doubleValue() == Math.rint(number.doubleValue()) && number.longValue() >= 1
        && number.longValue() <= Integer.MAX_VALUE)
      return number.intValue();
    throw new CommandParsingException("`" + key + "` must be a positive integer, found " + value);
  }

  /**
   * The analyzer class of a {@code fulltext.analyzer}: a Neo4j name, or the class name of a Lucene {@link Analyzer} with a
   * constructor that takes no arguments, checked here so a typo is refused by the statement rather than by every later write.
   */
  private static String analyzerClass(final Object value) {
    if (!(value instanceof String name) || name.isBlank())
      throw new CommandParsingException("`" + FULLTEXT_ANALYZER + "` must be an analyzer name, found " + value);

    final String mapped = ANALYZERS.get(name.toLowerCase(Locale.ROOT));
    if (mapped != null)
      return mapped;

    if (name.indexOf('.') > 0)
      try {
        // not initialized: a class that is no analyzer must not run its static initializer because a statement named it
        final Class<?> clazz = Class.forName(name, false, CypherIndexOptions.class.getClassLoader());
        if (Analyzer.class.isAssignableFrom(clazz)) {
          clazz.getConstructor();
          return name;
        }
      } catch (final ClassNotFoundException | NoSuchMethodException | LinkageError ignored) {
        // refused below
      }

    throw new CommandParsingException("Unsupported `" + FULLTEXT_ANALYZER + "` '" + name + "': use one of "
        + ANALYZERS.keySet().stream().sorted().toList() + " or the class name of a Lucene analyzer with a no-argument constructor");
  }
}
