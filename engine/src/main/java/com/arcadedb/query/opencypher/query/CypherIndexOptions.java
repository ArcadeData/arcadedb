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
import com.arcadedb.serializer.json.JSONObject;
import org.apache.lucene.analysis.Analyzer;

import java.util.Locale;
import java.util.Map;

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
  static final String FULLTEXT_ANALYZER              = "fulltext.analyzer";
  static final String FULLTEXT_EVENTUALLY_CONSISTENT = "fulltext.eventually_consistent";

  /**
   * The Neo4j analyzer names whose Lucene analyzer can be built without arguments, which is how a FULL_TEXT index builds its
   * analyzer. Neo4j's {@code standard} (the standard analyzer with English stop words) has no such constructor, so it is not
   * here: {@code standard-no-stop-words}, Neo4j's default, is the analyzer a FULL_TEXT index uses when none is named.
   */
  private static final Map<String, String> ANALYZERS = Map.ofEntries(//
      Map.entry("standard-no-stop-words", "org.apache.lucene.analysis.standard.StandardAnalyzer"),//
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
   * The LSM_VECTOR metadata of a {@code CREATE VECTOR INDEX}: {@code vector.dimensions} (required, ArcadeDB sizes the index
   * on it), {@code vector.similarity_function} ({@code cosine}, the default, or {@code euclidean}), {@code vector.hnsw.m}
   * (the graph degree, doubled into {@code maxConnections} since JVector does not double it at the base layer as Lucene's
   * HNSW does), {@code vector.hnsw.ef_construction} ({@code beamWidth}) and {@code vector.quantization.enabled}
   * ({@code INT8} scalar quantization when true).
   */
  static JSONObject vectorMetadata(final Object options) {
    final Map<String, Object> config = indexConfig(options, "CREATE VECTOR INDEX");
    final JSONObject metadata = new JSONObject();

    final Object dimensions = config.get(VECTOR_DIMENSIONS);
    if (dimensions == null)
      throw new CommandParsingException(
          "CREATE VECTOR INDEX needs the number of dimensions: OPTIONS {indexConfig: {`" + VECTOR_DIMENSIONS + "`: 768}}");
    metadata.put("dimensions", positiveInt(dimensions, VECTOR_DIMENSIONS));

    String similarity = "COSINE";
    for (final Map.Entry<String, Object> entry : config.entrySet()) {
      final String key = entry.getKey();
      final Object value = entry.getValue();
      switch (key) {
      case VECTOR_DIMENSIONS -> {
      }
      case VECTOR_SIMILARITY -> {
        final String function = value instanceof String s ? s.toLowerCase(Locale.ROOT) : null;
        // dot_product is ArcadeDB's own, which Neo4j does not have: accepted so the statement SHOW INDEXES lists for such an
        // index runs as it is
        if (!"cosine".equals(function) && !"euclidean".equals(function) && !"dot_product".equals(function))
          throw new CommandParsingException(
              "Unsupported `" + VECTOR_SIMILARITY + "` '" + value + "': use 'cosine', 'euclidean' or 'dot_product'");
        similarity = function.toUpperCase(Locale.ROOT);
      }
      case VECTOR_HNSW_M -> {
        final int m = positiveInt(value, key);
        if (m > Integer.MAX_VALUE / 2)
          throw new CommandParsingException("`" + key + "` is too large: " + m);
        metadata.put("maxConnections", 2 * m);
      }
      case VECTOR_HNSW_EF -> metadata.put("beamWidth", positiveInt(value, key));
      case VECTOR_QUANTIZED -> {
        if (!(value instanceof Boolean enabled))
          throw new CommandParsingException("`" + key + "` must be true or false, found " + value);
        metadata.put("quantization", enabled ? "INT8" : "NONE");
      }
      default -> throw new CommandParsingException(
          "Unsupported vector index setting `" + key + "`: supported are " + VECTOR_DIMENSIONS + ", " + VECTOR_SIMILARITY + ", "
              + VECTOR_HNSW_M + ", " + VECTOR_HNSW_EF + " and " + VECTOR_QUANTIZED);
      }
    }
    metadata.put("similarity", similarity);
    return metadata;
  }

  /**
   * The FULL_TEXT metadata of a {@code CREATE FULLTEXT INDEX}: {@code fulltext.analyzer}, a Neo4j analyzer name or the class
   * name of a Lucene analyzer, and {@code fulltext.eventually_consistent}, accepted with either value since a FULL_TEXT index
   * is updated by the transaction that changes the record, which is what Neo4j's default ({@code false}) promises.
   */
  static JSONObject fullTextMetadata(final Object options) {
    final Map<String, Object> config = indexConfig(options, "CREATE FULLTEXT INDEX");
    final JSONObject metadata = new JSONObject();
    for (final Map.Entry<String, Object> entry : config.entrySet()) {
      final String key = entry.getKey();
      final Object value = entry.getValue();
      switch (key) {
      case FULLTEXT_ANALYZER -> metadata.put("analyzer", analyzerClass(value));
      case FULLTEXT_EVENTUALLY_CONSISTENT -> {
        if (!(value instanceof Boolean))
          throw new CommandParsingException("`" + key + "` must be true or false, found " + value);
      }
      default -> throw new CommandParsingException(
          "Unsupported full-text index setting `" + key + "`: supported are " + FULLTEXT_ANALYZER + " and "
              + FULLTEXT_EVENTUALLY_CONSISTENT);
      }
    }
    return metadata;
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
