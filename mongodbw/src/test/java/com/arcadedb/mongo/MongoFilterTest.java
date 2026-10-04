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

import com.arcadedb.GlobalConfiguration;
import com.arcadedb.exception.TimeoutException;
import de.bwaldvogel.mongo.bson.BsonRegularExpression;
import de.bwaldvogel.mongo.bson.Document;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import java.util.List;
import java.util.Map;
import java.util.regex.Pattern;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Routing of a MongoDB filter: only an empty filter is answered by SQL alone. Every other one is evaluated on the documents, and one on
 * the {@code _id} (with equality, {@code $in} or a range) reads its candidates through the unique index on it first.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class MongoFilterTest {

  private static Document parse(final String json) {
    return (Document) convert(org.bson.Document.parse(json));
  }

  private static Object convert(final Object value) {
    if (value instanceof org.bson.Document document) {
      final Document result = new Document();
      for (final Map.Entry<String, Object> entry : document.entrySet())
        result.put(entry.getKey(), convert(entry.getValue()));
      return result;
    }
    if (value instanceof List<?> list)
      return list.stream().map(MongoFilterTest::convert).toList();
    return value;
  }

  @Test
  void onlyAnEmptyFilterIsAnsweredBySqlAlone() {
    assertThat(new MongoFilter(null, null).isEmpty()).isTrue();
    assertThat(new MongoFilter(null, new Document()).isEmpty()).isTrue();
    assertThat(new MongoFilter(null, parse("{_id: 1}")).isEmpty()).isFalse();
    assertThat(new MongoFilter(null, parse("{k: 1}")).isEmpty()).isFalse();
  }

  @Test
  void aFilterOnTheIdReadsItsCandidatesThroughTheIndex() {
    assertThat(new MongoFilter(null, parse("{_id: 1}")).narrowsById()).isTrue();
    assertThat(new MongoFilter(null, parse("{_id: {$in: [1, 2]}}")).narrowsById()).isTrue();
    assertThat(new MongoFilter(null, parse("{$or: [{_id: 1}, {_id: 2}]}")).narrowsById()).isTrue();
    assertThat(new MongoFilter(null, parse("{$and: [{$or: [{_id: 1}, {_id: 2}]}, {_id: {$in: [1]}}]}")).narrowsById()).isTrue();
    assertThat(new MongoFilter(null, parse("{_id: 1, k: 1}")).narrowsById()).isTrue();
    assertThat(new MongoFilter(null, parse("{_id: {$in: [1, 2]}, tags: 'a'}")).narrowsById()).isTrue();
  }

  @Test
  void aNegationOnTheIdNeverNarrowsTheCandidates() {
    assertThat(new MongoFilter(null, parse("{_id: {$ne: 1}, k: 1}")).narrowsById()).isFalse();
    assertThat(new MongoFilter(null, parse("{_id: {$nin: [1]}}")).narrowsById()).isFalse();
    assertThat(new MongoFilter(null, parse("{_id: {$not: {$gt: 2}}, k: 1}")).narrowsById()).isFalse();
    assertThat(new MongoFilter(null, parse("{$or: [{_id: 1}, {_id: {$ne: 2}}]}")).narrowsById()).isFalse();
    assertThat(new MongoFilter(null, parse("{_id: {$in: [1, 5]}, k: 1}")).narrowsById()).isTrue();
  }

  @Test
  void aRangeOnTheIdNeverNarrowsTheCandidates() {
    assertThat(new MongoFilter(null, parse("{_id: {$gt: 5}}")).narrowsById()).isFalse();
    assertThat(new MongoFilter(null, parse("{_id: {$gte: 0, $lte: 9}, k: 1}")).narrowsById()).isFalse();
    assertThat(new MongoFilter(null, parse("{_id: {$lte: null}}")).narrowsById()).isFalse();
    assertThat(new MongoFilter(null, parse("{$or: [{_id: 1}, {_id: {$lt: 3}}]}")).narrowsById()).isFalse();
  }

  @Test
  void anEmbeddedDocumentOrListOperandOnTheIdNeverNarrowsTheCandidates() {
    assertThat(new MongoFilter(null, parse("{_id: {$eq: {a: 1, b: 2}}}")).narrowsById()).isFalse();
    assertThat(new MongoFilter(null, parse("{_id: {$in: [{a: 1}, {b: 2}]}, k: 1}")).narrowsById()).isFalse();
    assertThat(new MongoFilter(null, parse("{_id: {$in: [[1, 2]]}}")).narrowsById()).isFalse();
    assertThat(new MongoFilter(null, parse("{_id: {a: 1, b: 2}}")).narrowsById()).isFalse();
    assertThat(new MongoFilter(null, parse("{_id: [1, 2]}")).narrowsById()).isFalse();
  }

  @Test
  void aRegexOperandOnTheIdNeverNarrowsTheCandidates() {
    assertThat(new MongoFilter(null, new Document("_id", new Document("$in", List.of(new BsonRegularExpression("^abc"))))).narrowsById()).isFalse();
    assertThat(new MongoFilter(null, new Document("_id", new Document("$eq", new BsonRegularExpression("^abc"))).append("k", 1)).narrowsById())
        .isFalse();
    assertThat(new MongoFilter(null, new Document("_id", new BsonRegularExpression("^abc")).append("k", 1)).narrowsById()).isFalse();
  }

  @Test
  void anyOtherFilterIsEvaluatedOnTheDocuments() {
    assertThat(new MongoFilter(null, parse("{k: 1}")).narrowsById()).isFalse();
    assertThat(new MongoFilter(null, parse("{$or: [{_id: 1}, {k: 2}]}")).narrowsById()).isFalse();
    assertThat(new MongoFilter(null, parse("{$nor: [{_id: 1}]}")).narrowsById()).isFalse();
  }

  @Test
  void matchesFollowsMongoDBRules() {
    final MongoFilter filter = new MongoFilter(null, parse("{tags: 'a', n: {$gt: 1}}"));
    assertThat(filter.matches(Map.<String, Object>of("tags", List.of("a", "b"), "n", 2))).isTrue();
    assertThat(filter.matches(Map.<String, Object>of("tags", List.of("b"), "n", 2))).isFalse();
    assertThat(filter.matches(Map.<String, Object>of("tags", List.of("a"), "n", "5"))).isFalse();
  }

  @Test
  void unsupportedOperatorsAreRefusedUpFront() {
    assertThatThrownBy(() -> new MongoFilter(null, parse("{$expr: {$eq: [1, 1]}}"))).isInstanceOf(IllegalArgumentException.class);
    assertThatThrownBy(() -> new MongoFilter(null, new Document("s", new Document("$regex", "a").append("$options", "z")))).isInstanceOf(
        IllegalArgumentException.class);
  }

  @Test
  void aFilterAnsweredBySqlRefusesToTestARecord() {
    final MongoFilter filter = new MongoFilter(null, new Document());
    assertThatThrownBy(() -> filter.matches(Map.<String, Object>of("_id", 1))).isInstanceOf(IllegalStateException.class);
  }

  @Test
  void regexesInsideLogicalOperatorsAndElemMatchFieldsMatchLikeMongoDB() {
    final Map<String, Object> hit = Map.of("s", "Alpha", "items", List.of(Map.of("name", "beta"), Map.of("name", "gamma")));
    assertThat(new MongoFilter(null, new Document("$or", List.of(new Document("s", new BsonRegularExpression("^al", "i")),
        new Document("n", 1)))).matches(hit)).isTrue();
    assertThat(new MongoFilter(null, new Document("$nor", List.of(new Document("s", new BsonRegularExpression("^zz"))))).matches(hit)).isTrue();
    assertThat(new MongoFilter(null, new Document("items", new Document("$elemMatch", new Document("name", new Document("$regex", "^ga"))))).matches(hit))
        .isTrue();
    assertThat(new MongoFilter(null, new Document("items", new Document("$elemMatch", new Document("name", new Document("$regex", "^zz"))))).matches(hit))
        .isFalse();
  }

  @Test
  void aDisabledRegexTimeoutStillMatches() {
    final long previous = GlobalConfiguration.COMMAND_REGEX_TIMEOUT.getValueAsLong();
    GlobalConfiguration.COMMAND_REGEX_TIMEOUT.setValue(0);
    try {
      final MongoFilter filter = new MongoFilter(null, new Document("s", new Document("$regex", "^al").append("$options", "i")));
      assertThat(filter.matches(Map.<String, Object>of("s", "Alpha"))).isTrue();
      assertThat(filter.matches(Map.<String, Object>of("s", "beta"))).isFalse();
    } finally {
      GlobalConfiguration.COMMAND_REGEX_TIMEOUT.setValue(previous);
    }
  }

  @Test
  void regexElementsOfAllAndNorMatchLikeMongoDB() {
    final Map<String, Object> hit = Map.of("s", List.of("alpha", "beta"));
    assertThat(new MongoFilter(null, new Document("s", new Document("$all", List.of(new BsonRegularExpression("^al"), new BsonRegularExpression("^be")))))
        .matches(hit)).isTrue();
    assertThat(new MongoFilter(null, new Document("s", new Document("$all", List.of(new BsonRegularExpression("^al"), new BsonRegularExpression("^zz")))))
        .matches(hit)).isFalse();
    assertThat(new MongoFilter(null, new Document("$nor", List.of(new Document("s", new Document("$regex", "^zz"))))).matches(hit)).isTrue();
    assertThat(new MongoFilter(null, new Document("$nor", List.of(new Document("s", new Document("$regex", "^al"))))).matches(hit)).isFalse();
  }

  @Test
  void anOversizedRegexTimeoutNeverExpires() {
    final MongoFilter.RegexBudget budget = MongoFilter.RegexBudget.ofMillis(Long.MAX_VALUE);
    for (int i = 0; i < 1000; i++)
      assertThat(budget.find(Pattern.compile("^al"), "alpha")).isTrue();
  }

  @Test
  @Timeout(60)
  void aSpentRegexBudgetRefusesTheNextSearch() {
    final MongoFilter.RegexBudget budget = MongoFilter.RegexBudget.ofMillis(1);
    final Pattern pathological = Pattern.compile("(.*a){20}$");
    final String input = "a".repeat(40) + "!";
    assertThatThrownBy(() -> budget.find(pathological, input)).isInstanceOf(TimeoutException.class);
    assertThatThrownBy(() -> budget.find(Pattern.compile("^al"), "alpha")).isInstanceOf(
        TimeoutException.class);
  }

  @Test
  void aRegexMatchesStringsAndArraysOfStrings() {
    final MongoFilter filter = new MongoFilter(null, new Document("n", new BsonRegularExpression("^1")));
    // (MongoDB matches a regex against strings only, the library also tests the string form of a number: a known leniency)
    assertThat(filter.matches(Map.<String, Object>of("n", "123"))).isTrue();
    assertThat(filter.matches(Map.<String, Object>of("n", List.of(5, "1x")))).isTrue();
  }

  /**
   * The library compares an {@code $eq} / {@code $ne} / range operand by value and never searches it as a pattern, so a regular
   * expression there cannot run unbounded. A library that started to do so would hang here, hence the generous timeout.
   */
  @Test
  @Timeout(60)
  void aRegexUnderEqualityAndRangeOperatorsIsNeverSearched() {
    final BsonRegularExpression pathological = new BsonRegularExpression("(.*a){20}$");
    final Map<String, Object> record = Map.of("s", "a".repeat(40) + "!");
    for (final String operator : List.of("$eq", "$ne", "$gt", "$gte", "$lt", "$lte"))
      new MongoFilter(null, new Document("s", new Document(operator, pathological))).matches(record);
  }

  @Test
  void onlyThePropertiesTheFilterReadsAreConsidered() {
    final Map<String, Object> record = Map.of("a", Map.of("b", 1), "tags", List.of("x"), "n", 5, "unrelated", Map.of("deep", List.of(1, 2, 3)));
    assertThat(new MongoFilter(null, parse("{'a.b': 1}")).matches(record)).isTrue();
    assertThat(new MongoFilter(null, parse("{$or: [{tags: 'x'}, {zzz: 1}]}")).matches(record)).isTrue();
    assertThat(new MongoFilter(null, parse("{$and: [{n: {$gt: 1}}, {'a.b': {$exists: true}}]}")).matches(record)).isTrue();
    assertThat(new MongoFilter(null, parse("{$nor: [{n: 6}]}")).matches(record)).isTrue();
    assertThat(new MongoFilter(null, parse("{'a.c': {$exists: false}}")).matches(record)).isTrue();
    assertThat(new MongoFilter(null, parse("{'unrelated.deep': 2}")).matches(record)).isTrue();
  }

  @Test
  void aRegexIsSearchedNotMatchedWhole() {
    final Map<String, Object> record = Map.of("s", "xxabcxx");
    // find() semantics: an unanchored pattern matches inside the value, anchors still apply
    assertThat(new MongoFilter(null, new Document("s", new BsonRegularExpression("abc"))).matches(record)).isTrue();
    assertThat(new MongoFilter(null, new Document("s", new BsonRegularExpression("^abc"))).matches(record)).isFalse();
    assertThat(new MongoFilter(null, new Document("s", new BsonRegularExpression("abc$"))).matches(record)).isFalse();
    assertThat(new MongoFilter(null, new Document("s", new BsonRegularExpression("^xx.*xx$"))).matches(record)).isTrue();
  }
}
