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
    assertThat(new MongoFilter(null, null).isSql()).isTrue();
    assertThat(new MongoFilter(null, new Document()).isEmpty()).isTrue();
    assertThat(new MongoFilter(null, parse("{_id: 1}")).isSql()).isFalse();
    assertThat(new MongoFilter(null, parse("{k: 1}")).isSql()).isFalse();
  }

  @Test
  void aFilterOnTheIdReadsItsCandidatesThroughTheIndex() {
    assertThat(new MongoFilter(null, parse("{_id: 1}")).narrowsById()).isTrue();
    assertThat(new MongoFilter(null, parse("{_id: {$in: [1, 2]}}")).narrowsById()).isTrue();
    assertThat(new MongoFilter(null, parse("{$or: [{_id: 1}, {_id: 2}]}")).narrowsById()).isTrue();
    assertThat(new MongoFilter(null, parse("{$and: [{$or: [{_id: 1}, {_id: 2}]}, {_id: {$gt: 0}}]}")).narrowsById()).isTrue();
    assertThat(new MongoFilter(null, parse("{_id: 1, k: 1}")).narrowsById()).isTrue();
    assertThat(new MongoFilter(null, parse("{_id: {$in: [1, 2]}, tags: 'a'}")).narrowsById()).isTrue();
  }

  @Test
  void aNegationOnTheIdNeverNarrowsTheCandidates() {
    assertThat(new MongoFilter(null, parse("{_id: {$ne: 1}, k: 1}")).narrowsById()).isFalse();
    assertThat(new MongoFilter(null, parse("{_id: {$nin: [1]}}")).narrowsById()).isFalse();
    assertThat(new MongoFilter(null, parse("{_id: {$not: {$gt: 2}}, k: 1}")).narrowsById()).isFalse();
    assertThat(new MongoFilter(null, parse("{$or: [{_id: 1}, {_id: {$ne: 2}}]}")).narrowsById()).isFalse();
    assertThat(new MongoFilter(null, parse("{_id: {$gt: 1, $lte: 5}, k: 1}")).narrowsById()).isTrue();
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
}
