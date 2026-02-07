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
package com.arcadedb.query.sql.executor;

import org.junit.jupiter.api.Test;

import java.util.Map;
import java.util.Optional;
import java.util.Set;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Tests for QueryStats, QueryOperatorEquals, EmptyResult, TraverseResult, and IndexCondPair.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class UtilityClassesCoverageTest {

  // ===== QueryStats =====

  @Test
  void queryStatsIndexStats() {
    final QueryStats stats = new QueryStats();
    // Initially -1
    assertThat(stats.getIndexStats("myIndex", 1, false, false)).isEqualTo(-1);

    // Push and get
    stats.pushIndexStats("myIndex", 1, false, false, 100L);
    assertThat(stats.getIndexStats("myIndex", 1, false, false)).isEqualTo(100L);

    // Push again - should apply weighted average
    stats.pushIndexStats("myIndex", 1, false, false, 200L);
    final long updated = stats.getIndexStats("myIndex", 1, false, false);
    assertThat(updated).isGreaterThan(0);
  }

  @Test
  void queryStatsEdgeSpans() {
    final QueryStats stats = new QueryStats();

    // Out edge
    assertThat(stats.getAverageOutEdgeSpan("Person", "Knows")).isEqualTo(-1);
    stats.pushAverageOutEdgeSpan("Person", "Knows", 50L);
    assertThat(stats.getAverageOutEdgeSpan("Person", "Knows")).isEqualTo(50L);

    // In edge
    assertThat(stats.getAverageInEdgeSpan("Person", "Knows")).isEqualTo(-1);
    stats.pushAverageInEdgeSpan("Person", "Knows", 30L);
    assertThat(stats.getAverageInEdgeSpan("Person", "Knows")).isEqualTo(30L);

    // Both edge
    assertThat(stats.getAverageBothEdgeSpan("Person", "Knows")).isEqualTo(-1);
    stats.pushAverageBothEdgeSpan("Person", "Knows", 40L);
    assertThat(stats.getAverageBothEdgeSpan("Person", "Knows")).isEqualTo(40L);
  }

  @Test
  void queryStatsPushNullValue() {
    final QueryStats stats = new QueryStats();
    stats.pushIndexStats("idx", 1, false, false, null);
    assertThat(stats.getIndexStats("idx", 1, false, false)).isEqualTo(-1);
  }

  @Test
  void queryStatsWeightedAverage() {
    final QueryStats stats = new QueryStats();
    stats.pushAverageOutEdgeSpan("V", "E", 100L);
    stats.pushAverageOutEdgeSpan("V", "E", 200L);
    final long avg = stats.getAverageOutEdgeSpan("V", "E");
    // Weighted: 100 * 0.9 + 200 * 0.1 = 110
    assertThat(avg).isEqualTo(110L);
  }

  @Test
  void queryStatsSmallValueFloor() {
    final QueryStats stats = new QueryStats();
    // Push a small value, then a slightly larger one to test the val > 0 && val == 0 case
    stats.pushAverageOutEdgeSpan("V", "E", 1L);
    stats.pushAverageOutEdgeSpan("V", "E", 1L);
    assertThat(stats.getAverageOutEdgeSpan("V", "E")).isGreaterThanOrEqualTo(1L);
  }

  @Test
  void queryStatsGenerateKey() {
    final QueryStats stats = new QueryStats();
    final String key = stats.generateKey("a", "b", "c");
    assertThat(key).contains("a").contains("b").contains("c");
  }

  // ===== QueryOperatorEquals =====

  @Test
  void equalsNulls() {
    assertThat(QueryOperatorEquals.equals(null, null)).isFalse();
    assertThat(QueryOperatorEquals.equals(null, "a")).isFalse();
    assertThat(QueryOperatorEquals.equals("a", null)).isFalse();
  }

  @Test
  void equalsSameReference() {
    final Object obj = "hello";
    assertThat(QueryOperatorEquals.equals(obj, obj)).isTrue();
  }

  @Test
  void equalsNumbers() {
    assertThat(QueryOperatorEquals.equals(42, 42)).isTrue();
    assertThat(QueryOperatorEquals.equals(1, 2)).isFalse();
  }

  @Test
  void equalsStrings() {
    assertThat(QueryOperatorEquals.equals("hello", "hello")).isTrue();
    assertThat(QueryOperatorEquals.equals("hello", "world")).isFalse();
  }

  @Test
  void equalsByteArrays() {
    assertThat(QueryOperatorEquals.equals(new byte[] { 1, 2, 3 }, new byte[] { 1, 2, 3 })).isTrue();
    assertThat(QueryOperatorEquals.equals(new byte[] { 1, 2 }, new byte[] { 1, 3 })).isFalse();
  }

  @Test
  void equalsResultWithValue() {
    final ResultInternal result = new ResultInternal();
    result.setProperty("value", "test");
    assertThat(QueryOperatorEquals.equals(result, "test")).isTrue();
  }

  @Test
  void equalsValueWithResult() {
    final ResultInternal result = new ResultInternal();
    result.setProperty("value", 42);
    assertThat(QueryOperatorEquals.equals(42, result)).isTrue();
  }

  @Test
  void equalsTypeConversion() {
    // Type conversion can succeed in some cases
    assertThat(QueryOperatorEquals.equals("hello", 42)).isFalse();
  }

  // ===== EmptyResult =====

  @Test
  void emptyResultGetProperty() {
    final EmptyResult result = new EmptyResult();
    assertThat(result.<String>getProperty("name")).isNull();
    assertThat(result.<String>getProperty("name", "default")).isEqualTo("default");
  }

  @Test
  void emptyResultGetElementProperty() {
    final EmptyResult result = new EmptyResult();
    assertThat(result.getElementProperty("name")).isNull();
  }

  @Test
  void emptyResultGetPropertyNames() {
    final EmptyResult result = new EmptyResult();
    assertThat(result.getPropertyNames()).isNull();
  }

  @Test
  void emptyResultGetIdentity() {
    final EmptyResult result = new EmptyResult();
    assertThat(result.getIdentity()).isEmpty();
  }

  @Test
  void emptyResultIsElement() {
    final EmptyResult result = new EmptyResult();
    assertThat(result.isElement()).isFalse();
  }

  @Test
  void emptyResultGetElement() {
    final EmptyResult result = new EmptyResult();
    assertThat(result.getElement()).isEmpty();
  }

  @Test
  void emptyResultToElement() {
    final EmptyResult result = new EmptyResult();
    assertThat(result.toElement()).isNull();
  }

  @Test
  void emptyResultGetRecord() {
    final EmptyResult result = new EmptyResult();
    assertThat(result.getRecord()).isEmpty();
  }

  @Test
  void emptyResultIsProjection() {
    final EmptyResult result = new EmptyResult();
    assertThat(result.isProjection()).isFalse();
  }

  @Test
  void emptyResultGetMetadata() {
    final EmptyResult result = new EmptyResult();
    assertThat(result.getMetadata("key")).isNull();
    assertThat(result.getMetadataKeys()).isNull();
  }

  @Test
  void emptyResultGetDatabase() {
    final EmptyResult result = new EmptyResult();
    assertThat(result.getDatabase()).isNull();
  }

  @Test
  void emptyResultHasProperty() {
    final EmptyResult result = new EmptyResult();
    assertThat(result.hasProperty("anything")).isFalse();
  }

  @Test
  void emptyResultToMap() {
    final EmptyResult result = new EmptyResult();
    assertThat(result.toMap()).isNull();
  }

  // ===== TraverseResult =====

  @Test
  void traverseResultDefaultDepth() {
    final TraverseResult result = new TraverseResult();
    assertThat(result.<Integer>getProperty("$depth")).isNull();
  }

  @Test
  void traverseResultSetAndGetDepth() {
    final TraverseResult result = new TraverseResult();
    result.setProperty("$depth", 3);
    assertThat(result.<Integer>getProperty("$depth")).isEqualTo(3);
  }

  @Test
  void traverseResultSetDepthCaseInsensitive() {
    final TraverseResult result = new TraverseResult();
    result.setProperty("$DEPTH", 5);
    assertThat(result.<Integer>getProperty("$depth")).isEqualTo(5);
    assertThat(result.<Integer>getProperty("$Depth")).isEqualTo(5);
  }

  @Test
  void traverseResultSetNonDepthProperty() {
    final TraverseResult result = new TraverseResult();
    result.setProperty("name", "test");
    assertThat(result.<String>getProperty("name")).isEqualTo("test");
    assertThat(result.<Integer>getProperty("$depth")).isNull();
  }

  @Test
  void traverseResultSetDepthFromLong() {
    final TraverseResult result = new TraverseResult();
    result.setProperty("$depth", 7L);
    assertThat(result.<Integer>getProperty("$depth")).isEqualTo(7);
  }

  // ===== IndexCondPair =====

  @Test
  void indexCondPairEquals() {
    final IndexCondPair pair1 = new IndexCondPair(null, null);
    final IndexCondPair pair2 = new IndexCondPair(null, null);
    assertThat(pair1).isEqualTo(pair2);
  }

  @Test
  void indexCondPairEqualsSameReference() {
    final IndexCondPair pair = new IndexCondPair(null, null);
    assertThat(pair).isEqualTo(pair);
  }

  @Test
  void indexCondPairNotEqualsNull() {
    final IndexCondPair pair = new IndexCondPair(null, null);
    assertThat(pair.equals(null)).isFalse();
  }

  @Test
  void indexCondPairNotEqualsDifferentClass() {
    final IndexCondPair pair = new IndexCondPair(null, null);
    assertThat(pair.equals("string")).isFalse();
  }

  // ===== InternalResultSet =====

  @Test
  void internalResultSetBasicOperations() {
    final InternalResultSet rs = new InternalResultSet();
    assertThat(rs.hasNext()).isFalse();

    final ResultInternal r1 = new ResultInternal();
    r1.setProperty("name", "a");
    final ResultInternal r2 = new ResultInternal();
    r2.setProperty("name", "b");

    rs.add(r1);
    rs.add(r2);
    assertThat(rs.hasNext()).isTrue();
    assertThat(rs.next().<String>getProperty("name")).isEqualTo("a");
    assertThat(rs.next().<String>getProperty("name")).isEqualTo("b");
    assertThat(rs.hasNext()).isFalse();
  }

  @Test
  void internalResultSetReset() {
    final InternalResultSet rs = new InternalResultSet();
    rs.add(new ResultInternal());
    rs.add(new ResultInternal());
    rs.next();
    rs.next();
    assertThat(rs.hasNext()).isFalse();
    rs.reset();
    assertThat(rs.hasNext()).isTrue();
  }

  @Test
  void internalResultSetCopy() {
    final InternalResultSet rs = new InternalResultSet();
    rs.add(new ResultInternal());
    rs.add(new ResultInternal());
    final ResultSet copy = rs.copy();
    int count = 0;
    while (copy.hasNext()) {
      copy.next();
      count++;
    }
    assertThat(count).isEqualTo(2);
  }
}
