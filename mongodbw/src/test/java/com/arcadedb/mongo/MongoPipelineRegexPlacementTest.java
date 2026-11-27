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

import de.bwaldvogel.mongo.backend.DefaultQueryMatcher;
import de.bwaldvogel.mongo.bson.BsonRegularExpression;
import de.bwaldvogel.mongo.bson.Document;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Where a regular expression may sit in the {@code $match} of an aggregation pipeline. The {@code $match} stages the plugin runs
 * itself (the top-level ones) read a document that carries {@code $regex} next to other fields as a plain value to compare with. The
 * ones the library runs (inside {@code $facet} and {@code $lookup} sub-pipelines, after a {@code $changeStream}) read it as a
 * pattern, so there the regular expressions must all be the ones {@link MongoFilter} bounds: a {@code $regex} anywhere else is
 * refused before the pipeline runs.
 *
 * @author lvca (l.garulli@arcadedata.com)
 */
class MongoPipelineRegexPlacementTest {

  /** The conditions that carry a {@code $regex} the filter does not turn into a bounded expression. */
  private static final List<Document> UNBOUNDED_SHAPES = List.of(
      doc("s", doc("x", 1, "$regex", "a")),
      doc("s", doc("x", 1, "$regex", "a", "$options", "i")),
      doc("s", doc("x", 1, "$options", "i", "$regex", "a")),
      doc("s", doc("$not", doc("x", 1, "$regex", "a"))),
      doc("s", doc("$in", List.of(doc("$regex", "a")))),
      doc("s", doc("$in", List.of("b", doc("$regex", "a", "$options", "i")))),
      doc("s", doc("$nin", List.of(doc("$regex", "a")))),
      doc("s", doc("$all", List.of(doc("$regex", "a")))),
      doc("s", doc("$in", List.of(doc("x", 1, "$regex", "a")))),
      doc("s", doc("$elemMatch", doc("x", 1, "$regex", "a"))),
      doc("s", doc("$elemMatch", doc("t", doc("x", 1, "$regex", "a")))),
      doc("s", doc("$elemMatch", doc("t", doc("$in", List.of(doc("$regex", "a")))))),
      doc("s", doc("x", doc("y", 1, "$regex", "a"))),
      doc("$or", List.of(doc("s", doc("x", 1, "$regex", "a")))),
      doc("$and", List.of(doc("n", 1), doc("$nor", List.of(doc("s", doc("x", 1, "$regex", "a")))))));

  /** The conditions whose regular expressions the filter bounds: they keep working wherever the library runs them. */
  private static final List<Document> BOUNDED_SHAPES = List.of(
      doc("s", doc("$regex", "a")),
      doc("s", doc("$regex", "a", "$options", "i")),
      doc("s", doc("$options", "i", "$regex", "a")),
      doc("s", doc("$regex", "a", "$ne", "b")),
      doc("s", new BsonRegularExpression("a")),
      doc("s", doc("$in", List.of(new BsonRegularExpression("a"), "b"))),
      doc("s", doc("$not", doc("$regex", "a"))),
      doc("s", doc("$not", new BsonRegularExpression("a"))),
      doc("s", doc("$elemMatch", doc("t", doc("$regex", "a")))),
      doc("s", doc("$elemMatch", doc("$not", doc("$regex", "a")))),
      doc("$or", List.of(doc("s", doc("$regex", "a")), doc("n", 1))),
      doc("$nor", List.of(doc("s", doc("$regex", "a")))),
      doc("s", doc("x", 1)),
      doc("s", doc("$in", List.of("a", "b"))),
      doc("s", doc("$gt", "a")),
      doc("n", doc("$in", List.of(1, 2)), "s", "a"));

  /** A document of the given key and value pairs: the library's own, so that {@code $regex} stays a key and is not read as a literal. */
  private static Document doc(final Object... keyValues) {
    final Document document = new Document();
    for (int i = 0; i < keyValues.length; i += 2)
      document.put((String) keyValues[i], keyValues[i + 1]);
    return document;
  }

  private static List<Document> bound(final Document... stages) {
    return MongoFilter.boundPipeline(List.of(stages), MongoFilter.RegexBudget.ofMillis(60_000));
  }

  private static Document match(final Document query) {
    return new Document("$match", query);
  }

  private static Document facet(final Document... branch) {
    return new Document("$facet", new Document("f", List.of(branch)));
  }

  private static Document lookup(final Document... pipeline) {
    return new Document("$lookup", new Document("from", "other").append("as", "o").append("pipeline", List.of(pipeline)));
  }

  private static Document unionWith(final Document... pipeline) {
    return new Document("$unionWith", new Document("coll", "other").append("pipeline", List.of(pipeline)));
  }

  private static Document graphLookup(final Document query) {
    return new Document("$graphLookup",
        new Document("from", "other").append("startWith", "$a").append("connectFromField", "a").append("connectToField", "b")
            .append("as", "r").append("restrictSearchWithMatch", query));
  }

  private static void assertRefused(final Document shape, final Document... pipeline) {
    assertThatThrownBy(() -> bound(pipeline)).as(shape.toString()).isInstanceOf(IllegalArgumentException.class).hasMessageContaining("$regex");
  }

  @Test
  void aFacetBranchRefusesARegexThatIsNotBounded() {
    for (final Document shape : UNBOUNDED_SHAPES)
      assertRefused(shape, facet(match(shape)));
  }

  @Test
  void aLookupPipelineRefusesARegexThatIsNotBounded() {
    for (final Document shape : UNBOUNDED_SHAPES)
      assertRefused(shape, lookup(match(shape)));
  }

  @Test
  void aUnionPipelineRefusesARegexThatIsNotBounded() {
    for (final Document shape : UNBOUNDED_SHAPES)
      assertRefused(shape, unionWith(match(shape)));
  }

  @Test
  void aGraphSearchRestrictionRefusesARegexThatIsNotBounded() {
    for (final Document shape : UNBOUNDED_SHAPES)
      assertRefused(shape, graphLookup(shape));
  }

  @Test
  void aSubPipelineNestedInASubPipelineRefusesARegexThatIsNotBounded() {
    for (final Document shape : UNBOUNDED_SHAPES) {
      assertRefused(shape, facet(lookup(match(shape))));
      assertRefused(shape, lookup(facet(match(shape))));
    }
  }

  @Test
  void theStagesAfterAChangeStreamRefuseARegexThatIsNotBounded() {
    for (final Document shape : UNBOUNDED_SHAPES)
      assertRefused(shape, new Document("$changeStream", new Document()), match(shape));
  }

  @Test
  void aTopLevelMatchStillReadsTheShapeAsAValueToCompareWith() {
    for (final Document shape : UNBOUNDED_SHAPES)
      assertThatCode(() -> bound(match(shape))).as(shape.toString()).doesNotThrowAnyException();

    // the plugin's matcher compares the embedded document, it does not search a pattern: "a" is not equal to it
    final MongoFilter filter = new MongoFilter(null, doc("s", doc("x", 1, "$regex", "a")));
    assertThat(filter.matches(Map.<String, Object>of("s", "a"))).isFalse();
    assertThat(filter.matches(Map.<String, Object>of("s", "alpha"))).isFalse();
  }

  @Test
  void theRegularExpressionsTheFilterBoundsStillWorkInASubPipeline() {
    for (final Document shape : BOUNDED_SHAPES) {
      assertThatCode(() -> bound(facet(match(shape)))).as(shape.toString()).doesNotThrowAnyException();
      assertThatCode(() -> bound(lookup(match(shape)))).as(shape.toString()).doesNotThrowAnyException();
      assertThatCode(() -> bound(graphLookup(shape))).as(shape.toString()).doesNotThrowAnyException();
      assertThatCode(() -> bound(new Document("$changeStream", new Document()), match(shape))).as(shape.toString())
          .doesNotThrowAnyException();
    }
  }

  @Test
  void aBoundedRegularExpressionOfASubPipelineMatchesLikeTheLibraryDoes() {
    final List<Document> pipeline = bound(new Document("$changeStream", new Document()),
        match(doc("s", doc("$regex", "^A", "$options", "i"), "n", doc("$in", List.of(1, 2)))),
        match(new Document("t", new BsonRegularExpression("^be"))));
    final DefaultQueryMatcher library = new DefaultQueryMatcher();

    final Document query = (Document) pipeline.get(1).get("$match");
    assertThat(library.matches(new Document("s", "alpha").append("n", 1), query)).isTrue();
    assertThat(library.matches(new Document("s", "beta").append("n", 1), query)).isFalse();
    assertThat(library.matches(new Document("s", "alpha").append("n", 3), query)).isFalse();

    final Document literal = (Document) pipeline.get(2).get("$match");
    assertThat(library.matches(new Document("t", "beta"), literal)).isTrue();
    assertThat(library.matches(new Document("t", "alpha"), literal)).isFalse();
  }
}
