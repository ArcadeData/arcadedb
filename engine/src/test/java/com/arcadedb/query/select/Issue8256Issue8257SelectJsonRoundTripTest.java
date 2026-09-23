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
package com.arcadedb.query.select;

import com.arcadedb.TestHelper;
import com.arcadedb.database.MutableDocument;
import com.arcadedb.schema.DocumentType;
import com.arcadedb.schema.Type;
import com.arcadedb.serializer.json.JSONObject;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * https://github.com/ArcadeData/arcadedb/issues/8268, tracking
 * https://github.com/ArcadeData/arcadedb/issues/8256 and https://github.com/ArcadeData/arcadedb/issues/8257.
 * <p>
 * #8256: {@code SelectTreeNode.toJSON()} writes a unary node as a two-element array for every operator whose
 * {@code right} is null - {@code not}, {@code is_null} and {@code is_not_null} - but the reader's arity gate only
 * accepted that shape for {@code not}, so a select whose WHERE contained {@code is null} or {@code is not null}
 * could not be read back at all. The fluent builder's {@code isNull()}/{@code isNotNull()} also used to leave a
 * meaningless {@code Boolean.TRUE} in the right slot instead of {@code null}, which is fixed alongside the reader so
 * both producers of this shape now agree with each other and with the convention {@code not} already used.
 * <p>
 * #8257: {@code ":name"} and {@code "#name"} are how the JSON condition format spells a property and a parameter, so
 * a LITERAL string that happens to begin with either sigil used to be indistinguishable from a reference once
 * written verbatim - {@code where s = ':alice'} round-tripped into {@code where s = <value of property alice>}
 * instead of comparing against the literal string.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
public class Issue8256Issue8257SelectJsonRoundTripTest extends TestHelper {

  public Issue8256Issue8257SelectJsonRoundTripTest() {
    autoStartTx = false;
  }

  @Override
  protected void beginTest() {
    final DocumentType type = database.getSchema().createDocumentType("D");
    type.createProperty("a", Type.INTEGER);
    type.createProperty("s", Type.STRING);

    database.transaction(() -> {
      for (int i = 0; i < 3; i++) {
        final MutableDocument doc = database.newDocument("D");
        doc.set("a", i);
        doc.save();
      }
      // A record whose "a" is genuinely absent, for is null / is not null to tell apart. The rest give "a" an
      // unrelated value so they do not also count as "a is null".
      database.newDocument("D").save();

      database.newDocument("D").set("a", -1).set("s", ":alice").save();
      database.newDocument("D").set("a", -1).set("s", "#p").save();
      database.newDocument("D").set("a", -1).set("s", "plain").save();
      database.newDocument("D").set("a", -1).set("s", "::already-escaped-looking").save();
    });
  }

  // ---------------------------------------------------------------------------------------------------------------
  // #8256: is null / is not null round trip
  // ---------------------------------------------------------------------------------------------------------------

  @Test
  void isNullRoundTripsThroughJson() {
    final JSONObject json = database.select().fromType("D").where().property("a").isNull().compile().json();
    // No third element: is_null's right is null, the same convention `not` already uses.
    assertThat(json.getJSONArray("where").toString()).isEqualTo("[[\":a\",\"is null\"]]");

    // Only the record with no "a" at all matches.
    assertThat(database.select().json(json).count()).isEqualTo(1);

    final JSONObject roundTripped = database.select().json(json).compile().json();
    assertThat(roundTripped.toString()).isEqualTo(json.toString());
  }

  @Test
  void isNotNullRoundTripsThroughJson() {
    final JSONObject json = database.select().fromType("D").where().property("a").isNotNull().compile().json();
    assertThat(json.getJSONArray("where").toString()).isEqualTo("[[\":a\",\"is not null\"]]");

    assertThat(database.select().json(json).count()).isEqualTo(7);

    final JSONObject roundTripped = database.select().json(json).compile().json();
    assertThat(roundTripped.toString()).isEqualTo(json.toString());
  }

  @Test
  void isNullNestedInsideALogicTreeAlsoRoundTrips() {
    final JSONObject json = new JSONObject(
        "{\"fromType\":\"D\",\"where\":[[\":a\",\"=\",0],\"or\",[\":a\",\"is null\"]]}");

    // a = 0 (1 record) or a is null (1 record) -> 2.
    assertThat(database.select().json(json).count()).isEqualTo(2);
    assertThat(database.select().json(json).compile().json().getJSONArray("where").toString())
        .isEqualTo(json.getJSONArray("where").toString());
  }

  @Test
  void unaryArityErrorNamesTheOperatorForEveryUnaryOperator() {
    for (final String operator : new String[] { "not", "is null", "is not null" }) {
      assertThatThrownBy(() -> database.select()
          .json(new JSONObject("{\"fromType\":\"D\",\"where\":[[\":a\",\"=\",1],\"" + operator + "\",[\":a\",\"=\",2]]}")))
          .as("operator '%s'", operator)
          .isInstanceOf(IllegalArgumentException.class)
          .hasMessageContaining("'" + operator + "' is unary");
    }
  }

  // ---------------------------------------------------------------------------------------------------------------
  // #8257: a literal beginning with ':' or '#' is not misread as a property/parameter reference
  // ---------------------------------------------------------------------------------------------------------------

  @Test
  void aLiteralBeginningWithAColonRoundTripsAsItself() {
    assertLiteralRoundTrips(":alice", "::alice", 1);
  }

  @Test
  void aLiteralBeginningWithAHashRoundTripsAsItself() {
    assertLiteralRoundTrips("#p", "##p", 1);
  }

  @Test
  void aLiteralThatAlreadyLooksEscapedRoundTripsAsItself() {
    assertLiteralRoundTrips("::already-escaped-looking", ":::already-escaped-looking", 1);
  }

  @Test
  void aPlainLiteralIsUnaffectedByTheEscaping() {
    assertLiteralRoundTrips("plain", "plain", 1);
  }

  @Test
  void aRealPropertyReferenceIsNotDoubleEscaped() {
    // A property on both sides is only reachable by handing the JSON condition directly: the fluent builder's right
    // block has no property-to-property comparator.
    final JSONObject json = new JSONObject("{\"fromType\":\"D\",\"where\":[\":s\",\"=\",\":a\"]}");
    final JSONObject compiled = database.select().json(json).compile().json();
    assertThat(compiled.getJSONArray("where").toString()).isEqualTo("[[\":s\",\"=\",\":a\"]]");
    assertThat(database.select().json(compiled).compile().json().toString()).isEqualTo(compiled.toString());
  }

  @Test
  void aRealParameterReferenceIsNotDoubleEscaped() {
    final JSONObject json = database.select().fromType("D").where().property("s").eq().parameter("p").compile().json();
    assertThat(json.getJSONArray("where").toString()).isEqualTo("[[\":s\",\"=\",\"#p\"]]");

    assertThat(database.select().json(json).compile().parameter("p", "plain").count()).isEqualTo(1);
    assertThat(database.select().json(json).compile().json().toString()).isEqualTo(json.toString());
  }

  @Test
  void anEscapedLiteralIsNotAcceptedAsTheLeftOperandOfAComparison() {
    assertThatThrownBy(() -> database.select()
        .json(new JSONObject("{\"fromType\":\"D\",\"where\":[\"::alice\",\"=\",\":s\"]}")))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("Unsupported value");
  }

  /**
   * Builds a native tree comparing property {@code s} against the given literal, checks the JSON encodes it with the
   * escaped form, checks the literal comparison (not a property/parameter lookup) answers the expected count, and
   * checks a further round trip is stable.
   */
  private void assertLiteralRoundTrips(final String literal, final String expectedEncodedLiteral, final long expectedCount) {
    final JSONObject json = database.select().fromType("D").where().property("s").eq().value(literal).compile().json();
    assertThat(json.getJSONArray("where").toString()).isEqualTo("[[\":s\",\"=\",\"" + expectedEncodedLiteral + "\"]]");

    assertThat(database.select().json(json).count()).as("literal comparison for %s", literal).isEqualTo(expectedCount);

    final JSONObject roundTripped = database.select().json(json).compile().json();
    assertThat(roundTripped.toString()).isEqualTo(json.toString());
  }
}
