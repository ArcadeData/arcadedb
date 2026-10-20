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
package com.arcadedb.server.http.handler;

import com.arcadedb.serializer.json.JSONArray;
import com.arcadedb.serializer.json.JSONObject;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Issue #10395: what a client is told of a request body that did not parse, and what the log keeps. It runs in the default lane, where
 * {@link Issue10395MalformedPayloadIT} does not: the parser position is told, a body or a member name never is, whatever the body holds.
 * The exceptions are the real ones the JSON classes throw.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class Issue10395DescribeParseFailureTest {
  private static final String FIXED_TEXT = "the body is not a JSON object";
  private static final String POSITION   = "[A-Za-z .()']+ at line \\d+ column \\d+";

  @Test
  void aTruncatedObjectIsToldByItsPosition() {
    assertThat(describeObject("{\"command\":")).matches(POSITION).startsWith("End of input");
    assertThat(describeObject("{\"command\":\"x\" \"password\":\"hunter2\"}")).matches(POSITION).doesNotContain("hunter2");
  }

  @Test
  void aTruncatedArrayIsToldByItsPosition() {
    final String described = describeArray("[1, 2,");

    assertThat(described).matches(POSITION);
  }

  @Test
  void aScalarBodyIsNotQuoted() {
    // "Not a JSON Object: <the body>" is what the parser raises for a scalar: it quotes the body, so the fixed text is told instead
    for (final String body : new String[] { "123", "true", "null", "\"hunter2-secret\"", "hunter2-secret", "\"hunter2 at line 5 column 3\"",
        "hunter2 at line 5 column 3" })
      assertThat(describeObject(body)).as(body).isEqualTo(FIXED_TEXT);
  }

  @Test
  void aMemberNameIsNotTold() {
    assertThat(describeObject("{hunter2-secret:")).doesNotContain("hunter2").doesNotContain("$.");
    assertThat(describeObject("{\"a\":{\"hunter2-secret\":")).doesNotContain("hunter2").doesNotContain("$.");
  }

  @Test
  void aMessageThatIsNotTheParsersPositionFallsBackToTheFixedText() {
    assertThat(AbstractServerHttpHandler.describeParseFailure(new IllegalStateException("Not a JSON Object: \"hunter2 at line 1 column 2\""))).isEqualTo(FIXED_TEXT);
    assertThat(AbstractServerHttpHandler.describeParseFailure(new IllegalStateException((String) null))).isEqualTo(FIXED_TEXT);
    assertThat(AbstractServerHttpHandler.describeParseFailure(new IllegalStateException("   "))).isEqualTo(FIXED_TEXT);
  }

  @Test
  void theInnermostCauseIsTheOneTold() {
    final Exception wrapped = new RuntimeException("Invalid JSON object format: {\"password\":\"hunter2\"",
        new IllegalArgumentException("End of input at line 1 column 22 path $.password"));

    assertThat(AbstractServerHttpHandler.describeParseFailure(wrapped)).isEqualTo("End of input at line 1 column 22");
  }

  @Test
  void aLongVerdictIsCapped() {
    final String described = AbstractServerHttpHandler.describeParseFailure(new IllegalStateException("x".repeat(500) + " at line 1 column 2"));

    assertThat(described).hasSizeLessThanOrEqualTo(203);
  }

  private static String describeObject(final String body) {
    try {
      new JSONObject(body);
    } catch (final Exception e) {
      return AbstractServerHttpHandler.describeParseFailure(e);
    }
    throw new AssertionError("the body parsed as an object: " + body);
  }

  private static String describeArray(final String body) {
    try {
      new JSONArray(body);
    } catch (final Exception e) {
      return AbstractServerHttpHandler.describeParseFailure(e);
    }
    throw new AssertionError("the body parsed as an array: " + body);
  }
}
