/*
 * Copyright 2021-present Arcade Data Ltd (info@arcadedata.com)
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
package com.arcadedb.index.fulltext;

import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * The key rule of a full-text index (issue #12162): an unprefixed token that would read as a field-prefixed key is stored behind one
 * escape, and the rule is reversible, so a walk over the keys can tell the three kinds of keys apart.
 *
 * @author lvca (l.garulli@arcadedata.com)
 */
class FullTextKeysTest {
  private static final List<String> PROPERTIES = List.of("title", "body");

  @Test
  void aTokenThatReadsAsNoFieldKeyIsStoredAsItIs() {
    for (final String token : new String[] { "java", "title", "body", "titlex:foo", "note:this", "tit:le", "", ":", "::", ":java", ":title", "::body",
        "a:title:java" })
      assertThat(FullTextKeys.unqualifiedKey(PROPERTIES, token)).as(token).isEqualTo(token);
  }

  @Test
  void aTokenThatReadsAsAFieldKeyIsStoredBehindTheEscape() {
    assertThat(FullTextKeys.unqualifiedKey(PROPERTIES, "title:java")).isEqualTo(":title:java");
    assertThat(FullTextKeys.unqualifiedKey(PROPERTIES, "body:")).isEqualTo(":body:");
    assertThat(FullTextKeys.unqualifiedKey(PROPERTIES, "body:title:java")).isEqualTo(":body:title:java");
  }

  @Test
  void aTokenThatReadsAsAnEscapedKeyIsStoredBehindTheEscapeToo() {
    // the escape itself, then a property: ":title:java" is a token (a keyword or whitespace analyzer keeps it) and must not meet the
    // escaped key of title:java
    assertThat(FullTextKeys.unqualifiedKey(PROPERTIES, ":title:java")).isEqualTo("::title:java");
    assertThat(FullTextKeys.unqualifiedKey(PROPERTIES, "::body:x")).isEqualTo(":::body:x");
  }

  @Test
  void aPropertyNameWithSpacesIsAFieldKeyToo() {
    // a property indexed with a modifier is stored under its full name, and a keyword analyzer keeps a token with spaces
    final List<String> properties = List.of("keywords by item", "body");
    assertThat(FullTextKeys.unqualifiedKey(properties, "keywords by item:x")).isEqualTo(":keywords by item:x");
    assertThat(FullTextKeys.unqualifiedKey(properties, "keywords:x")).isEqualTo("keywords:x");
  }

  @Test
  void aOnePropertyIndexStoresEveryTokenAsItIs() {
    final List<List<String>> oneProperty = Arrays.asList(List.of("title"), List.of(), null);
    for (final List<String> properties : oneProperty) {
      assertThat(FullTextKeys.qualifies(properties)).isFalse();
      assertThat(FullTextKeys.unqualifiedKey(properties, "title:java")).isEqualTo("title:java");
      assertThat(FullTextKeys.unqualifiedKey(properties, ":title:java")).isEqualTo(":title:java");
      assertThat(FullTextKeys.unqualifiedToken(properties, "title:java")).isEqualTo("title:java");
      assertThat(FullTextKeys.unqualifiedToken(properties, ":title:java")).isEqualTo(":title:java");
    }
    assertThat(FullTextKeys.qualifies(PROPERTIES)).isTrue();
  }

  @Test
  void theTokenOfAnUnprefixedKeyIsTheTokenItWasStoredFor() {
    for (final String token : new String[] { "java", "title:java", ":title:java", "::title:java", "body:", ":body:", "note:this", ":", "::", ":java",
        ":title", "", "titlex:foo", "a:title:java" }) {
      final String key = FullTextKeys.unqualifiedKey(PROPERTIES, token);
      assertThat(FullTextKeys.unqualifiedToken(PROPERTIES, key)).as(token).isEqualTo(token);
    }
  }

  @Test
  void aFieldKeyIsNoUnprefixedToken() {
    // the keys of the qualified postings never read as the key of an unprefixed token, whatever the token of the field is
    for (final String token : new String[] { "java", "title:java", ":title:java", "::x", ":", "" }) {
      assertThat(FullTextKeys.unqualifiedToken(PROPERTIES, "title:" + token)).as("title:" + token).isNull();
      assertThat(FullTextKeys.unqualifiedToken(PROPERTIES, "body:" + token)).as("body:" + token).isNull();
    }
  }

  @Test
  void anEscapedKeyAndAKeyThatMerelyStartsWithTheEscapeAreToldApart() {
    // ":title:java" stands for the token title:java, ":java" is the token :java
    assertThat(FullTextKeys.unqualifiedToken(PROPERTIES, ":title:java")).isEqualTo("title:java");
    assertThat(FullTextKeys.unqualifiedToken(PROPERTIES, "::title:java")).isEqualTo(":title:java");
    assertThat(FullTextKeys.unqualifiedToken(PROPERTIES, ":java")).isEqualTo(":java");
    assertThat(FullTextKeys.unqualifiedToken(PROPERTIES, ":")).isEqualTo(":");
    assertThat(FullTextKeys.unqualifiedToken(PROPERTIES, "")).isEmpty();
  }

  @Test
  void onlyEscapesSaysWhetherThePrefixCoversTheEscapedKeys() {
    assertThat(FullTextKeys.onlyEscapes(":")).isTrue();
    assertThat(FullTextKeys.onlyEscapes("::")).isTrue();
    assertThat(FullTextKeys.onlyEscapes(":a")).isFalse();
    assertThat(FullTextKeys.onlyEscapes("a")).isFalse();
    assertThat(FullTextKeys.onlyEscapes("a:")).isFalse();
  }
}
