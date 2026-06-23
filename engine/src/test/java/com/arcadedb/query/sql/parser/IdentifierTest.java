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
package com.arcadedb.query.sql.parser;

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Created by luigidellaquila on 26/04/16.
 */
class IdentifierTest {

  @Test
  void backTickQuoted() {
    final Identifier identifier = new Identifier("foo`bar");

    // getStringValue() returns the plain name (back-tick quoting removed), as used for schema/property lookups
    assertThat(identifier.getStringValue()).isEqualTo("foo`bar");
    // getValue() returns the internal value with back-ticks escaped with backslash
    assertThat(identifier.getValue()).isEqualTo("foo\\`bar");
  }

  @Test
  void plainNameRoundTrips() {
    final Identifier identifier = new Identifier("foobar");

    assertThat(identifier.getStringValue()).isEqualTo("foobar");
    assertThat(identifier.getValue()).isEqualTo("foobar");
  }

  @Test
  void multipleBackTicksUnescaped() {
    final Identifier identifier = new Identifier("a`b`c");

    assertThat(identifier.getStringValue()).isEqualTo("a`b`c");
    assertThat(identifier.getValue()).isEqualTo("a\\`b\\`c");
  }

  @Test
  void backslashWithoutBackTickPreserved() {
    // A literal backslash not followed by a back-tick must be left untouched by getStringValue()
    final Identifier identifier = new Identifier("na\\me");

    assertThat(identifier.getStringValue()).isEqualTo("na\\me");
    assertThat(identifier.getValue()).isEqualTo("na\\me");
  }

  @Test
  void nullValueIsNull() {
    final Identifier identifier = new Identifier((String) null);

    assertThat(identifier.getStringValue()).isNull();
    assertThat(identifier.getValue()).isNull();
  }
}
