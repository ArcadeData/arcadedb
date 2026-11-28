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
package com.arcadedb.schema;

import com.arcadedb.TestHelper;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * The names {@link Property#setCompression(String)} takes: case does not matter, {@code lz4} is the old name of {@code fast}, nothing, an empty
 * text and {@code none} are no compression, and anything else is refused with the list of the supported ones. What is accepted is what a reopen
 * reads back.
 */
class PropertyCompressionNormalizationTest extends TestHelper {

  @ParameterizedTest(name = "{0} is {1}")
  @CsvSource({ "fast,fast", "FAST,fast", "Max,max", "AUTO,auto", "lz4,fast", "LZ4,fast", "none,none", "NONE,none", "'',none" })
  void aNameIsReadAsItsTier(final String given, final String expected) {
    final DocumentType type = database.getSchema().createDocumentType("Holder");
    final Property property = type.createProperty("p", Type.STRING);

    property.setCompression(given);
    assertThat(property.getCompression()).isEqualTo(expected);

    reopenDatabase();
    assertThat(database.getSchema().getType("Holder").getProperty("p").getCompression()).isEqualTo(expected);
  }

  @ParameterizedTest(name = "{0}")
  @CsvSource({ "fast", "max", "auto" })
  void noCompressionTakesTheTierAwayAgain(final String tier) {
    final Property property = database.getSchema().createDocumentType("Holder").createProperty("p", Type.STRING);
    property.setCompression(tier);
    assertThat(property.getCompression()).isEqualTo(tier);

    property.setCompression(null);
    assertThat(property.getCompression()).isEqualTo("none");
    reopenDatabase();
    assertThat(database.getSchema().getType("Holder").getProperty("p").getCompression()).isEqualTo("none");
  }

  @ParameterizedTest(name = "{0}")
  @CsvSource({ "zstd", "gzip", "faster", "'fast '", "' '", "no" })
  void anUnknownNameIsRefusedAndLeavesTheTier(final String given) {
    final Property property = database.getSchema().createDocumentType("Holder").createProperty("p", Type.STRING);
    property.setCompression("max");

    assertThatThrownBy(() -> property.setCompression(given)).isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("Unsupported compression").hasMessageContaining("none, fast, max, auto");
    assertThat(property.getCompression()).isEqualTo("max");
  }
}
