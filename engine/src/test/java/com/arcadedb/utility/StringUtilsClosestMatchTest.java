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
package com.arcadedb.utility;

import org.junit.jupiter.api.Test;

import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Pins the contract of {@link StringUtils#closestMatch(String, Iterable)} and {@link StringUtils#editDistance(String, String)},
 * the "did you mean" hint of the importer and exporter settings (issues #11311, #11523).
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class StringUtilsClosestMatchTest {
  private static final List<String> SETTINGS = List.of("format", "overwrite", "includeTypes", "excludeTypes", "includeRecords");

  @Test
  void editDistanceIsLevenshtein() {
    assertThat(StringUtils.editDistance("", "")).isZero();
    assertThat(StringUtils.editDistance("abc", "abc")).isZero();
    assertThat(StringUtils.editDistance("", "abc")).isEqualTo(3);
    assertThat(StringUtils.editDistance("kitten", "sitting")).isEqualTo(3);
    assertThat(StringUtils.editDistance("overwite", "overwrite")).isEqualTo(1);
  }

  @Test
  void aTypoNamesTheSettingItMisspells() {
    assertThat(StringUtils.closestMatch("overwite", SETTINGS)).isEqualTo("overwrite");
    assertThat(StringUtils.closestMatch("exludeTypes", SETTINGS)).isEqualTo("excludeTypes");
    // CASE-INSENSITIVE: A NAME THAT DIFFERS ONLY IN CASE IS AT DISTANCE 0
    assertThat(StringUtils.closestMatch("includetypes", SETTINGS)).isEqualTo("includeTypes");
  }

  @Test
  void aNameTooFarFromEverySettingHasNoMatch() {
    assertThat(StringUtils.closestMatch("o", SETTINGS)).isNull();
    assertThat(StringUtils.closestMatch("bogusSetting", SETTINGS)).isNull();
    assertThat(StringUtils.closestMatch("anything", List.of())).isNull();
  }

  @Test
  void aTieGoesToTheFirstInNaturalOrder() {
    // "ab" IS ONE EDIT FROM BOTH: THE ANSWER DOES NOT DEPEND ON THE ITERATION ORDER OF THE CANDIDATES
    assertThat(StringUtils.closestMatch("ab", List.of("ac", "aa"))).isEqualTo("aa");
    assertThat(StringUtils.closestMatch("ab", List.of("aa", "ac"))).isEqualTo("aa");
  }
}
