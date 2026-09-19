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

import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Set;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Regression for issue #7907: {@code SingletonMap.hashCode()} was a bare {@code key.hashCode() ^ value.hashCode()}
 * while every other method on the class - {@code containsKey}, {@code containsValue}, {@code get}, {@code equals} -
 * is null-safe, and while {@code SingletonSet.hashCode()} right beside it already guards. Hashing a one-entry map
 * whose value is null therefore threw a {@link NullPointerException} instead of answering what
 * {@link java.util.AbstractMap#hashCode()} - the contract the class stands in for - answers.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class Issue7907SingletonMapNullHashCodeTest {

  @Test
  void hashCodeOfANullValueEntryMatchesTheMapContract() {
    final Map<String, Object> singleton = CollectionUtils.singletonMap("city", null);
    final Map<String, Object> reference = new HashMap<>();
    reference.put("city", null);

    assertThat(singleton).isInstanceOf(SingletonMap.class);
    assertThat(singleton.hashCode()).isEqualTo(reference.hashCode());
  }

  @Test
  void hashCodeOfANullKeyEntryMatchesTheMapContract() {
    final Map<String, Object> singleton = CollectionUtils.singletonMap(null, "x");
    final Map<String, Object> reference = new HashMap<>();
    reference.put(null, "x");

    assertThat(singleton).isInstanceOf(SingletonMap.class);
    assertThat(singleton.hashCode()).isEqualTo(reference.hashCode());
  }

  @Test
  void hashCodeOfAnAllNullEntryMatchesTheMapContract() {
    final Map<String, Object> singleton = CollectionUtils.singletonMap(null, null);
    final Map<String, Object> reference = new HashMap<>();
    reference.put(null, null);

    assertThat(singleton.hashCode()).isEqualTo(reference.hashCode());
  }

  /**
   * equals() already reported the two maps equal before the fix, so the hashCodes HAD to agree: this is the half of
   * the contract that was broken, not an extra.
   */
  @Test
  void equalMapsHashAlike() {
    final Map<String, Object> singleton = CollectionUtils.singletonMap("city", null);
    final Map<String, Object> reference = new LinkedHashMap<>();
    reference.put("city", null);

    assertThat(singleton).isEqualTo(reference);
    assertThat(reference).isEqualTo(singleton);
    assertThat(singleton.hashCode()).isEqualTo(reference.hashCode());
  }

  /**
   * The operator that actually reached this in production: DISTINCT and friends put the map in a hash container.
   */
  @Test
  void aNullValuedSingletonMapCanBeUsedAsAHashKey() {
    final Set<Map<String, Object>> distinct = new HashSet<>();

    assertThat(distinct.add(CollectionUtils.singletonMap("city", null))).isTrue();
    assertThat(distinct.add(CollectionUtils.singletonMap("city", null))).isFalse();
    assertThat(distinct.add(CollectionUtils.singletonMap("city", "Rome"))).isTrue();
    assertThat(distinct).hasSize(2);

    // And the dedupe agrees with the general (multi-entry) path it used to disagree with.
    final Map<String, Object> viaGeneralPath = new LinkedHashMap<>();
    viaGeneralPath.put("city", null);
    assertThat(distinct.contains(viaGeneralPath)).isTrue();
  }

  /**
   * {@code immutableMap()} reaches the same class from {@code LocalDatabase.getGlobalVariables()},
   * {@code Dictionary.getEntries()} and friends, so it needs the same guarantee.
   */
  @Test
  void immutableMapOfOneNullValuedEntryHashesToo() {
    final Map<String, Object> source = new LinkedHashMap<>();
    source.put("only", null);

    final Map<String, Object> immutable = CollectionUtils.immutableMap(source);
    assertThat(immutable).isInstanceOf(SingletonMap.class);
    assertThat(immutable.hashCode()).isEqualTo(source.hashCode());
  }

  /**
   * {@code SingletonSet} is the sibling that already guarded; kept here so the pair stays checked together.
   */
  @Test
  void singletonSetOfNullStillHashesToTheSetContract() {
    final Set<Object> singleton = CollectionUtils.singletonSet(null);
    final Set<Object> reference = new HashSet<>();
    reference.add(null);

    assertThat(singleton.hashCode()).isEqualTo(reference.hashCode());
  }
}
