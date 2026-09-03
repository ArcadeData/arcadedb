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
package com.arcadedb.query.sql.method.collection;

import com.arcadedb.query.sql.executor.SQLMethod;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.LinkedHashSet;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * The method form of the {@code last()} function (issue #7027).
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class SQLMethodLastTest {

  private SQLMethod method;

  @BeforeEach
  void setUp() {
    method = new SQLMethodLast();
  }

  @Test
  void collectionsOfEveryShape() {
    assertThat(method.execute(List.of(1, 2, 3), null, null, null)).isEqualTo(3);
    assertThat(method.execute(new Integer[] { 1, 2, 3 }, null, null, null)).isEqualTo(3);
    assertThat(method.execute(new int[] { 1, 2, 3 }, null, null, null)).isEqualTo(3);
    assertThat(method.execute(new LinkedHashSet<>(List.of(1, 2, 3)), null, null, null)).isEqualTo(3);
  }

  @Test
  void emptyCollectionAnswersNull() {
    assertThat(method.execute(List.of(), null, null, null)).isNull();
    assertThat(method.execute(new String[0], null, null, null)).isNull();
  }

  @Test
  void scalarsAndNullAreAnIdentity() {
    assertThat(method.execute("abc", null, null, null)).isEqualTo("abc");
    assertThat(method.execute(7, null, null, null)).isEqualTo(7);
    assertThat(method.execute(null, null, null, null)).isNull();
  }
}
