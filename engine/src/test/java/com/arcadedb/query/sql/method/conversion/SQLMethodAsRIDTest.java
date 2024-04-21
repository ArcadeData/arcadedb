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
package com.arcadedb.query.sql.method.conversion;

import com.arcadedb.database.RID;
import com.arcadedb.query.sql.executor.SQLMethod;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

class SQLMethodAsRIDTest {

  private SQLMethod method;

  @BeforeEach
  void setUp() {
    method = new SQLMethodAsRID();
  }

  @Test
  void testNulIsReturnedAsNull() {
    final Object result = method.execute(null, null, null, null);
    assertThat(result).isNull();
  }

  @Test
  void testFromString() {
    final Object result = method.execute("#10:10", null, null, null);
    assertThat(result).isEqualTo(new RID( "#10:10"));
  }

  @Test
  void testFromRID() {
    final Object result = method.execute(new RID( "#10:10"), null, null, null);
    assertThat(result).isEqualTo(new RID( "#10:10"));
  }

  @Test
  void testFromInvalidString() {
    final Object result = method.execute("INVALID", null, null, null);
    assertThat(result).isEqualTo(null);
  }

  @Test
  void testNotStringAsNull() {
    final Object result = method.execute(10, null, null, null);
    assertThat(result).isEqualTo(null);
  }
}
