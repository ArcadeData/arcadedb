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

import com.arcadedb.TestHelper;
import com.arcadedb.database.MutableDocument;
import com.arcadedb.database.Record;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

class SQLMethodAsRecordTest extends TestHelper {

  private SQLMethodAsRecord method = new SQLMethodAsRecord();

  @Override
  protected void beginTest() {
    database.getSchema().createDocumentType("Doc");
  }

  @Test
  void testNulIsReturnedAsNull() {
    final Object result = method.execute(null, null, null, null, null);
    assertThat(result).isNull();
  }

  @Test
  void testFromString() {
    database.transaction(() -> {
      final MutableDocument doc = database.newDocument("Doc").save();

      final Object result = method.execute(null, null, null, doc.getIdentity().toString(), null);
      assertThat(result).isInstanceOf(Record.class);
      assertThat(((Record) result).getIdentity()).isEqualTo(doc.getIdentity());
    });
  }

  @Test
  void testFromRID() {
    database.transaction(() -> {
      final MutableDocument doc = database.newDocument("Doc").save();

      final Object result = method.execute(null, null, null, doc.getIdentity(), null);
      assertThat(result).isInstanceOf(Record.class);
      assertThat(((Record) result).getIdentity()).isEqualTo(doc.getIdentity());
    });
  }

  @Test
  void testFromInvalidString() {
    final Object result = method.execute(null, null, null, "INVALID", null);
    assertThat(result).isEqualTo(null);
  }

  @Test
  void testNotStringAsNull() {
    final Object result = method.execute(null, null, null, 10, null);
    assertThat(result).isEqualTo(null);
  }
}
