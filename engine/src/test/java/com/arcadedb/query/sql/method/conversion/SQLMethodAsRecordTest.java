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
import com.arcadedb.query.sql.executor.BasicCommandContext;
import com.arcadedb.query.sql.executor.CommandContext;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

class SQLMethodAsRecordTest extends TestHelper {

  private final SQLMethodAsRecord method = new SQLMethodAsRecord();

  @Override
  protected void beginTest() {
    database.getSchema().createDocumentType("Doc");
  }

  @Test
  void nulIsReturnedAsNull() {
    final Object result = method.execute(null, null, null, null);
    assertThat(result).isNull();
  }

  @Test
  void fromString() {
    database.transaction(() -> {
      final MutableDocument doc = database.newDocument("Doc").save();
      final CommandContext context = new BasicCommandContext().setDatabase(database);

      final Object result = method.execute(doc.getIdentity().toString(), null, context, null);
      assertThat(result).isInstanceOf(Record.class);
      assertThat(((Record) result).getIdentity()).isEqualTo(doc.getIdentity());
    });
  }

  @Test
  void fromRID() {
    database.transaction(() -> {
      final MutableDocument doc = database.newDocument("Doc").save();

      final Object result = method.execute(doc.getIdentity(), null, null, null);
      assertThat(result).isInstanceOf(Record.class);
      assertThat(((Record) result).getIdentity()).isEqualTo(doc.getIdentity());
    });
  }

  @Test
  void fromInvalidString() {
    final Object result = method.execute("INVALID", null, null, null);
    assertThat(result).isEqualTo(null);
  }

  @Test
  void notStringAsNull() {
    final Object result = method.execute(10, null, null, null);
    assertThat(result).isEqualTo(null);
  }
}
