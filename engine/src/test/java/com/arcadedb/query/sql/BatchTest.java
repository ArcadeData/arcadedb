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
package com.arcadedb.query.sql;

import com.arcadedb.TestHelper;
import com.arcadedb.query.sql.executor.Result;
import com.arcadedb.query.sql.executor.ResultSet;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class BatchTest extends TestHelper {
  @Test
  public void testReturnArray() {

    database.transaction(() -> {
      ResultSet rs = database.execute("SQL", "let a = select 1 as result;let b = select 2 as result;return [$a,$b];");

      Assertions.assertTrue(rs.hasNext());
      Result record = rs.next();
      Assertions.assertNotNull(record);

      Assertions.assertEquals("{\"value\": [[{\"result\": 1}], [{\"result\": 2}]]}", record.toJSON());

      Assertions.assertFalse(rs.hasNext());
    });
  }
}
