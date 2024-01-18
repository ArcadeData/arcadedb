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
package com.arcadedb.query.sql.executor;

import com.arcadedb.TestHelper;
import com.arcadedb.database.MutableDocument;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class CountFromTypeStepTest {

  private static final String ALIAS = "size";

  @Test
  public void shouldCountRecordsOfClass() throws Exception {
    TestHelper.executeInNewDatabase((db) -> {
      final String className = TestHelper.createRandomType(db).getName();
      for (int i = 0; i < 20; i++) {
        final MutableDocument document = db.newDocument(className);
        document.save();
      }

      final BasicCommandContext context = new BasicCommandContext();
      context.setDatabase(db);
      final CountFromTypeStep step = new CountFromTypeStep(className, ALIAS, context, false);

      final ResultSet result = step.syncPull(context, 20);
      Assertions.assertEquals(20, (long) result.next().getProperty(ALIAS));
      Assertions.assertFalse(result.hasNext());
    });
  }
}
