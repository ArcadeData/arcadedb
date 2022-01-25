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
import com.arcadedb.exception.TimeoutException;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class CheckSafeDeleteStepTest {

  @Test
  public void shouldSafelyDeleteRecord() throws Exception {
    TestHelper.executeInNewDatabase((database) -> {
      CommandContext context = new BasicCommandContext();
      CheckSafeDeleteStep step = new CheckSafeDeleteStep(context, false);
      AbstractExecutionStep previous = new AbstractExecutionStep(context, false) {
        boolean done = false;

        @Override
        public ResultSet syncPull(CommandContext ctx, int nRecords) throws TimeoutException {
          InternalResultSet result = new InternalResultSet();
          if (!done) {
            for (int i = 0; i < 10; i++) {
              ResultInternal item = new ResultInternal();
              item.setElement(database.newDocument(TestHelper.createRandomType(database).getName()));
              result.add(item);
            }
            done = true;
          }
          return result;
        }
      };

      step.setPrevious(previous);
      ResultSet result = step.syncPull(context, 10);
      Assertions.assertEquals(10, result.stream().count());
      Assertions.assertFalse(result.hasNext());
    });
  }
}
