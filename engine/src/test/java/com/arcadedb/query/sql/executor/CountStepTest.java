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

import com.arcadedb.exception.TimeoutException;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;


public class CountStepTest {

  private static final String PROPERTY_NAME = "testPropertyName";
  private static final String PROPERTY_VALUE = "testPropertyValue";
  private static final String COUNT_PROPERTY_NAME = "count";

  @Test
  public void shouldCountRecords() {
    final CommandContext context = new BasicCommandContext();
    final CountStep step = new CountStep(context);

    final AbstractExecutionStep previous =
        new AbstractExecutionStep(context) {
          boolean done = false;

          @Override
          public ResultSet syncPull(final CommandContext ctx, final int nRecords) throws TimeoutException {
            final InternalResultSet result = new InternalResultSet();
            if (!done) {
              for (int i = 0; i < 100; i++) {
                final ResultInternal item = new ResultInternal();
                item.setProperty(PROPERTY_NAME, PROPERTY_VALUE);
                result.add(item);
              }
              done = true;
            }
            return result;
          }
        };

    step.setPrevious(previous);
    final ResultSet result = step.syncPull(context, 100);
    assertThat((long) result.next().getProperty(COUNT_PROPERTY_NAME)).isEqualTo(100);
    assertThat(result.hasNext()).isFalse();
  }
}
