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
import com.arcadedb.exception.CommandExecutionException;
import com.arcadedb.exception.TimeoutException;
import com.arcadedb.schema.DocumentType;

import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.*;
import static org.assertj.core.api.Assertions.fail;

public class CheckRecordTypeStepTest {

  @Test
  public void shouldCheckRecordsOfOneType() throws Exception {
    TestHelper.executeInNewDatabase((db) -> {
      final String className = TestHelper.createRandomType(db).getName();

      final CommandContext context = new BasicCommandContext();
      final CheckRecordTypeStep step = new CheckRecordTypeStep(context, className);
      final AbstractExecutionStep previous = new AbstractExecutionStep(context) {
        boolean done = false;

        @Override
        public ResultSet syncPull(final CommandContext ctx, final int nRecords) throws TimeoutException {
          final InternalResultSet result = new InternalResultSet();
          if (!done) {
            for (int i = 0; i < 10; i++) {
              final ResultInternal item = new ResultInternal();
              item.setElement(db.newDocument(className));
              result.add(item);
            }
            done = true;
          }
          return result;
        }
      };

      step.setPrevious(previous);
      final ResultSet result = step.syncPull(context, 20);
      assertThat(result.stream().count()).isEqualTo(10);
      assertThat(result.hasNext()).isFalse();
    });
  }

  @Test
  public void shouldCheckRecordsOfSubclasses() throws Exception {
    TestHelper.executeInNewDatabase((db) -> {
      final CommandContext context = new BasicCommandContext();
      final DocumentType parentClass = TestHelper.createRandomType(db);
      final DocumentType childClass = TestHelper.createRandomType(db).addSuperType(parentClass);
      final CheckRecordTypeStep step = new CheckRecordTypeStep(context, parentClass.getName());
      final AbstractExecutionStep previous = new AbstractExecutionStep(context) {
        boolean done = false;

        @Override
        public ResultSet syncPull(final CommandContext ctx, final int nRecords) throws TimeoutException {
          final InternalResultSet result = new InternalResultSet();
          if (!done) {
            for (int i = 0; i < 10; i++) {
              final ResultInternal item = new ResultInternal();
              item.setElement(db.newDocument(i % 2 == 0 ? parentClass.getName() : childClass.getName()));
              result.add(item);
            }
            done = true;
          }
          return result;
        }
      };

      step.setPrevious(previous);
      final ResultSet result = step.syncPull(context, 20);
      assertThat(result.stream().count()).isEqualTo(10);
      assertThat(result.hasNext()).isFalse();
    });
  }

  @Test
  public void shouldThrowExceptionWhenTypeIsDifferent() throws Exception {
    try {
      TestHelper.executeInNewDatabase((db) -> {
        final CommandContext context = new BasicCommandContext();
        final String firstClassName = TestHelper.createRandomType(db).getName();
        final String secondClassName = TestHelper.createRandomType(db).getName();
        final CheckRecordTypeStep step = new CheckRecordTypeStep(context, firstClassName);
        final AbstractExecutionStep previous = new AbstractExecutionStep(context) {
          boolean done = false;

          @Override
          public ResultSet syncPull(final CommandContext ctx, final int nRecords) throws TimeoutException {
            final InternalResultSet result = new InternalResultSet();
            if (!done) {
              for (int i = 0; i < 10; i++) {
                final ResultInternal item = new ResultInternal();
                item.setElement(db.newDocument(i % 2 == 0 ? firstClassName : secondClassName));
                result.add(item);
              }
              done = true;
            }
            return result;
          }
        };

        step.setPrevious(previous);
        final ResultSet result = step.syncPull(context, 20);
        while (result.hasNext()) {
          result.next();
        }
      });
      fail("Expected CommandExecutionException");

    } catch (final CommandExecutionException e) {
      // OK
    }
  }
}
