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
import com.arcadedb.database.Document;
import com.arcadedb.database.MutableDocument;
import com.arcadedb.exception.TimeoutException;

import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.*;

import static org.assertj.core.api.Assertions.*;

public class ConvertToResultInternalStepTest {

  private static final String         STRING_PROPERTY  = "stringPropertyName";
  private static final String         INTEGER_PROPERTY = "integerPropertyName";
  private final        List<Document> documents        = new ArrayList<>();

  @Test
  public void shouldConvertUpdatableResult() throws Exception {
    TestHelper.executeInNewDatabase((database) -> {
      database.getSchema().createDocumentType("test");

      final CommandContext context = new BasicCommandContext();
      final ConvertToResultInternalStep step = new ConvertToResultInternalStep(context);
      final AbstractExecutionStep previous = new AbstractExecutionStep(context) {
        boolean done = false;

        @Override
        public ResultSet syncPull(final CommandContext ctx, final int nRecords) throws TimeoutException {
          final InternalResultSet result = new InternalResultSet();
          if (!done) {
            for (int i = 0; i < 10; i++) {
              final MutableDocument document = database.newDocument("test");
              document.set(STRING_PROPERTY, UUID.randomUUID());
              document.set(INTEGER_PROPERTY, new Random().nextInt());
              documents.add(document);
              final UpdatableResult item = new UpdatableResult(document);
              result.add(item);
            }
            done = true;
          }
          return result;
        }
      };

      step.setPrevious(previous);
      final ResultSet result = step.syncPull(context, 10);

      int counter = 0;
      while (result.hasNext()) {
        final Result currentItem = result.next();
        if (!(currentItem.getClass().equals(ResultInternal.class))) {
          fail("There is an item in result set that is not an instance of ResultInternal");
        }
        if (!currentItem.getElement().get().get(STRING_PROPERTY).equals(documents.get(counter).get(STRING_PROPERTY))) {
          fail("String Document property inside Result instance is not preserved");
        }
        if (!currentItem.getElement().get().get(INTEGER_PROPERTY).equals(documents.get(counter).get(INTEGER_PROPERTY))) {
          fail("Integer Document property inside Result instance is not preserved");
        }
        counter++;
      }
    });
  }
}
