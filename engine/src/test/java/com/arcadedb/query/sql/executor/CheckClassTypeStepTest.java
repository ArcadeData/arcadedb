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
import com.arcadedb.schema.DocumentType;
import com.arcadedb.schema.EmbeddedDocumentType;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class CheckClassTypeStepTest {

  @Test
  public void shouldCheckSubclasses() throws Exception {
    TestHelper.executeInNewDatabase((db) -> {
      final BasicCommandContext context = new BasicCommandContext();
      context.setDatabase(db);
      final DocumentType parentClass = TestHelper.createRandomType(db);
      final DocumentType childClass = TestHelper.createRandomType(db).addSuperType(parentClass);
      final CheckClassTypeStep step = new CheckClassTypeStep(childClass.getName(), parentClass.getName(), context, false);

      final ResultSet result = step.syncPull(context, 20);
      Assertions.assertEquals(0, result.stream().count());
    });
  }

  @Test
  public void shouldCheckOneType() throws Exception {
    TestHelper.executeInNewDatabase((db) -> {
      final BasicCommandContext context = new BasicCommandContext();
      context.setDatabase(db);
      final String className = TestHelper.createRandomType(db).getName();
      final CheckClassTypeStep step = new CheckClassTypeStep(className, className, context, false);

      final ResultSet result = step.syncPull(context, 20);
      Assertions.assertEquals(0, result.stream().count());
    });
  }

  @Test
  public void shouldThrowExceptionWhenClassIsNotParent() throws Exception {
    try {
      TestHelper.executeInNewDatabase((db) -> {
        final BasicCommandContext context = new BasicCommandContext();
        context.setDatabase(db);
        final CheckClassTypeStep step = new CheckClassTypeStep(TestHelper.createRandomType(db).getName(),
            TestHelper.createRandomType(db).getName(), context, false);

        step.syncPull(context, 20);
      });
      Assertions.fail("Expected CommandExecutionException");
    } catch (final CommandExecutionException e) {
      // OK
    }
  }
}
