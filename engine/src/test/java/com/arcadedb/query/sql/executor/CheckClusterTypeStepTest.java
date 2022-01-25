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
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class CheckClusterTypeStepTest {

  private static final String CLASS_CLUSTER_NAME = "ClassClusterName";
  private static final String CLUSTER_NAME       = "ClusterName";

  @Test
  public void shouldCheckClusterType() throws Exception {
    TestHelper.executeInNewDatabase((db) -> {
      DocumentType clazz = (db.getSchema().createDocumentType(CLASS_CLUSTER_NAME).addBucket(db.getSchema().createBucket(CLASS_CLUSTER_NAME)));
      BasicCommandContext context = new BasicCommandContext();
      context.setDatabase(db);
      CheckClusterTypeStep step = new CheckClusterTypeStep(CLASS_CLUSTER_NAME, clazz.getName(), context, false);

      ResultSet result = step.syncPull(context, 20);
      Assertions.assertEquals(0, result.stream().count());
    });
  }

  public void shouldThrowExceptionWhenClusterIsWrong() throws Exception {
    try {
      TestHelper.executeInNewDatabase((db) -> {
        db.getSchema().createBucket(CLUSTER_NAME);
        BasicCommandContext context = new BasicCommandContext();
        context.setDatabase(db);
        CheckClusterTypeStep step = new CheckClusterTypeStep(CLUSTER_NAME, TestHelper.createRandomType(db).getName(), context, false);

        step.syncPull(context, 20);
      });
      Assertions.fail("Expected CommandExecutionException");
    } catch (CommandExecutionException e) {
      // OK
    }
  }
}
