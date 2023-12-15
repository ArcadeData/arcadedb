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
import com.arcadedb.engine.Bucket;
import com.arcadedb.exception.CommandExecutionException;
import com.arcadedb.schema.Schema;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

/**
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
public class DropBucketStatementExecutionTest extends TestHelper {
  @Test
  public void testDropBucketWithExistentType() {
    final String className = "testPlain";
    final Schema schema = database.getSchema();
    schema.createDocumentType(className);

    Assertions.assertNotNull(schema.getType(className));

    for (Bucket bucket : database.getSchema().getType(className).getBuckets(false)) {
      try {
        database.command("sql", "drop bucket " + bucket.getName());
        Assertions.fail();
      } catch (CommandExecutionException e) {
        // EXPECTED
      }

      database.command("sql", "alter type " + className + " bucket -" + bucket.getName());

      final ResultSet result = database.command("sql", "drop bucket " + bucket.getName());
      Assertions.assertTrue(result.hasNext());
      final Result next = result.next();
      Assertions.assertEquals("drop bucket", next.getProperty("operation"));
      Assertions.assertFalse(result.hasNext());
      result.close();

      Assertions.assertFalse(schema.existsBucket(bucket.getName()));
    }
  }
}
