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
package com.arcadedb.database;

import com.arcadedb.GlobalConfiguration;
import com.arcadedb.TestHelper;
import com.arcadedb.engine.Bucket;
import com.arcadedb.query.sql.executor.ResultSet;
import com.arcadedb.schema.DocumentType;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.*;

public class BucketSQLTest extends TestHelper {
  @Test
  public void testPopulate() {
  }

  @Override
  protected void beginTest() {
    database.getConfiguration().setValue(GlobalConfiguration.TX_WAL_FLUSH, 2);

    database.transaction(() -> {
      database.command("sql", "CREATE BUCKET Customer_Europe");
      database.command("sql", "CREATE BUCKET Customer_Americas");
      database.command("sql", "CREATE BUCKET Customer_Asia");
      database.command("sql", "CREATE BUCKET Customer_Other");

      database.command("sql", "CREATE DOCUMENT TYPE Customer BUCKET Customer_Europe,Customer_Americas,Customer_Asia,Customer_Other");

      final DocumentType customer = database.getSchema().getType("Customer");
      List<Bucket> buckets = customer.getBuckets(true);
      Assertions.assertEquals(4, buckets.size());

      ResultSet resultset = database.command("sql", "INSERT INTO BUCKET:Customer_Europe CONTENT { firstName: 'Enzo', lastName: 'Ferrari' }");
      Assertions.assertTrue(resultset.hasNext());
      Document enzo = resultset.next().getRecord().get().asDocument();
      Assertions.assertFalse(resultset.hasNext());
      Assertions.assertEquals(database.getSchema().getBucketByName("Customer_Europe").getId(), enzo.getIdentity().bucketId);

      resultset = database.command("sql", "INSERT INTO BUCKET:Customer_Americas CONTENT { firstName: 'Jack', lastName: 'Tramiel' }");
      Assertions.assertTrue(resultset.hasNext());
      Document jack = resultset.next().getRecord().get().asDocument();
      Assertions.assertFalse(resultset.hasNext());
      Assertions.assertEquals(database.getSchema().getBucketByName("Customer_Americas").getId(), jack.getIdentity().bucketId);

      resultset = database.command("sql", "INSERT INTO BUCKET:Customer_Asia CONTENT { firstName: 'Bruce', lastName: 'Lee' }");
      Assertions.assertTrue(resultset.hasNext());
      Document bruce = resultset.next().getRecord().get().asDocument();
      Assertions.assertFalse(resultset.hasNext());
      Assertions.assertEquals(database.getSchema().getBucketByName("Customer_Asia").getId(), bruce.getIdentity().bucketId);

      resultset = database.command("sql", "INSERT INTO BUCKET:Customer_Other CONTENT { firstName: 'Penguin', lastName: 'Hungry' }");
      Assertions.assertTrue(resultset.hasNext());
      Document penguin = resultset.next().getRecord().get().asDocument();
      Assertions.assertFalse(resultset.hasNext());
      Assertions.assertEquals(database.getSchema().getBucketByName("Customer_Other").getId(), penguin.getIdentity().bucketId);
    });
  }
}
