/*
 * Copyright 2021 Arcade Data Ltd
 *
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package com.arcadedb.query.sql.executor;

import com.arcadedb.TestHelper;
import com.arcadedb.database.Document;
import com.arcadedb.database.MutableDocument;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

/**
 * Created by tglman on 09/06/17.
 */
public class OSelectStatementExecutionTestIT extends TestHelper {

  public OSelectStatementExecutionTestIT() {
    autoStartTx = true;
  }

  @Test
  public void stressTestNew() {
    String className = "stressTestNew";
    database.getSchema().createDocumentType(className);
    for (int i = 0; i < 1000000; i++) {
      MutableDocument doc = database.newDocument(className);
      doc.set("name", "name" + i);
      doc.set("surname", "surname" + i);
      doc.save();
    }

    for (int run = 0; run < 5; run++) {
      long begin = System.nanoTime();
      ResultSet result = database.query("sql", "select name from " + className + " where name <> 'name1' ");
      for (int i = 0; i < 999999; i++) {
        //        Assertions.assertTrue(result.hasNext());
        Result item = result.next();
        //        Assertions.assertNotNull(item);
        Object name = item.getProperty("name");
        Assertions.assertFalse("name1".equals(name));
      }
      Assertions.assertFalse(result.hasNext());
      result.close();
      long end = System.nanoTime();
      //System.out.println("new: " + ((end - begin) / 1000000));
    }
  }

  public void stressTestOld() {
    String className = "stressTestOld";
    database.getSchema().createDocumentType(className);
    for (int i = 0; i < 1000000; i++) {
      MutableDocument doc = database.newDocument(className);
      doc.set("name", "name" + i);
      doc.set("surname", "surname" + i);
      doc.save();
    }
    for (int run = 0; run < 5; run++) {
      long begin = System.nanoTime();
      ResultSet r = database.query("sql", "select name from " + className + " where name <> 'name1' ");
      //      Iterator<Document> result = r.iterator();
      for (int i = 0; i < 999999; i++) {
        //        Assertions.assertTrue(result.hasNext());
        //        Document item = result.next();
        Document item = r.next().toElement();

        //        Assertions.assertNotNull(item);
        Object name = item.get("name");
        Assertions.assertFalse("name1".equals(name));
      }
      //      Assertions.assertFalse(result.hasNext());
      long end = System.nanoTime();
      System.out.println("old: " + ((end - begin) / 1000000));
    }
  }
}
