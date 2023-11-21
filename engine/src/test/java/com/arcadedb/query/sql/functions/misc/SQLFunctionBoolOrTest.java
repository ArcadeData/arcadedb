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
package com.arcadedb.query.sql.functions.misc;

import com.arcadedb.TestHelper;
import com.arcadedb.query.sql.executor.ResultSet;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

/**
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
public class SQLFunctionBoolOrTest extends TestHelper {

  @Test
  public void testBoolOr_SingleNull() {
    database.transaction(() -> {
      database.command("sql", "create document type doc0;");
      database.command("sql", "create property doc0.bool boolean;");
      database.command("sql", "insert into doc0 set bool = null;");
      ResultSet result = database.query("sql","select bool_or(bool) as bool_or from doc0;");
      Assertions.assertTrue(result.hasNext());
      Assertions.assertFalse((Boolean) result.next().getProperty("bool_or"));
    });
  }

  @Test
  public void testBoolOr_SingleTrue() {
    database.transaction(() -> {
      database.command("sql", "create document type doc1;");
      database.command("sql", "create property doc1.bool boolean;");
      database.command("sql", "insert into doc1 set bool = true;");
      ResultSet result = database.query("sql","select bool_or(bool) as bool_or from doc1;");
      Assertions.assertTrue(result.hasNext());
      Assertions.assertTrue((Boolean) result.next().getProperty("bool_or"));
    });
  }

  @Test
  public void testBoolOr_SingleFalse() {
    database.transaction(() -> {
      database.command("sql", "create document type doc2;");
      database.command("sql", "create property doc2.bool boolean;");
      database.command("sql", "insert into doc2 set bool = false;");
      ResultSet result = database.query("sql","select bool_or(bool) as bool_or from doc2;");
      Assertions.assertTrue(result.hasNext());
      Assertions.assertFalse((Boolean) result.next().getProperty("bool_or"));
    });
  }

  @Test
  public void testBoolOr_MultiNull() {
    database.transaction(() -> {
      database.command("sql", "create document type doc3;");
      database.command("sql", "create property doc3.bool boolean;");
      database.command("sql", "insert into doc3 (bool) values (null), (null), (null);");
      ResultSet result = database.query("sql","select bool_or(bool) as bool_or from doc3;");
      Assertions.assertTrue(result.hasNext());
      Assertions.assertFalse((Boolean) result.next().getProperty("bool_or"));
    });
  }

  @Test
  public void testBoolOr_MultiTrue() {
    database.transaction(() -> {
      database.command("sql", "create document type doc4;");
      database.command("sql", "create property doc4.bool boolean;");
      database.command("sql", "insert into doc4 (bool) values (false), (false), (true);");
      ResultSet result = database.query("sql","select bool_or(bool) as bool_or from doc4;");
      Assertions.assertTrue(result.hasNext());
      Assertions.assertTrue((Boolean) result.next().getProperty("bool_or"));
    });
  }

  @Test
  public void testBoolOr_MultiFalse() {
    database.transaction(() -> {
      database.command("sql", "create document type doc5;");
      database.command("sql", "create property doc5.bool boolean;");
      database.command("sql", "insert into doc5 (bool) values (false), (false), (false);");
      ResultSet result = database.query("sql","select bool_or(bool) as bool_or from doc5;");
      Assertions.assertTrue(result.hasNext());
      Assertions.assertFalse((Boolean) result.next().getProperty("bool_or"));
    });
  }

  @Test
  public void testBoolOr_MultiTrueHasNull() {
    database.transaction(() -> {
      database.command("sql", "create document type doc6;");
      database.command("sql", "create property doc6.bool boolean;");
      database.command("sql", "insert into doc6 (bool) values (false), (null), (true);");
      ResultSet result = database.query("sql","select bool_or(bool) as bool_or from doc6;");
      Assertions.assertTrue(result.hasNext());
      Assertions.assertTrue((Boolean) result.next().getProperty("bool_or"));
    });
  }

  @Test
  public void testBoolOr_MultiFalseHasNull() {
    database.transaction(() -> {
      database.command("sql", "create document type doc7;");
      database.command("sql", "create property doc7.bool boolean;");
      database.command("sql", "insert into doc7 (bool) values (false), (null), (false);");
      ResultSet result = database.query("sql","select bool_or(bool) as bool_or from doc7;");
      Assertions.assertTrue(result.hasNext());
      Assertions.assertFalse((Boolean) result.next().getProperty("bool_or"));
    });
  }

  @Test
  public void testBoolOr_MultiNullIsNull() {
    database.transaction(() -> {
      database.command("sql", "create document type doc8;");
      database.command("sql", "create property doc8.bool boolean;");
      database.command("sql", "insert into doc8 (bool) values (null), (null), (null);");
      ResultSet result = database.query("sql","select bool_or((bool is not null)) as bool_or from doc8;");
      Assertions.assertTrue(result.hasNext());
      Assertions.assertFalse((Boolean) result.next().getProperty("bool_or"));
    });
  }

  @Test
  public void testBoolOr_MultiHasNull() {
    database.transaction(() -> {
      database.command("sql", "create document type doc9;");
      database.command("sql", "create property doc9.bool boolean;");
      database.command("sql", "insert into doc9 (bool) values (true), (null), (false);");
      ResultSet result = database.query("sql","select bool_or((bool is null)) as bool_or from doc9;");
      Assertions.assertTrue(result.hasNext());
      Assertions.assertTrue((Boolean) result.next().getProperty("bool_or"));
    });
  }
}
