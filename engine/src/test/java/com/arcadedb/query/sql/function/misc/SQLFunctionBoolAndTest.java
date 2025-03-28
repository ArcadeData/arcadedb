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
package com.arcadedb.query.sql.function.misc;

import com.arcadedb.TestHelper;
import com.arcadedb.query.sql.executor.ResultSet;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
public class SQLFunctionBoolAndTest extends TestHelper {

  @Test
  public void testBoolAnd_SingleNull() {
    database.transaction(() -> {
      database.command("sql", "create document type doc0;");
      database.command("sql", "create property doc0.bool boolean;");
      database.command("sql", "insert into doc0 set bool = null;");
      ResultSet result = database.query("sql", "select bool_and(bool) as bool_and from doc0;");
      assertThat(result.hasNext()).isTrue();
      assertThat((Boolean) result.next().getProperty("bool_and")).isTrue();
    });
  }

  @Test
  public void testBoolAnd_SingleTrue() {
    database.transaction(() -> {
      database.command("sql", "create document type doc1;");
      database.command("sql", "create property doc1.bool boolean;");
      database.command("sql", "insert into doc1 set bool = true;");
      ResultSet result = database.query("sql", "select bool_and(bool) as bool_and from doc1;");
      assertThat(result.hasNext()).isTrue();
      assertThat((Boolean) result.next().getProperty("bool_and")).isTrue();
    });
  }

  @Test
  public void testBoolAnd_SingleFalse() {
    database.transaction(() -> {
      database.command("sql", "create document type doc2;");
      database.command("sql", "create property doc2.bool boolean;");
      database.command("sql", "insert into doc2 set bool = false;");
      ResultSet result = database.query("sql", "select bool_and(bool) as bool_and from doc2;");
      assertThat(result.hasNext()).isTrue();
      assertThat((Boolean) result.next().getProperty("bool_and")).isFalse();
    });
  }

  @Test
  public void testBoolAnd_MultiNull() {
    database.transaction(() -> {
      database.command("sql", "create document type doc3;");
      database.command("sql", "create property doc3.bool boolean;");
      database.command("sql", "insert into doc3 (bool) values (null), (null), (null);");
      ResultSet result = database.query("sql", "select bool_and(bool) as bool_and from doc3;");
      assertThat(result.hasNext()).isTrue();
      assertThat((Boolean) result.next().getProperty("bool_and")).isTrue();
    });
  }

  @Test
  public void testBoolAnd_MultiTrue() {
    database.transaction(() -> {
      database.command("sql", "create document type doc4;");
      database.command("sql", "create property doc4.bool boolean;");
      database.command("sql", "insert into doc4 (bool) values (true), (true), (true);");
      ResultSet result = database.query("sql", "select bool_and(bool) as bool_and from doc4;");
      assertThat(result.hasNext()).isTrue();
      assertThat((Boolean) result.next().getProperty("bool_and")).isTrue();
    });
  }

  @Test
  public void testBoolAnd_MultiFalse() {
    database.transaction(() -> {
      database.command("sql", "create document type doc5;");
      database.command("sql", "create property doc5.bool boolean;");
      database.command("sql", "insert into doc5 (bool) values (true), (true), (false);");
      ResultSet result = database.query("sql", "select bool_and(bool) as bool_and from doc5;");
      assertThat(result.hasNext()).isTrue();
      assertThat((Boolean) result.next().getProperty("bool_and")).isFalse();
    });
  }

  @Test
  public void testBoolAnd_MultiTrueHasNull() {
    database.transaction(() -> {
      database.command("sql", "create document type doc6;");
      database.command("sql", "create property doc6.bool boolean;");
      database.command("sql", "insert into doc6 (bool) values (true), (null), (true);");
      ResultSet result = database.query("sql", "select bool_and(bool) as bool_and from doc6;");
      assertThat(result.hasNext()).isTrue();
      assertThat((Boolean) result.next().getProperty("bool_and")).isTrue();
    });
  }

  @Test
  public void testBoolAnd_MultiFalseHasNull() {
    database.transaction(() -> {
      database.command("sql", "create document type doc7;");
      database.command("sql", "create property doc7.bool boolean;");
      database.command("sql", "insert into doc7 (bool) values (true), (null), (false);");
      ResultSet result = database.query("sql", "select bool_and(bool) as bool_and from doc7;");
      assertThat(result.hasNext()).isTrue();
      assertThat((Boolean) result.next().getProperty("bool_and")).isFalse();
    });
  }

  @Test
  public void testBoolAnd_MultiNullIsNull() {
    database.transaction(() -> {
      database.command("sql", "create document type doc8;");
      database.command("sql", "create property doc8.bool boolean;");
      database.command("sql", "insert into doc8 (bool) values (null), (null), (null);");
      ResultSet result = database.query("sql", "select bool_and((bool is null)) as bool_and from doc8;");
      assertThat(result.hasNext()).isTrue();
      assertThat((Boolean) result.next().getProperty("bool_and")).isTrue();
    });
  }

  @Test
  public void testBoolAnd_MultiHasNull() {
    database.transaction(() -> {
      database.command("sql", "create document type doc9;");
      database.command("sql", "create property doc9.bool boolean;");
      database.command("sql", "insert into doc9 (bool) values (true), (null), (false);");
      ResultSet result = database.query("sql", "select bool_and((bool is not null)) as bool_and from doc9;");
      assertThat(result.hasNext()).isTrue();
      assertThat((Boolean) result.next().getProperty("bool_and")).isFalse();
    });
  }

  @Test
  public void testBoolAndNull() {
    database.transaction(() -> {
      ResultSet result = database.query("sql", "SELECT (true AND null) as result");
      assertThat(result.hasNext()).isTrue();
      assertThat(result.next().<Boolean>getProperty("result")).isNull();

      result = database.query("sql", "SELECT (false AND null) as result");
      assertThat(result.hasNext()).isTrue();
      assertThat((Boolean) result.next().getProperty("result")).isFalse();

      result = database.query("sql", "SELECT (null AND null) as result");
      assertThat(result.hasNext()).isTrue();
      assertThat(result.next().<Boolean>getProperty("result")).isNull();

      result = database.query("sql", "SELECT (true OR null) as result");
      assertThat(result.hasNext()).isTrue();
      assertThat((Boolean) result.next().getProperty("result")).isTrue();

      result = database.query("sql", "SELECT (false OR null) as result");
      assertThat(result.hasNext()).isTrue();
      assertThat(result.next().<Boolean>getProperty("result")).isNull();

      result = database.query("sql", "SELECT (null OR null) as result");
      assertThat(result.hasNext()).isTrue();
      assertThat(result.next().<Boolean>getProperty("result")).isNull();
    });
  }
}
