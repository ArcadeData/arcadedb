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
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Test for GitHub issue #3813: SQL: UPDATE does not resolve expression/command resulting in RID
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
public class GitHubIssue3813Test extends TestHelper {

  public GitHubIssue3813Test() {
    autoStartTx = true;
  }

  @Override
  public void beginTest() {
    database.command("sql", "CREATE DOCUMENT TYPE Doc");
  }

  /**
   * UPDATE (INSERT INTO Doc RETURN @rid) SET b = 3
   * The INSERT subquery returns a projected result with @rid.
   * The UPDATE should resolve the RID and update the record.
   */
  @Test
  void updateSubqueryInsertReturnRid() {
    final ResultSet result = database.command("sql", "UPDATE (INSERT INTO Doc RETURN @rid) SET b = 3");
    assertThat(result.hasNext()).isTrue();
    assertThat((long) result.next().getProperty("count")).isEqualTo(1L);

    final ResultSet check = database.query("sql", "SELECT FROM Doc WHERE b = 3");
    assertThat(check.hasNext()).isTrue();
    assertThat((int) check.next().getProperty("b")).isEqualTo(3);
    assertThat(check.hasNext()).isFalse();
  }

  /**
   * UPDATE (INSERT INTO Doc) SET b = 3
   * The INSERT subquery returns the full element (no RETURN projection).
   */
  @Test
  void updateSubqueryInsertNoReturn() {
    final ResultSet result = database.command("sql", "UPDATE (INSERT INTO Doc) SET b = 3");
    assertThat(result.hasNext()).isTrue();
    assertThat((long) result.next().getProperty("count")).isEqualTo(1L);

    final ResultSet check = database.query("sql", "SELECT FROM Doc WHERE b = 3");
    assertThat(check.hasNext()).isTrue();
    assertThat((int) check.next().getProperty("b")).isEqualTo(3);
    assertThat(check.hasNext()).isFalse();
  }

  /**
   * SQLScript: LET + UPDATE with WHERE @rid = $x.@rid[0] (reported as working).
   */
  @Test
  void sqlScriptLetUpdateWhereRid() {
    final ResultSet result = database.command("sqlscript",
        "LET $x = INSERT INTO Doc RETURN @rid;\n" +
            "UPDATE Doc SET b = 3 WHERE @rid = $x.@rid[0];");
    assertThat(result.hasNext()).isTrue();
    assertThat((long) result.next().getProperty("count")).isEqualTo(1L);

    final ResultSet check = database.query("sql", "SELECT FROM Doc WHERE b = 3");
    assertThat(check.hasNext()).isTrue();
    assertThat((int) check.next().getProperty("b")).isEqualTo(3);
    assertThat(check.hasNext()).isFalse();
  }

  /**
   * SQLScript: LET + UPDATE $x.@rid[0] SET b = 3
   * Using the LET variable directly as the UPDATE target.
   */
  @Test
  void sqlScriptLetUpdateTarget() {
    final ResultSet result = database.command("sqlscript",
        "LET $x = INSERT INTO Doc RETURN @rid;\n" +
            "UPDATE $x.@rid[0] SET b = 3;");
    assertThat(result.hasNext()).isTrue();
    assertThat((long) result.next().getProperty("count")).isEqualTo(1L);

    final ResultSet check = database.query("sql", "SELECT FROM Doc WHERE b = 3");
    assertThat(check.hasNext()).isTrue();
    assertThat((int) check.next().getProperty("b")).isEqualTo(3);
    assertThat(check.hasNext()).isFalse();
  }

  /**
   * UPDATE Doc SET b = 3 WHERE @rid = (INSERT INTO Doc RETURN @rid).@rid[0]
   * The INSERT subquery in WHERE clause should resolve to a RID for comparison.
   */
  @Test
  void updateWhereRidEqualsSubqueryExpression() {
    final ResultSet result = database.command("sql",
        "UPDATE Doc SET b = 3 WHERE @rid = (INSERT INTO Doc RETURN @rid).@rid[0]");
    assertThat(result.hasNext()).isTrue();
    assertThat((long) result.next().getProperty("count")).isEqualTo(1L);

    final ResultSet check = database.query("sql", "SELECT FROM Doc WHERE b = 3");
    assertThat(check.hasNext()).isTrue();
    assertThat((int) check.next().getProperty("b")).isEqualTo(3);
    assertThat(check.hasNext()).isFalse();
  }

  /**
   * DELETE FROM Doc WHERE @rid = (INSERT INTO Doc RETURN @rid).@rid[0]
   * The INSERT subquery in WHERE clause should resolve to a RID for comparison in DELETE.
   */
  @Test
  void deleteWhereRidEqualsSubqueryExpression() {
    // First insert a record
    database.command("sql", "INSERT INTO Doc SET a = 1");
    final long countBefore = database.countType("Doc", true);

    // DELETE the record that was just inserted via subquery expression
    final ResultSet result = database.command("sql",
        "DELETE FROM Doc WHERE @rid = (INSERT INTO Doc RETURN @rid).@rid[0]");
    assertThat(result.hasNext()).isTrue();
    assertThat((long) result.next().getProperty("count")).isEqualTo(1L);

    // Count should be same as before (inserted 1 then deleted the new one)
    assertThat(database.countType("Doc", true)).isEqualTo(countBefore);
  }
}
