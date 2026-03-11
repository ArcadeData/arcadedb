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
package com.arcadedb.query.sql;

import com.arcadedb.TestHelper;
import com.arcadedb.query.sql.executor.Result;
import com.arcadedb.query.sql.executor.ResultSet;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Regression test for link dot-notation property access in SQL projections.
 * <p>
 * Verifies that projections like {@code CreatedBy.Email as _CreatedBy_Email} work correctly
 * when navigating LINK properties to access sub-properties on the linked record.
 * Also tests that null link values are handled gracefully without ClassCastException.
 * </p>
 */
class IssueLinkDotNotationProjectionTest extends TestHelper {

  @Test
  void testLinkDotNotationProjection() {
    database.transaction(() -> {
      database.command("sql", "CREATE DOCUMENT TYPE AppUser IF NOT EXISTS");
      database.command("sql", "CREATE VERTEX TYPE Product IF NOT EXISTS");
      database.command("sql", "CREATE PROPERTY AppUser.Email IF NOT EXISTS STRING");
      database.command("sql", "CREATE PROPERTY Product.Name IF NOT EXISTS STRING");
      database.command("sql", "CREATE PROPERTY Product.CreatedBy IF NOT EXISTS LINK OF AppUser");
      database.command("sql", "CREATE PROPERTY Product.Owner IF NOT EXISTS LINK OF AppUser");
      database.command("sql", "CREATE PROPERTY Product.ModifiedBy IF NOT EXISTS LINK OF AppUser");

      // Create users
      database.command("sql", "INSERT INTO AppUser SET Email = 'alice@example.com'");
      database.command("sql", "INSERT INTO AppUser SET Email = 'bob@example.com'");
      database.command("sql", "INSERT INTO AppUser SET Email = 'charlie@example.com'");

      // Create product with all three link properties
      database.command("sql",
          "INSERT INTO Product SET Name = 'TestProduct', "
              + "CreatedBy = (SELECT FROM AppUser WHERE Email = 'alice@example.com'), "
              + "Owner = (SELECT FROM AppUser WHERE Email = 'bob@example.com'), "
              + "ModifiedBy = (SELECT FROM AppUser WHERE Email = 'charlie@example.com')");

      // Test single dot-notation projection
      try (final ResultSet rs = database.query("sql",
          "SELECT CreatedBy.Email as _CreatedBy_Email FROM Product")) {
        assertThat(rs.hasNext()).isTrue();
        final Result row = rs.next();
        assertThat(row.<String>getProperty("_CreatedBy_Email")).isEqualTo("alice@example.com");
      }

      // Test multiple dot-notation projections (the pattern from the client's query)
      try (final ResultSet rs = database.query("sql",
          "SELECT CreatedBy.Email as _CreatedBy_Email, "
              + "ModifiedBy.Email as _ModifiedBy_Email, "
              + "Owner.Email as _Owner_Email FROM Product")) {
        assertThat(rs.hasNext()).isTrue();
        final Result row = rs.next();
        assertThat(row.<String>getProperty("_CreatedBy_Email")).isEqualTo("alice@example.com");
        assertThat(row.<String>getProperty("_ModifiedBy_Email")).isEqualTo("charlie@example.com");
        assertThat(row.<String>getProperty("_Owner_Email")).isEqualTo("bob@example.com");
      }
    });
  }

  @Test
  void testLinkDotNotationWithNullLink() {
    database.transaction(() -> {
      database.command("sql", "CREATE DOCUMENT TYPE AppUser2 IF NOT EXISTS");
      database.command("sql", "CREATE VERTEX TYPE Product2 IF NOT EXISTS");
      database.command("sql", "CREATE PROPERTY AppUser2.Email IF NOT EXISTS STRING");
      database.command("sql", "CREATE PROPERTY Product2.Name IF NOT EXISTS STRING");
      database.command("sql", "CREATE PROPERTY Product2.CreatedBy IF NOT EXISTS LINK OF AppUser2");

      // Create product WITHOUT setting CreatedBy (null link)
      database.command("sql", "INSERT INTO Product2 SET Name = 'TestProduct'");

      // Should return null for the projection, not throw ClassCastException
      try (final ResultSet rs = database.query("sql",
          "SELECT CreatedBy.Email as _CreatedBy_Email FROM Product2")) {
        assertThat(rs.hasNext()).isTrue();
        final Result row = rs.next();
        assertThat(row.<String>getProperty("_CreatedBy_Email")).isNull();
      }
    });
  }

  @Test
  void testLinkDotNotationWithVertexLink() {
    database.transaction(() -> {
      database.command("sql", "CREATE VERTEX TYPE UserVertex IF NOT EXISTS");
      database.command("sql", "CREATE VERTEX TYPE ProductVertex IF NOT EXISTS");
      database.command("sql", "CREATE PROPERTY UserVertex.Email IF NOT EXISTS STRING");
      database.command("sql", "CREATE PROPERTY ProductVertex.Name IF NOT EXISTS STRING");
      database.command("sql", "CREATE PROPERTY ProductVertex.CreatedBy IF NOT EXISTS LINK OF UserVertex");
      database.command("sql", "CREATE PROPERTY ProductVertex.Owner IF NOT EXISTS LINK OF UserVertex");

      // Create users as vertices
      database.command("sql", "INSERT INTO UserVertex SET Email = 'alice@test.com'");
      database.command("sql", "INSERT INTO UserVertex SET Email = 'bob@test.com'");

      // Create product with vertex links
      database.command("sql",
          "INSERT INTO ProductVertex SET Name = 'TestProduct', "
              + "CreatedBy = (SELECT FROM UserVertex WHERE Email = 'alice@test.com'), "
              + "Owner = (SELECT FROM UserVertex WHERE Email = 'bob@test.com')");

      // Test dot-notation projection with vertex links
      try (final ResultSet rs = database.query("sql",
          "SELECT CreatedBy.Email as _CreatedBy_Email, "
              + "Owner.Email as _Owner_Email FROM ProductVertex")) {
        assertThat(rs.hasNext()).isTrue();
        final Result row = rs.next();
        assertThat(row.<String>getProperty("_CreatedBy_Email")).isEqualTo("alice@test.com");
        assertThat(row.<String>getProperty("_Owner_Email")).isEqualTo("bob@test.com");
      }
    });
  }

  @Test
  void testLinkDotNotationInLetSubquery() {
    database.transaction(() -> {
      database.command("sql", "CREATE DOCUMENT TYPE AppUser3 IF NOT EXISTS");
      database.command("sql", "CREATE VERTEX TYPE Product3 IF NOT EXISTS");
      database.command("sql", "CREATE PROPERTY AppUser3.Email IF NOT EXISTS STRING");
      database.command("sql", "CREATE PROPERTY Product3.Name IF NOT EXISTS STRING");
      database.command("sql", "CREATE PROPERTY Product3.CreatedBy IF NOT EXISTS LINK OF AppUser3");
      database.command("sql", "CREATE PROPERTY Product3.Owner IF NOT EXISTS LINK OF AppUser3");
      database.command("sql", "CREATE PROPERTY Product3.ModifiedBy IF NOT EXISTS LINK OF AppUser3");

      // Create users
      database.command("sql", "INSERT INTO AppUser3 SET Email = 'alice@example.com'");
      database.command("sql", "INSERT INTO AppUser3 SET Email = 'bob@example.com'");

      // Create product
      database.command("sql",
          "INSERT INTO Product3 SET Name = 'TestProduct', "
              + "CreatedBy = (SELECT FROM AppUser3 WHERE Email = 'alice@example.com'), "
              + "Owner = (SELECT FROM AppUser3 WHERE Email = 'bob@example.com'), "
              + "ModifiedBy = (SELECT FROM AppUser3 WHERE Email = 'alice@example.com')");

      // Test with LET + UNIONALL pattern (mirrors client's actual query)
      try (final ResultSet rs = database.query("sql",
          "SELECT ($c) LET "
              + "$a = (SELECT count(Name) FROM Product3), "
              + "$b = (SELECT *, CreatedBy.Email as _CreatedBy_Email, "
              + "  ModifiedBy.Email as _ModifiedBy_Email, "
              + "  Owner.Email as _Owner_Email FROM Product3), "
              + "$c = UNIONALL($a, $b) LIMIT -1")) {
        assertThat(rs.hasNext()).isTrue();
      }
    });
  }

  @Test
  void testLinkDotNotationMixedNullAndNonNull() {
    database.transaction(() -> {
      database.command("sql", "CREATE DOCUMENT TYPE AppUser4 IF NOT EXISTS");
      database.command("sql", "CREATE VERTEX TYPE Product4 IF NOT EXISTS");
      database.command("sql", "CREATE PROPERTY AppUser4.Email IF NOT EXISTS STRING");
      database.command("sql", "CREATE PROPERTY Product4.Name IF NOT EXISTS STRING");
      database.command("sql", "CREATE PROPERTY Product4.CreatedBy IF NOT EXISTS LINK OF AppUser4");
      database.command("sql", "CREATE PROPERTY Product4.Owner IF NOT EXISTS LINK OF AppUser4");

      // Create user
      database.command("sql", "INSERT INTO AppUser4 SET Email = 'alice@example.com'");

      // Create product with CreatedBy set but Owner null
      database.command("sql",
          "INSERT INTO Product4 SET Name = 'TestProduct', "
              + "CreatedBy = (SELECT FROM AppUser4 WHERE Email = 'alice@example.com')");

      // Test mixed null/non-null links in same query - should NOT throw ClassCastException
      try (final ResultSet rs = database.query("sql",
          "SELECT CreatedBy.Email as _CreatedBy_Email, "
              + "Owner.Email as _Owner_Email FROM Product4")) {
        assertThat(rs.hasNext()).isTrue();
        final Result row = rs.next();
        assertThat(row.<String>getProperty("_CreatedBy_Email")).isEqualTo("alice@example.com");
        assertThat(row.<String>getProperty("_Owner_Email")).isNull();
      }
    });
  }
}
