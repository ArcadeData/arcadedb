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
package com.arcadedb.query.opencypher;

import com.arcadedb.TestHelper;
import com.arcadedb.exception.CommandExecutionException;
import com.arcadedb.query.sql.executor.ResultSet;
import com.arcadedb.schema.Property;
import com.arcadedb.schema.Type;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Issue #9991: a type constraint replaces the declared type of a property, and {@code DROP CONSTRAINT} puts the previous
 * declaration back through the same checks the retype went through. Those checks refused a REGEXP on any type but a STRING
 * with a hand-written rule, so a LONG carrying {@code '[0-9]+'}, retyped to STRING by a constraint, could never be put back:
 * the constraint could be created but not dropped, {@code IF EXISTS} included. The REGEXP now goes through the check the
 * declaration itself is held to, so whatever the schema accepted is what the drop restores.
 * <p>
 * The guarantee is the round trip below, over a matrix of declared types and constraint types: a constraint that could be
 * created can be dropped, and the drop gives back the declaration it found.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class Issue9991DropConstraintRestoresRegexpTest extends TestHelper {

  @Test
  void theConstraintOfTheIssueCanBeDropped() {
    database.command("sql", "CREATE VERTEX TYPE R");
    database.command("sql", "CREATE PROPERTY R.code LONG (regexp '[0-9]+')");
    database.command("opencypher", "CREATE CONSTRAINT rc FOR (n:R) REQUIRE n.code IS :: STRING").close();
    assertThat(property("R", "code").getType()).isEqualTo(Type.STRING);

    database.command("opencypher", "DROP CONSTRAINT rc").close();
    assertThat(property("R", "code").getType()).isEqualTo(Type.LONG);
    assertThat(property("R", "code").getRegexp()).isEqualTo("[0-9]+");
    assertThat(constraintNames()).doesNotContain("rc");

    // and so with IF EXISTS
    database.command("opencypher", "CREATE CONSTRAINT rc FOR (n:R) REQUIRE n.code IS :: STRING").close();
    database.command("opencypher", "DROP CONSTRAINT rc IF EXISTS").close();
    assertThat(property("R", "code").getType()).isEqualTo(Type.LONG);
    assertThat(constraintNames()).doesNotContain("rc");
  }

  @Test
  void aRegexpNoValueOfTheNewTypeMatchesIsStillRefusedByTheRetype() {
    database.command("sql", "CREATE VERTEX TYPE B");
    database.command("sql", "CREATE PROPERTY B.code STRING (regexp '[A-Z]+')");
    assertThatThrownBy(() -> database.command("opencypher", "CREATE CONSTRAINT FOR (n:B) REQUIRE n.code IS :: INTEGER"))
        .isInstanceOf(CommandExecutionException.class).hasMessageContaining("REGEXP '[A-Z]+'")
        .hasMessageContaining("matches no text a value of LONG");
    assertThat(property("B", "code").getType()).isEqualTo(Type.STRING);

    // one some value of the new type matches is carried, both ways
    database.command("sql", "CREATE PROPERTY B.zip STRING (regexp '[0-9]{5}')");
    database.command("opencypher", "CREATE CONSTRAINT bz FOR (n:B) REQUIRE n.zip IS :: INTEGER").close();
    assertThat(property("B", "zip").getType()).isEqualTo(Type.LONG);
    assertThat(property("B", "zip").getRegexp()).isEqualTo("[0-9]{5}");
    database.command("opencypher", "DROP CONSTRAINT bz").close();
    assertThat(property("B", "zip").getType()).isEqualTo(Type.STRING);
    assertThat(property("B", "zip").getRegexp()).isEqualTo("[0-9]{5}");
  }

  @Test
  void everyConstraintThatCanBeCreatedCanBeDropped() {
    // declared type, then attributes the type accepts: a pattern some value of it matches, bounds where it has an order
    final List<String> declarations = List.of(//
        "LONG (regexp '[0-9]+')", "LONG (regexp '-?[0-9]{1,5}', min 0, max 99999)", "INTEGER (regexp '.*')",
        "DOUBLE (regexp '[0-9]+\\\\.[0-9]+', min 0)", "DECIMAL (regexp '[0-9]+')", "STRING (regexp '[0-9]+', max 10)",
        "STRING (regexp '[A-Za-z]+')", "STRING (regexp 'true|false')", "BOOLEAN (regexp 'true')",
        "DATE (regexp '[0-9]{4}-.*')", "DATETIME (regexp '.*T.*')", "STRING (regexp '[0-9]{4}-[0-9]{2}-[0-9]{2}')");
    final List<String> cypherTypes = List.of("STRING", "INTEGER", "FLOAT", "BOOLEAN", "DATE", "LOCAL DATETIME",
        "ZONED DATETIME", "DURATION");

    int created = 0;
    final List<String> notDroppable = new ArrayList<>();
    for (int d = 0; d < declarations.size(); d++)
      for (int c = 0; c < cypherTypes.size(); c++) {
        final String typeName = "M" + d + "_" + c;
        database.command("sql", "CREATE VERTEX TYPE " + typeName);
        database.command("sql", "CREATE PROPERTY " + typeName + ".p " + declarations.get(d));
        final Property before = property(typeName, "p");
        final Type declaredType = before.getType();
        final String regexp = before.getRegexp();
        final String min = before.getMin();
        final String max = before.getMax();

        final String constraint = "k" + d + "_" + c;
        try {
          database.command("opencypher",
              "CREATE CONSTRAINT " + constraint + " FOR (n:" + typeName + ") REQUIRE n.p IS :: " + cypherTypes.get(c)).close();
        } catch (final RuntimeException refused) {
          // a combination the retype refuses never exists, so there is nothing to drop
          continue;
        }
        ++created;

        try {
          database.command("opencypher", "DROP CONSTRAINT " + constraint).close();
        } catch (final RuntimeException e) {
          notDroppable.add(declarations.get(d) + " IS :: " + cypherTypes.get(c) + ": " + e.getMessage());
          continue;
        }
        final Property after = property(typeName, "p");
        assertThat(after.getType()).as("%s IS :: %s", declarations.get(d), cypherTypes.get(c)).isEqualTo(declaredType);
        assertThat(after.getRegexp()).as("%s IS :: %s", declarations.get(d), cypherTypes.get(c)).isEqualTo(regexp);
        assertThat(after.getMin()).as("%s IS :: %s", declarations.get(d), cypherTypes.get(c)).isEqualTo(min);
        assertThat(after.getMax()).as("%s IS :: %s", declarations.get(d), cypherTypes.get(c)).isEqualTo(max);
        assertThat(constraintNames()).doesNotContain(constraint);
      }

    assertThat(notDroppable).as("constraints that could be created but not dropped").isEmpty();
    // the matrix exercises the round trip, not only refusals
    assertThat(created).isGreaterThan(10);
  }

  private Property property(final String type, final String name) {
    return database.getSchema().getType(type).getProperty(name);
  }

  private List<String> constraintNames() {
    final List<String> names = new ArrayList<>();
    try (final ResultSet rs = database.query("opencypher", "SHOW CONSTRAINTS")) {
      while (rs.hasNext())
        names.add(rs.next().getProperty("name"));
    }
    return names;
  }
}
