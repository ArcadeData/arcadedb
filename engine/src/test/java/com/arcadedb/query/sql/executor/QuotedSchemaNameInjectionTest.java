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
 * A schema object name may contain a back-tick or a backslash. Any caller that embeds such a name into a SQL command has to escape
 * both characters, so that the quoted identifier ends where it is meant to end and cannot absorb the rest of the command. This
 * covers the escaping that the Studio applies before it addresses a type, bucket or index by name.
 */
class QuotedSchemaNameInjectionTest extends TestHelper {

  /**
   * The reference implementation of the escaping, mirroring {@code quoteSqlName()} in studio-utils.js.
   */
  private static String quoteSqlName(final String name) {
    final StringBuilder buffer = new StringBuilder(name.length() + 2).append('`');
    for (int i = 0; i < name.length(); i++) {
      final char c = name.charAt(i);
      if (c == '`' || c == '\\')
        buffer.append('\\');
      buffer.append(c);
    }
    return buffer.append('`').toString();
  }

  private void assertNameAddressableAndIsolated(final String typeName) {
    database.getSchema().createDocumentType(typeName);
    database.transaction(() -> database.newDocument(typeName).set("tag", "kept").save());

    final String quoted = quoteSqlName(typeName);

    final ResultSet result = database.query("sql", "SELECT FROM " + quoted);
    assertThat(result.stream().count()).isEqualTo(1);

    // the identifier must not swallow the clause that follows it: the WHERE has to be applied, not absorbed into the name
    final ResultSet filtered = database.query("sql", "SELECT FROM " + quoted + " WHERE `tag` = 'absent'");
    assertThat(filtered.stream().count()).isZero();
  }

  @Test
  void nameWithBackTickIsAddressable() {
    assertNameAddressableAndIsolated("Back`Tick");
  }

  @Test
  void nameEndingWithBackslashIsAddressable() {
    assertNameAddressableAndIsolated("Trailing\\");
  }

  @Test
  void nameWithInnerBackslashIsAddressable() {
    assertNameAddressableAndIsolated("Inner\\Slash");
  }

  @Test
  void nameWithBackslashBeforeBackTickIsAddressable() {
    assertNameAddressableAndIsolated("Mixed\\`Name");
  }

  /**
   * A DDL statement names the object it creates, so the name has to survive the round-trip through the statement rather than being
   * silently altered on the way in: an unescaped backslash would make {@code CREATE DOCUMENT TYPE `My\Type`} create MyType.
   */
  @Test
  void createdTypeKeepsTheNameItWasGiven() {
    final String typeName = "My\\Type";

    database.command("sql", "CREATE DOCUMENT TYPE " + quoteSqlName(typeName));

    assertThat(database.getSchema().existsType(typeName)).isTrue();
    assertThat(database.getSchema().existsType("MyType")).isFalse();
  }

  @Test
  void injectionAttemptInNameIsTreatedAsPlainName() {
    // a name crafted to close the identifier early and append a command must survive as a single, literal type name
    final String hostile = "Evil` ; DROP TYPE Victim; --";
    database.getSchema().createDocumentType("Victim");
    database.getSchema().createDocumentType(hostile);

    final ResultSet result = database.query("sql", "SELECT FROM " + quoteSqlName(hostile));

    assertThat(result.stream().count()).isZero();
    assertThat(database.getSchema().existsType("Victim")).isTrue();
    assertThat(database.getSchema().existsType(hostile)).isTrue();
  }
}
