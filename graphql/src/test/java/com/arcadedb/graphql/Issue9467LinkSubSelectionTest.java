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
package com.arcadedb.graphql;

import com.arcadedb.database.Database;
import com.arcadedb.database.MutableDocument;
import com.arcadedb.query.sql.executor.Result;
import com.arcadedb.query.sql.executor.ResultSet;
import com.arcadedb.schema.Type;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.function.Consumer;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Regression test for issue #9467: a sub-selection on a LINK property, or on a list of links, silently dropped the field
 * instead of resolving the linked record(s).
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class Issue9467LinkSubSelectionTest extends AbstractGraphQLTest {

  private static final String SDL = """
      type Query { docs: [DocView] @sql(statement: "SELECT FROM Doc") }
      type DocView { id: String owner: AddressView links: [AddressView] }
      type AddressView { city: String }""";

  @Test
  void linkWithSubSelectionResolvesTheLinkedRecord() {
    executeLinkTest(database -> {
      try (final ResultSet resultSet = database.query("graphql", "{ docs { id owner { city } } }")) {
        final Result doc = resultSet.next();
        assertThat(doc.<String>getProperty("id")).isEqualTo("d1");
        final Result owner = doc.getProperty("owner");
        assertThat(owner).isNotNull();
        assertThat(owner.<String>getProperty("city")).isEqualTo("Rome");
      }
    });
  }

  @Test
  void listOfLinksWithSubSelectionResolvesEveryLinkedRecord() {
    executeLinkTest(database -> {
      try (final ResultSet resultSet = database.query("graphql", "{ docs { links { city } } }")) {
        final List<Result> links = resultSet.next().getProperty("links");
        assertThat(links).hasSize(2);
        assertThat(links.stream().map(r -> r.<String>getProperty("city")).toList()).containsExactly("Rome", "Milan");
      }
    });
  }

  @Test
  void linkWithoutSubSelectionStillReturnsTheRid() {
    executeLinkTest(database -> {
      try (final ResultSet resultSet = database.query("graphql", "{ docs { id } }")) {
        assertThat(resultSet.next().<String>getProperty("id")).isEqualTo("d1");
      }
    });
  }

  @Test
  void linkWithoutExplicitSubSelectionExpandsBySchemaType() {
    executeLinkTest(database -> {
      try (final ResultSet resultSet = database.query("graphql", "{ docs { owner { __typename city } } }")) {
        final Result owner = resultSet.next().getProperty("owner");
        assertThat(owner.<String>getProperty("__typename")).isEqualTo("AddressView");
        assertThat(owner.<String>getProperty("city")).isEqualTo("Rome");
      }
    });
  }

  @Test
  void danglingLinkResolvesToNothingAndIsDroppedFromAList() {
    executeLinkTest(database -> {
      database.command("sql", "DELETE FROM LinkedAddress WHERE city = 'Milan'");
      try (final ResultSet resultSet = database.query("graphql", "{ docs { owner { city } links { city } } }")) {
        final Result doc = resultSet.next();
        assertThat(doc.<Result>getProperty("owner").<String>getProperty("city")).isEqualTo("Rome");
        final List<Result> links = doc.getProperty("links");
        assertThat(links).hasSize(1);
        assertThat(links.getFirst().<String>getProperty("city")).isEqualTo("Rome");
      }
      database.command("sql", "DELETE FROM LinkedAddress");
      try (final ResultSet resultSet = database.query("graphql", "{ docs { id owner { city } links { city } } }")) {
        final Result doc = resultSet.next();
        assertThat(doc.<String>getProperty("id")).isEqualTo("d1");
        assertThat(doc.<List<Result>>getProperty("links")).isEmpty();
      }
    });
  }

  @Test
  void linkToLinkIsResolvedTwoLevelsDeep() {
    executeLinkTest(database -> {
      database.getSchema().getType("LinkedAddress").createProperty("next", Type.LINK, "LinkedAddress");
      database.command("sql", "UPDATE LinkedAddress SET next = (SELECT FROM LinkedAddress WHERE city = 'Milan') WHERE city = 'Rome'");
      database.command("graphql", "type AddressView { city: String next: AddressView }");
      try (final ResultSet resultSet = database.query("graphql", "{ docs { owner { city next { city } } } }")) {
        final Result owner = resultSet.next().getProperty("owner");
        assertThat(owner.<Result>getProperty("next").<String>getProperty("city")).isEqualTo("Milan");
      }
    });
  }

  private void executeLinkTest(final Consumer<Database> assertions) {
    executeTest(database -> {
      database.getSchema().getOrCreateDocumentType("Doc");
      database.getSchema().getOrCreateDocumentType("LinkedAddress");
      database.getSchema().getType("Doc").createProperty("owner", Type.LINK, "LinkedAddress");
      database.getSchema().getType("Doc").createProperty("links", Type.LIST, "LinkedAddress");

      final MutableDocument rome = database.newDocument("LinkedAddress").set("city", "Rome").save();
      final MutableDocument milan = database.newDocument("LinkedAddress").set("city", "Milan").save();
      database.newDocument("Doc").set("id", "d1").set("owner", rome.getIdentity())
          .set("links", List.of(rome.getIdentity(), milan.getIdentity())).save();

      database.command("graphql", SDL);
      assertions.accept(database);
      return null;
    });
  }
}
