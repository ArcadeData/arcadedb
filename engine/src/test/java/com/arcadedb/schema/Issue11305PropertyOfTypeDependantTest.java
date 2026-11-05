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
package com.arcadedb.schema;

import com.arcadedb.TestHelper;
import com.arcadedb.database.Document;
import com.arcadedb.database.MutableDocument;
import com.arcadedb.database.RID;
import com.arcadedb.exception.SchemaException;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Issue #11305: a property declared {@code EMBEDDED}, {@code LINK}, {@code LIST} or {@code MAP OF} a type stores the NAME of that
 * type, and {@code setOfType} refuses a name that resolves to no type. Nothing maintained it afterwards: {@code DROP TYPE} let the
 * type go, and {@code ALTER TYPE ... NAME} renamed it without touching the property, so every write of the property failed with
 * "Type with name 'Address' was not found" and every read of a record holding one dropped the field. Such a property is now a
 * {@link TypeNameDependant}, in the one list the drop guard and the rename carry walk.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class Issue11305PropertyOfTypeDependantTest extends TestHelper {

  @Override
  protected void beginTest() {
    database.command("sql", "CREATE DOCUMENT TYPE Address");
    database.command("sql", "CREATE PROPERTY Address.city STRING");
    database.command("sql", "CREATE DOCUMENT TYPE Person");
    database.command("sql", "CREATE PROPERTY Person.addr EMBEDDED OF Address");
    database.command("sql", "CREATE PROPERTY Person.others LIST OF Address");
    database.command("sql", "CREATE PROPERTY Person.byName MAP OF Address");
    database.command("sql", "CREATE PROPERTY Person.home LINK OF Address");
    database.command("sql", "CREATE PROPERTY Person.tags LIST OF STRING");
  }

  @Test
  void dropTypeIsRefusedWhileAPropertyIsDeclaredOfIt() {
    assertThatThrownBy(() -> database.command("sql", "DROP TYPE Address")).isInstanceOf(SchemaException.class)
        .hasMessageContaining("Person.")
        .hasMessageContaining("DROP PROPERTY Person.");
    assertThat(database.getSchema().existsType("Address")).isTrue();

    // EACH KIND OF DECLARATION HOLDS THE TYPE ON ITS OWN
    for (final String property : List.of("addr", "others", "byName", "home")) {
      assertThatThrownBy(() -> database.getSchema().dropType("Address")).isInstanceOf(SchemaException.class)
          .hasMessageContaining("Person.");
      database.command("sql", "DROP PROPERTY Person." + property);
    }

    // THE LAST DECLARATION GONE, THE TYPE CAN GO
    database.command("sql", "DROP TYPE Address");
    assertThat(database.getSchema().existsType("Address")).isFalse();
  }

  @Test
  void dropTypeByAnAliasIsRefusedToo() {
    database.getSchema().getType("Address").setAliases(Set.of("Addr"));

    assertThatThrownBy(() -> database.getSchema().dropType("Addr")).isInstanceOf(SchemaException.class)
        .hasMessageContaining("Person.");
    assertThat(database.getSchema().existsType("Address")).isTrue();
  }

  @Test
  void aPropertyDeclaredByAnAliasHoldsTheTypeAndSurvivesItsRename() {
    database.getSchema().getType("Address").setAliases(Set.of("Addr"));
    database.command("sql", "CREATE PROPERTY Person.work EMBEDDED OF Addr");
    for (final String property : List.of("addr", "others", "byName", "home"))
      database.command("sql", "DROP PROPERTY Person." + property);

    assertThatThrownBy(() -> database.getSchema().dropType("Address")).isInstanceOf(SchemaException.class)
        .hasMessageContaining("Person.work");

    // THE ALIAS STAYS WITH THE TYPE ACROSS A RENAME, SO THE NAME THE PROPERTY STORES KEEPS RESOLVING
    database.command("sql", "ALTER TYPE Address NAME Address2");
    assertThat(database.getSchema().getType("Person").getProperty("work").getOfType()).isEqualTo("Addr");
    database.transaction(() -> database.newDocument("Person").set("work", Map.of("city", "Rome")).save());
  }

  @Test
  void aTypeWhoseOwnPropertyIsDeclaredOfItCanBeDropped() {
    database.command("sql", "CREATE DOCUMENT TYPE Node");
    database.command("sql", "CREATE PROPERTY Node.parent LINK OF Node");
    database.command("sql", "CREATE PROPERTY Node.children LIST OF Node");

    database.command("sql", "DROP TYPE Node");
    assertThat(database.getSchema().existsType("Node")).isFalse();
  }

  @Test
  void aScalarListDoesNotHoldAType() {
    for (final String property : List.of("addr", "others", "byName", "home"))
      database.command("sql", "DROP PROPERTY Person." + property);

    database.command("sql", "DROP TYPE Address");
    assertThat(database.getSchema().getType("Person").getProperty("tags").getOfType()).isEqualTo("STRING");
  }

  @Test
  void renameTypeCarriesEveryPropertyDeclaredOfIt() {
    final RID[] target = new RID[1];
    database.transaction(() -> {
      final MutableDocument address = database.newDocument("Address").set("city", "Paris");
      address.save();
      target[0] = address.getIdentity();
    });

    database.command("sql", "ALTER TYPE Address NAME Address2");

    assertCarried();

    // EVERY KIND OF PROPERTY STILL TAKES A WRITE: THEY USED TO THROW "Type with name 'Address' was not found"
    final RID[] written = new RID[1];
    database.transaction(() -> {
      written[0] = database.command("sql", "INSERT INTO Person CONTENT { \"addr\": { \"city\": \"Rome\" }, "
          + "\"others\": [ { \"city\": \"Milan\" } ], \"byName\": { \"y\": { \"city\": \"Naples\" } } }").next().getIdentity().get();
      database.newDocument("Person").set("home", database.lookupByRID(target[0], true)).save();
    });
    final Document person = database.lookupByRID(written[0], true).asDocument();
    assertThat(person.getEmbedded("addr").getTypeName()).isEqualTo("Address2");
    assertThat(person.getEmbedded("addr").getString("city")).isEqualTo("Rome");
    assertThat(person.getList("others")).hasSize(1);
    assertThat(person.getMap("byName")).containsKey("y");

    // THE DROP GUARD FOLLOWS THE NEW NAME
    assertThatThrownBy(() -> database.getSchema().dropType("Address2")).isInstanceOf(SchemaException.class)
        .hasMessageContaining("Person.");

    // AND THE CARRIED NAME IS THE ONE PERSISTED
    reopenDatabase();
    assertCarried();
  }

  /**
   * An embedded document stores its type as the dictionary id of the type's name, which only the creation of a type registered:
   * after a rename no embedded document of the type could be written, declared OF it or not.
   */
  @Test
  void aRenamedTypeTakesNewEmbeddedDocuments() {
    database.command("sql", "CREATE PROPERTY Person.anything EMBEDDED");
    database.command("sql", "ALTER TYPE Address NAME Address2");

    final RID[] written = new RID[1];
    database.transaction(() -> written[0] = database.command("sql",
        "INSERT INTO Person CONTENT { \"anything\": { \"@type\": \"Address2\", \"city\": \"Rome\" } }").next().getIdentity().get());

    reopenDatabase();
    final Document person = database.lookupByRID(written[0], true).asDocument();
    assertThat(person.getEmbedded("anything").getTypeName()).isEqualTo("Address2");
    assertThat(person.getEmbedded("anything").getString("city")).isEqualTo("Rome");
  }

  private void assertCarried() {
    final DocumentType person = database.getSchema().getType("Person");
    for (final String property : List.of("addr", "others", "byName", "home"))
      assertThat(person.getProperty(property).getOfType()).as("Person.%s", property).isEqualTo("Address2");
    assertThat(person.getProperty("tags").getOfType()).isEqualTo("STRING");
  }
}
