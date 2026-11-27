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
import com.arcadedb.exception.ValidationException;
import com.arcadedb.schema.DocumentType;
import com.arcadedb.schema.Type;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Issue #12932: {@code CREATE PROPERTY} with a LIST, MAP, EMBEDDED or LINK declaration (with or without {@code OF}) did not check
 * the values records already hold, yet the save path refuses a stored value of the wrong shape on every later write of the
 * record, even one that only touches another property. The invariant held here, for every declaration and stored value:
 * either {@code CREATE PROPERTY} refuses, or a later {@code UPDATE} of another property of that record succeeds. And a refusal
 * is only for a value the save path refuses too, checked by declaring the same property through the Java API, which does not
 * scan, and updating the record.
 */
class Issue12932CreatePropertyStoredCollectionTest extends TestHelper {
  private static final String[] DECLARATIONS = { "LIST", "MAP", "EMBEDDED", "EMBEDDED OF Addr", "LIST OF INTEGER",
      "LIST OF STRING", "LIST OF LONG", "LIST OF DOUBLE", "MAP OF INTEGER", "MAP OF LONG", "LINK", "LINK OF Addr",
      "LIST OF Addr" };

  private String[] storedValues;
  private String   addrLink;
  private String   otherLink;
  private int      typeCounter;

  @Override
  protected void beginTest() {
    database.command("sql", "CREATE DOCUMENT TYPE Addr");
    database.command("sql", "CREATE DOCUMENT TYPE Other");
    database.transaction(() -> {
      addrLink = database.newDocument("Addr").set("x", 1).save().getIdentity().toString();
      otherLink = database.newDocument("Other").set("x", 1).save().getIdentity().toString();
    });
    storedValues = new String[] { "10", "'abc'", "'12'", "[1, 2]", "['a', 'b']", "[1.5, 2.5]", "{'k': 1}", "{'k': 'v'}", "true",
        "1.5", "{'@type': 'Addr', 'x': 1}", "{'@type': 'Other', 'x': 1}", addrLink, otherLink, "[" + addrLink + "]",
        "[" + otherLink + "]" };
  }

  @Test
  void createPropertyRefusesExactlyWhatEverySaveWouldRefuse() {
    final List<String> violations = new ArrayList<>();
    for (final String declaration : DECLARATIONS)
      for (final String stored : storedValues) {
        final String typeName = newTypeWithRecord(stored);
        String refusal = null;
        try {
          database.command("sql", "CREATE PROPERTY " + typeName + ".p " + declaration);
        } catch (final RuntimeException e) {
          refusal = e.getMessage();
        }

        final String combination = declaration + " over " + stored;
        if (refusal == null) {
          final String updateFailure = updateOtherProperty(typeName);
          if (updateFailure != null)
            violations.add(combination + ": CREATE PROPERTY accepted, then every save refused: " + updateFailure);
        } else {
          if (!refusal.contains("but a stored value cannot be read as such"))
            violations.add(combination + ": refused with an unexpected message: " + refusal);
          if (saveAcceptsUnderDeclaration(stored, declaration))
            violations.add(combination + ": CREATE PROPERTY refused a value the save path accepts: " + refusal);
        }
      }
    assertThat(violations).isEmpty();
  }

  @Test
  void conformingStoredShapesAreStillAccepted() {
    assertAccepted("[1, 2]", "LIST");
    assertAccepted("['a', 'b']", "LIST");
    assertAccepted("{'k': 1}", "MAP");
    assertAccepted("{'@type': 'Addr', 'x': 1}", "EMBEDDED");
    assertAccepted("{'@type': 'Addr', 'x': 1}", "EMBEDDED OF Addr");
    assertAccepted("[1, 2]", "LIST OF INTEGER");
    assertAccepted("['a', 'b']", "LIST OF STRING");
    assertAccepted("{'k': 1}", "MAP OF INTEGER");
    // integers written before the declaration widen to LONG or DOUBLE without loss: accepted, and the record still saves
    assertAccepted("[1, 2, 3]", "LIST OF LONG");
    assertAccepted("[1, 2]", "LIST OF DOUBLE");
    assertAccepted("{'a': 1}", "MAP OF LONG");
    assertAccepted(addrLink, "LINK");
    assertAccepted(addrLink, "LINK OF Addr");
    assertAccepted("[" + addrLink + "]", "LIST OF Addr");
    // a plain LINK has never refused a scalar on save, so neither does the declaration
    assertAccepted("10", "LINK");
  }

  @Test
  void linkDeclarationsRefuseWhatEverySaveWouldRefuse() {
    // the save path used to cast the stored value to a link blindly: a ClassCastException on every later write of the record
    assertRefused("10", "LINK OF Addr", "is declared as LINK OF Addr but a stored value cannot be read as such: 10");
    assertRefused("'abc'", "LINK OF Addr", "is declared as LINK OF Addr but a stored value cannot be read as such: abc");
    assertRefused(otherLink, "LINK OF Addr", "is declared as LINK OF Addr but a stored value cannot be read as such: " + otherLink);
    assertRefused("{'@type': 'Addr', 'x': 1}", "LINK", "is declared as LINK but a stored value cannot be read as such");
    assertRefused("[" + otherLink + "]", "LIST OF Addr", "is declared as LIST OF Addr but a stored value cannot be read as such");
  }

  @Test
  void saveOfANonLinkUnderLinkOfIsAValidationErrorNotAClassCastException() {
    final String typeName = newTypeWithRecord("10");
    database.getSchema().getType(typeName).createProperty("p", Type.LINK, "Addr");
    assertThatThrownBy(() -> database.transaction(() -> database.command("sql", "UPDATE " + typeName + " SET z = 1")))
        .isInstanceOf(ValidationException.class)
        .hasMessageContaining("has been declared as LINK of 'Addr' but a value that is not a link is used");
  }

  @Test
  void nonConformingStoredShapesAreRefusedWithTheScalarMessage() {
    assertRefused("[1, 2]", "MAP", "is declared as MAP but a stored value cannot be read as such: [1, 2]");
    assertRefused("10", "LIST", "is declared as LIST but a stored value cannot be read as such: 10");
    assertRefused("'abc'", "EMBEDDED", "is declared as EMBEDDED but a stored value cannot be read as such: abc");
    assertRefused("[1, 2]", "LIST OF STRING", "is declared as LIST OF STRING but a stored value cannot be read as such: [1, 2]");
    assertRefused("10", "LIST OF INTEGER", "is declared as LIST OF INTEGER but a stored value cannot be read as such: 10");
    assertRefused("{'@type': 'Other', 'x': 1}", "EMBEDDED OF Addr",
        "is declared as EMBEDDED OF Addr but a stored value cannot be read as such");
    // no lossless widening: a string or a fraction is not an INTEGER
    assertRefused("['a', 'b']", "LIST OF INTEGER", "is declared as LIST OF INTEGER but a stored value cannot be read as such");
    assertRefused("[1.5, 2.5]", "LIST OF INTEGER", "is declared as LIST OF INTEGER but a stored value cannot be read as such");
    assertRefused("[1.5, 2.5]", "LIST OF LONG", "is declared as LIST OF LONG but a stored value cannot be read as such");
  }

  private void assertAccepted(final String stored, final String declaration) {
    final String typeName = newTypeWithRecord(stored);
    database.command("sql", "CREATE PROPERTY " + typeName + ".p " + declaration);
    assertThat(database.getSchema().getType(typeName).existsProperty("p")).isTrue();
    assertThat(updateOtherProperty(typeName)).as("%s over %s", declaration, stored).isNull();
  }

  private void assertRefused(final String stored, final String declaration, final String message) {
    final String typeName = newTypeWithRecord(stored);
    assertThatThrownBy(() -> database.command("sql", "CREATE PROPERTY " + typeName + ".p " + declaration))
        .hasMessageContaining(message);
    // the refused declaration is not left behind
    assertThat(database.getSchema().getType(typeName).existsProperty("p")).isFalse();
    assertThat(updateOtherProperty(typeName)).isNull();
  }

  /** Declares the property through the Java API, which does not scan the stored records, and tries to save the record. */
  private boolean saveAcceptsUnderDeclaration(final String stored, final String declaration) {
    final String typeName = newTypeWithRecord(stored);
    final DocumentType type = database.getSchema().getType(typeName);
    final int of = declaration.indexOf(" OF ");
    if (of < 0)
      type.createProperty("p", Type.valueOf(declaration));
    else
      type.createProperty("p", Type.valueOf(declaration.substring(0, of)), declaration.substring(of + 4));
    return updateOtherProperty(typeName) == null;
  }

  private String newTypeWithRecord(final String stored) {
    final String typeName = "T" + typeCounter++;
    database.command("sql", "CREATE DOCUMENT TYPE " + typeName);
    database.transaction(() -> database.command("sql", "INSERT INTO " + typeName + " SET p = " + stored));
    return typeName;
  }

  /** @return null when the record took the update, otherwise why it was refused */
  private String updateOtherProperty(final String typeName) {
    try {
      database.transaction(() -> database.command("sql", "UPDATE " + typeName + " SET z = 1"));
      return null;
    } catch (final RuntimeException e) {
      return e.getMessage();
    }
  }
}
