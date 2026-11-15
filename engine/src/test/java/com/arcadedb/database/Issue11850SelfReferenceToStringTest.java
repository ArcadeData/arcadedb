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
package com.arcadedb.database;

import com.arcadedb.TestHelper;
import com.arcadedb.exception.ValidationException;
import com.arcadedb.graph.MutableVertex;
import com.arcadedb.schema.Type;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Issue #11850: {@code MutableDocument.toString()} printed every property value, so a record that holds itself printed itself until
 * the stack overflowed. That reached clients through error messages that name the value: {@code INSERT INTO V SET m = @this} on a
 * MAP property failed its type conversion with a message built from the record, and answered HTTP 500 StackOverflowError instead
 * of the conversion error.
 */
class Issue11850SelfReferenceToStringTest extends TestHelper {

  @Test
  void aRecordHoldingItselfPrintsTheInnerOccurrenceShort() {
    database.getSchema().createVertexType("V");
    database.transaction(() -> {
      final MutableVertex vertex = database.newVertex("V");
      vertex.set("name", "a");
      vertex.set("self", vertex);
      vertex.set("list", List.of(1, vertex));

      final String printed = vertex.toString();

      assertThat(printed).startsWith("@V[").contains("name=a").contains("self=@V[...]").contains("list=[1, @V[...]]");
    });
  }

  @Test
  void aSavedRecordHoldingItselfPrintsItsRidInside() {
    database.getSchema().createDocumentType("D");
    database.transaction(() -> {
      final MutableDocument document = database.newDocument("D").set("name", "a").save();
      document.set("self", document);

      final String rid = document.getIdentity().toString();
      assertThat(document.toString()).isEqualTo(rid + "@D[name=a,self=" + rid + "@D[...]]");
    });
  }

  @Test
  void aValidationErrorOnASelfReferenceIsTheValidationError() {
    database.getSchema().createVertexType("V").createProperty("m", Type.MAP);

    // before the fix: DatabaseOperationException "Error during read lock" caused by StackOverflowError
    assertThatThrownBy(() -> database.transaction(() -> database.command("sql", "INSERT INTO V SET m = @this")))
        .isInstanceOf(ValidationException.class)
        .hasMessageContaining("declared as MAP")
        .hasMessageContaining("Value: @V[m=@V[...]]");
  }
}
