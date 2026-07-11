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
package com.arcadedb.gremlin;

import com.arcadedb.exception.CommandExecutionException;
import com.arcadedb.exception.CommandParsingException;
import com.arcadedb.utility.FileUtils;
import org.junit.jupiter.api.Test;

import java.io.File;
import java.util.NoSuchElementException;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Regression test for issue #5219: a runtime {@code NoSuchElementException} raised by an eager terminal step such
 * as {@code .next()} on an empty traversal was misclassified as a {@link CommandParsingException} (HTTP 400,
 * "Error on parsing gremlin query"). Since the syntax is valid, it must surface as a
 * {@link CommandExecutionException} instead. Genuine parse failures (issue #5201) must keep their
 * {@link CommandParsingException} classification.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class GremlinNextOnEmptyTest {
  @Test
  void nextOnEmptyTraversalIsExecutionErrorNotParsingError() {
    final ArcadeGraph graph = ArcadeGraph.open("./target/testgremlinnextempty");
    try {
      graph.getDatabase().getSchema().createVertexType("Bug");

      // Valid syntax, but the traversal yields no results so the eager .next() raises NoSuchElementException.
      assertThatThrownBy(() -> graph.gremlin("g.V().hasLabel('Bug').drop().next()").execute())
          .isInstanceOf(CommandExecutionException.class)
          .hasMessageContaining("Error on executing gremlin query")
          .hasRootCauseInstanceOf(NoSuchElementException.class);
    } finally {
      graph.drop();
      FileUtils.deleteRecursively(new File("./target/testgremlinnextempty"));
    }
  }

  @Test
  void dropIterateOnEmptyStillSucceeds() {
    final ArcadeGraph graph = ArcadeGraph.open("./target/testgremlinnextempty2");
    try {
      graph.getDatabase().getSchema().createVertexType("Bug");

      // .iterate() consumes the (empty) traversal without demanding an element: no error.
      assertThat(graph.gremlin("g.V().hasLabel('Bug').drop().iterate()").execute().hasNext()).isFalse();
    } finally {
      graph.drop();
      FileUtils.deleteRecursively(new File("./target/testgremlinnextempty2"));
    }
  }

  @Test
  void genuineSyntaxErrorStaysParsingError() {
    final ArcadeGraph graph = ArcadeGraph.open("./target/testgremlinnextempty3");
    try {
      graph.getDatabase().getSchema().createVertexType("Bug");

      // Invalid syntax must remain a parsing error (issue #5201 must not regress).
      assertThatThrownBy(() -> graph.gremlin("g.V(.hasLabel(").execute())
          .isInstanceOf(CommandParsingException.class)
          .hasMessageContaining("Error on parsing gremlin query");
    } finally {
      graph.drop();
      FileUtils.deleteRecursively(new File("./target/testgremlinnextempty3"));
    }
  }
}
