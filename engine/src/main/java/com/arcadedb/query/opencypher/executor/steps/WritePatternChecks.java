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
package com.arcadedb.query.opencypher.executor.steps;

import com.arcadedb.database.Identifiable;
import com.arcadedb.database.Record;
import com.arcadedb.exception.CommandExecutionException;
import com.arcadedb.function.cypher.CypherFunctionHelper;
import com.arcadedb.graph.Vertex;
import com.arcadedb.query.opencypher.executor.DeletedEntityMarker;
import com.arcadedb.query.opencypher.parser.CypherSemanticValidator;
import com.arcadedb.query.sql.executor.Result;

/**
 * The checks CREATE and MERGE share on the values a write pattern is given at run time (issue #10191), with the outcomes
 * Neo4j has for them.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
final class WritePatternChecks {
  private WritePatternChecks() {
  }

  /**
   * The node an earlier clause bound {@code variable} to, for a pattern that connects it: {@code null} when the row does
   * not bind the variable at all, so the pattern introduces the node.
   *
   * @throws CommandExecutionException when the variable is bound to null - an OPTIONAL MATCH that found nothing - or to a
   *                                   value that is not a node: a relationship cannot have a missing endpoint, and a node
   *                                   created in its place is one the statement never asked for
   */
  static Vertex boundEndpoint(final Result row, final String variable) {
    if (variable == null || !row.hasProperty(variable))
      return null;
    final Object bound = row.getProperty(variable);
    if (bound instanceof Vertex vertex)
      return vertex;
    // A bare RID (or another handle) is resolved to its record; a record that is not a vertex is refused below
    if (bound instanceof Identifiable identifiable && !(bound instanceof Record)) {
      final Record record = identifiable.getRecord();
      if (record instanceof Vertex vertex)
        return vertex;
    }
    if (bound == null)
      throw new CommandExecutionException(
          "Failed to create the relationship: node `" + variable + "` is missing. Expected the value NULL to be of type NODE, but was of type NULL");
    // An earlier DELETE replaced the node with its marker: say so, as Neo4j does, rather than naming the marker (issue #10196)
    if (bound instanceof DeletedEntityMarker)
      throw new CommandExecutionException(
          "DeletedEntityAccess: Failed to create the relationship: node `" + variable + "` was deleted in this transaction");
    throw new CommandExecutionException(
        "Invalid input '" + CypherFunctionHelper.cypherTypeName(bound) + "' for " + variable + ". Expected to be NODE");
  }

  /** Refuses a relationship variable an earlier clause bound to null, which a pattern cannot connect either. */
  static void checkRelationshipNotNull(final Result row, final String variable) {
    if (variable != null && row.hasProperty(variable) && row.getProperty(variable) == null)
      throw new CommandExecutionException(
          "Failed to merge the relationship: `" + variable + "` is missing. Expected the value NULL to be of type RELATIONSHIP, but was of type NULL");
  }

  /**
   * Refuses a null inline property of a MERGE pattern, whatever produced it: a variable, a parameter, a property that is
   * missing. Nothing can match a null - it is not equal to anything - so the MERGE would create the element every time,
   * without the property, and a second row with the same null would create a second one.
   */
  static void checkMergePropertyNotNull(final String key, final Object value) {
    if (value == null)
      throw new CommandExecutionException(CypherSemanticValidator.NULL_MERGE_PROPERTY + " (property `" + key + "`)");
  }
}
