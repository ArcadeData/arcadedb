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
package com.arcadedb.query.opencypher.parser;

import com.arcadedb.exception.CommandParsingException;
import com.arcadedb.query.opencypher.ast.*;
import java.util.*;

/**
 * The phase of the semantic validation that checks what the writing clauses are handed: the patterns a CREATE or a
 * MERGE may be written with, the null property values a MERGE refuses, the targets of a DELETE, and a relationship
 * variable used twice in one pattern.
 * <p>
 * Split out of {@link CypherSemanticValidator}, which runs the phases of the validation in order.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
final class WriteClauseValidator {
  private WriteClauseValidator() {
  }

  static void validateCreateConstraints(final CypherStatement statement) {
    final List<ClauseEntry> clausesInOrder = statement.getClausesInOrder();
    if (clausesInOrder != null && !clausesInOrder.isEmpty()) {
      for (final ClauseEntry entry : clausesInOrder) {
        if (entry.getType() == ClauseEntry.ClauseType.CREATE) {
          final CreateClause createClause = entry.getTypedClause();
          if (createClause != null && !createClause.isEmpty())
            for (final PathPattern path : createClause.getPathPatterns())
              validateCreatePathPattern(path);
        } else if (entry.getType() == ClauseEntry.ClauseType.MERGE) {
          final MergeClause mergeClause = entry.getTypedClause();
          if (mergeClause != null)
            validateMergePathPattern(mergeClause.getPathPattern());
        } else if (entry.getType() == ClauseEntry.ClauseType.DELETE) {
          final DeleteClause deleteClause = entry.getTypedClause();
          if (deleteClause != null && !deleteClause.isEmpty())
            validateDeleteTargets(deleteClause);
        }
      }
    } else {
      // Fallback: check statement-level clauses
      if (statement.getCreateClause() != null && !statement.getCreateClause().isEmpty())
        for (final PathPattern path : statement.getCreateClause().getPathPatterns())
          validateCreatePathPattern(path);

      if (statement.getMergeClause() != null)
        validateMergePathPattern(statement.getMergeClause().getPathPattern());

      if (statement.getDeleteClause() != null && !statement.getDeleteClause().isEmpty())
        validateDeleteTargets(statement.getDeleteClause());
    }
  }

  private static void validateCreatePathPattern(final PathPattern path) {
    checkNoLabelDisjunction(path, "CREATE");
    for (final RelationshipPattern rel : path.getRelationships()) {
      // CREATE relationships must specify exactly one type
      if (!rel.hasTypes())
        throw new CommandParsingException("NoSingleRelationshipType: Relationships must have a type in CREATE");
      // CREATE relationships must not have multiple types
      if (rel.getTypes().size() > 1)
        throw new CommandParsingException("NoSingleRelationshipType: Relationships must have exactly one type in CREATE, got: " + rel.getTypes());
      // CREATE relationships must be directed
      if (rel.getDirection() == Direction.BOTH)
        throw new CommandParsingException("RequiresDirectedRelationship: Relationships must be directed in CREATE");
      // CREATE cannot use variable-length patterns
      if (rel.isVariableLength())
        throw new CommandParsingException("CreatingVarLength: Variable-length relationships are not allowed in CREATE");
    }
  }

  private static void validateMergePathPattern(final PathPattern path) {
    checkNoLabelDisjunction(path, "MERGE");
    for (final RelationshipPattern rel : path.getRelationships()) {
      // MERGE relationships must specify exactly one type
      if (!rel.hasTypes())
        throw new CommandParsingException("NoSingleRelationshipType: Relationships must have a type in MERGE");
      // MERGE relationships must not have multiple types
      if (rel.getTypes().size() > 1)
        throw new CommandParsingException("NoSingleRelationshipType: Relationships must have exactly one type in MERGE, got: " + rel.getTypes());
      // MERGE cannot use variable-length patterns
      if (rel.isVariableLength())
        throw new CommandParsingException("CreatingVarLength: Variable-length relationships are not allowed in MERGE");
    }
    // MERGE cannot have null property values
    checkMergeNullProperties(path);
  }

  /**
   * A label disjunction says which labels a node may have, which is a question only a read can answer: there is no
   * node a write could produce that satisfies "A or B" without picking one. Neo4j refuses a label expression in
   * CREATE and MERGE for that reason, and so does this - it used to be accepted and then quietly given conjunction
   * meaning, which made {@code MERGE (n:A|B {k:'x'})} miss an existing {@code :B} node and create a second one under
   * an invented {@code A~B} composite type instead of matching it (issue #6338).
   */
  private static void checkNoLabelDisjunction(final PathPattern path, final String clause) {
    if (path == null)
      return;
    for (final NodePattern node : path.getNodes()) {
      // On the flag alone, never on how many labels came back with it. The flag means a '|' was written, while the
      // static list can hold fewer than two of them: a Cypher 25 dynamic $(expression) alternative is collected
      // separately and is not in it, so (n:A|$(x)) would slip past a count-based guard - which is the one shape
      // where letting a label expression through would matter most, the labels not even being known until runtime.
      if (!node.isLabelDisjunction())
        continue;
      final List<String> labels = node.getLabels();
      throw new CommandParsingException("UnexpectedSyntax: Label expressions are not allowed in " + clause
          + ", only in MATCH and in expressions: '|' says which labels a node MAY have, which is not something a "
          + "write can act on"
          + (labels.size() > 1 ? ". Use ':" + String.join(":", labels) + "' to give the node all of them" : ""));
    }
  }

  private static void checkMergeNullProperties(final PathPattern path) {
    for (final NodePattern node : path.getNodes())
      if (node.hasProperties())
        for (final Object value : node.getProperties().values())
          checkMergePropertyNotNull(value);
    for (final RelationshipPattern rel : path.getRelationships())
      if (rel.hasProperties())
        for (final Object value : rel.getProperties().values())
          checkMergePropertyNotNull(value);
  }

  /** A literal null, refused before anything runs. One that arrives at run time is refused by {@code MergeStep} (#10191). */
  private static void checkMergePropertyNotNull(final Object value) {
    if (value == null || (value instanceof LiteralExpression literal && literal.getValue() == null))
      throw new CommandParsingException(CypherSemanticValidator.NULL_MERGE_PROPERTY);
  }

  private static void validateDeleteTargets(final DeleteClause deleteClause) {
    for (final String target : deleteClause.getVariables()) {
      if (target == null)
        continue;
      // DELETE n:Label or DELETE r:TYPE is invalid (InvalidDelete)
      if (target.contains(":"))
        throw new CommandParsingException("InvalidDelete: Cannot delete a label or relationship type: " + target);
      // DELETE <arithmetic expression> like DELETE 1+1 is invalid (InvalidArgumentType)
      if (!CypherSemanticValidator.isValidVariableName(target) && !target.contains(".") && !target.contains("["))
        throw new CommandParsingException("InvalidArgumentType: DELETE requires a node, relationship, or path variable, got: " + target);
    }
  }

  // ============================================
  // Phase 5b: Relationship Uniqueness Validation
  // ============================================

  /**
   * A relationship variable may name one relationship of a pattern, not two: {@code (a)-[r]->()<-[r]-()} asks for a
   * relationship that is simultaneously two different ones, which no graph can answer.
   * <p>
   * Run against every pattern the statement contains, wherever it was written. It used to iterate the path patterns of
   * the {@code MATCH} clauses only, so the same pattern written as a {@code WHERE} predicate or inside an
   * {@code EXISTS { }} was accepted - and then answered by a path that could not correlate a relationship variable
   * either, which is how it came to be reported as "no match" rather than as the contradiction it is (issue #5656).
   * Neo4j rejects all three spellings.
   */
  static void validateRelationshipUniqueness(final CypherStatement statement) {
    CypherExpressionWalker.walk(statement, new RelationshipUniquenessChecks());
  }

  private static final class RelationshipUniquenessChecks implements CypherExpressionWalker.Visitor {
    @Override
    public void visit(final Expression expression) {
      // Patterns only.
    }

    @Override
    public void visitPattern(final PathPattern path) {
      final List<RelationshipPattern> relationships = path.getRelationships();
      if (relationships == null || relationships.size() < 2)
        return;

      final Set<String> relVars = new HashSet<>();
      for (final RelationshipPattern rel : relationships) {
        final String var = rel.getVariable();
        if (var != null && !var.isEmpty() && !relVars.add(var))
          throw new CommandParsingException(
              "RelationshipUniquenessViolation: Relationship variable '" + var + "' is used more than once in the "
                  + "same pattern");
      }
    }
  }
}
