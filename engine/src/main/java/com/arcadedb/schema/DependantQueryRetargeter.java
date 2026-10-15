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

import com.arcadedb.database.DatabaseInternal;
import com.arcadedb.query.sql.parser.FromClause;
import com.arcadedb.query.sql.parser.FromItem;
import com.arcadedb.query.sql.parser.Identifier;
import com.arcadedb.query.sql.parser.SelectStatement;
import com.arcadedb.query.sql.parser.Statement;

import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Re-points the defining query of a materialized view or a continuous aggregate at a RENAMED source type (issue #10096).
 * <p>
 * Both store the SQL text of their query and re-run it on every refresh, so renaming the type their {@code FROM} names breaks
 * the refresh unless the text follows. The text is free SQL, though, and rewriting it is only safe when it is certain what is
 * being rewritten. This keeps the user's own text (formatting and comments included) and changes exactly one token - the name
 * - and refuses to answer unless the result PROVES to be the same statement:
 * <ol>
 *   <li>the old name occurs in the text as exactly one identifier token. Two occurrences are a subquery, a join or a second
 *   reference this cannot tell apart from the {@code FROM}, and a rewrite of one would leave the other on a type that no longer
 *   exists; an occurrence inside a string literal counts too, because telling it apart takes a parser and refusing costs nothing
 *   more than the view being recreated;</li>
 *   <li>both texts parse as a {@code SELECT} whose {@code FROM} names the type, and the two statements are identical once that
 *   one identifier is set to the same name in both: the token replaced WAS the {@code FROM}, and nothing else changed.</li>
 * </ol>
 * A {@code null} answer means "cannot be re-pointed with certainty", never "nothing to do", and the caller refuses the rename
 * before a file moves.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
final class DependantQueryRetargeter {
  private static final Pattern PLAIN_IDENTIFIER = Pattern.compile("[A-Za-z_][A-Za-z0-9_]*");

  private DependantQueryRetargeter() {
  }

  /**
   * @return {@code query} with {@code oldName} replaced by {@code newName}, or {@code null} when that cannot be done with certainty
   */
  static String retarget(final DatabaseInternal database, final String query, final String oldName, final String newName) {
    if (query == null)
      return null;

    final Matcher occurrence = occurrencesOf(oldName).matcher(query);
    if (!occurrence.find())
      return null;
    final int start = occurrence.start();
    final int end = occurrence.end();
    if (occurrence.find())
      return null;

    final String spelling = PLAIN_IDENTIFIER.matcher(newName).matches() ? newName : Identifier.quote(newName);
    final String retargeted = query.substring(0, start) + spelling + query.substring(end);

    try {
      final Statement before = database.getStatementCache().get(query);
      final Statement after = database.getStatementCache().get(retargeted);
      if (!(before instanceof SelectStatement selectBefore) || !(after instanceof SelectStatement selectAfter))
        return null;
      if (!namesSourceType(selectBefore, oldName) || !namesSourceType(selectAfter, newName))
        return null;

      return sameStatementOnceSourceIs(selectBefore, selectAfter, newName) ? retargeted : null;
    } catch (final RuntimeException e) {
      // A TEXT THAT DOES NOT PARSE IS A TEXT THIS DOES NOT UNDERSTAND
      return null;
    }
  }

  /**
   * The name as one identifier token, plain or between back-ticks, and not as a part of a longer name or of a path. Unicode aware:
   * a type name may hold letters outside ASCII, and the longer name {@code CittàX} must not be taken for an occurrence of
   * {@code Città} just because its last letter is not an ASCII word character.
   */
  private static Pattern occurrencesOf(final String name) {
    return Pattern.compile("(?<![\\w$`.])" + Pattern.quote(name) + "(?![\\w$`])|" + Pattern.quote(Identifier.quote(name)),
        Pattern.UNICODE_CHARACTER_CLASS);
  }

  private static boolean namesSourceType(final SelectStatement select, final String typeName) {
    final FromItem item = fromItemOf(select);
    return item != null && item.getIdentifier() != null && typeName.equals(item.getIdentifier().getStringValue());
  }

  private static FromItem fromItemOf(final SelectStatement select) {
    final FromClause from = select.getTarget();
    return from != null ? from.getItem() : null;
  }

  /** Whether the two statements render the same SQL once the source of each is the type {@code typeName}. */
  private static boolean sameStatementOnceSourceIs(final SelectStatement first, final SelectStatement second, final String typeName) {
    return rendered(first, typeName).equals(rendered(second, typeName));
  }

  /** The statement rendered back to SQL with its source set to {@code typeName}; the cached statement is only copied, never changed. */
  private static String rendered(final SelectStatement select, final String typeName) {
    final SelectStatement copy = select.copy();
    fromItemOf(copy).setIdentifier(Identifier.quoted(Identifier.quote(typeName)));
    return copy.toString();
  }
}
