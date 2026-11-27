/*
 * Copyright 2021-present Arcade Data Ltd (info@arcadedata.com)
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
package com.arcadedb.index.fulltext;

import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.index.Term;
import org.apache.lucene.queryparser.charstream.FastCharStream;
import org.apache.lucene.queryparser.classic.MultiFieldQueryParser;
import org.apache.lucene.queryparser.classic.QueryParser;
import org.apache.lucene.queryparser.classic.QueryParserConstants;
import org.apache.lucene.queryparser.classic.QueryParserTokenManager;
import org.apache.lucene.queryparser.classic.Token;
import org.apache.lucene.queryparser.classic.TokenMgrError;
import org.apache.lucene.search.BooleanClause;
import org.apache.lucene.search.BooleanQuery;
import org.apache.lucene.search.BoostQuery;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.QueryVisitor;

import java.io.StringReader;
import java.util.ArrayList;
import java.util.List;

/**
 * Builds the parsers of the full-text query syntax. They are Lucene's classic parsers but for two clauses, the regexp
 * ({@code /pattern/}) and the wildcard ({@code te?t*}) ones: Lucene compiles each of them into an automaton while it parses, which
 * the {@link FullTextQueryExecutor} never uses, as it matches the keys of the index with {@link java.util.regex.Pattern} under the
 * shared regex deadline. These parsers keep the text of the clause in a query that holds nothing else, so parsing a clause costs
 * what its text is long.
 * <p>
 * It also states how large a query may be: {@link #checkText} and {@link #checkTree} refuse a query that goes past
 * {@link #MAX_PARENTHESES_DEPTH}, {@link #MAX_GROUP_DEPTH} or {@link #MAX_CLAUSES}, before and after it is parsed.
 *
 * @author lvca (l.garulli@arcadedata.com)
 */
final class FullTextQueryParsers {
  /**
   * Levels of groups a parsed query may nest, a boosted group or clause ({@code (a b)^2}) counting as one more, and the OR of a field
   * list a restricted search expands each clause into as well. Elasticsearch allows 20 levels of bool queries.
   */
  static final int MAX_GROUP_DEPTH = 16;

  /** Clauses, groups included, a parsed query may hold in all: Lucene's own limit of 1024 applies to each group alone. */
  static final int MAX_CLAUSES = 16_384;

  /**
   * Levels of parentheses the text of a query may open, checked before it is parsed. Looser than {@link #MAX_GROUP_DEPTH}: a group
   * of one clause is the clause, so {@code ((((a))))} nests no group at all.
   */
  static final int MAX_PARENTHESES_DEPTH = 64;

  private FullTextQueryParsers() {
  }

  /**
   * Refuses a query whose parentheses nest deeper than {@link #MAX_PARENTHESES_DEPTH}, before it reaches the parser. Where the
   * parentheses are is told by the parser's own lexer, which knows the ones inside a quoted phrase, a regexp, a range or after a
   * backslash from the ones that open a group. A text that does not even split into tokens is left to the parser to refuse.
   *
   * @throws FullTextQueryParseException when the parentheses nest too deep
   */
  static void checkText(final String text) {
    if (text == null)
      return;

    // The parentheses of a text cannot nest deeper than there are opening ones: the usual query is cleared by this pass alone
    int opening = 0;
    for (int i = 0, length = text.length(); i < length && opening <= MAX_PARENTHESES_DEPTH; i++)
      if (text.charAt(i) == '(')
        opening++;
    if (opening <= MAX_PARENTHESES_DEPTH)
      return;

    final QueryParserTokenManager tokens = new QueryParserTokenManager(new FastCharStream(new StringReader(text)));
    int depth = 0;
    try {
      for (Token token = tokens.getNextToken(); token.kind != QueryParserConstants.EOF; token = tokens.getNextToken()) {
        if (token.kind == QueryParserConstants.LPAREN) {
          if (++depth > MAX_PARENTHESES_DEPTH)
            throw new FullTextQueryParseException("Invalid search query: parentheses nest deeper than " + MAX_PARENTHESES_DEPTH + " levels");
        } else if (token.kind == QueryParserConstants.RPAREN && depth > 0)
          depth--;
      }
    } catch (final TokenMgrError e) {
      // not a text the parser can read: it refuses it with the position of the fault
    }
  }

  /**
   * Refuses a parsed query whose groups nest deeper than {@link #MAX_GROUP_DEPTH}, or that holds more than {@link #MAX_CLAUSES}
   * clauses. It reads the query one level at a time instead of recursing: it is not known to be small yet.
   *
   * @throws FullTextQueryParseException when the query is too deep or too large
   */
  static void checkTree(final Query root) {
    List<Query> level = List.of(root);
    int depth = 0;
    int clauses = 0;
    while (!level.isEmpty()) {
      final List<Query> below = new ArrayList<>();
      for (final Query query : level) {
        if (query instanceof final BooleanQuery group) {
          for (final BooleanClause clause : group.clauses())
            below.add(clause.query());
        } else if (query instanceof final BoostQuery boost)
          below.add(boost.getQuery());
      }
      if (below.isEmpty())
        return;
      if (++depth > MAX_GROUP_DEPTH)
        throw new FullTextQueryParseException("Invalid search query: groups nest deeper than " + MAX_GROUP_DEPTH + " levels");
      clauses += below.size();
      if (clauses > MAX_CLAUSES)
        throw new FullTextQueryParseException("Invalid search query: more than " + MAX_CLAUSES + " clauses");
      level = below;
    }
  }

  /**
   * @param defaultField     the field a clause targets when it is not field-qualified
   * @param restrictedFields the fields an unqualified clause is expanded over, or null to leave it on {@code defaultField}
   */
  static QueryParser create(final String defaultField, final String[] restrictedFields, final Analyzer analyzer) {
    return restrictedFields != null ? new RestrictedFieldsParser(restrictedFields, analyzer) : new SingleFieldParser(defaultField, analyzer);
  }

  private static final class SingleFieldParser extends QueryParser {
    private SingleFieldParser(final String defaultField, final Analyzer analyzer) {
      super(defaultField, analyzer);
    }

    @Override
    protected Query newRegexpQuery(final Term regexp) {
      return new RegexpClause(regexp);
    }

    @Override
    protected Query newWildcardQuery(final Term term) {
      return new WildcardClause(term);
    }
  }

  private static final class RestrictedFieldsParser extends MultiFieldQueryParser {
    private RestrictedFieldsParser(final String[] fields, final Analyzer analyzer) {
      super(fields, analyzer);
    }

    @Override
    protected Query newRegexpQuery(final Term regexp) {
      return new RegexpClause(regexp);
    }

    @Override
    protected Query newWildcardQuery(final Term term) {
      return new WildcardClause(term);
    }
  }

  /**
   * A clause that matches index keys by their text: the term it carries, and nothing a search of a Lucene index would need. It is
   * parsed, then read by the executor, and never run against a Lucene index.
   */
  abstract static class TermPatternClause extends Query {
    private final Term term;

    private TermPatternClause(final Term term) {
      this.term = term;
    }

    Term getTerm() {
      return term;
    }

    @Override
    public void visit(final QueryVisitor visitor) {
      if (visitor.acceptField(term.field()))
        visitor.visitLeaf(this);
    }

    @Override
    public boolean equals(final Object other) {
      return sameClassAs(other) && term.equals(((TermPatternClause) other).term);
    }

    @Override
    public int hashCode() {
      return 31 * classHash() + term.hashCode();
    }
  }

  /** A {@code /pattern/} clause. */
  static final class RegexpClause extends TermPatternClause {
    private RegexpClause(final Term term) {
      super(term);
    }

    @Override
    public String toString(final String field) {
      final Term term = getTerm();
      final StringBuilder buffer = new StringBuilder();
      if (!term.field().equals(field))
        buffer.append(term.field()).append(':');
      return buffer.append('/').append(term.text()).append('/').toString();
    }
  }

  /** A clause with {@code *} and {@code ?} in it. */
  static final class WildcardClause extends TermPatternClause {
    private WildcardClause(final Term term) {
      super(term);
    }

    @Override
    public String toString(final String field) {
      final Term term = getTerm();
      final StringBuilder buffer = new StringBuilder();
      if (!term.field().equals(field))
        buffer.append(term.field()).append(':');
      return buffer.append(term.text()).toString();
    }
  }
}
