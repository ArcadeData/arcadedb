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
import org.apache.lucene.queryparser.classic.MultiFieldQueryParser;
import org.apache.lucene.queryparser.classic.QueryParser;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.QueryVisitor;

/**
 * Builds the parsers of the full-text query syntax. They are Lucene's classic parsers but for two clauses, the regexp
 * ({@code /pattern/}) and the wildcard ({@code te?t*}) ones: Lucene compiles each of them into an automaton while it parses, which
 * the {@link FullTextQueryExecutor} never uses, as it matches the keys of the index with {@link java.util.regex.Pattern} under the
 * shared regex deadline. These parsers keep the text of the clause in a query that holds nothing else, so parsing a clause costs
 * what its text is long.
 *
 * @author lvca (l.garulli@arcadedata.com)
 */
final class FullTextQueryParsers {
  private FullTextQueryParsers() {
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
