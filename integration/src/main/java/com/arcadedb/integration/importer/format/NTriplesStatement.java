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
package com.arcadedb.integration.importer.format;

import com.arcadedb.integration.importer.ImportException;

/**
 * Reads the terms of one N-Triples statement (one line) as the RDF importer keys them: the subject and the object become
 * the key of a vertex, the predicate the label of the edge between them (issue #12049).
 * <ul>
 * <li>an IRI {@code <...>} is its text, with its {@code \}{@code u} escapes decoded;</li>
 * <li>a blank node is its label as written ({@code _:b1});</li>
 * <li>a literal is its canonical N-Triples form: the lexical form decoded and then re-escaped the canonical way (only
 * {@code "}, {@code \}, line feed and carriage return), between quotes, followed by its language tag or its datatype IRI
 * ({@code xsd:string}, the datatype of every untagged literal, is left out). Kept with its quotes, a literal can never be
 * the key of an IRI vertex, and a literal is one key whichever of the equivalent escapings the file uses. The value a user
 * reads is separate: the decoded lexical form, the language tag and the datatype of an object literal are answered by
 * {@link #objectLexical()}, {@link #objectLanguage()} and {@link #objectDatatype()}.</li>
 * </ul>
 * Terms are separated by runs of whitespace when the source's delimiter is a space or a tab (canonical N-Triples). With any
 * other delimiter (a comma-separated triple file, or a delimiter the user set) the terms are separated by that delimiter,
 * whitespace around it is ignored, and text that follows a term without the delimiter ends the statement there - the way
 * the delimited-text reading this replaces put it in the same cell. A term that is none of the above is taken as written.
 * What follows the third term (the {@code .}, or the graph of an N-Quads statement) is ignored. A term longer than
 * {@code maxLength} (when positive) is refused, as the {@code maxPropertySize} setting refused it before.
 */
final class NTriplesStatement {
  static final String XSD_STRING = "http://www.w3.org/2001/XMLSchema#string";

  private final char          delimiter;
  private final boolean       whitespaceSeparates;
  private final int           maxLength;
  private final StringBuilder buffer  = new StringBuilder();
  // THE DECODED LEXICAL FORM OF THE LITERAL BEING READ: WHAT A USER READS, WHERE buffer HOLDS THE KEY
  private final StringBuilder lexical = new StringBuilder();
  private       String        line;
  private       int           pos;
  private       boolean       literalRead;
  private       String        literalLanguage;
  private       String        literalDatatype;
  private       String        objectLexical;
  private       String        objectLanguage;
  private       String        objectDatatype;

  NTriplesStatement(final char delimiter, final int maxLength) {
    this.delimiter = delimiter;
    this.whitespaceSeparates = delimiter == ' ' || delimiter == '\t';
    this.maxLength = maxLength;
  }

  /**
   * Parses {@code statement} and writes its first three terms into {@code terms}, answering how many it found (fewer than
   * three is a malformed statement, left to the caller to report).
   */
  int parse(final String statement, final String[] terms) {
    this.line = statement;
    this.pos = 0;
    objectLexical = null;
    objectLanguage = null;
    objectDatatype = null;
    int found = 0;
    while (found < 3) {
      skipWhitespace();
      if (pos >= line.length())
        break;
      if (found > 0 && !whitespaceSeparates) {
        // A TERM IS FOLLOWED BY THE DELIMITER OR BY NOTHING: ANYTHING ELSE ENDS THE STATEMENT, LEAVING IT SHORT
        if (line.charAt(pos) != delimiter)
          break;
        while (pos < line.length() && (line.charAt(pos) == delimiter || isWhitespace(line.charAt(pos))))
          ++pos;
        if (pos >= line.length())
          break;
      }
      final int begin = pos;
      literalRead = false;
      terms[found] = readTerm();
      if (found == 2 && literalRead) {
        objectLexical = lexical.toString();
        objectLanguage = literalLanguage;
        objectDatatype = literalDatatype;
      }
      if (maxLength > 0 && pos - begin > maxLength)
        throw new ImportException(
            "Term " + (found + 1) + " is " + (pos - begin) + " characters long, more than maxPropertySize=" + maxLength + ": "
                + line.substring(begin, Math.min(pos, begin + 80)) + "...");
      ++found;
    }
    return found;
  }

  /** Whether the object of the last statement parsed is a literal. */
  boolean objectIsLiteral() {
    return objectLexical != null;
  }

  /** The lexical form of the object literal, its escapes decoded and without quotes, or null when the object is not a literal. */
  String objectLexical() {
    return objectLexical;
  }

  /** The language tag of the object literal as written, or null. */
  String objectLanguage() {
    return objectLanguage;
  }

  /** The datatype IRI of the object literal, or null for a plain (xsd:string) or language-tagged literal. */
  String objectDatatype() {
    return objectDatatype;
  }

  private void skipWhitespace() {
    while (pos < line.length() && (isWhitespace(line.charAt(pos)) || (whitespaceSeparates && line.charAt(pos) == delimiter)))
      ++pos;
  }

  private static boolean isWhitespace(final char c) {
    return c == ' ' || c == '\t';
  }

  private String readTerm() {
    final char c = line.charAt(pos);
    if (c == '<')
      return readIri();
    if (c == '"')
      return readLiteral();
    final int begin = pos;
    while (pos < line.length()) {
      final char ch = line.charAt(pos);
      if (isWhitespace(ch) || ch == delimiter)
        break;
      ++pos;
    }
    return line.substring(begin, pos);
  }

  /** {@code <...>} at {@code pos}: answers the IRI with its escapes decoded and leaves {@code pos} past the {@code >}. */
  private String readIri() {
    final int begin = pos;
    buffer.setLength(0);
    ++pos;
    while (pos < line.length()) {
      final char c = line.charAt(pos++);
      if (c == '>')
        return buffer.toString();
      if (c == '\\')
        appendEscape(true);
      else
        buffer.append(c);
    }
    throw new ImportException("Unterminated IRI starting at column " + (begin + 1) + ": " + line);
  }

  /** A quoted literal at {@code pos}, with its language tag or datatype: answers its canonical N-Triples form. */
  private String readLiteral() {
    final int begin = pos;
    buffer.setLength(0);
    lexical.setLength(0);
    literalLanguage = null;
    literalDatatype = null;
    buffer.append('"');
    ++pos;
    boolean closed = false;
    while (pos < line.length()) {
      final char c = line.charAt(pos++);
      if (c == '"') {
        closed = true;
        break;
      }
      if (c == '\\')
        appendEscape(false);
      else
        appendCanonical(c);
    }
    if (!closed)
      throw new ImportException("Unterminated literal starting at column " + (begin + 1) + ": " + line);
    buffer.append('"');
    literalRead = true;

    if (pos < line.length() && line.charAt(pos) == '@') {
      final int tagBegin = pos;
      ++pos;
      while (pos < line.length() && (Character.isLetterOrDigit(line.charAt(pos)) || line.charAt(pos) == '-'))
        ++pos;
      buffer.append(line, tagBegin, pos);
      literalLanguage = line.substring(tagBegin + 1, pos);
    } else if (line.startsWith("^^<", pos)) {
      pos += 2;
      final String literal = buffer.toString();
      final String datatype = readIri();
      if (XSD_STRING.equals(datatype))
        return literal;
      literalDatatype = datatype;
      return literal + "^^<" + datatype + ">";
    }
    return buffer.toString();
  }

  /**
   * The escape whose backslash was just read. In an IRI only {@code \}{@code u} and {@code \}{@code U} are allowed; a literal
   * also has the string escapes. Decoded characters of a literal are re-escaped the canonical way.
   */
  private void appendEscape(final boolean iri) {
    if (pos >= line.length())
      throw new ImportException("Incomplete escape at the end of: " + line);
    final char e = line.charAt(pos++);
    switch (e) {
    case 'u' -> appendCodePoint(4, iri);
    case 'U' -> appendCodePoint(8, iri);
    default -> {
      if (iri)
        throw new ImportException("Invalid escape '\\" + e + "' in an IRI: " + line);
      final char decoded = switch (e) {
        case 't' -> '\t';
        case 'b' -> '\b';
        case 'n' -> '\n';
        case 'r' -> '\r';
        case 'f' -> '\f';
        case '"' -> '"';
        case '\'' -> '\'';
        case '\\' -> '\\';
        default -> throw new ImportException("Invalid escape '\\" + e + "' in a literal: " + line);
      };
      appendCanonical(decoded);
    }
    }
  }

  private void appendCodePoint(final int digits, final boolean iri) {
    if (pos + digits > line.length())
      throw new ImportException("Incomplete \\u escape in: " + line);
    final int codePoint;
    try {
      codePoint = Integer.parseInt(line, pos, pos + digits, 16);
    } catch (final NumberFormatException e) {
      throw new ImportException("Invalid \\u escape in: " + line);
    }
    if (!Character.isValidCodePoint(codePoint))
      throw new ImportException("Invalid code point in a \\U escape in: " + line);
    pos += digits;
    if (iri)
      buffer.appendCodePoint(codePoint);
    else if (codePoint > 0xFFFF) {
      buffer.appendCodePoint(codePoint);
      lexical.appendCodePoint(codePoint);
    } else
      appendCanonical((char) codePoint);
  }

  /** A character of a literal's lexical form, kept decoded and escaped the way canonical N-Triples writes it. */
  private void appendCanonical(final char c) {
    lexical.append(c);
    switch (c) {
    case '"' -> buffer.append("\\\"");
    case '\\' -> buffer.append("\\\\");
    case '\n' -> buffer.append("\\n");
    case '\r' -> buffer.append("\\r");
    default -> buffer.append(c);
    }
  }
}
