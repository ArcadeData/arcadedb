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

import java.io.IOException;
import java.io.Reader;

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
 * A literal is followed by the end of the line, whitespace, the delimiter, {@code .}, {@code #} or the next term; any other
 * text after it refuses the row, rather than cutting the literal short and ignoring the rest: a CSV doubled quote
 * ({@code "say ""hi"""}) or a prefixed datatype ({@code "42"^^xsd:integer}) is not N-Triples. What follows the third term
 * (the {@code .}, or the graph of an N-Quads statement) is ignored. A blank-node label ends
 * before a final {@code .}, which is the terminator, not part of the label. A term longer than {@code maxLength} (when
 * positive) is refused, as the {@code maxPropertySize} setting refused a cell before, as soon as the scan passes the limit and
 * before the term's value is built.
 * <p>
 * {@link #readLine} reads one line of the source keeping at most {@link #lineLimit} characters of it: a statement within
 * the term limit is never longer, so a longer line is refused without being held whole in memory. A byte-order mark at the
 * start of the source is skipped, as the delimited-text reading skipped it.
 */
final class NTriplesStatement {
  static final String XSD_STRING = "http://www.w3.org/2001/XMLSchema#string";

  // A STATEMENT IS AT MOST FOUR TERMS (AN N-QUADS GRAPH IS THE FOURTH): THIS MUCH ROOM IS LEFT FOR SEPARATORS AND THE TERMINATOR
  private static final int SEPARATOR_ROOM = 4096;

  // HOW MUCH OF THE SOURCE readLine TAKES FROM THE READER AT A TIME
  static final int CHUNK_SIZE = 8192;

  private final String        delimiter;
  private final boolean       whitespaceSeparates;
  private final int           maxLength;
  private final long          lineLimit;
  private final char[]        chunk      = new char[CHUNK_SIZE];
  private       int           chunkPos;
  private       int           chunkLen;
  private final StringBuilder lineBuffer = new StringBuilder();
  private       long          lineLength;
  private       boolean       skipLineFeed;
  private       boolean       atSourceStart = true;
  private final StringBuilder buffer  = new StringBuilder();
  // THE DECODED LEXICAL FORM OF THE LITERAL BEING READ: WHAT A USER READS, WHERE buffer HOLDS THE KEY
  private final StringBuilder lexical = new StringBuilder();
  private       String        line;
  private       int           pos;
  // WHERE THE TERM BEING READ BEGINS, AND ITS NUMBER (1 TO 3), FOR THE LENGTH CHECK THE SCAN LOOPS MAKE
  private       int           termBegin;
  private       int           termNumber;
  private       boolean       literalRead;
  private       String        literalLanguage;
  private       String        literalDatatype;
  private       String        objectLexical;
  private       String        objectLanguage;
  private       String        objectDatatype;

  NTriplesStatement(final String delimiter, final int maxLength) {
    this.delimiter = delimiter;
    this.whitespaceSeparates = " ".equals(delimiter) || "\t".equals(delimiter);
    this.maxLength = maxLength;
    this.lineLimit = maxLength > 0 ? 4L * maxLength + SEPARATOR_ROOM : Long.MAX_VALUE;
  }

  /**
   * Reads the next line of {@code reader} (ended by LF, CR or CRLF), keeping at most {@link #lineLimit} characters of it in
   * {@link #line()} and counting the rest in {@link #lineLength()}. Answers false at the end of the source.
   * <p>
   * The source is read a chunk at a time and scanned for the line end in the chunk, then the line is copied out whole: reading
   * a character per call took the reader's lock once per character of a file that can be many gigabytes. The chunk holds the
   * source read past the line, so every call of one instance must be given the same {@code reader}.
   */
  boolean readLine(final Reader reader) throws IOException {
    lineBuffer.setLength(0);
    lineLength = 0;
    if (atSourceStart) {
      atSourceStart = false;
      // A UTF-8 FILE SAVED WITH A BYTE-ORDER MARK: KEPT, IT BECAME PART OF THE FIRST SUBJECT'S KEY ("\uFEFF<http://...>")
      if (fillChunk(reader) && chunk[chunkPos] == '\uFEFF')
        ++chunkPos;
    }
    if (skipLineFeed) {
      skipLineFeed = false;
      if (chunkPos < chunkLen || fillChunk(reader)) {
        if (chunk[chunkPos] == '\n')
          ++chunkPos;
      }
    }
    // WHETHER A CHARACTER OF THIS LINE, ITS TERMINATOR INCLUDED, WAS READ: AN EMPTY LINE IS A LINE, THE END OF THE SOURCE IS NOT
    boolean read = false;
    while (chunkPos < chunkLen || fillChunk(reader)) {
      final int begin = chunkPos;
      int end = begin;
      while (end < chunkLen && chunk[end] != '\n' && chunk[end] != '\r')
        ++end;
      if (end > begin) {
        read = true;
        if (lineLength < lineLimit)
          lineBuffer.append(chunk, begin, (int) Math.min(end - begin, lineLimit - lineLength));
        lineLength += end - begin;
      }
      chunkPos = end;
      if (end < chunkLen) {
        // THE LINE END: A CR MAY BE THE FIRST HALF OF A CRLF WHOSE LF IS THE NEXT CHARACTER, WHICH THE NEXT CALL SKIPS
        skipLineFeed = chunk[end] == '\r';
        ++chunkPos;
        return true;
      }
    }
    return read;
  }

  /** Reads the next chunk of {@code reader}: answers false, with the chunk empty, at the end of the source. */
  private boolean fillChunk(final Reader reader) throws IOException {
    int n;
    do {
      n = reader.read(chunk, 0, CHUNK_SIZE);
    } while (n == 0);
    chunkPos = 0;
    chunkLen = Math.max(n, 0);
    return n > 0;
  }

  /** The line {@link #readLine} read, cut at {@link #lineLimit} characters when it was longer. */
  String line() {
    return lineBuffer.toString();
  }

  /** The length of the line {@link #readLine} read, whatever of it was kept. */
  long lineLength() {
    return lineLength;
  }

  /** Whether the line {@link #readLine} read is longer than a statement whose terms are within the term limit can be. */
  boolean lineTooLong() {
    return lineLength > lineLimit;
  }

  int maxLength() {
    return maxLength;
  }

  /** How many characters of the last term's key and decoded value the reader built. Visible for testing the term limit. */
  int builtTermLength() {
    return Math.max(buffer.length(), lexical.length());
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
        if (!atDelimiter())
          break;
        while (pos < line.length()) {
          if (atDelimiter())
            pos += delimiter.length();
          else if (isWhitespace(line.charAt(pos)))
            ++pos;
          else
            break;
        }
        if (pos >= line.length())
          break;
      }
      // THE TERMINATOR AND A COMMENT ARE NOT TERMS: "<s> <p> ." HAS NO OBJECT, AND THE '.' READ AS ONE MADE AN EDGE TO A VERTEX KEYED "."
      if (atTerminatorOrComment())
        break;
      termBegin = pos;
      termNumber = found + 1;
      literalRead = false;
      terms[found] = readTerm();
      if (found == 2 && literalRead) {
        objectLexical = lexical.toString();
        objectLanguage = literalLanguage;
        objectDatatype = literalDatatype;
      }
      checkTermLength();
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
    while (pos < line.length() && isWhitespace(line.charAt(pos)))
      ++pos;
  }

  private boolean atDelimiter() {
    return line.startsWith(delimiter, pos);
  }

  /**
   * Whether the next characters are the statement terminator (a {@code .} alone, before whitespace, a comment or the end of the
   * line) or the start of a comment. No term begins so: an IRI begins with {@code <}, a literal with a quote, and a quoted
   * {@code "."} or an IRI with a {@code #} in it does not start with the marker.
   */
  private boolean atTerminatorOrComment() {
    final char c = line.charAt(pos);
    if (c == '#')
      return true;
    if (c != '.')
      return false;
    return pos + 1 >= line.length() || isWhitespace(line.charAt(pos + 1)) || line.charAt(pos + 1) == '#';
  }

  private static boolean isWhitespace(final char c) {
    return c == ' ' || c == '\t';
  }

  /**
   * Refuses the term being read once the scan has passed {@code maxLength} characters of it. The scan loops call it per
   * character, so an overlong term is refused before its decoded value and its key are built.
   */
  private void checkTermLength() {
    if (maxLength > 0 && pos - termBegin > maxLength)
      throw new ImportException(
          "Term " + termNumber + " is longer than maxPropertySize=" + maxLength + " characters: " + line.substring(termBegin,
              Math.min(pos, termBegin + 80)) + "...");
  }

  private String readTerm() {
    final char c = line.charAt(pos);
    if (c == '<')
      return readIri();
    if (c == '"')
      return readLiteral();
    final int begin = pos;
    // NO CHECK IN THIS SCAN: IT BUILDS NOTHING, AND A BLANK-NODE LABEL ONE CHARACTER OVER THE LIMIT MAY BE ITS TERMINATOR
    while (pos < line.length() && !isWhitespace(line.charAt(pos)) && !atDelimiter())
      ++pos;
    // A BLANK-NODE LABEL NEVER ENDS WITH '.': A FINAL ONE IS THE STATEMENT TERMINATOR ("_:b1."), AND KEEPING IT MADE A LATER
    // "_:b1" ANOTHER VERTEX. A '.' INSIDE THE LABEL ("_:a.b") IS PART OF IT
    if (line.startsWith("_:", begin))
      while (pos - begin > 2 && line.charAt(pos - 1) == '.')
        --pos;
    return line.substring(begin, pos);
  }

  /** {@code <...>} at {@code pos}: answers the IRI with its escapes decoded and leaves {@code pos} past the {@code >}. */
  private String readIri() {
    final int begin = pos;
    buffer.setLength(0);
    ++pos;
    while (pos < line.length()) {
      final char c = line.charAt(pos++);
      checkTermLength();
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
      checkTermLength();
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
      // THE LANGTAG OF N-TRIPLES: ASCII LETTERS, THEN GROUPS OF A '-' AND ASCII LETTERS OR DIGITS ("en", "en-GB", "zh-Hant-TW"). A TAG
      // THAT IS NONE OF THAT (EMPTY, STARTING WITH A DIGIT, AN EMPTY GROUP, A NON-ASCII LETTER) REFUSES THE ROW RATHER THAN BE STORED
      boolean valid = skipLanguageTagGroup(true);
      while (valid && pos < line.length() && line.charAt(pos) == '-') {
        ++pos;
        valid = skipLanguageTagGroup(false);
      }
      if (!valid || (pos < line.length() && Character.isLetterOrDigit(line.charAt(pos))))
        throw new ImportException("Invalid language tag at column " + (tagBegin + 1) + ", a tag is ASCII letters followed by groups "
            + "of '-' and ASCII letters or digits (en-GB): " + line);
      buffer.append(line, tagBegin, pos);
      literalLanguage = line.substring(tagBegin + 1, pos);
    } else if (line.startsWith("^^", pos)) {
      // A PREFIXED NAME ("42"^^xsd:integer) IS TURTLE, NOT N-TRIPLES: LEFT UNREAD IT LOST THE DATATYPE, AND THE LITERAL WAS THE PLAIN "42"
      if (!line.startsWith("^^<", pos))
        throw new ImportException("Invalid datatype at column " + (pos + 1) + ", a datatype is an IRI between '<' and '>': " + line);
      pos += 2;
      final String literal = buffer.toString();
      final String datatype = readIri();
      checkLiteralEnd();
      if (XSD_STRING.equals(datatype))
        return literal;
      literalDatatype = datatype;
      return literal + "^^<" + datatype + ">";
    }
    checkLiteralEnd();
    return buffer.toString();
  }

  /**
   * What may follow a literal with its language tag or datatype: the end of the line, whitespace, the delimiter, the statement
   * terminator, a comment, or the next term. Any other character is text the scan would leave unread, which cut the literal
   * short and went on without a word: the CSV doubled quote of {@code "say ""hi"""} read as the literal {@code "say "} and the
   * rest of the row ignored, with a result of OK. The row is refused instead.
   */
  private void checkLiteralEnd() {
    if (pos >= line.length())
      return;
    final char c = line.charAt(pos);
    if (isWhitespace(c) || c == '.' || c == '#' || c == '<' || line.startsWith("_:", pos) || atDelimiter())
      return;
    throw new ImportException("Unexpected text after the literal that ends at column " + pos + ", a quote inside a literal is "
        + "escaped as \\\" in N-Triples and not doubled: " + line);
  }

  /** Skips one group of a language tag, ASCII letters for the first, letters and digits for the others: answers whether it is not empty. */
  private boolean skipLanguageTagGroup(final boolean lettersOnly) {
    final int begin = pos;
    while (pos < line.length() && isAsciiLetterOrDigit(line.charAt(pos), lettersOnly)) {
      ++pos;
      checkTermLength();
    }
    return pos > begin;
  }

  private static boolean isAsciiLetterOrDigit(final char c, final boolean lettersOnly) {
    return (c >= 'a' && c <= 'z') || (c >= 'A' && c <= 'Z') || (!lettersOnly && c >= '0' && c <= '9');
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
    // ASCII HEX DIGITS ONLY: Integer.parseInt TAKES A LEADING SIGN AND NON-ASCII DIGITS TOO, SO A SIGNED ESCAPE WAS READ AS A LETTER
    int codePoint = 0;
    for (int i = 0; i < digits; i++) {
      final char h = line.charAt(pos + i);
      final int digit = h >= '0' && h <= '9' ? h - '0' : h >= 'a' && h <= 'f' ? h - 'a' + 10 : h >= 'A' && h <= 'F' ? h - 'A' + 10 : -1;
      if (digit < 0)
        throw new ImportException("Invalid \\u or \\U escape, " + digits + " hexadecimal digits are required, in: " + line);
      codePoint = (codePoint << 4) | digit;
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
