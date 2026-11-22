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
package com.arcadedb.utility;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.util.BitSet;
import java.util.IllegalFormatException;
import java.util.Locale;

/**
 * String helpers that the engine needs on paths where the obvious form allocates.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
public class StringUtils {

  /**
   * Widest field a {@code %<width>s}-style conversion may ask for in a user-supplied format. Deliberately generous -
   * a padded report column is nowhere near it - and the point is only that {@code format('%99999999s', 'x')} must not
   * turn eight characters of query text into a 100MB string (issue #6389).
   */
  public static final int MAX_FORMAT_WIDTH = 1_000_000;

  /**
   * Most characters the specifiers of one user-supplied format may add together, on top of the text the caller already
   * holds: the pattern itself and one copy of each argument that is text. {@link #MAX_FORMAT_WIDTH} bounds one specifier,
   * and a pattern is free to repeat it: a thousand {@code %1000000s} are 9KB of query asking for a gigabyte, and
   * {@code %1$s%1$s...} writes a long argument once per five characters of pattern (issue #12880). Sixteen full-width
   * fields, so that no pattern with a single wide field, or a handful of them, is refused.
   */
  public static final int MAX_FORMAT_OUTPUT = 16 * MAX_FORMAT_WIDTH;

  // What a conversion of a number, a boolean or a character writes at most, sign, exponent and separators included
  private static final int SCALAR_TEXT_SIZE = 32;
  // The longest date/time conversion (%tc) with a long zone name
  private static final int TEMPORAL_TEXT_SIZE = 48;
  // The characters upper-cased at a time to measure what %S writes
  private static final int UPPER_CASE_PIECE = 1024;

  /**
   * Strict boolean parse for configuration text: {@code Boolean.parseBoolean()} reads "yes", "1", "on" and every typo as
   * false without a word, which for a switch whose safe side is true (TLS) silently turns the protection off. Only
   * {@code true} and {@code false} are accepted, trimmed and case-insensitively (issue #8935).
   *
   * @throws IllegalArgumentException when the text is neither
   */
  public static boolean parseStrictBoolean(final String text) {
    final String trimmed = text.trim();
    if ("true".equalsIgnoreCase(trimmed))
      return true;
    if ("false".equalsIgnoreCase(trimmed))
      return false;
    throw new IllegalArgumentException("'" + text + "' is neither true nor false");
  }

  /**
   * The candidate {@code name} most likely misspells: the closest by case-insensitive edit distance, ties broken by the
   * natural order, or null when even the closest differs in more than half of {@code name}'s characters. For a "did you
   * mean" hint on a refused setting name (issues #11311, #11523).
   */
  public static String closestMatch(final String name, final Iterable<String> candidates) {
    final String lowerName = name.toLowerCase(Locale.ENGLISH);
    String closest = null;
    int closestDistance = Integer.MAX_VALUE;
    for (final String candidate : candidates) {
      final int distance = editDistance(lowerName, candidate.toLowerCase(Locale.ENGLISH));
      if (distance < closestDistance || (distance == closestDistance && candidate.compareTo(closest) < 0)) {
        closest = candidate;
        closestDistance = distance;
      }
    }
    return closestDistance <= name.length() / 2 ? closest : null;
  }

  /** The Levenshtein distance between {@code a} and {@code b}. */
  public static int editDistance(final String a, final String b) {
    int[] previous = new int[b.length() + 1];
    int[] current = new int[b.length() + 1];
    for (int j = 0; j <= b.length(); j++)
      previous[j] = j;

    for (int i = 1; i <= a.length(); i++) {
      current[0] = i;
      for (int j = 1; j <= b.length(); j++)
        current[j] = Math.min(Math.min(current[j - 1], previous[j]) + 1, previous[j - 1] + (a.charAt(i - 1) == b.charAt(j - 1) ? 0 : 1));

      final int[] swap = previous;
      previous = current;
      current = swap;
    }
    return previous[b.length()];
  }

  protected StringUtils() {
  }

  /**
   * Splits a {@code <key>=<value>} pair on the FIRST '=' only, so a value that contains further separators (a
   * connection string, a base64 padding, a date pattern) is kept whole and an empty value is a value, not a missing
   * one. Returns null when there is no separator at all, leaving to the caller the choice between rejecting the
   * input and reading it as an empty value.
   * <p>
   * Shared by the console's {@code -D<key>=<value>} arguments and {@code SET} command (issue #6392), the Postgres
   * wire's {@code SET} command, and the Studio include directive's parameters (issue #6423), which used to each
   * carry their own {@code split("=")} that truncated a value containing a further '='.
   */
  public static String[] splitKeyValue(final String pair) {
    final int separatorPos = pair.indexOf('=');
    if (separatorPos < 0)
      return null;
    return new String[] { pair.substring(0, separatorPos), pair.substring(separatorPos + 1) };
  }

  /**
   * Case-insensitive {@code contains} that allocates nothing.
   * <p>
   * The obvious form, {@code haystack.toUpperCase().contains(needle)}, copies the whole haystack to answer a question
   * about a needle a dozen characters long - which is why every caller of this had written its own copy rather than
   * use it: the async DDL classifier scanning a whole script, the Cypher parser deciding on its own parse hot path,
   * the HTTP handler looking for a LIMIT in a command, and the MCP prompt fence looking for its own delimiter. Four
   * private copies of six lines, and this is the one they share.
   *
   * @return true when {@code needle} occurs anywhere in {@code haystack}, ignoring case. An empty needle is contained
   *     by everything, as {@code String.contains} has it.
   */
  public static boolean containsIgnoreCase(final String haystack, final String needle) {
    final int needleLength = needle.length();
    if (needleLength == 0)
      return true;

    final int last = haystack.length() - needleLength;
    for (int i = 0; i <= last; i++)
      if (haystack.regionMatches(true, i, needle, 0, needleLength))
        return true;

    return false;
  }

  /**
   * Applies a user-supplied {@link java.util.Formatter} pattern, answering a typed argument error for everything the
   * JDK reports by throwing.
   * <p>
   * {@code String.format} raises a family of unchecked exceptions for input that is simply wrong - a conversion that
   * does not match its argument ({@code format('%d','x')}), a conversion that does not exist ({@code format('%y')}),
   * a null pattern - and every one of them used to escape the SQL {@code format()} function and its method twin as a
   * raw JDK exception, i.e. an HTTP 500 for a mistake in the query (issue #6389). The width ceiling is checked BEFORE
   * formatting, because that one is not an error the JDK reports at all: it allocates.
   *
   * @param caller the function or method name, for the error message
   * @param format the caller-supplied format pattern
   * @param args   the format arguments
   *
   * @return the formatted string
   *
   * @throws IllegalArgumentException if the pattern is null, malformed, mismatched or asks for an excessive width
   */
  public static String format(final String caller, final String format, final Object... args) {
    return format(caller, Locale.ROOT, format, args);
  }

  /**
   * As {@link #format(String, String, Object...)}, with the locale that decides the decimal and grouping separators.
   */
  public static String format(final String caller, final Locale locale, final String format, final Object... args) {
    if (format == null)
      throw new IllegalArgumentException(caller + "() requires a non-null format");

    checkFormat(caller, locale, format, args);

    try {
      return String.format(locale, format, args);
    } catch (final IllegalFormatException e) {
      throw new IllegalArgumentException(
          caller + "() cannot apply the format '" + format + "': " + e.getClass().getSimpleName() + " - " + e.getMessage(), e);
    }
  }

  /**
   * Rejects a conversion whose width or precision exceeds {@link #MAX_FORMAT_WIDTH}. Scans the pattern once rather
   * than compiling a regex: this runs per formatted value.
   * <p>
   * A specifier is {@code %[argument_index$][flags][width][.precision]conversion}, and BOTH the width and the
   * precision size an allocation, so the largest number the specifier names is what is measured. Checking only the
   * last one read let {@code %2000000.1s} through - the argument is truncated to one character and then padded back
   * out to a two-million-character field.
   * <p>
   * The pattern is not the only thing that sizes the output: a fixed-point ({@code %f}) conversion spells a
   * {@link BigDecimal} out in plain digits, so the argument each one consumes is measured too (issue #12837). The scan
   * follows the argument the way {@link java.util.Formatter} does - the next one in order, the one an explicit
   * {@code n$} names, or with the {@code <} flag the one the previous specifier used, while {@code %n} and {@code %%}
   * consume none - so a huge decimal printed by {@code %s} or {@code %e} beside an unrelated {@code %f} is not refused.
   * <p>
   * The same pass adds up what the specifiers write, and refuses the pattern once the sum passes
   * {@link #MAX_FORMAT_OUTPUT}: a ceiling on each specifier is none on a pattern that repeats it (issue #12880). Each
   * specifier counts for the larger of its width and of the text its argument becomes ({@link #textSize}). What is NOT
   * counted is what the caller holds already: the literal text of the pattern, and the first print of an argument that is
   * its own text (a string, a big integer, an object under {@code %s}), of which only the padding counts. So a single
   * {@code %s} over a string of any length formats as it always did, and the output stays under the pattern, plus one
   * copy of such arguments, plus the ceiling.
   */
  private static void checkFormat(final String caller, final Locale locale, final String format, final Object[] args) {
    // The argument index of the last specifier without an explicit index, and of the last specifier of any kind.
    long ordinary = -1;
    long last = -1;
    // What the specifiers add to the output. No overflow: one specifier adds an int at most, and the scan stops at the
    // first one that takes the sum over the ceiling.
    long total = 0;
    // The arguments printed once already as their own text: a bit each for the first 64, a set past them.
    long printed = 0;
    BitSet printedBeyond = null;
    // The last argument whose text had to be built to be measured, so that '%1$s%1$s%1$s' builds it once.
    long measured = -1;
    long measuredSize = 0;
    // The last argument upper-cased to be measured, its length and what the upper case adds to it
    long upperCased = -1;
    long upperCasedFrom = 0;
    long upperCasedGrowth = 0;
    for (int i = 0; i < format.length(); i++) {
      if (format.charAt(i) != '%')
        continue;

      // Skip the argument index and the flags, then read the width - and, past a '.', the precision - as plain
      // digits. A number too long to be an int is over the ceiling by definition, so digits are counted rather than
      // parsed.
      int j = i + 1;
      long number = 0;
      int digits = 0;
      long width = 0;
      long explicit = 0;
      boolean previous = false;
      boolean precise = false;
      boolean grouping = false;
      while (j < format.length()) {
        final char c = format.charAt(j);
        if (c >= '0' && c <= '9') {
          if (++digits <= 10)
            number = number * 10 + (c - '0');
          else
            number = Long.MAX_VALUE;
        } else if (c == '$') {
          // The digits just read were an argument index, which allocates nothing: keep it as the index and discard
          // them as a size.
          explicit = number;
          number = 0;
          digits = 0;
        } else if (c == '.') {
          // The digits just read were the width, and the precision starts now. The width still has to be measured -
          // it is what the truncated argument is padded out to. The largest, for a malformed specifier with two.
          width = Math.max(width, number);
          precise = true;
          number = 0;
          digits = 0;
        } else if (c == '<') {
          previous = true;
        } else if (c == ',') {
          grouping = true;
        } else if (!isFormatFlag(c)) {
          // Not a flag either: the conversion character, so the specifier ends here.
          break;
        }
        j++;
      }
      final long precision = precise ? number : -1;
      if (!precise)
        width = number;
      // Both size an allocation, so the largest number the specifier names is what one field is measured by
      final long widest = Math.max(width, precision);

      if (widest > MAX_FORMAT_WIDTH)
        throw new IllegalArgumentException(
            caller + "() format '" + format + "' asks for a field of " + widest + " characters, over the " + MAX_FORMAT_WIDTH
                + " limit");

      if (j < format.length()) {
        final char conversion = format.charAt(j);
        if (conversion == '%')
          // A literal percent sign, padded to the width
          total += width;
        else if (conversion != 'n') {
          // '%n' is left out: a line separator is no longer than the two characters that ask for it, and it takes no
          // width.
          // The '<' flag wins over an explicit index, and leaves both counters where they were.
          if (!previous)
            last = explicit > 0 ? explicit - 1 : ++ordinary;
          // A null array is not a missing argument for the formatter: every specifier prints null
          if (last >= 0 && (args == null || last < args.length)) {
            final Object arg = args == null ? null : args[(int) last];
            if (conversion == 'f' && arg instanceof BigDecimal decimal)
              checkPlainDigits(caller, format, decimal);

            final char kind = Character.toLowerCase(conversion);
            boolean firstPrint = false;
            if (isItsOwnText(kind, arg)) {
              if (last < Long.SIZE) {
                firstPrint = (printed & 1L << last) == 0;
                printed |= 1L << last;
              } else {
                if (printedBeyond == null)
                  printedBeyond = new BitSet();
                firstPrint = !printedBeyond.get((int) last);
                printedBeyond.set((int) last);
              }
            }

            long size;
            // What the upper case adds to the text: 'ß' is printed as 'SS' by %S, on the first print as on any other
            long growth = 0;
            if (conversion == 'S' && isUpperCasedText(arg)) {
              // Only the upper-cased text says how long it is, so it is measured, once
              if (upperCased != last) {
                final String text = String.valueOf(arg);
                upperCasedFrom = text.length();
                upperCasedGrowth = upperCaseGrowth(text, locale != null ? locale : Locale.getDefault(Locale.Category.FORMAT));
                upperCased = last;
              }
              size = upperCasedFrom;
              growth = upperCasedGrowth;
            } else if (isTextBuiltToBeMeasured(kind, arg)) {
              // Only its text says how long it is. The first print is not counted, so it is not built for it, and the
              // padding is then counted in full.
              if (firstPrint)
                size = 0;
              else {
                if (measured != last) {
                  measuredSize = String.valueOf(arg).length();
                  measured = last;
                }
                size = measuredSize;
              }
            } else
              size = textSize(kind, arg);

            if (kind == 's' || kind == 'b' || kind == 'h') {
              // The precision truncates
              if (precise)
                size = Math.min(size, precision);
              // The case changes after the truncation, and a character becomes three at most
              growth = Math.min(growth, 2 * size);
            } else if (kind != 'c' && kind != 't') {
              // One separator every three integer digits, which for %g are as many as the precision
              if (grouping)
                size += kind == 'g' ? (precise ? precision : 6) / 3 : size / 3;
              // For a floating-point conversion the precision is the number of fraction digits, six when the pattern
              // names none
              if (kind != 'd' && kind != 'o' && kind != 'x')
                size += precise ? precision : 6;
            }

            final long written = Math.max(width, size + growth);
            total += firstPrint ? written - size : written;
          }
        }

        if (total > MAX_FORMAT_OUTPUT)
          throw new IllegalArgumentException(
              caller + "() format '" + abbreviate(format) + "' writes more than " + MAX_FORMAT_OUTPUT
                  + " characters over its arguments, over the limit");
      }

      i = j;
    }
  }

  /**
   * Whether the argument is, for this conversion, text the caller holds already at about the size it is printed at: a
   * string, a big integer in digits, any other object under {@code %s}. Its first print adds nothing to what the query
   * already costs, unlike a number spelled out ({@code 1E+999999} under {@code %f}) which is a few bytes until printed.
   */
  private static boolean isItsOwnText(final char kind, final Object arg) {
    if (kind == 's')
      return arg != null && !isScalar(arg);
    return arg instanceof BigInteger && (kind == 'd' || kind == 'o' || kind == 'x');
  }

  /** Whether {@code %S} changes the case of text that may not be ASCII: a number, a boolean and null print as they are long. */
  private static boolean isUpperCasedText(final Object arg) {
    return arg != null && !(arg instanceof Number) && !(arg instanceof Boolean);
  }

  /**
   * The characters the upper case adds to a text. Taken a piece at a time: {@code String.toUpperCase} extends its buffer
   * by a character for each one that becomes two, copying it whole, which a long run of them makes quadratic. A piece
   * that ends inside a sequence the locale would shorten only measures a character more.
   */
  private static long upperCaseGrowth(final String text, final Locale locale) {
    final int length = text.length();
    long growth = 0;
    for (int from = 0; from < length; from += UPPER_CASE_PIECE) {
      final int to = Math.min(from + UPPER_CASE_PIECE, length);
      growth += text.substring(from, to).toUpperCase(locale).length() - (to - from);
    }
    return Math.max(growth, 0);
  }

  /** Whether the size of what {@code %s} prints is known only by building it: anything but text and numbers. */
  private static boolean isTextBuiltToBeMeasured(final char kind, final Object arg) {
    return kind == 's' && arg != null && !(arg instanceof CharSequence) && !(arg instanceof Number) && !isScalar(arg);
  }

  private static boolean isScalar(final Object arg) {
    return arg instanceof Boolean || arg instanceof Character || (arg instanceof Number && !(arg instanceof BigInteger)
        && !(arg instanceof BigDecimal));
  }

  /**
   * The characters a conversion (lower case) writes for an argument, before the precision and the width are applied:
   * exact for text, an upper estimate for the rest, and never by building the text. A conversion the argument does not
   * fit is left to the formatter, which refuses it.
   */
  private static long textSize(final char kind, final Object arg) {
    switch (kind) {
    case 's':
      if (arg == null)
        return 4;
      if (arg instanceof CharSequence text)
        return text.length();
      if (arg instanceof BigInteger integer)
        return decimalDigits(integer.bitLength()) + 1;
      if (arg instanceof BigDecimal decimal)
        // The digits, with a sign, a point and an exponent or a few leading zeros
        return decimal.precision() + SCALAR_TEXT_SIZE;
      return SCALAR_TEXT_SIZE;
    case 'd', 'o', 'x':
      if (arg instanceof BigInteger integer)
        return (kind == 'o' ? integer.bitLength() / 3 + 1 : decimalDigits(integer.bitLength())) + 1;
      return SCALAR_TEXT_SIZE;
    case 'f':
      // The integer digits, a sign and a point: the fraction digits are the precision
      if (arg instanceof BigDecimal decimal)
        return Math.abs((long) decimal.precision() - decimal.scale()) + 2;
      if (arg instanceof Double || arg instanceof Float) {
        final int exponent = Math.getExponent(((Number) arg).doubleValue());
        return (exponent > 0 ? decimalDigits(exponent) : 1) + 2;
      }
      return SCALAR_TEXT_SIZE;
    case 't':
      return TEMPORAL_TEXT_SIZE;
    case 'c':
      return 2;
    case 'b':
      return 5;
    case 'h':
      return 8;
    default:
      // %e, %g and %a: one digit, the exponent and the sign, beside the precision
      return SCALAR_TEXT_SIZE;
    }
  }

  /** The decimal digits of a number of that many bits, within a few digits in a million. */
  private static long decimalDigits(final int bits) {
    // log10(2) = 0.30103 and 1233 / 4096 = 0.30103
    return (bits * 1233L >>> 12) + 1;
  }

  /** The start of a pattern, for an error message: the one refused for its length is not repeated in full. */
  private static String abbreviate(final String format) {
    return format.length() <= 200 ? format : format.substring(0, 200) + "...";
  }

  /**
   * Rejects a {@link BigDecimal} that a fixed-point ({@code %f}) conversion would spell out in more than
   * {@link #MAX_FORMAT_WIDTH} digits. {@code 1E+999999999} is a few bytes that {@code %f} turns into a billion digits
   * ({@code OutOfMemoryError}), {@code 1E-999999999} makes the formatter round at a scale of 999999999, and a zero is no
   * exception: {@code 0E+999999999} is padded with as many zeros. {@code |precision - scale|} counts the integer digits of
   * the first and the leading zeros of the second, in a long because the scale alone spans the whole int range.
   * {@code %s}, {@code %e} and {@code %g} print the scientific text and stay valid (issue #12837).
   */
  private static void checkPlainDigits(final String caller, final String format, final BigDecimal decimal) {
    if (Math.abs((long) decimal.precision() - decimal.scale()) > MAX_FORMAT_WIDTH)
      throw new IllegalArgumentException(
          caller + "() format '" + format + "' would spell a decimal with the exponent " + (
              (long) decimal.precision() - decimal.scale() - 1) + " out in more than " + MAX_FORMAT_WIDTH
              + " characters, over the limit");
  }

  /**
   * The {@link java.util.Formatter} flag set, {@code '<'} (reuse the previous argument) included. Leaving that one
   * out made the scanner above mistake it for the conversion character and stop before it ever reached the width, so
   * {@code %s%<2000000s} bypassed the ceiling entirely.
   */
  private static boolean isFormatFlag(final char c) {
    return c == '-' || c == '#' || c == '+' || c == ' ' || c == '0' || c == ',' || c == '(' || c == '<';
  }
}
