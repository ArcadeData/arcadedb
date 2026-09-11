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
package com.arcadedb.integration.importer;

import com.arcadedb.GlobalConfiguration;
import com.arcadedb.integration.importer.format.CSVImporterFormat;
import com.arcadedb.integration.importer.format.FormatImporter;
import com.arcadedb.integration.importer.format.GloVeImporterFormat;
import com.arcadedb.integration.importer.format.JSONImporterFormat;
import com.arcadedb.integration.importer.format.JsonlImporterFormat;
import com.arcadedb.integration.importer.format.Neo4jImporterFormat;
import com.arcadedb.integration.importer.format.OrientDBImporterFormat;
import com.arcadedb.integration.importer.format.RDFImporterFormat;
import com.arcadedb.integration.importer.format.Word2VecImporterFormat;
import com.arcadedb.integration.importer.format.XMLImporterFormat;
import com.arcadedb.log.LogManager;
import com.arcadedb.utility.FileUtils;

import java.io.BufferedInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.lang.reflect.InvocationTargetException;
import java.net.HttpURLConnection;
import java.net.URL;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Callable;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.logging.Level;
import java.util.zip.GZIPInputStream;
import java.util.zip.ZipEntry;
import java.util.zip.ZipInputStream;

public class SourceDiscovery {
  /**
   * The number of leading comment or blank lines content sniffing is willing to skip before it concludes the source
   * is not a commented data file. A bound and not a limit anyone is expected to reach: without it a source that is
   * comments all the way down would be read end to end just to decide its format.
   * <p>
   * Raising it costs more than it looks: {@link #rewindTo} replays the skipped prefix after a {@link Parser#reset()},
   * and for a remote source that reset re-opens the connection - so the prefix is fetched TWICE, and this constant is
   * what bounds the second fetch.
   */
  private static final int    MAX_COMMENT_LINES  = 10_000;
  private static final String RESOURCE_SEPARATOR = ":::";
  private static final String FILE_PREFIX        = "file://";
  private static final String CLASSPATH_PREFIX   = "classpath://";
  private              String  url;
  private final        Boolean allowLocalUrls;
  private              long    limitBytes         = 10000000;
  private              long    limitEntries       = 0;

  public SourceDiscovery(final String url) {
    this(url, null);
  }

  /**
   * @param allowLocalUrls explicit override for whether a remote fetch may reach a private/loopback/link-local host,
   *                       resolved by a caller that already validated the URL against its own policy (issue #6474).
   *                       {@code null} (the default via {@link #SourceDiscovery(String)}) falls back to {@link
   *                       GlobalConfiguration#SERVER_SECURITY_IMPORT_BLOCK_LOCAL_NETWORKS}.
   */
  public SourceDiscovery(final String url, final Boolean allowLocalUrls) {
    this.url = url;
    this.allowLocalUrls = allowLocalUrls;
  }

  public SourceSchema getSchema(final ImporterSettings settings,
      final AnalyzedEntity.EntityType entityType,
      final AnalyzedSchema analyzedSchema,
      final ConsoleLogger logger) throws IOException {
    LogManager.instance().log(this, Level.INFO, "Analyzing url: %s...", url);

    final Source source = getSource();

    final Parser parser = new Parser(source, 0);

    final FormatImporter formatImporter = analyzeSourceContent(parser, entityType, settings, logger);
    parser.reset();

    SourceSchema sourceSchema = null;

    sourceSchema = formatImporter.analyze(entityType, parser, settings, analyzedSchema);
    LogManager.instance()
        .log(this, Level.INFO, "Recognized format %s (parsingLimitBytes=%s parsingLimitEntries=%d)", formatImporter.getFormat(),
            FileUtils.getSizeAsString(limitBytes), limitEntries);

    if (sourceSchema != null && !sourceSchema.getOptions().isEmpty()) {
      for (final Map.Entry<String, String> o : sourceSchema.getOptions().entrySet())
        LogManager.instance().log(this, Level.INFO, "- %s = %s", o.getKey(), o.getValue());
    }

    source.close();

    return sourceSchema;
  }

  public Source getSource() throws IOException {
    final Source source;
    if (url.startsWith("http://") || url.startsWith("https://")) {
      // NOTE: no separate validate-then-fetch step here. Validating the URL and then handing the raw string to
      // URL.openConnection() checks one thing and connects to another (the connection re-resolves the name and follows
      // redirects unvalidated); getSourceFromURL now validates inside the connection open itself, on every hop.
      source = getSourceFromURL(url);
    } else {
      ImportSecurityValidator.validateLocalURL(url);
      source = getSourceFromFile(url);
    }
    return source;
  }

  private Source getSourceFromURL(final String url) throws IOException {
    final int sep = url.lastIndexOf(RESOURCE_SEPARATOR);
    final String urlPath = sep > -1 ? url.substring(0, sep) : url;
    final String resource = sep > -1 ? url.substring(sep + RESOURCE_SEPARATOR.length()) : null;

    // Every connection is opened through ImportSecurityValidator.openRemoteConnection, which re-runs the scheme and
    // address checks on each redirect hop instead of letting HttpURLConnection follow redirects unvalidated. The reset
    // callback below re-opens the source and MUST go through the same path: it previously built a second raw
    // connection with no validation at all, so a redirect (or a rebind) on that second fetch was entirely unchecked
    // even once the first one was validated (GHSA-4w2m-77c8-83mw).
    //
    // blockLocalNetworks resolves allowLocalUrls once, up front, so both the initial fetch and the reset callback's
    // re-fetch below agree with each other (and with whatever caller-resolved policy allowLocalUrls carries - #6474).
    final boolean blockLocalNetworks = allowLocalUrls != null ?
        !allowLocalUrls : GlobalConfiguration.SERVER_SECURITY_IMPORT_BLOCK_LOCAL_NETWORKS.getValueAsBoolean();

    final HttpURLConnection connection = ImportSecurityValidator.openRemoteConnection(urlPath, blockLocalNetworks);

    return getSourceFromContent(new BufferedInputStream(connection.getInputStream()), connection.getContentLengthLong(), resource,
        source -> {
          try {
            source.inputStream.close();
            connection.disconnect();

            final HttpURLConnection connection1 = ImportSecurityValidator.openRemoteConnection(urlPath, blockLocalNetworks);

            if (source.inputStream instanceof GZIPInputStream)
              source.inputStream = new GZIPInputStream(connection1.getInputStream(), 2048);
            else if (source.inputStream instanceof ZipInputStream) {
              final ZipInputStream zip = new ZipInputStream(connection1.getInputStream());
              positionZipStream(zip, resource);
              source.inputStream = zip;
            } else
              source.inputStream = new BufferedInputStream(connection1.getInputStream());
          } catch (final Exception e) {
            throw new ImportException("Error on reset remote resource", e);
          }
          return null;
        }, () -> {
          connection.disconnect();
          return null;
        });
  }

  private Source getSourceFromFile(final String path) throws IOException {
    final int sep = path.lastIndexOf(RESOURCE_SEPARATOR);
    String filePath = sep > -1 ? path.substring(0, sep) : path;
    final String resource = sep > -1 ? path.substring(sep + RESOURCE_SEPARATOR.length()) : null;

    if (filePath.startsWith(FILE_PREFIX))
      filePath = filePath.substring(FILE_PREFIX.length());
    else if (filePath.startsWith(CLASSPATH_PREFIX)) {
      filePath = filePath.substring(CLASSPATH_PREFIX.length());
      filePath = getClass().getClassLoader().getResource(filePath).getFile();
    }

    final String resolvedPath = filePath;
    final File file = new File(resolvedPath);

    final InputStream fis = openLocalStream(file, resolvedPath);

    return getSourceFromContent(fis, file.length(), resource, source -> {
      try {
        source.inputStream.close();
        if (source.inputStream instanceof GZIPInputStream)
          source.inputStream = new GZIPInputStream(openLocalStream(file, resolvedPath), 2048);
        else if (source.inputStream instanceof ZipInputStream) {
          final ZipInputStream zip = new ZipInputStream(openLocalStream(file, resolvedPath));
          positionZipStream(zip, resource);
          source.inputStream = zip;
        } else
          source.inputStream = openLocalStream(file, resolvedPath);
      } catch (final IOException e) {
        throw new ImportException("Error on reset local resource", e);
      }
      return null;
    }, () -> {
      fis.close();
      return null;
    });
  }

  /**
   * Opens the local source the same way for the initial read and for every {@link Source#reset()}: as a file when one
   * exists at that path, otherwise as a classpath resource. Reset used to unconditionally re-open a
   * {@link FileInputStream}, which cannot work for the classpath fallback.
   */
  private InputStream openLocalStream(final File file, final String filePath) throws IOException {
    if (file.exists())
      return new BufferedInputStream(new FileInputStream(file));

    final InputStream stream = getClass().getClassLoader().getResourceAsStream(filePath);
    if (stream == null)
      throw new FileNotFoundException(filePath);

    return stream;
  }

  /**
   * The delimiter a CSV source is parsed with when content sniffing found {@code detected}: the user's own delimiter when
   * there is one, the guess otherwise. A guess never overrides an explicit choice - that is what overwrote the user's
   * {@code -delimiter} / {@code WITH delimiter = ...} whenever the file extension did not short-circuit detection (issue
   * #6946, the sibling of #6811) - and a discarded guess is logged so the "best separator candidate" line just above it
   * does not read as authoritative.
   */
  static String resolveDelimiter(final String userDelimiter, final char detected) {
    if (userDelimiter == null)
      return String.valueOf(detected);
    if (!userDelimiter.equals(String.valueOf(detected)))
      LogManager.instance().log(SourceDiscovery.class, Level.INFO,
          "Detected separator '%s' discarded: using the delimiter '%s' explicitly set by the user", detected, userDelimiter);
    return userDelimiter;
  }

  /**
   * Decides the format of a source: the file type the user named or the extension implies first, then the content
   * itself through {@link #analyzeChar} and {@link #analyzeText}.
   * <p>
   * Package-private rather than private so that a test can assert the format a source is recognised as WITHOUT
   * going on to parse it, the way {@link #getSchema} does. The two are not the same question: the comment prefix
   * this class skips is handed on intact to the format it picks, and only the delimited-text formats skip anything
   * there - univocity's single comment character, {@code #} (issue #7490). For a commented XML or JSON source, or
   * for a {@code //}-commented source of any kind, only the first question has an answer.
   */
  FormatImporter analyzeSourceContent(final Parser parser, final AnalyzedEntity.EntityType entityType,
      final ImporterSettings settings,
      final ConsoleLogger logger) throws IOException {

    String knownFileType = null;
    String knownDelimiter = null;

    switch (entityType) {
    case DOCUMENT:
      knownFileType = settings.documentsFileType != null ? settings.documentsFileType : getFileTypeByExtension(settings.documents);
      knownDelimiter = settings.documentsDelimiter;
      break;

    case VERTEX:
      knownFileType = settings.verticesFileType != null ?
          settings.verticesFileType :
          getFileTypeByExtension(settings.vertices != null ? settings.vertices : settings.url);
      knownDelimiter = settings.verticesDelimiter;
      break;

    case EDGE:
      knownFileType = settings.edgesFileType != null ?
          settings.edgesFileType :
          getFileTypeByExtension(settings.edges != null ? settings.edges : settings.url);
      knownDelimiter = settings.edgesDelimiter;
      break;

    case DATABASE:
      // NO PER-ENTITY SETTINGS: THE GENERIC `delimiter` OPTION IS THE ONE THE USER CAN SET ON THIS FORM
      // (-delimiter / IMPORT DATABASE ... WITH delimiter = ';'), SO READ IT BACK FROM THE OPTIONS AND FALL BACK TO
      // -documentsDelimiter WHEN IT IS ABSENT (ISSUE #6811)
      knownFileType = getFileTypeByExtension(settings.url);
      final Object genericDelimiter = settings.options.get("delimiter");
      knownDelimiter = genericDelimiter != null ? genericDelimiter.toString() : settings.documentsDelimiter;
      break;

    default:
      throw new IllegalArgumentException("entityType '" + entityType + "' not supported");
    }

    // THE USER'S DELIMITER FOR THIS ENTITY, RESOLVED ONCE AND KEPT LOCAL: THE PER-ENTITY OVERRIDE FIRST, THEN THE GENERIC
    // `delimiter` OPTION (-delimiter / IMPORT DATABASE ... WITH delimiter = ';'). AN ABSENT PER-ENTITY DELIMITER IS NOT A
    // VALUE (CLOBBERING THE OPTION WITH NULL MADE EVERY NON-COMMA CSV UNIMPORTABLE, ISSUE #6811). IT IS HANDED TO THE
    // FORMAT RATHER THAN WRITTEN INTO settings.options, WHICH ONE IMPORT SHARES ACROSS ITS DOCUMENTS, VERTICES AND EDGES
    // FILES: A DELIMITER SETTLED OR DETECTED FOR ONE OF THEM MUST NOT STAND IN FOR THE NEXT ONE'S (ISSUE #6946)
    final String userDelimiter = knownDelimiter != null ? knownDelimiter : settings.getValue("delimiter", null);

    if (knownFileType != null) {
      if ("csv".equalsIgnoreCase(knownFileType)) {
        return new CSVImporterFormat(userDelimiter);
      } else if ("json".equalsIgnoreCase(knownFileType)) {
        return new JSONImporterFormat();
      } else if ("jsonl".equalsIgnoreCase(knownFileType)) {
        return new JsonlImporterFormat();
      } else if ("xml".equalsIgnoreCase(knownFileType)) {
        return new XMLImporterFormat();
      } else if ("graphml".equalsIgnoreCase(knownFileType)) {

        try {
          final Class<FormatImporter> clazz = (Class<FormatImporter>) Class.forName(
              "com.arcadedb.gremlin.integration.importer.format.GraphMLImporterFormat");
          return clazz.getConstructor().newInstance();
        } catch (final ClassNotFoundException | InvocationTargetException | InstantiationException | IllegalAccessException |
                       NoSuchMethodException e) {
          LogManager.instance().log(this, Level.SEVERE, "Impossible to find importer for 'graphml' ", e);
        }

      } else if ("graphson".equalsIgnoreCase(knownFileType)) {

        try {
          final Class<FormatImporter> clazz = (Class<FormatImporter>) Class.forName(
              "com.arcadedb.gremlin.integration.importer.format.GraphSONImporterFormat");
          return clazz.getConstructor().newInstance();
        } catch (final ClassNotFoundException | InvocationTargetException | InstantiationException | IllegalAccessException |
                       NoSuchMethodException e) {
          LogManager.instance().log(this, Level.SEVERE, "Impossible to find importer for 'graphson' ", e);
        }

      } else {
        LogManager.instance()
            .log(this, Level.WARNING, "File type '%s' is not supported. Trying to understand file type...", knownFileType);
      }
    }

    parser.nextChar();

    FormatImporter format = analyzeChar(parser, settings, userDelimiter);
    if (format != null)
      return format;

    return analyzeText(parser, settings, logger, userDelimiter);
  }

  /**
   * The content sniffing that follows {@link #analyzeChar}: the leading comment and blank lines are skipped, the
   * first line that carries content is dispatched on, and failing that it is read for a separator to decide between
   * the delimited-text formats.
   *
   * @param userDelimiter the delimiter the user supplied for this entity, or null - a detected one yields to it
   */
  private FormatImporter analyzeText(final Parser parser, final ImporterSettings settings, final ConsoleLogger logger,
      final String userDelimiter) throws IOException {
    FormatImporter format = null;

    // SKIP THE LEADING COMMENT LINES, THEN DISPATCH ON THE FIRST CHARACTER OF THE LINE THAT FOLLOWS THEM. THE TWO
    // LOOPS THIS REPLACES DID NEITHER, SO THE SEPARATOR SCAN BELOW READ THE COMMENT ITSELF AS THE FIRST DATA LINE
    // (ISSUE #7347)
    final long commentChars = skipComments(parser);

    if (commentChars > 0) {
      // analyzeSourceContent() RAN THE FIRST-CHARACTER DISPATCH ON THE COMMENT'S OWN FIRST CHARACTER, SO THIS IS THE
      // CALL THAT LETS A COMMENTED N-TRIPLES, XML OR JSON SOURCE BE RECOGNISED AS ONE. GUARDED ON commentChars SO A
      // SOURCE WITHOUT COMMENTS IS NOT DISPATCHED ON TWICE, THE SECOND TIME FROM WHEREVER THE FIRST CALL LEFT THE
      // PARSER
      format = analyzeChar(parser, settings, userDelimiter);
      if (format != null)
        return format;
    }

    // BACK TO THE FIRST CHARACTER OF THE FIRST LINE THAT CARRIES CONTENT, WHICH THE SEPARATOR SCAN BELOW HAS TO SEE:
    // analyzeChar() CONSUMES THE LINE IT INSPECTS WHENEVER IT DISPATCHES ON IT. WITH NO COMMENTS THIS REWINDS TO THE
    // START OF THE SOURCE, WHICH IS WHAT THE reset() IT REPLACES DID
    rewindTo(parser, commentChars);

    try {
      // CHECK FOR CSV-LIKE FILES
      final Map<Character, AtomicInteger> candidateSeparators = new HashMap<>();

      final StringBuilder line = new StringBuilder();
      while (parser.isAvailable() && parser.nextChar() != '\n') {
        final char c = parser.getCurrentChar();
        line.append(c);

        if (isSeparator(c)) {
          final AtomicInteger sep = candidateSeparators.get(c);
          if (sep == null) {
            candidateSeparators.put(c, new AtomicInteger(1));
          } else
            sep.incrementAndGet();
        }
      }

      if (!candidateSeparators.isEmpty()) {
        final ArrayList<Map.Entry<Character, AtomicInteger>> list = new ArrayList(candidateSeparators.entrySet());
        list.sort((o1, o2) -> {
          if (o1.getValue().get() == o2.getValue().get())
            return 0;
          return o1.getValue().get() < o2.getValue().get() ? 1 : -1;
        });

        final Map.Entry<Character, AtomicInteger> bestSeparator = list.getFirst();

        // A DELIMITER THE USER SUPPLIED SETTLES THE QUESTION THE SNIFFING IS ASKING: THE FILE IS DELIMITED TEXT WITH
        // THAT DELIMITER, SO THE SPACE-SEPARATED VECTOR FORMATS ARE NOT A CANDIDATE HOWEVER MANY SPACES THE FIRST LINE
        // CARRIES INSIDE ITS VALUES (ISSUE #6946)
        if (bestSeparator.getKey() == ' ' && userDelimiter == null) {
          // CHECK IF IS A VECTOR EMBEDDING TEXT FILE
          final StringBuilder line2 = new StringBuilder();
          while (parser.isAvailable() && parser.nextChar() != '\n')
            line2.append(parser.getCurrentChar());

          final String[] fields1 = line.toString().split(" ");
          final String[] fields2 = line2.toString().split(" ");

          if (fields1.length == 2 && fields2.length > 2)
            format = new Word2VecImporterFormat();
          else if (fields1.length == fields2.length)
            format = new GloVeImporterFormat();
        }

        if (format == null) {
          LogManager.instance()
              .log(this, Level.INFO, "Best separator candidate='%s' (all candidates=%s)", bestSeparator.getKey(), list);
          format = new CSVImporterFormat(resolveDelimiter(userDelimiter, bestSeparator.getKey()));
        }
      }

    } finally {
      if (format != null)
        logger.logLine(1, "Recognized format %s", format.getFormat());
    }

    if (format != null)
      return format;

    // UNKNOWN
    throw new ImportException("Cannot determine the file type. If it is a CSV file, please specify the header via settings");
  }

  /**
   * The character separating the terms of an RDF triple line, or {@code 0} when {@code line} is not one
   * (issue #7346).
   * <p>
   * The line is recognised by its SHAPE. The grammar the four private helpers below implement between them,
   * in one place:
   * <pre>
   *   statement := subject SEP predicate SEP object (SEP '.')?
   *   subject   := IRI | blank
   *   predicate := IRI
   *   object    := IRI | blank | literal
   *   IRI       := '&lt;' (any char N-Triples permits in an IRI)+ '&gt;'
   *   blank     := "_:" [A-Za-z0-9_-]+
   *   literal   := '"' (escaped char | any char but '"')* '"' ( '@' langTag | "^^" IRI )?
   *   SEP       := one or more of ONE character from {@link #isTermSeparator}
   * </pre>
   * The whole line has to be consumed by it, so an XML element or a delimited-text row cannot satisfy it.
   * <p>
   * What it replaces was "collect every character outside {@code <...>} and require them all to be equal", whose
   * loop bound of {@code size() - 1} tolerated exactly one trailing character - the {@code .} of a canonical
   * triple. Both reported failures follow from that single tolerated character: a CRLF line ending adds a second
   * one, and a literal object puts every character of its own text outside the brackets. Both fell through to the
   * CSV fallback, which reported a {@code NumberFormatException} about an IRI.
   * <p>
   * The separator is RETURNED rather than assumed, because it is also the source's field delimiter:
   * {@link RDFImporterFormat} inherits {@link com.arcadedb.integration.importer.format.CSVImporterFormat}'s parser
   * construction, whose fallback is a comma (issue #7315). It must be the SAME character in both gaps, which is
   * what the old uniqueness test was really testing, and it is taken from {@link #isTermSeparator}'s set rather
   * than fixed to whitespace: a comma- or semicolon-separated triple file was accepted before this change and
   * still is.
   */
  static char nTriplesSeparator(final CharSequence line) {
    final int end = endOfContent(line);
    if (end == 0)
      return 0;

    final int subjectEnd = endOfSubjectOrObject(line, 0, end, false);
    if (subjectEnd <= 0 || subjectEnd >= end)
      return 0;

    final char separator = line.charAt(subjectEnd);
    if (!isTermSeparator(separator))
      return 0;

    final int predicateEnd = endOfIri(line, skipRunOf(line, subjectEnd, end, separator), end);
    if (predicateEnd < 0 || predicateEnd >= end || line.charAt(predicateEnd) != separator)
      return 0;

    int pos = endOfSubjectOrObject(line, skipRunOf(line, predicateEnd, end, separator), end, true);
    if (pos < 0)
      return 0;

    // The terminating '.' is optional: the detection this replaces accepted a file without one, and three terms
    // are already unambiguous. What is not optional is that nothing else follows it.
    if (pos < end) {
      if (line.charAt(pos) != separator)
        return 0;
      pos = skipRunOf(line, pos, end, separator);
      if (pos >= end || line.charAt(pos) != '.')
        return 0;
      ++pos;
    }

    return pos == end ? separator : 0;
  }

  /**
   * Whether the line opens with a subject term and an IRI predicate, i.e. it is an RDF statement whatever went
   * wrong after them. Used only to phrase the diagnostic that the CSV fallback cannot phrase for itself.
   */
  static boolean looksLikeRdfStatementStart(final CharSequence line) {
    final int end = endOfContent(line);
    final int subjectEnd = endOfSubjectOrObject(line, 0, end, false);
    if (subjectEnd <= 0 || subjectEnd >= end || !isTermSeparator(line.charAt(subjectEnd)))
      return false;
    return endOfIri(line, skipRunOf(line, subjectEnd, end, line.charAt(subjectEnd)), end) > 0;
  }

  /**
   * The characters that may stand between two terms, which are also the ones an RDF file is plausibly delimited
   * by. Deliberately not the full {@link #isSeparator} set: {@code _} opens a blank node and {@code -} belongs
   * inside a language tag, so either would be ambiguous with the term that follows, and {@code .} is the
   * statement terminator.
   */
  private static boolean isTermSeparator(final char c) {
    return c == ' ' || c == '\t' || c == ',' || c == ';' || c == '|';
  }

  /**
   * The length of {@code line} with its trailing whitespace removed - the {@code \r} of a CRLF line ending above
   * all, which the parser leaves on the line because it splits on {@code \n} alone.
   */
  private static int endOfContent(final CharSequence line) {
    int end = line.length();
    while (end > 0 && (line.charAt(end - 1) == ' ' || line.charAt(end - 1) == '\t' || line.charAt(end - 1) == '\r'))
      --end;
    return end;
  }

  /** The first index after the run of {@code separator} starting at {@code from}. */
  private static int skipRunOf(final CharSequence line, final int from, final int end, final char separator) {
    int pos = from;
    while (pos < end && line.charAt(pos) == separator)
      ++pos;
    return pos;
  }

  /**
   * The index just past an {@code <IRI>} starting at {@code pos}, or {@code -1}. The characters refused inside the
   * brackets are the ones N-Triples itself forbids there, which is what keeps an XML tag - {@code <a>} followed by
   * text and {@code </a>} - from passing as a predicate.
   */
  private static int endOfIri(final CharSequence line, final int pos, final int end) {
    if (pos >= end || line.charAt(pos) != '<')
      return -1;
    for (int i = pos + 1; i < end; ++i) {
      final char c = line.charAt(i);
      if (c == '>')
        return i > pos + 1 ? i + 1 : -1;
      if (c <= ' ' || c == '<' || c == '"' || c == '{' || c == '}' || c == '|' || c == '^' || c == '`' || c == '\\')
        return -1;
    }
    return -1;
  }

  /** The index just past a {@code _:blank} node label starting at {@code pos}, or {@code -1}. */
  private static int endOfBlankNode(final CharSequence line, final int pos, final int end) {
    if (pos + 2 >= end || line.charAt(pos) != '_' || line.charAt(pos + 1) != ':')
      return -1;
    int i = pos + 2;
    while (i < end && (Character.isLetterOrDigit(line.charAt(i)) || line.charAt(i) == '_' || line.charAt(i) == '-'))
      ++i;
    return i > pos + 2 ? i : -1;
  }

  /**
   * The index just past a term starting at {@code pos}, or {@code -1}. An {@code <IRI>} and a {@code _:blank} node
   * are terms in both positions; a quoted literal is one only as an object, which is the single difference between
   * the two roles and the reason they share this method.
   */
  private static int endOfSubjectOrObject(final CharSequence line, final int pos, final int end,
      final boolean literalAllowed) {
    final int iri = endOfIri(line, pos, end);
    if (iri > 0)
      return iri;
    final int blank = endOfBlankNode(line, pos, end);
    if (blank > 0)
      return blank;
    return literalAllowed ? endOfLiteral(line, pos, end) : -1;
  }

  /**
   * The index just past a {@code "quoted literal"} and its optional {@code @lang} or {@code ^^<datatype>} suffix,
   * or {@code -1}. A backslash escapes the next character, so an embedded {@code \"} does not close the literal.
   */
  private static int endOfLiteral(final CharSequence line, final int pos, final int end) {
    if (pos >= end || line.charAt(pos) != '"')
      return -1;

    int i = pos + 1;
    while (i < end) {
      final char c = line.charAt(i);
      if (c == '\\') {
        i += 2;
        continue;
      }
      if (c == '"')
        break;
      ++i;
    }
    if (i >= end)
      return -1;

    int after = i + 1;
    if (after < end && line.charAt(after) == '@') {
      int tag = after + 1;
      while (tag < end && (Character.isLetterOrDigit(line.charAt(tag)) || line.charAt(tag) == '-'))
        ++tag;
      return tag > after + 1 ? tag : -1;
    }
    if (after + 1 < end && line.charAt(after) == '^' && line.charAt(after + 1) == '^')
      return endOfIri(line, after + 2, end);
    return after;
  }

  private boolean isSeparator(final char c) {
    // ';' IS THE OTHER MAINSTREAM CSV DELIMITER (LOCALES WHERE ',' IS THE DECIMAL SEPARATOR). WITHOUT IT, A
    // SEMICOLON-SEPARATED FILE WHOSE EXTENSION ISN'T ".csv" PRODUCED NO CANDIDATE AT ALL AND THE IMPORT DIED WITH
    // "Cannot determine the file type" (ISSUE #6811)
    return c == ' ' || c == '\t' || c == ',' || c == ';' || c == '|' || c == '-' || c == '_';
  }

  private String getFileTypeByExtension(final String fileName) {
    return switch (getFormatFromExtension(fileName)) {
      case "csv" -> "csv";
      case "graphml" -> "graphml";
      case "graphson" -> "graphson";
      case "jsonl" -> "jsonl";
      default -> null;
    };
  }

  /**
   * Consumes the rest of the line the parser is on AND the terminator that ends it, so that {@link
   * Parser#getCurrentChar()} holds the first character of the NEXT line when this returns - or the last character of
   * the source when it has no trailing terminator.
   * <p>
   * Stopping on the {@code '\n'} instead is what made comment skipping a no-op for as long as it has existed
   * (issue #7347): every caller inspects {@code getCurrentChar()} straight afterwards, and {@code '\n'} answers no
   * question either of them asks.
   * <p>
   * All three terminators end a line here - {@code "\n"}, {@code "\r\n"} and a bare {@code "\r"}. The bare
   * {@code '\r'} is in because without it a source that uses it as its only terminator has no line ends at all as
   * far as this method is concerned, so the first comment line swallows the whole source and sniffing is left with
   * nothing to look at. The rest of the file's line handling is still {@code '\n'}-only, so such a source is not
   * yet READ correctly - but its comment block is skipped, which is this method's job.
   *
   * @return the number of characters read, so the caller can rewind to exactly this position with {@link #rewindTo}
   */
  private long skipLine(final Parser parser) throws IOException {
    long consumed = 0;
    while (parser.getCurrentChar() != '\n' && parser.getCurrentChar() != '\r') {
      if (!parser.isAvailable())
        // NO TRAILING TERMINATOR: THE SOURCE ENDS ON THIS LINE
        return consumed;
      parser.nextChar();
      ++consumed;
    }

    if (parser.getCurrentChar() == '\r') {
      if (!parser.isAvailable())
        return consumed;
      parser.nextChar();
      ++consumed;
      if (parser.getCurrentChar() != '\n')
        // A BARE '\r' ENDED THE LINE, SO THE PARSER IS ALREADY ON THE NEXT ONE
        return consumed;
    }

    if (parser.isAvailable()) {
      parser.nextChar();
      ++consumed;
    }
    return consumed;
  }

  /**
   * Advances the parser past every leading {@code #} comment line, {@code //} comment line and blank line, leaving
   * {@link Parser#getCurrentChar()} on the first character of the first line that carries content to sniff.
   * <p>
   * The two comment markers are handled by one loop rather than by two, because they interleave in real files and
   * because the second of the two loops this replaces could only ever have run after the first had already rewound
   * the source out from under it.
   *
   * @return the number of characters consumed - the offset of the first data line, and the argument {@link #rewindTo}
   * takes to come back to it
   */
  private long skipComments(final Parser parser) throws IOException {
    long skipped = 0;
    int lines = 0;

    while (parser.isAvailable()) {
      final char first = parser.getCurrentChar();
      if (first != '#' && first != '/' && first != '\n' && first != '\r')
        break;

      if (++lines > MAX_COMMENT_LINES) {
        // A SOURCE WHOSE FIRST MAX_COMMENT_LINES LINES ARE ALL COMMENTS IS NOT A COMMENTED DATA FILE, AND READING IT
        // TO ITS END LOOKING FOR ONE WOULD MAKE FORMAT SNIFFING COST THE WHOLE FILE. REPORTING NO COMMENT PREFIX AT
        // ALL PUTS THE SCAN BACK ON LINE 1, WHICH IS WHERE IT LOOKED BEFORE COMMENT SKIPPING WORKED AT ALL - THE
        // CALLER REWINDS TO THE OFFSET THIS RETURNS BEFORE READING ANYTHING.
        LogManager.instance().log(this, Level.WARNING,
            "The source opens with more than %d comment or blank lines: content sniffing gives up skipping them and "
                + "analyzes the first line as data", MAX_COMMENT_LINES);
        return 0;
      }

      if (first == '#' || first == '\n' || first == '\r') {
        // A BLANK LINE CARRIES NO CONTENT TO SNIFF EITHER, AND ONE BETWEEN THE COMMENT BLOCK AND THE DATA IS
        // ORDINARY. SKIPPING THE COMMENTS AND THEN STOPPING ON THE BLANK LINE BELOW THEM WOULD HAND THE SEPARATOR
        // SCAN AN EMPTY LINE, WHICH PRODUCES NO CANDIDATE AND SO "Cannot determine the file type" - A WORSE ANSWER
        // THAN THE WRONG-DELIMITER ONE THAT SHAPE USED TO GET. THE SAME TEST ALSO SKIPS A BLANK FIRST LINE, WHICH
        // USED TO REACH THE SCAN AND FAIL THERE FOR THE SAME REASON.
        skipped += skipLine(parser);
        continue;
      }

      // '//' OPENS A COMMENT LINE, A LONE '/' IS DATA - A CSV COLUMN HOLDING A PATH, SAY. TELLING THEM APART NEEDS
      // THE SECOND CHARACTER, AND THE PARSER CANNOT PUSH ONE BACK, SO A LONE '/' IS UNDONE BY REWINDING TO WHERE
      // THIS LINE STARTED AND RE-READING ITS FIRST CHARACTER.
      if (!parser.isAvailable())
        break;

      if (parser.nextChar() != '/') {
        rewindTo(parser, skipped);
        parser.nextChar();
        break;
      }

      // +1 FOR THE SECOND '/', WHICH skipLine() DOES NOT COUNT BECAUSE IT IS ALREADY PAST IT
      skipped += skipLine(parser) + 1;
    }

    return skipped;
  }

  /**
   * Rewinds the parser to the start of the source and reads {@code offset} characters back, so that the NEXT
   * {@link Parser#nextChar()} returns the character at {@code offset}.
   * <p>
   * Counted in characters rather than in bytes because that is what {@link #skipComments} counts and what a
   * multi-byte encoding makes different: {@link Parser#getPosition()} is a byte-ish counter that the reader's own
   * bulk reads also advance, so it cannot be used to seek.
   */
  private void rewindTo(final Parser parser, final long offset) throws IOException {
    parser.reset();
    for (long i = 0; i < offset; ++i)
      parser.nextChar();
  }

  /**
   * The first-character dispatch: {@code <} opens either an RDF triple line or XML, {@code _} can only open an
   * N-Triples blank-node subject, <code>{</code> opens JSON.
   *
   * @param userDelimiter the delimiter the user supplied for this entity, or null - a detected one yields to it, the
   *                      same way {@link #analyzeText} treats its own guess
   */
  private FormatImporter analyzeChar(final Parser parser, final ImporterSettings settings, final String userDelimiter)
      throws IOException {
    char currentChar = parser.getCurrentChar();
    if (currentChar == '<' || currentChar == '_') {
      // READ THE FIRST LINE, COUNTING THE CHARACTERS THAT FALL OUTSIDE <...> - WHICH IS ALL THE XML ARM BELOW
      // NEEDS. THE LINE ITSELF IS KEPT TOO, BECAUSE THE RDF ARM DECIDES ON THE SHAPE OF THE WHOLE STATEMENT AND
      // NOT ON THE CHARACTERS BETWEEN ITS TERMS.
      // THE OPEN/CLOSE TAG COUNTERS THAT USED TO BE KEPT HERE WERE READ BY ONE CONDITION ONLY - THE RDF ARM'S
      // `beginTag == endTag` - AND THE SHAPE TEST THAT REPLACED IT DOES NOT NEED THEM: A LINE WITH UNBALANCED
      // BRACKETS FAILS `endOfIri` ANYWAY, AND STRICTLY, SINCE THE COUNTS BALANCE FOR AN XML ELEMENT TOO
      boolean insideTag = currentChar == '<';
      final List<Character> delimiters = new ArrayList<>();
      final StringBuilder line = new StringBuilder(128).append(currentChar);
      while (parser.isAvailable() && parser.nextChar() != '\n') {
        final char c = parser.getCurrentChar();
        line.append(c);

        if (insideTag) {
          if (c == '>')
            insideTag = false;
        } else {
          if (c == '<')
            insideTag = true;
          else
            delimiters.add(c);
        }
      }

      // RDF. RECOGNISED BY THE SHAPE OF THE STATEMENT - subject predicate object [.] - AND NO LONGER BY "EVERY
      // CHARACTER OUTSIDE THE ANGLE BRACKETS IS THE SAME ONE", WHICH TOLERATED EXACTLY ONE TRAILING CHARACTER AND SO
      // REJECTED TWO CANONICAL FORMS: A CRLF LINE ENDING (THE '\r' IS A SECOND ONE) AND ANY LITERAL OBJECT (EVERY
      // CHARACTER OF IT IS OUTSIDE THE BRACKETS). BOTH FELL THROUGH TO THE CSV FALLBACK AND DIED ON A
      // NumberFormatException ABOUT AN IRI (ISSUE #7346). THE SHAPE TEST IS STRICTER THAN THE UNIQUENESS ONE, NOT
      // LOOSER: IT REQUIRES TWO IRI-SHAPED TERMS WHERE THE OLD ONE REQUIRED ONLY MATCHING '<' AND '>' COUNTS.
      // THE SEPARATOR IS TAKEN FROM BETWEEN THE SUBJECT AND THE PREDICATE AND HANDED TO THE FORMAT, WHICH INHERITS
      // CSVImporterFormat'S PARSER CONSTRUCTION AND WOULD OTHERWISE FALL BACK TO A COMMA (ISSUE #7315). PER-FORMAT
      // AND NOT THROUGH settings.options, WHICH ONE IMPORT SHARES ACROSS ITS DOCUMENTS, VERTICES AND EDGES FILES -
      // WRITING IT THERE IS WHAT LEAKED IT INTO THE NEXT CSV ENTITY (ISSUE #6946)
      final char separator = nTriplesSeparator(line);
      if (separator != 0) {
        settings.typeIdProperty = "id";
        return new RDFImporterFormat(resolveDelimiter(userDelimiter, separator));
      }

      // A LINE THAT OPENS WITH TWO IRI TERMS AND IS STILL NOT A TRIPLE IS AN RDF FILE THIS METHOD CANNOT PLACE.
      // SAYING SO HERE IS THE ONLY PLACE IT CAN BE SAID: THE CSV FALLBACK BELOW REPORTS A NumberFormatException
      // ABOUT AN IRI, WHICH NAMES NEITHER RDF NOR THE -delimiter WORKAROUND (ISSUE #7346)
      if (looksLikeRdfStatementStart(line))
        LogManager.instance().log(this, Level.WARNING,
            "The source's first line begins with two RDF terms but is not a well-formed N-Triples statement, so it is "
                + "analyzed as delimited text and a term may be reported as an unparseable number. Check the line for a "
                + "malformed object or terminator, or set the field delimiter explicitly with -delimiter: %s",
            line.length() > 200 ? line.substring(0, 200) + "..." : line.toString());

      if (currentChar == '<' && delimiters.size() <= 1)
        return new XMLImporterFormat();

    } else if (currentChar == '{') {

      final StringBuilder buffer = new StringBuilder();

      for (int i = 0; i < 1024 && parser.isAvailable(); ++i) {
        currentChar = parser.nextChar();
        if (currentChar == '}')
          break;

        buffer.append(currentChar);
      }

      if (buffer.toString().startsWith("\"info\":{\"name\":\""))
        return new OrientDBImporterFormat();
      else if (buffer.toString().startsWith("\"type\":\"node\",\"id\":\""))
        return new Neo4jImporterFormat();

      return new JSONImporterFormat();
    }

    return null;
  }

  protected void parseParameters(final String[] args) {
    for (int i = 0; i < args.length - 1; i += 2)
      parseParameter(args[i], args[i + 1]);

    if (url == null)
      throw new IllegalArgumentException("Missing URL");
  }

  protected void parseParameter(final String name, final String value) {
    if ("url".equals(name))
      url = value;
    else if ("analyzeLimitBytes".equals(name))
      limitBytes = FileUtils.getSizeAsNumber(value);
    else if ("analyzeLimitEntries".equals(name))
      limitEntries = Long.parseLong(value);
    else
      throw new IllegalArgumentException("Invalid setting '" + name + "'");
  }

  private Source getSourceFromContent(final InputStream in, final long totalSize, final String resource,
      final com.arcadedb.utility.Callable<Void, Source> resetCallback, final Callable<Void> closeCallback) throws IOException {
    in.mark(0);

    final ZipInputStream zip = new ZipInputStream(in);
    final ZipEntry entry = zip.getNextEntry();
    if (entry != null) {
      // ZIPPED FILE
      if (resource != null)
        // SEARCH FOR THE RIGHT ENTRY
        seekZipEntry(zip, entry, resource);

      return new Source(url, zip, totalSize, true, resetCallback, closeCallback);
    }

    in.reset();
    in.mark(in.available());

    try {
      final GZIPInputStream gzip = new GZIPInputStream(in, 8192);
      return new Source(url, gzip, totalSize, true, resetCallback, closeCallback);
    } catch (final IOException e) {
      // NOT GZIP
    }

    in.reset();

    // ANALYZE THE INPUT AS TEXT
    return new Source(url, in, totalSize, false, resetCallback, closeCallback);
  }

  /**
   * Positions a freshly opened {@link ZipInputStream} exactly the way {@link #getSourceFromContent} positioned the
   * original one: on the entry named {@code resource}, or on the first entry when no resource was requested. The
   * reset callbacks used to call {@code getNextEntry()} on the <b>old</b>, already closed stream instead, so the new
   * one was left with no current entry - and a {@link ZipInputStream} with no current entry reads as an empty file
   * rather than failing, which turned every ZIP import into a silent "0 records imported, completed" (issue #6810).
   */
  private static void positionZipStream(final ZipInputStream zip, final String resource) throws IOException {
    final ZipEntry entry = zip.getNextEntry();
    if (resource != null)
      seekZipEntry(zip, entry, resource);
  }

  /**
   * Advances {@code zip} from {@code entry} until the entry named {@code resource} is the current one.
   */
  private static void seekZipEntry(final ZipInputStream zip, ZipEntry entry, final String resource) throws IOException {
    while (entry != null) {
      if (resource.equals(entry.getName()))
        return;

      zip.closeEntry();
      entry = zip.getNextEntry();
    }

    throw new IllegalArgumentException("Resource '" + resource + "' not found");
  }

  private String getFormatFromExtension(String fileName) {
    if (fileName.lastIndexOf(File.separator) > -1)
      fileName = fileName.substring(fileName.lastIndexOf(File.separator) + 1);

    if (fileName.endsWith(".tgz"))
      fileName = fileName.substring(0, fileName.length() - ".tgz".length());
    else if (fileName.endsWith(".gz"))
      fileName = fileName.substring(0, fileName.length() - ".gz".length());
    else if (fileName.endsWith(".zip"))
      fileName = fileName.substring(0, fileName.length() - ".zip".length());

    if (fileName.lastIndexOf('.') > -1)
      fileName = fileName.substring(fileName.lastIndexOf('.') + 1);

    return fileName;
  }
}
