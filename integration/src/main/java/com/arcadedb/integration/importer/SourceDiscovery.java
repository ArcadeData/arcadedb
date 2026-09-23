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

import com.arcadedb.ContextConfiguration;
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
import com.arcadedb.utility.SafeHttpFetcher;

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
  private static final String RESOURCE_SEPARATOR = ":::";
  /** The label {@code ImportSecurityValidator} opens every remote connection with, so a read timeout says the same. */
  private static final String IMPORT_CONTEXT      = "IMPORT DATABASE";
  private static final String FILE_PREFIX        = "file://";
  private static final String CLASSPATH_PREFIX   = "classpath://";
  private              String  url;
  private final        Boolean allowLocalUrls;
  /**
   * The importing database's settings overlay, or null for a CLI caller. Only the fetch TIMEOUTS are read from it,
   * and only because they are {@code SCOPE.SERVER} settings a {@code ContextConfiguration} never writes through to
   * the enum for (PR #7755 review).
   */
  private              ContextConfiguration configuration;
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

    // THE SNIFFER WALKS THE COMMENT BLOCK ITSELF - IT COUNTS THE LINES AND REWINDS OVER THEM - SO IT NEEDS THE
    // SOURCE WHOLE. THE analyze() BELOW IS A FORMAT AND WANTS THE OPPOSITE, AND THE reset() BETWEEN THE TWO IS
    // WHERE THE STREAM IS REBUILT, SO ONE PARSER CAN SERVE BOTH (ISSUE #7490)
    final Parser parser = new Parser(source, 0, false);

    final FormatImporter formatImporter = analyzeSourceContent(parser, entityType, settings, logger);
    parser.setSkipLeadingComments(true);
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

  /**
   * Hands this discovery the importing database's configuration, so a remote fetch is bounded by the timeout the
   * OPERATOR configured rather than by the enum default. Null-tolerant: a CLI import has no overlay.
   */
  public SourceDiscovery setConfiguration(final ContextConfiguration configuration) {
    this.configuration = configuration;
    return this;
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

    final HttpURLConnection connection = ImportSecurityValidator.openRemoteConnection(urlPath, blockLocalNetworks,
        configuration);

    // EVERY READ OF A REMOTE SOURCE IS BOUNDED BY NETWORK_REMOTE_FETCH_READ_TIMEOUT, WHICH openRemoteConnection
    // APPLIES. SafeHttpFetcher.body() IS WHAT MAKES THAT BOUND LEGIBLE WHEN IT FIRES: SINCE #7494 THE SNIFFER BLOCKS
    // IN reader.read() RATHER THAN GUESSING THAT A QUIET SOCKET MEANS END-OF-INPUT, SO A SOURCE THAT STOPS SENDING
    // AND NEVER CLOSES IS EXACTLY THE CASE THAT REACHES A CLIENT - AND IT USED TO REACH IT AS "Error on parsing
    // source ...", NAMING NEITHER THE TIMEOUT NOR THE SETTING (ISSUE #7500)
    return getSourceFromContent(new BufferedInputStream(SafeHttpFetcher.body(connection, IMPORT_CONTEXT)),
        connection.getContentLengthLong(), resource,
        source -> {
          try {
            source.inputStream.close();
            connection.disconnect();

            final HttpURLConnection connection1 = ImportSecurityValidator.openRemoteConnection(urlPath,
                blockLocalNetworks, configuration);
            final InputStream body1 = SafeHttpFetcher.body(connection1, IMPORT_CONTEXT);

            if (source.inputStream instanceof GZIPInputStream)
              source.inputStream = new GZIPInputStream(body1, 2048);
            else if (source.inputStream instanceof ZipInputStream) {
              final ZipInputStream zip = new ZipInputStream(body1);
              positionZipStream(zip, resource);
              source.inputStream = zip;
            } else
              source.inputStream = new BufferedInputStream(body1);
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
   * The vertex property an RDF source keys its subject and object IRIs by: the user's own
   * {@code -typeIdProperty} / {@code WITH typeIdProperty = ...} when there is one, {@code "id"} otherwise.
   * <p>
   * The sibling of {@link #resolveDelimiter} one line up, and it used to be the unguarded half of the pair: the
   * detection arm assigned {@code settings.typeIdProperty = "id"} unconditionally, so recognising N-Triples silently
   * discarded an explicit choice that names a real schema artefact - the property, its index and the
   * {@code newEdgeByKeys} lookup all follow it - and did so without the INFO line the discarded delimiter gets
   * (issue #7891).
   */
  static String resolveTypeIdProperty(final String userTypeIdProperty) {
    if (userTypeIdProperty == null)
      return RDFImporterFormat.DEFAULT_TYPE_ID_PROPERTY;
    if (!RDFImporterFormat.DEFAULT_TYPE_ID_PROPERTY.equals(userTypeIdProperty))
      LogManager.instance().log(SourceDiscovery.class, Level.INFO,
          "RDF default key property '%s' discarded: using the typeIdProperty '%s' explicitly set by the user",
          RDFImporterFormat.DEFAULT_TYPE_ID_PROPERTY, userTypeIdProperty);
    return userTypeIdProperty;
  }

  private FormatImporter analyzeSourceContent(final Parser parser, final AnalyzedEntity.EntityType entityType,
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
        return gremlinFormatImporter(knownFileType, "com.arcadedb.gremlin.integration.importer.format.GraphMLImporterFormat");
      } else if ("graphson".equalsIgnoreCase(knownFileType)) {
        return gremlinFormatImporter(knownFileType, "com.arcadedb.gremlin.integration.importer.format.GraphSONImporterFormat");
      } else {
        LogManager.instance()
            .log(this, Level.WARNING, "File type '%s' is not supported. Trying to understand file type...", knownFileType);
      }
    }

    // GUARDED: AN EMPTY SOURCE WOULD OTHERWISE MAKE Parser.END_OF_STREAM THE CURRENT CHARACTER AND CARRY IT INTO
    // THE LINE THE SNIFFER BUILDS OUT OF IT (ISSUE #7494)
    if (parser.isAvailable())
      parser.nextChar();

    FormatImporter format = analyzeChar(parser, settings, userDelimiter);
    if (format != null)
      return format;

    return analyzeText(parser, settings, logger, userDelimiter);
  }

  /**
   * The importer for a file type the optional {@code arcadedb-gremlin} module supplies, resolved by name because
   * {@code arcadedb-integration} deliberately does not depend on it.
   * <p>
   * A failed lookup THROWS. It used to log {@code SEVERE} and fall out of the known-file-type chain into the generic
   * content sniffer below, which is written for an UNKNOWN type - its own message says so - and for a {@code .graphml}
   * source answered "XML". {@code XMLImporterFormat} then imported the GraphML container as ONE ordinary record and
   * {@code Importer.load()} RETURNED NORMALLY with {@code createdVertices=1}: the CLI exited 0 and
   * {@code IMPORT DATABASE} answered 200 while the two nodes and the edge the file described were gone, the only
   * trace being a log line nobody reads after a command that just said it worked (issue #7781). A known file type
   * whose handler is absent is not a candidate for sniffing - it is a refusal, and one that has to name the module
   * that supplies the handler, because "Error on parsing source" sent the operator to look at their file.
   */
  @SuppressWarnings("unchecked")
  private static FormatImporter gremlinFormatImporter(final String fileType, final String className) {
    try {
      final Class<FormatImporter> clazz = (Class<FormatImporter>) Class.forName(className);
      return clazz.getConstructor().newInstance();
    } catch (final ClassNotFoundException | InvocationTargetException | InstantiationException | IllegalAccessException |
                   NoSuchMethodException | ClassCastException e) {
      // ClassCastException too: the cast above is unchecked, so a class that RESOLVES but is not a FormatImporter -
      // a gremlin module whose version does not match this one - would otherwise escape as a raw cast failure naming
      // neither the format nor the module, which is the exact shape of failure this method exists to replace.
      throw new ImportException(
          "Cannot import a '" + fileType + "' source: its importer is provided by the optional arcadedb-gremlin module, "
              + "which is not available on this classpath", e);
    }
  }

  /**
   * The content sniffing that follows {@link #analyzeChar}: comments are skipped, then the first line is read for a
   * separator to decide between the delimited-text formats.
   *
   * @param userDelimiter the delimiter the user supplied for this entity, or null - a detected one yields to it
   */
  private FormatImporter analyzeText(final Parser parser, final ImporterSettings settings, final ConsoleLogger logger,
      final String userDelimiter) throws IOException {
    FormatImporter format = null;

    // SKIP THE LEADING COMMENT LINES, '#' AND '//' ALIKE. THIS USED TO BE TWO LOOPS THAT BETWEEN THEM SKIPPED
    // NOTHING: skipLine() LEFT THE PARSER ON THE '\n' THAT ENDED THE COMMENT, SO BOTH THE LOOP CONDITION AND THE
    // analyzeChar() CALL INSIDE IT SAW '\n' - NEITHER CAN MATCH ONE - AND THE LOOP GAVE UP AFTER A SINGLE PASS,
    // WHILE THE parser.reset() BETWEEN THEM PUT THE ONE LINE IT HAD CONSUMED BACK ANYWAY. A '#'-COMMENTED SOURCE
    // WAS THEREFORE SNIFFED ON ITS COMMENT: THE COMMENT'S OWN SPACES BECAME THE DELIMITER CANDIDATES AND AN
    // N-TRIPLES FILE WAS IMPORTED AS CSV (ISSUE #7347)
    int commentLines = 0;
    while (parser.isAvailable() && isCommentLineStart(parser)) {
      skipLine(parser);
      ++commentLines;
    }

    if (commentLines > 0) {
      // THE FIRST DATA LINE IS REACHABLE BY THE FIRST-CHARACTER DISPATCH ONLY HERE: THE CALL getSchema() MADE RAN ON
      // THE COMMENT'S OWN FIRST CHARACTER AND COULD ONLY RETURN NULL
      format = analyzeChar(parser, settings, userDelimiter);
      if (format != null) {
        logger.logLine(1, "Recognized format %s", format.getFormat());
        return format;
      }
    }

    // analyzeChar() CONSUMES THE LINE IT SNIFFS - THE CALL ABOVE AND THE ONE getSchema() MADE BOTH DO - AND THE
    // SEPARATOR SCAN BELOW HAS TO SEE THAT SAME LINE. parser.reset() IS THE ONLY WAY BACK AND IT REWINDS TO THE HEAD
    // OF THE SOURCE, COMMENTS INCLUDED, SO THEY ARE STEPPED OVER AGAIN
    parser.reset();
    for (int i = 0; i < commentLines; ++i)
      skipLine(parser);

    try {
      // CHECK FOR CSV-LIKE FILES
      final Map<Character, AtomicInteger> candidateSeparators = new HashMap<>();

      final String line = readLine(parser);
      for (int i = 0; i < line.length(); ++i) {
        final char c = line.charAt(i);

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
          final String line2 = parser.isAvailable() ? readLine(parser) : "";

          final String[] fields1 = line.split(" ");
          final String[] fields2 = line2.split(" ");

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
   * What counts as a leading comment line, in ONE place: a {@code #}, or the first {@code /} of a {@code //}.
   * <p>
   * Content sniffing and the row loops have to agree about it exactly, or a line the sniffer skipped arrives as
   * data - a bogus edge for RDF, a bogus header for CSV - and the two work on different abstractions (the
   * character-oriented {@link Parser} here, a {@code PushbackReader} in
   * {@link com.arcadedb.integration.importer.format.CSVImporterFormat#sourceReader}), so the LOOPS cannot be
   * shared even though the rule must be (issue #7347).
   *
   * @param second the character after {@code first}, or {@code 0} when there is none. Only consulted when
   *               {@code first} is a {@code /}, so a caller with one character in hand can pass {@code 0}.
   */
  public static boolean isCommentLineStart(final char first, final char second) {
    return first == '#' || (first == '/' && second == '/');
  }

  /**
   * Whether the parser's CURRENT character opens a comment line. The second character is PEEKED rather than read,
   * and only when it can matter, so a data line that merely begins with a single {@code /} keeps its first
   * character and is sniffed whole (issue #7347).
   */
  private static boolean isCommentLineStart(final Parser parser) throws IOException {
    if (parser.isBeforeFirstChar())
      // NOTHING HAS BEEN READ, SO THERE IS NO CURRENT CHARACTER TO JUDGE. THE PLACEHOLDER getCurrentChar() ANSWERS
      // IS 0, WHICH OPENS NEITHER COMMENT FORM, SO THIS IS THE SAME ANSWER SPELLED HONESTLY RATHER THAN A CHANGE -
      // AND IT KEEPS THE "0 MEANS NOTHING YET" READING OUT OF A SECOND PLACE (ISSUE #7501)
      return false;

    final char first = parser.getCurrentChar();
    return isCommentLineStart(first, first == '/' ? parser.peekChar() : 0);
  }

  private static void skipLine(final Parser parser) throws IOException {
    readLine(parser);
  }

  /**
   * Reads from the parser's CURRENT character INCLUSIVE to the end of the line, WITHOUT the line terminator, and
   * leaves the parser on the first character of the NEXT line.
   * <p>
   * That trailing step is the whole of issue #7347: what this replaces stopped on the {@code '\n'} that ended the
   * line, so every caller that went on to look at {@code getCurrentChar()} - the comment loops in
   * {@link #analyzeText} and the {@link #analyzeChar} dispatch they call - was looking at a newline rather than at
   * the first character of the line they had just uncovered.
   * <p>
   * A parser that has read nothing yet ({@link Parser#isBeforeFirstChar()}) starts from the first character of the
   * source. That question is asked of the PARSER and not of {@code getCurrentChar()}, which answers {@code 0} both
   * for "nothing read yet" and for a NUL the source really carries: testing the character value dropped a genuine
   * leading NUL and worked on a line one character shorter than the source (issue #7501).
   * <p>
   * Package-private and static for direct unit testing: this is where the sentinel collision lived, and the line it
   * returns reaches the separator scan and {@link #analyzeChar} rather than any caller outside this class.
   */
  static String readLine(final Parser parser) throws IOException {
    final char first = parser.getCurrentChar();
    if (first == '\n') {
      // AN EMPTY LINE: THE PARSER IS ALREADY ON ITS TERMINATOR
      if (parser.isAvailable())
        parser.nextChar();
      return "";
    }

    final StringBuilder line = new StringBuilder(128);
    // THE isEndOfStream() HALF IS DEFENCE, NOT A LIVE CASE: EVERY nextChar() IN THIS CLASS IS GUARDED BY AN
    // isAvailable(), SO first IS A REAL CHARACTER WHENEVER THE PARSER HAS READ ANYTHING. IT IS KEPT BECAUSE THE
    // COST OF A FUTURE CALLER LOSING THAT GUARD IS Parser.END_OF_STREAM SILENTLY BECOMING THE FIRST CHARACTER OF A
    // SNIFFED LINE
    if (!parser.isBeforeFirstChar() && !parser.isEndOfStream())
      line.append(first);

    boolean terminated = false;
    while (parser.isAvailable()) {
      if (parser.nextChar() == '\n') {
        terminated = true;
        break;
      }
      line.append(parser.getCurrentChar());
    }

    // A LINE THE SOURCE ENDED WITHOUT TERMINATING LEAVES THE PARSER WHERE IT IS: THERE IS NO NEXT LINE TO STEP ONTO,
    // AND isAvailable() IS ALREADY FALSE FOR EVERY CALLER THAT ASKS
    if (terminated && parser.isAvailable())
      parser.nextChar();

    return line.toString();
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
      // WRITING IT THERE IS WHAT LEAKED IT INTO THE NEXT CSV ENTITY (ISSUE #6946).
      // THE KEY PROPERTY TRAVELS THE SAME WAY AND FOR BOTH OF THE SAME REASONS: settings.typeIdProperty = "id" USED
      // TO BE ASSIGNED HERE UNCONDITIONALLY, WHICH DISCARDED AN EXPLICIT -typeIdProperty AND THEN OUTLIVED THE RDF
      // SOURCE IT HAD BEEN DECIDED FOR, DRIVING THE PROPERTY AND UNIQUE-INDEX AUTO-CREATION OF THE NEXT ENTITY
      // (ISSUE #7891)
      final char separator = nTriplesSeparator(line);
      if (separator != 0)
        return new RDFImporterFormat(resolveDelimiter(userDelimiter, separator),
            resolveTypeIdProperty(settings.typeIdProperty));

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
    // EITHER SEPARATOR CONVENTION: THE NAME COMES FROM A CALLER-SUPPLIED -url / -documents / -vertices / -edges
    // VALUE, WHICH ON WINDOWS IS AS LIKELY TO USE '/' AS '\' (ISSUE #7588)
    fileName = FileUtils.getFileNameFromPath(fileName);

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
