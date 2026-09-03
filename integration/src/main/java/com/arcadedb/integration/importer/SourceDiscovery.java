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
   * Records a separator detected by sniffing the source content, unless the user already supplied a delimiter: a guess
   * never overrides an explicit choice. The one place every detection site goes through, so a new site cannot fall
   * back to the unconditional write that overwrote the user's {@code -delimiter} / {@code WITH delimiter = ...} whenever
   * the file extension did not short-circuit detection (issue #6946, the sibling of #6811). A discarded guess is logged
   * so the "best separator candidate" line just above it does not read as authoritative.
   */
  static void applyDetectedDelimiter(final ImporterSettings settings, final char detected) {
    final Object userDelimiter = settings.options.get("delimiter");
    if (userDelimiter != null) {
      if (!userDelimiter.toString().equals(String.valueOf(detected)))
        LogManager.instance().log(SourceDiscovery.class, Level.INFO,
            "Detected separator '%s' discarded: using the delimiter '%s' explicitly set by the user", detected, userDelimiter);
      return;
    }
    settings.options.put("delimiter", String.valueOf(detected));
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

    // THE USER'S DELIMITER IS SETTLED HERE, ONCE, BEFORE ANY DETECTION RUNS: THE PER-ENTITY DELIMITER IS AN OVERRIDE
    // OF THE GENERIC `delimiter` OPTION, NOT A MANDATORY VALUE, SO AN ABSENT ONE LEAVES THE OPTION ALONE (CLOBBERING IT
    // WITH NULL MADE EVERY NON-COMMA CSV UNIMPORTABLE, ISSUE #6811). FROM HERE ON `settings.options` HOLDS THE USER'S
    // CHOICE, WHICH EVERY DETECTED SEPARATOR BELOW HAS TO YIELD TO THROUGH applyDetectedDelimiter() (ISSUE #6946)
    if (knownDelimiter != null)
      settings.options.put("delimiter", knownDelimiter);

    if (knownFileType != null) {
      if ("csv".equalsIgnoreCase(knownFileType)) {
        return new CSVImporterFormat();
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

    FormatImporter format = analyzeChar(parser, settings);
    if (format != null)
      return format;

    parser.mark();

    // SKIP COMMENTS '#' IF ANY
    while (parser.isAvailable() && parser.getCurrentChar() == '#') {
      skipLine(parser);
      format = analyzeChar(parser, settings);
      if (format != null)
        return format;
    }

    // SKIP COMMENTS '//' IF ANY
    parser.reset();

    try {
      while (parser.getCurrentChar() == '/' && parser.nextChar() == '/') {
        skipLine(parser);
        format = analyzeChar(parser, settings);
        if (format != null)
          return format;
      }

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

        final Map.Entry<Character, AtomicInteger> bestSeparator = list.get(0);

        // A DELIMITER THE USER SUPPLIED SETTLES THE QUESTION THE SNIFFING IS ASKING: THE FILE IS DELIMITED TEXT WITH
        // THAT DELIMITER, SO THE SPACE-SEPARATED VECTOR FORMATS ARE NOT A CANDIDATE HOWEVER MANY SPACES THE FIRST LINE
        // CARRIES INSIDE ITS VALUES (ISSUE #6946)
        if (bestSeparator.getKey() == ' ' && settings.options.get("delimiter") == null) {
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
          applyDetectedDelimiter(settings, bestSeparator.getKey());
          format = new CSVImporterFormat();
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

  private void skipLine(final Parser parser) throws IOException {
    while (parser.isAvailable() && parser.nextChar() != '\n')
      ;
  }

  private FormatImporter analyzeChar(final Parser parser, final ImporterSettings settings) throws IOException {
    char currentChar = parser.getCurrentChar();
    if (currentChar == '<') {
      // READ THE FIRST LINE
      int beginTag = 1;
      int endTag = 0;
      boolean insideTag = true;
      final List<Character> delimiters = new ArrayList<>();
      while (parser.isAvailable() && parser.nextChar() != '\n') {
        final char c = parser.getCurrentChar();

        if (insideTag) {
          if (c == '>') {
            endTag++;
            insideTag = false;
          }
        } else {
          if (c == '<') {
            beginTag++;
            insideTag = true;
          } else
            delimiters.add(c);
        }
      }

      if (!delimiters.isEmpty() && beginTag == endTag) {
        boolean allDelimitersAreTheSame = true;
        final char delimiter = delimiters.get(0);
        for (int i = 1; i < delimiters.size() - 1; ++i) {
          if (delimiters.get(i) != delimiter) {
            allDelimitersAreTheSame = false;
            break;
          }
        }

        if (allDelimitersAreTheSame) {
          // RDF
          settings.typeIdProperty = "id";
          applyDetectedDelimiter(settings, delimiters.getFirst());
          return new RDFImporterFormat();
        }
      }

      if (delimiters.size() <= 1)
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
