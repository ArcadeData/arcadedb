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

import java.io.*;
import java.lang.reflect.*;
import java.net.*;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.*;
import java.util.logging.*;
import java.util.zip.*;

public class SourceDiscovery {
  private static final String RESOURCE_SEPARATOR = ":::";
  private static final String FILE_PREFIX        = "file://";
  private static final String CLASSPATH_PREFIX   = "classpath://";
  private              String url;
  private              long   limitBytes         = 10000000;
  private              long   limitEntries       = 0;

  public SourceDiscovery(final String url) {
    this.url = url;
  }

  public SourceSchema getSchema(final ImporterSettings settings, final AnalyzedEntity.ENTITY_TYPE entityType, final AnalyzedSchema analyzedSchema,
      final ConsoleLogger logger) throws IOException {
    LogManager.instance().log(this, Level.INFO, "Analyzing url: %s...", url);

    final Source source = getSource();

    final Parser parser = new Parser(source, 0);

    final FormatImporter formatImporter = analyzeSourceContent(parser, entityType, settings, logger);
    parser.reset();

    SourceSchema sourceSchema = null;

    if (formatImporter == null)
      LogManager.instance().log(this, Level.INFO, "Unknown format");
    else {
      sourceSchema = formatImporter.analyze(entityType, parser, settings, analyzedSchema);
      LogManager.instance().log(this, Level.INFO, "Recognized format %s (parsingLimitBytes=%s parsingLimitEntries=%d)", formatImporter.getFormat(),
          FileUtils.getSizeAsString(limitBytes), limitEntries);
      if (sourceSchema != null && !sourceSchema.getOptions().isEmpty()) {
        for (final Map.Entry<String, String> o : sourceSchema.getOptions().entrySet())
          LogManager.instance().log(this, Level.INFO, "- %s = %s", o.getKey(), o.getValue());
      }
    }

    source.close();

    return sourceSchema;
  }

  public Source getSource() throws IOException {
    final Source source;
    if (url.startsWith("http://") || url.startsWith("https://"))
      source = getSourceFromURL(url);
    else
      source = getSourceFromFile(url);
    return source;
  }

  private Source getSourceFromURL(final String url) throws IOException {
    final int sep = url.lastIndexOf(RESOURCE_SEPARATOR);
    final String urlPath = sep > -1 ? url.substring(0, sep) : url;
    final String resource = sep > -1 ? url.substring(sep + RESOURCE_SEPARATOR.length()) : null;

    final HttpURLConnection connection = (HttpURLConnection) new URL(urlPath).openConnection();
    connection.setRequestMethod("GET");
    connection.setDoOutput(true);

    connection.connect();

    return getSourceFromContent(new BufferedInputStream(connection.getInputStream()), connection.getContentLengthLong(), resource, source -> {
      try {
        connection.disconnect();

        final HttpURLConnection connection1 = (HttpURLConnection) new URL(urlPath).openConnection();
        connection1.setRequestMethod("GET");
        connection1.setDoOutput(true);
        connection1.connect();

        if (source.inputStream instanceof GZIPInputStream)
          source.inputStream = new GZIPInputStream(connection1.getInputStream(), 2048);
        else if (source.inputStream instanceof ZipInputStream stream) {
          source.inputStream = new ZipInputStream(connection1.getInputStream());
          stream.getNextEntry();
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

    final File file = new File(filePath);

    final InputStream fis;
    if (file.exists())
      fis = new BufferedInputStream(new FileInputStream(file));
    else {
      fis = getClass().getClassLoader().getResourceAsStream(filePath);
      if (fis == null)
        throw new FileNotFoundException(filePath);
    }

    return getSourceFromContent(fis, file.length(), resource, source -> {
      try {
        source.inputStream.close();
        if (source.inputStream instanceof GZIPInputStream)
          source.inputStream = new GZIPInputStream(new FileInputStream(file), 2048);
        else if (source.inputStream instanceof ZipInputStream stream) {
          source.inputStream = new ZipInputStream(new FileInputStream(file));
          stream.getNextEntry();
        } else
          source.inputStream = new BufferedInputStream(new FileInputStream(file));
      } catch (final IOException e) {
        throw new ImportException("Error on reset local resource", e);
      }
      return null;
    }, () -> {
      fis.close();
      return null;
    });
  }

  private FormatImporter analyzeSourceContent(final Parser parser, final AnalyzedEntity.ENTITY_TYPE entityType, final ImporterSettings settings,
      final ConsoleLogger logger) throws IOException {

    String knownFileType = null;
    String knownDelimiter = null;

    switch (entityType) {
    case DOCUMENT:
      knownFileType = settings.documentsFileType != null ? settings.documentsFileType : getFileTypeByExtension(settings.documents);
      knownDelimiter = settings.documentsDelimiter;
      break;

    case VERTEX:
      knownFileType = settings.verticesFileType != null ? settings.verticesFileType : getFileTypeByExtension(settings.vertices);
      knownDelimiter = settings.verticesDelimiter;
      break;

    case EDGE:
      knownFileType = settings.edgesFileType != null ? settings.edgesFileType : getFileTypeByExtension(settings.edgeTypeName);
      knownDelimiter = settings.edgesDelimiter;
      break;

    case DATABASE:
      // NO SPECIAL SETTINGS
      knownFileType = getFileTypeByExtension(settings.url);
      break;

    default:
      throw new IllegalArgumentException("entityType '" + entityType + "' not supported");
    }

    if (knownFileType != null) {
      if (knownFileType.equalsIgnoreCase("csv")) {
        settings.options.put("delimiter", knownDelimiter);
        return new CSVImporterFormat();
      } else if (knownFileType.equalsIgnoreCase("json")) {
        return new JSONImporterFormat();
      } else if (knownFileType.equalsIgnoreCase("jsonl")) {
        return new JsonlImporterFormat();
      } else if (knownFileType.equalsIgnoreCase("xml")) {
        return new XMLImporterFormat();
      } else if (knownFileType.equalsIgnoreCase("graphml")) {

        try {
          final Class<FormatImporter> clazz = (Class<FormatImporter>) Class.forName("com.arcadedb.gremlin.integration.importer.format.GraphMLImporterFormat");
          return clazz.getConstructor().newInstance();
        } catch (final ClassNotFoundException | InvocationTargetException | InstantiationException | IllegalAccessException | NoSuchMethodException e) {
          LogManager.instance().log(this, Level.SEVERE, "Impossible to find importer for 'graphml' ", e);
        }

      } else if (knownFileType.equalsIgnoreCase("graphson")) {

        try {
          final Class<FormatImporter> clazz = (Class<FormatImporter>) Class.forName("com.arcadedb.gremlin.integration.importer.format.GraphSONImporterFormat");
          return clazz.getConstructor().newInstance();
        } catch (final ClassNotFoundException | InvocationTargetException | InstantiationException | IllegalAccessException | NoSuchMethodException e) {
          LogManager.instance().log(this, Level.SEVERE, "Impossible to find importer for 'graphson' ", e);
        }

      } else {
        LogManager.instance().log(this, Level.WARNING, "File type '%s' is not supported. Trying to understand file type...", knownFileType);
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

        if (bestSeparator.getKey() == ' ') {
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
          LogManager.instance().log(this, Level.INFO, "Best separator candidate='%s' (all candidates=%s)", bestSeparator.getKey(), list);
          settings.options.put("delimiter", "" + bestSeparator.getKey());
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
    return c == ' ' || c == '\t' || c == ',' || c == '|' || c == '-' || c == '_';
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
          settings.options.put("delimiter", "" + delimiters.get(0));
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
    ZipEntry entry = zip.getNextEntry();
    if (entry != null) {
      // ZIPPED FILE
      if (resource != null) {
        // SEARCH FOR THE RIGHT ENTRY
        while (entry != null) {
          if (resource.equals(entry.getName()))
            return new Source(url, zip, totalSize, true, resetCallback, closeCallback);

          zip.closeEntry();
          entry = zip.getNextEntry();
        }

        throw new IllegalArgumentException("Resource '" + resource + "' not found");
      }

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
