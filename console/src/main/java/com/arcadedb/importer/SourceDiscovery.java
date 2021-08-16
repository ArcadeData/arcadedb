/*
 * Copyright 2021 Arcade Data Ltd
 *
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package com.arcadedb.importer;

import com.arcadedb.log.LogManager;
import com.arcadedb.utility.FileUtils;

import java.io.BufferedInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.net.HttpURLConnection;
import java.net.URL;
import java.util.*;
import java.util.concurrent.Callable;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.logging.Level;
import java.util.zip.GZIPInputStream;
import java.util.zip.ZipEntry;
import java.util.zip.ZipInputStream;

public class SourceDiscovery {
  private static final String  RESOURCE_SEPARATOR = ":::";
  private              String  url;
  private              long    limitBytes         = 10000000;
  private              long    limitEntries       = 0;
  private              int     objectNestLevel    = 1;
  private              int     maxValueSampling   = 300;
  private              boolean trimText           = true;

  public SourceDiscovery(final String url) {
    this.url = url;
  }

  public SourceSchema getSchema(final ImporterSettings settings, final AnalyzedEntity.ENTITY_TYPE entityType,
      final AnalyzedSchema analyzedSchema) throws IOException {
    LogManager.instance().log(this, Level.INFO, "Analyzing url: %s...", null, url);

    final Source source = getSource();

    final Parser parser = new Parser(source, 0);

    final ContentImporter contentImporter = analyzeSourceContent(parser, entityType, settings);
    parser.reset();

    final SourceSchema sourceSchema = contentImporter.analyze(entityType, parser, settings, analyzedSchema);

    if (contentImporter == null)
      LogManager.instance().log(this, Level.INFO, "Unknown format");
    else {
      LogManager.instance()
          .log(this, Level.INFO, "Recognized format %s (parsingLimitBytes=%s parsingLimitEntries=%d)", null, contentImporter.getFormat(),
              FileUtils.getSizeAsString(limitBytes), limitEntries);
      if (!sourceSchema.getOptions().isEmpty()) {
        for (Map.Entry<String, String> o : sourceSchema.getOptions().entrySet())
          LogManager.instance().log(this, Level.INFO, "- %s = %s", null, o.getKey(), o.getValue());
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

    return getSourceFromContent(new BufferedInputStream(connection.getInputStream()), connection.getContentLengthLong(), resource,
        new Callable<Void>() {
          @Override
          public Void call() throws Exception {
            connection.disconnect();
            return null;
          }
        });
  }

  private Source getSourceFromFile(final String path) throws IOException {
    final int sep = path.lastIndexOf(RESOURCE_SEPARATOR);
    final String filePath = sep > -1 ? path.substring(0, sep) : path;
    final String resource = sep > -1 ? path.substring(sep + RESOURCE_SEPARATOR.length()) : null;

    final File file = new File(filePath);
    final BufferedInputStream fis = new BufferedInputStream(new FileInputStream(file));

    return getSourceFromContent(fis, file.length(), resource, new Callable<Void>() {
      @Override
      public Void call() throws Exception {
        fis.close();
        return null;
      }
    });
  }

  private ContentImporter analyzeSourceContent(final Parser parser, final AnalyzedEntity.ENTITY_TYPE entityType,
      final ImporterSettings settings) throws IOException {

    String knownFileType = null;
    String knownDelimiter = null;

    switch (entityType) {
    case DOCUMENT:
      knownFileType = settings.documentsFileType;
      knownDelimiter = settings.documentsDelimiter;
      break;

    case VERTEX:
      knownFileType = settings.verticesFileType;
      knownDelimiter = settings.verticesDelimiter;
      break;

    case EDGE:
      knownFileType = settings.edgesFileType;
      knownDelimiter = settings.edgesDelimiter;
      break;
    }

    if (knownFileType != null) {
      if (knownFileType.equalsIgnoreCase("csv")) {
        settings.options.put("delimiter", knownDelimiter);
        return new CSVImporter();
      } else if (knownFileType.equalsIgnoreCase("json"))
        return new JSONImporter();
      else if (knownFileType.equalsIgnoreCase("xml"))
        return new XMLImporter();
      else
        LogManager.instance()
            .log(this, Level.WARNING, "File type '%s' is not supported. Trying to understand file type...", null, knownFileType);
    }

    parser.nextChar();

    ContentImporter format = analyzeChar(parser, settings);
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

    while (parser.getCurrentChar() == '/' && parser.nextChar() == '/') {
      skipLine(parser);
      format = analyzeChar(parser, settings);
      if (format != null)
        return format;
    }

    // CHECK FOR CSV-LIKE FILES
    final Map<Character, AtomicInteger> candidateSeparators = new HashMap<>();

    while (parser.isAvailable() && parser.nextChar() != '\n') {
      final char c = parser.getCurrentChar();
      if (!Character.isLetterOrDigit(c)) {
        final AtomicInteger sep = candidateSeparators.get(c);
        if (sep == null) {
          candidateSeparators.put(c, new AtomicInteger(1));
        } else
          sep.incrementAndGet();
      }
    }

    if (!candidateSeparators.isEmpty()) {
      if (candidateSeparators.size() > 1) {
        final ArrayList<Map.Entry<Character, AtomicInteger>> list = new ArrayList(candidateSeparators.entrySet());
        list.sort(new Comparator<Map.Entry<Character, AtomicInteger>>() {
          @Override
          public int compare(final Map.Entry<Character, AtomicInteger> o1, final Map.Entry<Character, AtomicInteger> o2) {
            if (o1.getValue().get() == o2.getValue().get())
              return 0;
            return o1.getValue().get() < o2.getValue().get() ? 1 : -1;
          }
        });

        final Map.Entry<Character, AtomicInteger> bestSeparator = list.get(0);

        LogManager.instance()
            .log(this, Level.INFO, "Best separator candidate='%s' (all candidates=%s)", null, bestSeparator.getKey(), list);

        settings.options.put("delimiter", "" + bestSeparator.getKey());
        return new CSVImporter();
      }
    }

    // UNKNOWN
    throw new ImportException("Cannot determine the file type. If it is a CSV file, please specify the header via settings");
  }

  private void skipLine(final Parser parser) throws IOException {
    while (parser.isAvailable() && parser.nextChar() != '\n')
      ;
  }

  private ContentImporter analyzeChar(final Parser parser, final ImporterSettings settings) throws IOException {
    final char currentChar = parser.getCurrentChar();
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
        char delimiter = delimiters.get(0);
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
          return new RDFImporter();
        }
      }

      return new XMLImporter();
    } else if (currentChar == '{')
      return new JSONImporter();

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
    else if ("analyzeMaxValueSampling".equals(name))
      maxValueSampling = Integer.parseInt(value);
    else if ("analyzeTrimText".equals(name))
      trimText = Boolean.parseBoolean(value);
    else if ("objectNestLevel".equals(name))
      objectNestLevel = Integer.parseInt(value);
    else
      throw new IllegalArgumentException("Invalid setting '" + name + "'");
  }

  private Source getSourceFromContent(final BufferedInputStream in, final long totalSize, final String resource,
      final Callable<Void> closeCallback) throws IOException {
    in.mark(0);

    final ZipInputStream zip = new ZipInputStream(in);

    ZipEntry entry = zip.getNextEntry();
    if (entry != null) {
      // ZIPPED FILE
      if (resource != null) {
        // SEARCH FOR THE RIGHT ENTRY
        while (entry != null) {
          if (resource.equals(entry.getName()))
            return new Source(url, zip, totalSize, true, closeCallback);

          zip.closeEntry();
          entry = zip.getNextEntry();
        }

        throw new IllegalArgumentException("Resource '" + resource + "' not found");
      }

      return new Source(url, zip, totalSize, true, closeCallback);
    }

    in.reset();
    in.mark(0);

    try {
      final GZIPInputStream gzip = new GZIPInputStream(in, 8192);
      return new Source(url, gzip, totalSize, true, closeCallback);
    } catch (IOException e) {
      // NOT GZIP
    }

    in.reset();

    // ANALYZE THE INPUT AS TEXT
    return new Source(url, in, totalSize, false, closeCallback);
  }
}
