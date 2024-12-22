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

import com.arcadedb.database.DatabaseInternal;
import com.arcadedb.graph.MutableVertex;
import com.arcadedb.integration.importer.AnalyzedEntity;
import com.arcadedb.integration.importer.AnalyzedSchema;
import com.arcadedb.integration.importer.ImportException;
import com.arcadedb.integration.importer.ImporterContext;
import com.arcadedb.integration.importer.ImporterSettings;
import com.arcadedb.integration.importer.Parser;
import com.arcadedb.integration.importer.SourceSchema;
import com.arcadedb.log.LogManager;
import com.arcadedb.utility.FileUtils;

import javax.xml.XMLConstants;
import javax.xml.stream.XMLInputFactory;
import javax.xml.stream.XMLStreamException;
import javax.xml.stream.XMLStreamReader;
import java.io.*;
import java.util.*;
import java.util.logging.*;

public class XMLImporterFormat implements FormatImporter {
  @Override
  public void load(final SourceSchema sourceSchema, final AnalyzedEntity.ENTITY_TYPE entityType, final Parser parser,
      final DatabaseInternal database,
      final ImporterContext context, final ImporterSettings settings) throws IOException {
    try {
      final int objectNestLevel = settings.getIntValue("objectNestLevel", 1);

      final XMLInputFactory xmlFactory = XMLInputFactory.newInstance();
      xmlFactory.setProperty(XMLConstants.ACCESS_EXTERNAL_DTD, "");
      xmlFactory.setProperty(XMLConstants.ACCESS_EXTERNAL_SCHEMA, "");
      final XMLStreamReader xmlReader = xmlFactory.createXMLStreamReader(parser.getInputStream());

      int nestLevel = 0;

      String entityName = null;
      String lastName = null;
      String lastContent = null;

      final Map<String, Object> object = new LinkedHashMap<>();

      while (xmlReader.hasNext()) {
        final int eventType = xmlReader.next();

        switch (eventType) {
        case XMLStreamReader.COMMENT:
        case XMLStreamReader.SPACE:
          // IGNORE IT
          break;

        case XMLStreamReader.START_ELEMENT:
          if (nestLevel == objectNestLevel) {
            entityName = "v_" + xmlReader.getName().toString();

            // GET ELEMENT'S ATTRIBUTES AS PROPERTIES
            for (int i = 0; i < xmlReader.getAttributeCount(); ++i) {
              object.put(xmlReader.getAttributeName(i).toString(), xmlReader.getAttributeValue(i));
              lastName = null;
            }
          } else if (nestLevel == objectNestLevel + 1) {
            // GET ELEMENT'S SUB-NODES AS PROPERTIES
            if (lastName != null)
              object.put(lastName, lastContent);

            lastName = xmlReader.getName().toString();
          }

          ++nestLevel;
          break;

        case XMLStreamReader.END_ELEMENT:
          if (lastName != null)
            object.put(lastName, lastContent);

          LogManager.instance().log(this, Level.FINE, "</%s> (nestLevel=%d)", null, xmlReader.getName(), nestLevel);

          --nestLevel;

          if (nestLevel == objectNestLevel) {
            context.parsed.incrementAndGet();

            final MutableVertex record = database.newVertex(entityName);
            record.fromMap(object);
            database.async().createRecord(record, newDocument -> context.createdVertices.incrementAndGet());
          }
          break;

        case XMLStreamReader.ATTRIBUTE:
          ++nestLevel;
          LogManager.instance()
              .log(this, Level.FINE, "- attribute %s attributes=%d (nestLevel=%d)", null, xmlReader.getName(),
                  xmlReader.getAttributeCount(), nestLevel);
          break;

        case XMLStreamReader.CHARACTERS:
        case XMLStreamReader.CDATA:
          final String text = xmlReader.getText();
          if (!text.isEmpty() && !text.equals("\n")) {
            if (settings.trimText)
              lastContent = text.trim();
            else
              lastContent = text;
          } else
            lastContent = null;
          break;

        default:
          // IGNORE IT
        }

        if (settings.parsingLimitEntries > 0 && context.parsed.get() > settings.parsingLimitEntries)
          break;
      }
    } catch (final Exception e) {
      throw new ImportException("Error on importing from source '" + parser.getSource() + "'", e);
    }
  }

  @Override
  public SourceSchema analyze(final AnalyzedEntity.ENTITY_TYPE entityType, final Parser parser, final ImporterSettings settings,
      final AnalyzedSchema analyzedSchema) {
    final int analyzingLimitEntries = settings.getIntValue("analyzingLimitEntries", 0);
    final int objectNestLevel = settings.getIntValue("objectNestLevel", 1);

    long parsedObjects = 0;

    final String currentUnit = parser.isCompressed() ? "uncompressed " : "";
    final String totalUnit = parser.isCompressed() ? "compressed " : "";

    try {
      parser.reset();

      final XMLInputFactory xmlFactory = XMLInputFactory.newInstance();
      xmlFactory.setProperty(XMLConstants.ACCESS_EXTERNAL_DTD, "");
      xmlFactory.setProperty(XMLConstants.ACCESS_EXTERNAL_SCHEMA, "");

      final XMLStreamReader xmlReader = xmlFactory.createXMLStreamReader(parser.getInputStream());

      int nestLevel = 0;

      boolean parsedStructure = false;

      String entityName = null;
      String lastName = null;
      String lastContent = null;

      while (xmlReader.hasNext()) {
        final int eventType = xmlReader.next();

        switch (eventType) {
        case XMLStreamReader.COMMENT:
        case XMLStreamReader.SPACE:
          // IGNORE IT
          break;

        case XMLStreamReader.START_ELEMENT:
          LogManager.instance()
              .log(this, Level.FINE, "<%s> attributes=%d (nestLevel=%d)", null, xmlReader.getName(), xmlReader.getAttributeCount(),
                  nestLevel);

          if (nestLevel == objectNestLevel) {
            entityName = xmlReader.getName().toString();

            // GET ELEMENT'S ATTRIBUTES AS PROPERTIES
            for (int i = 0; i < xmlReader.getAttributeCount(); ++i) {
              analyzedSchema.getOrCreateEntity(entityName, entityType)
                  .getOrCreateProperty(xmlReader.getAttributeName(i).toString(), xmlReader.getAttributeValue(i));
              lastName = null;
            }
          } else if (nestLevel == objectNestLevel + 1) {
            // GET ELEMENT'S SUB-NODES AS PROPERTIES
            if (lastName != null)
              analyzedSchema.getOrCreateEntity(entityName, entityType).getOrCreateProperty(lastName, lastContent);

            lastName = xmlReader.getName().toString();
          }

          ++nestLevel;
          break;

        case XMLStreamReader.END_ELEMENT:
          if (lastName != null)
            analyzedSchema.getOrCreateEntity(entityName, entityType).getOrCreateProperty(lastName, lastContent);

          LogManager.instance().log(this, Level.FINE, "</%s> (nestLevel=%d)", null, xmlReader.getName(), nestLevel);

          --nestLevel;

          if (nestLevel == objectNestLevel) {
            ++parsedObjects;

            if (!parsedStructure)
              parsedStructure = true;

            if (parsedObjects % 10000 == 0) {
              LogManager.instance().log(this, Level.INFO, "- Parsed %d XML objects (%s%s/%s%s)", null, parsedObjects, currentUnit,
                  FileUtils.getSizeAsString(parser.getPosition()), totalUnit, FileUtils.getSizeAsString(parser.getTotal()));
            }
          }
          break;

        case XMLStreamReader.ATTRIBUTE:
          ++nestLevel;
          LogManager.instance()
              .log(this, Level.FINE, "- attribute %s attributes=%d (nestLevel=%d)", null, xmlReader.getName(),
                  xmlReader.getAttributeCount(), nestLevel);
          break;

        case XMLStreamReader.CHARACTERS:
        case XMLStreamReader.CDATA:
          final String text = xmlReader.getText();
          if (!text.isEmpty() && !text.equals("\n")) {
            if (settings.trimText)
              lastContent = text.trim();
            else
              lastContent = text;
          } else
            lastContent = null;
          break;

        default:
          // IGNORE IT
        }

        if (analyzingLimitEntries > 0 && parsedObjects > analyzingLimitEntries)
          break;
      }

    } catch (final XMLStreamException e) {
      // IGNORE IT

    } catch (final Exception e) {
      LogManager.instance().log(this, Level.SEVERE, "Error on parsing XML", e);
      return null;
    }

    // END OF PARSING. THIS DETERMINES THE TYPE
    analyzedSchema.endParsing();

    return new SourceSchema(this, parser.getSource(), analyzedSchema);
  }

  @Override
  public String getFormat() {
    return "XML";
  }
}
