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
package com.arcadedb.integration.importer.graph;

import javax.xml.stream.XMLInputFactory;
import javax.xml.stream.XMLStreamConstants;
import javax.xml.stream.XMLStreamReader;
import java.io.BufferedInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.util.HashMap;
import java.util.Map;

/**
 * {@link GraphImporter.RecordSource} that reads XML elements from a file.
 * Supports two modes:
 * <ul>
 *   <li><b>Attribute mode</b> (default): reads self-closing {@code <row Id="1" Name="..." />} elements.
 *       Used by StackOverflow data dumps.</li>
 *   <li><b>Element mode</b>: reads elements with child sub-elements as fields,
 *       e.g. {@code <book id="1"><title>...</title><author>...</author></book>}.
 *       Attributes of the parent element are also available.</li>
 * </ul>
 * Uses StAX for streaming with constant memory.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
public class XmlRowSource implements GraphImporter.RecordSource {

  private final String  filePath;
  private final String  elementName;
  private final boolean readChildElements;

  /** Creates an attribute-mode source reading {@code <row ... />} elements. */
  public XmlRowSource(final String filePath) {
    this(filePath, "row", false);
  }

  /**
   * Creates a source reading elements with the given name.
   *
   * @param filePath          path to the XML file
   * @param elementName       name of the elements to read (e.g. "row", "book", "user")
   * @param readChildElements if true, child element text content is included as fields alongside attributes
   */
  public XmlRowSource(final String filePath, final String elementName, final boolean readChildElements) {
    this.filePath = filePath;
    this.elementName = elementName;
    this.readChildElements = readChildElements;
  }

  public static XmlRowSource from(final String dir, final String fileName) {
    return new XmlRowSource(new File(dir, fileName).getPath());
  }

  public static XmlRowSource from(final String dir, final String fileName, final String elementName) {
    return new XmlRowSource(new File(dir, fileName).getPath(), elementName, true);
  }

  @Override
  public void forEach(final GraphImporter.RecordVisitor visitor) throws Exception {
    System.setProperty("jdk.xml.maxGeneralEntitySizeLimit", "0");
    System.setProperty("jdk.xml.totalEntitySizeLimit", "0");
    System.setProperty("jdk.xml.entityExpansionLimit", "0");

    final XMLInputFactory factory = XMLInputFactory.newInstance();
    factory.setProperty(XMLInputFactory.IS_COALESCING, true);
    factory.setProperty(XMLInputFactory.IS_NAMESPACE_AWARE, false);
    factory.setProperty(XMLInputFactory.SUPPORT_DTD, false);

    final XMLStreamReader reader = factory.createXMLStreamReader(
        new BufferedInputStream(new FileInputStream(filePath), 1 << 16));
    try {
      if (readChildElements)
        forEachWithChildren(reader, visitor);
      else
        forEachAttributes(reader, visitor);
    } finally {
      reader.close();
    }
  }

  /** Fast path: reads attributes only (e.g. StackOverflow <row ... /> format). */
  private void forEachAttributes(final XMLStreamReader reader, final GraphImporter.RecordVisitor visitor) throws Exception {
    final AttrRecordReader rec = new AttrRecordReader();
    while (reader.hasNext()) {
      if (reader.next() == XMLStreamConstants.START_ELEMENT && elementName.equals(reader.getLocalName())) {
        rec.reader = reader;
        visitor.visit(rec);
      }
    }
  }

  /** Reads parent attributes + child element text as fields. */
  private void forEachWithChildren(final XMLStreamReader reader, final GraphImporter.RecordVisitor visitor) throws Exception {
    final MapRecordReader rec = new MapRecordReader();
    while (reader.hasNext()) {
      final int event = reader.next();
      if (event == XMLStreamConstants.START_ELEMENT && elementName.equals(reader.getLocalName())) {
        rec.fields.clear();
        // Collect parent element attributes
        for (int i = 0; i < reader.getAttributeCount(); i++)
          rec.fields.put(reader.getAttributeLocalName(i), reader.getAttributeValue(i));
        // Collect child elements as key=textContent
        while (reader.hasNext()) {
          final int childEvent = reader.next();
          if (childEvent == XMLStreamConstants.START_ELEMENT) {
            final String childName = reader.getLocalName();
            final String text = reader.getElementText(); // advances to END_ELEMENT
            if (text != null && !text.isEmpty())
              rec.fields.put(childName, text.trim());
          } else if (childEvent == XMLStreamConstants.END_ELEMENT && elementName.equals(reader.getLocalName())) {
            break;
          }
        }
        visitor.visit(rec);
      }
    }
  }

  /** Reads attributes directly from the StAX reader (zero-alloc per record). */
  private static class AttrRecordReader implements GraphImporter.RecordReader {
    XMLStreamReader reader;

    @Override
    public String get(final String attribute) {
      return reader.getAttributeValue(null, attribute);
    }

    @Override
    public int getInt(final String attribute) {
      final String v = reader.getAttributeValue(null, attribute);
      return v != null && !v.isEmpty() ? Integer.parseInt(v) : 0;
    }
  }

  /** Reads from a pre-collected map (attributes + child elements). */
  private static class MapRecordReader implements GraphImporter.RecordReader {
    final Map<String, String> fields = new HashMap<>();

    @Override
    public String get(final String attribute) {
      return fields.get(attribute);
    }

    @Override
    public int getInt(final String attribute) {
      final String v = fields.get(attribute);
      return v != null && !v.isEmpty() ? Integer.parseInt(v) : 0;
    }
  }
}
