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

import com.arcadedb.database.Document;
import com.arcadedb.database.Identifiable;
import com.arcadedb.database.Record;
import com.arcadedb.serializer.BinaryComparator;

import java.text.*;
import java.util.*;
import java.util.Map.*;

public class TableFormatter {
  public static final  int    DEFAULT_MAX_WIDTH = 150;
  private static final String TYPE_COLUMN       = "@TYPE";
  private static final String RID_COLUMN        = "@RID";
  private static final String TYPE_PROPERTY     = "@type";
  private static final String RID_PROPERTY      = "@rid";

  public enum ALIGNMENT {
    LEFT, CENTER, RIGHT
  }

  protected final static String           MORE           = "...";
  protected final static SimpleDateFormat DEF_DATEFORMAT = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS");

  protected       Pair<String, Boolean>            columnSorting        = null;
  protected final Map<String, ALIGNMENT>           columnAlignment      = new LinkedHashMap<String, ALIGNMENT>();
  protected final Map<String, Map<String, String>> columnMetadata       = new LinkedHashMap<String, Map<String, String>>();
  protected final Set<String>                      columnHidden         = new HashSet<String>();
  protected       Set<String>                      prefixedColumns      = new LinkedHashSet<>();
  protected final TableOutput                      out;
  protected       int                              maxMultiValueEntries = 10;
  protected       int                              minColumnSize        = 4;
  protected       int                              maxWidthSize         = DEFAULT_MAX_WIDTH;
  protected       String                           nullValue            = "";
  protected       boolean                          leftBorder           = true;
  protected       boolean                          rightBorder          = true;
  protected       TableRow                         footer;
  protected       boolean                          lastResultShrunk     = false;

  public interface TableOutput {
    void onMessage(String text, Object... args);
  }

  public interface TableRow {
    Object getField(String field);

    Set<String> getFields();
  }

  public static class TableMapRow implements TableRow {
    private final Map<String, Object> map;

    public TableMapRow(final Map<String, Object> map) {
      this.map = map;
    }

    public TableMapRow() {
      map = new LinkedHashMap<>();
    }

    @Override
    public Object getField(final String field) {
      return map.get(field);
    }

    @Override
    public Set<String> getFields() {
      return map.keySet();
    }

    public void setField(final String name, final Object value) {
      map.put(name, value);
    }
  }

  public TableFormatter(final TableOutput iConsole) {
    this.out = iConsole;
  }

  public Set<String> getPrefixedColumns() {
    return prefixedColumns;
  }

  public void setPrefixedColumns(final String... prefixedColumns) {
    this.prefixedColumns = prefixedColumns != null ? new LinkedHashSet<>(Arrays.asList(prefixedColumns)) : new LinkedHashSet<>();
  }

  public void setColumnSorting(final String column, final boolean ascending) {
    columnSorting = new Pair<>(column, ascending);
  }

  public void setColumnHidden(final String column) {
    columnHidden.add(column);
  }

  public void writeRows(final List<? extends TableRow> rows, final int limit) {
    final Map<String, Integer> columns = parseColumns(rows, limit);

    if (columnSorting != null) {
      rows.sort((Comparator<Object>) (o1, o2) -> {
          final Document doc1 = (Document) ((Identifiable) o1).getRecord();
          final Document doc2 = (Document) ((Identifiable) o2).getRecord();
          final Object value1 = doc1.get(columnSorting.getFirst());
          final Object value2 = doc2.get(columnSorting.getFirst());
          final boolean ascending = columnSorting.getSecond();

          final int result;
          if (value2 == null)
              result = 1;
          else if (value1 == null)
              result = 0;
          else if (value1 instanceof Comparable)
              result = BinaryComparator.compareTo(value1, value2);
          else
              result = BinaryComparator.compareTo(value1.toString(), value2.toString());

          return ascending ? result : result * -1;
      });
    }

    int fetched = 0;
    for (TableRow record : rows) {
      dumpRecordInTable(fetched++, record, columns);

      if (limit > -1 && fetched >= limit) {
        printHeaderLine(columns);

        if (lastResultShrunk)
          out.onMessage("\nNOTE: the result set did not fit the screen (" + maxWidthSize
              + " columns). Please consider changing max width (example: set maxwidth = 200) or reduce the selected fields");

        out.onMessage("\nLIMIT EXCEEDED: result set contains more items not displayed (limit=" + limit + ")");
        return;
      }
    }

    if (fetched > 0)
      printHeaderLine(columns);

    if (lastResultShrunk)
      out.onMessage("\nNOTE: the result set did not fit the screen (" + maxWidthSize
          + " columns). Please consider changing max width (example: set maxwidth = 200) or reduce the selected fields");

    if (footer != null) {
      dumpRecordInTable(-1, footer, columns);
      printHeaderLine(columns);
    }
  }

  public void setColumnAlignment(final String column, final ALIGNMENT alignment) {
    columnAlignment.put(column, alignment);
  }

  public void setColumnMetadata(final String columnName, final String metadataName, final String metadataValue) {
    Map<String, String> metadata = columnMetadata.get(columnName);
    if (metadata == null) {
      metadata = new LinkedHashMap<String, String>();
      columnMetadata.put(columnName, metadata);
    }
    metadata.put(metadataName, metadataValue);
  }

  public int getMaxWidthSize() {
    return maxWidthSize;
  }

  public TableFormatter setMaxWidthSize(final int maxWidthSize) {
    this.maxWidthSize = maxWidthSize;
    return this;
  }

  public boolean isLastResultShrunk() {
    return lastResultShrunk;
  }

  public int getMaxMultiValueEntries() {
    return maxMultiValueEntries;
  }

  public TableFormatter setMaxMultiValueEntries(final int maxMultiValueEntries) {
    this.maxMultiValueEntries = maxMultiValueEntries;
    return this;
  }

  public void dumpRecordInTable(final int iIndex, final TableRow iRecord, final Map<String, Integer> iColumns) {
    if (iIndex == 0)
      printHeader(iColumns);

    // FORMAT THE LINE DYNAMICALLY
    List<String> vargs = new ArrayList<String>();
    try {
      final StringBuilder format = new StringBuilder(maxWidthSize);

      if (leftBorder)
        format.append('|');

      int i = 0;
      for (Entry<String, Integer> col : iColumns.entrySet()) {
        final String columnName = col.getKey();
        final int columnWidth = col.getValue();

        if (i++ > 0)
          format.append('|');

        format.append("%-" + columnWidth + "s");

        Object value = getFieldValue(iIndex, iRecord, columnName);
        String valueAsString = null;
        String strippedValue = null;

        if (value != null) {
          valueAsString = value.toString();
          strippedValue = getStrippedString(valueAsString);
          if (strippedValue.length() > columnWidth) {
            // APPEND ...
            valueAsString = valueAsString.substring(0, columnWidth - 3) + MORE;
          }
        }

        valueAsString = formatCell(columnName, columnWidth, valueAsString, strippedValue);

        vargs.add(AnsiCode.format(valueAsString));
      }

      if (rightBorder)
        format.append('|');

      out.onMessage("\n" + format, vargs.toArray());

    } catch (Exception t) {
      out.onMessage("%3d|%9s|%s\n", iIndex, iRecord, "Error on loading record due to: " + t);
    }
  }

  protected String formatCell(final String columnName, final int columnWidth, String valueAsString, String strippedValue) {
    if (valueAsString == null)
      valueAsString = nullValue;
    if (strippedValue == null)
      strippedValue = nullValue;

    final ALIGNMENT alignment = columnAlignment.get(columnName);
    if (alignment != null) {
      switch (alignment) {
      case LEFT: {
        final int room = columnWidth - strippedValue.length();
        if (room > 0) {
          for (int k = 0; k < room; ++k)
            valueAsString = valueAsString + " ";
        }
        break;
      }

      case CENTER: {
        final int room = columnWidth - strippedValue.length();
        if (room > 1) {
          for (int k = 0; k < room / 2; ++k)
            valueAsString = " " + valueAsString;
          for (int k = strippedValue.length() + (room / 2); k < columnWidth; ++k)
            valueAsString = valueAsString + " ";
        }
        break;
      }

      case RIGHT: {
        final int room = columnWidth - strippedValue.length();
        if (room > 0) {
          for (int k = 0; k < room; ++k)
            valueAsString = " " + valueAsString;
        }
        break;
      }
      }
    }
    return valueAsString;
  }

  protected Object getFieldValue(final int iIndex, final TableRow row, final String iColumnName) {
    Object value = null;

    if (iColumnName.equals("#"))
      // RECORD NUMBER
      value = iIndex > -1 ? iIndex : "";
    else
      value = row.getField(iColumnName);

    return getPrettyFieldValue(value, maxMultiValueEntries);
  }

  public void setNullValue(final String s) {
    nullValue = s;
  }

  public void setLeftBorder(final boolean value) {
    leftBorder = value;
  }

  public void setRightBorder(final boolean value) {
    rightBorder = value;
  }

  public static String getPrettyFieldMultiValue(final Iterator<?> iterator, final int maxMultiValueEntries) {
    final StringBuilder value = new StringBuilder("[");
    int i = 0;
    boolean more = false;
    for (; iterator.hasNext(); i++) {
      final Object v = iterator.next();

      if (i >= maxMultiValueEntries)
        more = true;
      else {
        if (i > 0)
          value.append(',');

        value.append(getPrettyFieldValue(v, maxMultiValueEntries));
      }
    }

    value.append("]");
    value.insert(0, i);
    if (more)
      value.append("(more)");

    return value.toString();
  }

  public void setFooter(final TableRow footer) {
    this.footer = footer;
  }

  public static Object getPrettyFieldValue(Object value, final int multiValueMaxEntries) {
    if (value instanceof MultiIterator<?>)
      value = getPrettyFieldMultiValue(((MultiIterator<?>) value).iterator(), multiValueMaxEntries);
    else if (value instanceof Iterator)
      value = getPrettyFieldMultiValue((Iterator<?>) value, multiValueMaxEntries);
    else if (value instanceof Collection<?>)
      value = getPrettyFieldMultiValue(((Collection<?>) value).iterator(), multiValueMaxEntries);
    else if (value instanceof Record) {
      if (((Record) value).getIdentity() == null) {
        value = value.toString();
      } else {
        value = ((Record) value).getIdentity().toString();
      }
    } else if (value instanceof Date) {
      synchronized (DEF_DATEFORMAT) {
        value = DEF_DATEFORMAT.format((Date) value);
      }
    } else if (value instanceof byte[])
      value = "byte[" + ((byte[]) value).length + "]";

    return value;
  }

  private void printHeader(final Map<String, Integer> iColumns) {
    final StringBuilder columnRow = new StringBuilder("\n");
    final Map<String, StringBuilder> metadataRows = new HashMap<String, StringBuilder>();

    // INIT METADATA
    final LinkedHashSet<String> allMetadataNames = new LinkedHashSet<String>();

    for (Entry<String, Map<String, String>> entry : columnMetadata.entrySet()) {
      for (Entry<String, String> entry2 : entry.getValue().entrySet()) {
        allMetadataNames.add(entry2.getKey());

        StringBuilder metadataRow = metadataRows.get(entry2.getKey());
        if (metadataRow == null) {
          metadataRow = new StringBuilder("\n");
          metadataRows.put(entry2.getKey(), metadataRow);
        }
      }
    }

    printHeaderLine(iColumns);
    int i = 0;

    if (leftBorder) {
      columnRow.append('|');
      if (!metadataRows.isEmpty()) {
        for (StringBuilder buffer : metadataRows.values())
          buffer.append('|');
      }
    }

    for (Entry<String, Integer> column : iColumns.entrySet()) {
      String colName = column.getKey();

      if (columnHidden.contains(colName))
        continue;

      if (i > 0) {
        columnRow.append('|');
        if (!metadataRows.isEmpty()) {
          for (StringBuilder buffer : metadataRows.values())
            buffer.append('|');
        }
      }

      if (colName.length() > column.getValue())
        colName = colName.substring(0, column.getValue());
      columnRow.append(String.format("%-" + column.getValue() + "s", formatCell(colName, column.getValue(), colName, colName)));

      if (!metadataRows.isEmpty()) {
        // METADATA VALUE
        for (String metadataName : allMetadataNames) {
          final StringBuilder buffer = metadataRows.get(metadataName);
          final Map<String, String> metadataColumn = columnMetadata.get(column.getKey());
          String metadataValue = metadataColumn != null ? metadataColumn.get(metadataName) : null;
          if (metadataValue == null)
            metadataValue = "";

          if (metadataValue.length() > column.getValue())
            metadataValue = metadataValue.substring(0, column.getValue());
          buffer.append(String.format("%-" + column.getValue() + "s", formatCell(colName, column.getValue(), metadataValue, colName)));
        }
      }

      ++i;
    }

    if (rightBorder) {
      columnRow.append('|');
      if (!metadataRows.isEmpty()) {
        for (StringBuilder buffer : metadataRows.values())
          buffer.append('|');
      }
    }

    if (!metadataRows.isEmpty()) {
      // PRINT METADATA IF ANY
      for (StringBuilder buffer : metadataRows.values())
        out.onMessage(buffer.toString());
      printHeaderLine(iColumns);
    }

    out.onMessage(columnRow.toString());

    printHeaderLine(iColumns);
  }

  private void printHeaderLine(final Map<String, Integer> iColumns) {
    final StringBuilder buffer = new StringBuilder("\n");

    if (iColumns.size() > 0) {
      if (leftBorder)
        buffer.append('+');

      int i = 0;
      for (Entry<String, Integer> column : iColumns.entrySet()) {
        final String colName = column.getKey();
        if (columnHidden.contains(colName))
          continue;

        if (i++ > 0)
          buffer.append("+");

        for (int k = 0; k < column.getValue(); ++k)
          buffer.append("-");
      }

      if (rightBorder)
        buffer.append('+');
    }

    out.onMessage(AnsiCode.format(buffer.toString()));
  }

  /**
   * Fill the column map computing the maximum size for a field.
   *
   * @param rows
   * @param limit
   *
   * @return
   */
  private Map<String, Integer> parseColumns(final List<? extends TableRow> rows, final int limit) {
    final Map<String, Integer> columns = new LinkedHashMap<>();

    for (String c : prefixedColumns)
      columns.put(c, minColumnSize);

    boolean tempRids = false;
    boolean hasClass = false;

    int fetched = 0;
    for (TableRow row : rows) {
      for (String c : prefixedColumns)
        columns.put(c, getColumnSize(fetched, row, c, columns.get(c)));

      // PARSE ALL THE DOCUMENT'S FIELDS
      for (String fieldName : row.getFields()) {
        if (fieldName.equals(RID_PROPERTY) || fieldName.equals(TYPE_PROPERTY))
          continue;

        columns.put(fieldName, getColumnSize(fetched, row, fieldName, columns.get(fieldName)));
      }

      if (!hasClass && row.getField(TYPE_PROPERTY) != null)
        hasClass = true;

      if (!tempRids && row.getField(RID_PROPERTY) == null)
        tempRids = true;

      if (limit > -1 && fetched++ >= limit)
        break;
    }

    if (tempRids)
      columns.remove(RID_COLUMN);

    if (!hasClass)
      columns.remove(TYPE_COLUMN);

    if (footer != null) {
      // PARSE ALL THE DOCUMENT'S FIELDS
      for (String fieldName : footer.getFields()) {
        columns.put(fieldName, getColumnSize(fetched, footer, fieldName, columns.get(fieldName)));
      }
    }

    // COMPUTE MAXIMUM WIDTH
    int width = 0;
    for (Entry<String, Integer> col : columns.entrySet())
      width += col.getValue();

    lastResultShrunk = false;
    if (width > maxWidthSize) {
      // SCALE COLUMNS AUTOMATICALLY
      final List<Entry<String, Integer>> orderedColumns = new ArrayList<Entry<String, Integer>>(columns.entrySet());
      orderedColumns.sort((o1, o2) -> o1.getValue().compareTo(o2.getValue()));

      lastResultShrunk = true;

      // START CUTTING THE BIGGEST ONES
      Collections.reverse(orderedColumns);
      while (width > maxWidthSize) {
        int oldWidth = width;

        for (Entry<String, Integer> entry : orderedColumns) {
          final int redux = entry.getValue() * 10 / 100;

          if (entry.getValue() - redux < minColumnSize)
            // RESTART FROM THE LARGEST COLUMN
            break;

          entry.setValue(entry.getValue() - redux);

          width -= redux;
          if (width <= maxWidthSize)
            break;
        }

        if (width == oldWidth)
          // REACHED THE MINIMUM
          break;
      }

      // POPULATE THE COLUMNS WITH THE REDUXED VALUES
      for (String c : prefixedColumns)
        columns.put(c, minColumnSize);
      Collections.reverse(orderedColumns);
      for (Entry<String, Integer> col : orderedColumns)
        // if (!col.getKey().equals("#") && !col.getKey().equals("@RID"))
        columns.put(col.getKey(), col.getValue());
    }

    if (tempRids)
      columns.remove(RID_COLUMN);
    if (!hasClass)
      columns.remove(TYPE_COLUMN);

    for (String c : columnHidden)
      columns.remove(c);

    return columns;
  }

  private Integer getColumnSize(final Integer iIndex, final TableRow row, final String fieldName, final Integer origSize) {
    int newColumnSize;
    if (origSize == null)
      // START FROM THE FIELD NAME SIZE
      newColumnSize = fieldName.length();
    else
      newColumnSize = Math.max(origSize, fieldName.length());

    final Map<String, String> metadata = columnMetadata.get(fieldName);
    if (metadata != null) {
      // UPDATE WIDTH WITH METADATA VALUES
      for (String v : metadata.values()) {
        if (v != null) {
          if (v.length() > newColumnSize)
            newColumnSize = v.length();
        }
      }
    }

    final Object fieldValue = getFieldValue(iIndex, row, fieldName);

    if (fieldValue != null) {
      final String fieldValueAsString = fieldValue.toString();

      final String formattedString = getStrippedString(fieldValueAsString);

      if (formattedString.length() > newColumnSize)
        newColumnSize = formattedString.length();
    }

    if (newColumnSize < minColumnSize)
      // SET THE MINIMUM SIZE
      newColumnSize = minColumnSize;

    return newColumnSize;
  }

  private String getStrippedString(final String fieldValueAsString) {
    final StringBuilder formattedString = new StringBuilder();

    int lastPos = 0;
    while (true) {
      int pos = fieldValueAsString.indexOf("$ANSI{", lastPos);
      if (pos < 0) {
        formattedString.append(fieldValueAsString.substring(lastPos));
        break;
      }

      formattedString.append(fieldValueAsString, lastPos, pos);

      final int sepPos = fieldValueAsString.indexOf(" ", pos);

      final int closePos = fieldValueAsString.indexOf("}", sepPos);

      formattedString.append(fieldValueAsString, sepPos + 1, closePos);

      lastPos = closePos + 1;
    }
    return formattedString.toString();
  }
}
