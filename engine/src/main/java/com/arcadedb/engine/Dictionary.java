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
package com.arcadedb.engine;

import com.arcadedb.database.Binary;
import com.arcadedb.database.DatabaseFactory;
import com.arcadedb.database.DatabaseInternal;
import com.arcadedb.exception.DatabaseMetadataException;
import com.arcadedb.exception.SchemaException;
import com.arcadedb.log.LogManager;
import com.arcadedb.schema.DocumentType;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.logging.Level;

/**
 * HEADER = [itemCount(int:4),pageSize(int:4)] CONTENT-PAGES = [propertyName(string)]
 * <br>
 */
public class Dictionary extends PaginatedComponent {
  public static final  String               DICT_EXT               = "dict";
  public static final  int                  DEF_PAGE_SIZE          = 65536 * 5;
  private static final int                  CURRENT_VERSION        = 0;
  private              List<String>         dictionary             = new CopyOnWriteArrayList<>();
  private              Map<String, Integer> dictionaryMap          = new ConcurrentHashMap<>();
  // THIS IS LEGACY BECAUSE THE NUMBER OF ITEMS WAS STORED IN THE HEADER. NOW THE DICTIONARY IS POPULATED FROM THE ACTUAL CONTENT IN THE PAGES
  private static final int                  DICTIONARY_HEADER_SIZE = Binary.INT_SERIALIZED_SIZE;

  public static class PaginatedComponentFactoryHandler implements PaginatedComponentFactory.PaginatedComponentFactoryHandler {
    @Override
    public PaginatedComponent createOnLoad(final DatabaseInternal database, final String name, final String filePath, final int fileId,
        final PaginatedFile.MODE mode, final int pageSize, final int version) throws IOException {
      return new Dictionary(database, name, filePath, fileId, mode, pageSize, version);
    }
  }

  /**
   * Called at creation time.
   */
  public Dictionary(final DatabaseInternal database, final String name, String filePath, final PaginatedFile.MODE mode, final int pageSize) throws IOException {
    super(database, name, filePath, DICT_EXT, mode, pageSize, CURRENT_VERSION);
    if (file.getSize() == 0) {
      // NEW FILE, CREATE HEADER PAGE
      final MutablePage header = database.getTransaction().addPage(new PageId(file.getFileId(), 0), pageSize);
      updateCounters(header);
    }
  }

  /**
   * Called at load time.
   */
  public Dictionary(final DatabaseInternal database, final String name, final String filePath, final int id, final PaginatedFile.MODE mode, final int pageSize,
      final int version) throws IOException {
    super(database, name, filePath, id, mode, pageSize, version);
    reload();
  }

  public int getIdByName(final String name, final boolean create) {
    if (name == null)
      throw new IllegalArgumentException("Dictionary item name was null");

    Integer pos = dictionaryMap.get(name);
    if (pos == null && create) {
      // SYNCHRONIZE THIS BLOCK TO AVOID RACE CONDITIONS WITH RETRIES
      synchronized (this) {
        pos = dictionaryMap.get(name);
        if (pos == null) {

          final AtomicInteger newPos = new AtomicInteger();

          database.transaction(() -> {
            newPos.set(dictionary.size());
            addItemToPage(name);
          }, false);

          if (dictionaryMap.putIfAbsent(name, newPos.get()) == null) {
            dictionary.add(name);
            if (dictionary.size() != newPos.get() + 1) {
              try {
                reload();
              } catch (IOException e) {
                // IGNORE IT
              }
              throw new SchemaException("Error on updating dictionary for key '" + name + "'");
            }
          }
          pos = dictionaryMap.get(name);
        }
      }
    }

    if (pos == null)
      return -1;

    return pos;
  }

  public String getNameById(final int nameId) {
    if (nameId < 0 || nameId >= dictionary.size())
      throw new IllegalArgumentException("Dictionary item with id " + nameId + " is not valid (total=" + dictionary.size() + ")");

    final String itemName = dictionary.get(nameId);
    if (itemName == null)
      throw new IllegalArgumentException("Dictionary item with id " + nameId + " was not found");

    return itemName;
  }

  public Map<String, Integer> getDictionaryMap() {
    return Collections.unmodifiableMap(dictionaryMap);
  }

  /**
   * Updates a name. The update will impact the entire database with both properties and values (if used as ENUM). The update is valid only if the name has not been used as type name.
   *
   * @param oldName The old name to rename. Must be already present in the schema dictionary
   * @param newName The new name. Can be already present in the schema dictionary
   */
  public void updateName(final String oldName, final String newName) {
    if (!database.isTransactionActive())
      throw new SchemaException("Error on adding new item to the database schema dictionary because no transaction was active");

    if (oldName == null)
      throw new IllegalArgumentException("Dictionary old item name was null");

    if (newName == null)
      throw new IllegalArgumentException("Dictionary new item name was null");

    try {
      dictionaryMap.remove(oldName);

      final List<Integer> oldIndexes = new ArrayList<>();
      while (true) {
        final int oldIndex = dictionary.indexOf(oldName);
        if (oldIndex == -1)
          break;

        oldIndexes.add(oldIndex);

        dictionary.set(oldIndex, newName);
      }

      if (oldIndexes.isEmpty())
        throw new IllegalArgumentException("Item '" + oldName + "' not found in the dictionary");

      for (DocumentType t : database.getSchema().getTypes())
        if (oldName.equals(t.getName()))
          throw new IllegalArgumentException("Cannot rename the item '" + oldName + "' in the dictionary because it has been used as a type name");

      final MutablePage header = database.getTransaction().getPageToModify(new PageId(file.getFileId(), 0), pageSize, false);

      header.clearContent();
      updateCounters(header);

      for (String d : dictionary) {
        final byte[] property = d.getBytes(DatabaseFactory.getDefaultCharset());

        if (header.getAvailableContentSize() < Binary.SHORT_SERIALIZED_SIZE + property.length)
          throw new DatabaseMetadataException("No space left in dictionary file (items=" + dictionary.size() + ")");

        header.writeString(header.getContentSize(), d);
      }

      final Integer newIndex = dictionaryMap.get(newName);
      if (newIndex == null)
        dictionaryMap.putIfAbsent(newName, oldIndexes.get(0)); // IF ALREADY PRESENT, USE THE PREVIOUS KEY INDEX

    } catch (IOException e) {
      try {
        reload();
      } catch (IOException ioException) {
        LogManager.instance().log(this, Level.SEVERE, "Error on reloading dictionary", ioException);
      }
      throw new SchemaException("Error on updating name in dictionary");
    }
  }

  private void addItemToPage(final String propertyName) {
    if (!database.isTransactionActive())
      throw new SchemaException("Error on adding new item to the database schema dictionary because no transaction was active");

    final byte[] property = propertyName.getBytes(DatabaseFactory.getDefaultCharset());

    final MutablePage header;
    try {
      header = database.getTransaction().getPageToModify(new PageId(file.getFileId(), 0), pageSize, false);

      if (header.getAvailableContentSize() < Binary.SHORT_SERIALIZED_SIZE + property.length)
        throw new DatabaseMetadataException("No space left in dictionary file (items=" + dictionary.size() + ")");

      header.writeString(header.getContentSize(), propertyName);

    } catch (IOException e) {
      throw new SchemaException("Error on adding new item to the database schema dictionary");
    }
  }

  private void updateCounters(final MutablePage header) {
    // THIS IS LEGACY CODE CONTAINING THE NUMBER OF ITEMS. NOW THE ITEMS ARE DIRECTLY READ FORM THE PAGE
    header.writeInt(0, 0);
  }

  public void reload() throws IOException {
    if (file.getSize() == 0) {
      // NEW FILE, CREATE HEADER PAGE
      final MutablePage header = database.getTransaction().addPage(new PageId(file.getFileId(), 0), pageSize);
      updateCounters(header);

    } else {
      final BasePage header = database.getTransaction().getPage(new PageId(file.getFileId(), 0), pageSize);

      header.setBufferPosition(0);

      final List<String> newDictionary = new CopyOnWriteArrayList<>();

      // LOAD THE DICTIONARY IN RAM
      header.setBufferPosition(DICTIONARY_HEADER_SIZE);
      for (int i = 0; header.getBufferPosition() < header.getContentSize(); ++i)
        newDictionary.add(header.readString());

      final Map<String, Integer> newDictionaryMap = new ConcurrentHashMap<>();
      for (int i = 0; i < newDictionary.size(); ++i)
        newDictionaryMap.putIfAbsent(newDictionary.get(i), i);

      this.dictionary = newDictionary;
      this.dictionaryMap = newDictionaryMap;
    }
  }
}
