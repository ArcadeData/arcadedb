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
import com.arcadedb.utility.CollectionUtils;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.logging.Level;

/**
 * HEADER = [itemCount(int:4),pageSize(int:4)] CONTENT-PAGES = [propertyName(string)]
 * <br>
 * The whole dictionary lives in page 0: the maximum total size of the names it can hold is
 * {@code pageSize - BasePage.PAGE_HEADER_SIZE - DICTIONARY_HEADER_SIZE} bytes, each name costing its UTF-8 length plus a
 * 1-3 byte varint prefix. With the default page size that is ~327Kb of names, i.e. tens of thousands of entries.
 * Entries are only ever appended and are never reclaimed.
 * <br>
 */
public class Dictionary extends PaginatedComponent {
  public static final  String DICT_EXT        = "dict";
  public static final  int    DEF_PAGE_SIZE   = 65536 * 5;
  private static final int    CURRENT_VERSION = 0;
  // THIS IS LEGACY BECAUSE THE NUMBER OF ITEMS WAS STORED IN THE HEADER. NOW THE DICTIONARY IS POPULATED FROM THE ACTUAL CONTENT IN THE PAGES
  private static final int    DICTIONARY_HEADER_SIZE = Binary.INT_SERIALIZED_SIZE;

  /**
   * The name list and the name->id map are replaced together by {@link #reload()}, which runs on threads that do not hold this
   * component's monitor (transaction rollback and replication apply). Publishing them as one immutable pair through a single
   * volatile field is what makes a reader either see both halves of the old snapshot or both halves of the new one, and is
   * what gives the reader a happens-before edge with the rebuild at all.
   */
  private record Entries(List<String> names, ConcurrentMap<String, Integer> ids) {
  }

  private volatile Entries entries = new Entries(new CopyOnWriteArrayList<>(), new ConcurrentHashMap<>(1024));

  public static class PaginatedComponentFactoryHandler implements ComponentFactory.PaginatedComponentFactoryHandler {
    @Override
    public PaginatedComponent createOnLoad(final DatabaseInternal database, final String name, final String filePath,
        final int fileId,
        final ComponentFile.MODE mode, final int pageSize, final int version) throws IOException {
      return new Dictionary(database, name, filePath, fileId, mode, pageSize, version);
    }
  }

  /**
   * Called at creation time.
   */
  public Dictionary(final DatabaseInternal database, final String name, final String filePath, final ComponentFile.MODE mode,
      final int pageSize)
      throws IOException {
    super(database, name, filePath, DICT_EXT, mode, pageSize, CURRENT_VERSION);
    if (file.getSize() == 0) {
      // NEW FILE, CREATE HEADER PAGE
      final MutablePage header = database.getTransaction().addPage(new PageId(database, file.getFileId(), 0), pageSize);
      updateCounters(header);
    }
  }

  /**
   * Called at load time.
   */
  public Dictionary(final DatabaseInternal database, final String name, final String filePath, final int id,
      final ComponentFile.MODE mode, final int pageSize,
      final int version) throws IOException {
    super(database, name, filePath, id, mode, pageSize, version);
    reload();
  }

  public int getIdByName(final String name, final boolean create) {
    if (name == null)
      throw new IllegalArgumentException("Dictionary item name was null");

    Integer pos = entries.ids().get(name);
    if (pos == null && create) {
      // SYNCHRONIZE THIS BLOCK TO AVOID RACE CONDITIONS WITH RETRIES
      synchronized (this) {
        Entries current = entries;
        pos = current.ids().get(name);
        if (pos == null) {

          final AtomicInteger newPos = new AtomicInteger();

          database.transaction(() -> {
            newPos.set(entries.names().size());
            addItemToPage(name);
          }, false);

          // THE COMMIT ABOVE CAN HAVE SWAPPED THE SNAPSHOT (RELOAD ON ROLLBACK/RETRY): RE-READ IT
          current = entries;

          if (current.ids().putIfAbsent(name, newPos.get()) == null) {
            current.names().add(name);
            if (current.names().size() != newPos.get() + 1) {
              try {
                reload();
              } catch (final IOException e) {
                // IGNORE IT
              }
              throw new SchemaException("Error on updating dictionary for key '" + name + "'");
            }
          }
          pos = current.ids().get(name);
        }
      }
    }

    if (pos == null)
      return -1;

    return pos;
  }

  public String getNameById(final int nameId) {
    final List<String> names = entries.names();
    if (nameId < 0 || nameId >= names.size())
      throw new IllegalArgumentException("Dictionary item with id " + nameId + " is not valid (total=" + names.size() + ")");

    final String itemName = names.get(nameId);
    if (itemName == null)
      throw new IllegalArgumentException("Dictionary item with id " + nameId + " was not found");

    return itemName;
  }

  public Map<String, Integer> getDictionaryMap() {
    return CollectionUtils.immutableMap(entries.ids());
  }

  /**
   * Returns the bytes still available in the dictionary page for new names, prefix included.
   */
  public int getAvailableSpace() {
    try {
      final BasePage header = database.getTransaction().getPage(new PageId(database, file.getFileId(), 0), pageSize);
      return header.getMaxContentSize() - header.getContentSize();
    } catch (final IOException e) {
      throw new SchemaException("Error on reading the database schema dictionary", e);
    }
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

    if (oldName.equals(newName))
      // NOTHING TO DO. WITHOUT THIS THE LOOP BELOW WOULD NEVER FIND ITS TERMINATION CONDITION
      return;

    final List<String> dictionary = entries.names();
    final ConcurrentMap<String, Integer> dictionaryMap = entries.ids();

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

      for (final DocumentType t : database.getSchema().getTypes())
        if (oldName.equals(t.getName()))
          throw new IllegalArgumentException(
              "Cannot rename the item '" + oldName + "' in the dictionary because it has been used as a type name");

      final MutablePage header = database.getTransaction()
          .getPageToModify(new PageId(database, file.getFileId(), 0), pageSize, false);

      header.clearContent();
      updateCounters(header);

      for (final String d : dictionary) {
        final byte[] property = d.getBytes(DatabaseFactory.getDefaultCharset());

        checkSpaceLeft(header, property, dictionary.size());

        header.writeString(header.getContentSize(), d);
      }

      final Integer newIndex = dictionaryMap.get(newName);
      if (newIndex == null)
        dictionaryMap.putIfAbsent(newName, oldIndexes.getFirst()); // IF ALREADY PRESENT, USE THE PREVIOUS KEY INDEX

    } catch (final IOException e) {
      try {
        reload();
      } catch (final IOException ioException) {
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
      header = database.getTransaction().getPageToModify(new PageId(database, file.getFileId(), 0), pageSize, false);

      checkSpaceLeft(header, property, entries.names().size());

      header.writeString(header.getContentSize(), propertyName);

    } catch (final IOException e) {
      throw new SchemaException("Error on adding new item to the database schema dictionary");
    }
  }

  /**
   * The dictionary is a single page, so the free space is what is left of the page content area. Both the varint length prefix
   * that {@link MutablePage#writeString} emits and the 8 bytes of page header have to be accounted for: overestimating the
   * space by even one byte turns the actionable "no space left in dictionary" into a raw "cannot write outside the page space".
   */
  private void checkSpaceLeft(final BasePage header, final byte[] name, final int items) {
    final int required = Binary.getUnsignedNumberSpace(name.length) + name.length;
    if (header.getMaxContentSize() - header.getContentSize() < required)
      throw new DatabaseMetadataException(
          "No space left in dictionary file (items=" + items + ", pageSize=" + pageSize + "). The dictionary holds every type "
              + "name, property name and enumerated string value ever used in this database and entries are never reclaimed");
  }

  private void updateCounters(final MutablePage header) {
    // THIS IS LEGACY CODE CONTAINING THE NUMBER OF ITEMS. NOW THE ITEMS ARE DIRECTLY READ FORM THE PAGE
    header.writeInt(0, 0);
  }

  public void reload() throws IOException {
    if (file.getSize() == 0) {
      // No header page on disk. Creating it commits a transaction, and the commit resolves this file id
      // against the schema - which has not registered this component yet when the load path builds it,
      // so committing here fails with "File with id '0' was not found" and the database cannot be
      // opened at all. The creation is deferred to createHeaderPageIfMissing(), which LocalSchema calls
      // once the component is registered. The in-RAM dictionary is already empty, which is the correct
      // state for an empty file.
      return;

    } else {
      final BasePage header = database.getTransaction().getPage(new PageId(database, file.getFileId(), 0), pageSize);

      // LOAD THE DICTIONARY IN RAM. THE NAMES ARE COLLECTED IN AN ARRAYLIST AND THE COPY-ON-WRITE LIST IS BUILT ONCE FROM IT:
      // APPENDING TO A CopyOnWriteArrayList COPIES THE WHOLE BACKING ARRAY PER ITEM, MAKING THIS QUADRATIC ON THE ENTRY COUNT
      // (~150ms FOR 48K ENTRIES, PAID ON EVERY OPEN, EVERY ROLLBACK OF A DICTIONARY TX AND EVERY REPLICATED DICTIONARY CHANGE).
      final List<String> loaded = new ArrayList<>();

      header.setBufferPosition(DICTIONARY_HEADER_SIZE);
      for (int i = 0; header.getBufferPosition() < header.getContentSize(); ++i)
        loaded.add(header.readString());

      final int size = loaded.size();

      final ConcurrentMap<String, Integer> newDictionaryMap = new ConcurrentHashMap<>(size + (size / 3) + 1);
      for (int i = 0; i < size; ++i)
        newDictionaryMap.putIfAbsent(loaded.get(i), i);

      this.entries = new Entries(new CopyOnWriteArrayList<>(loaded), newDictionaryMap);
    }
  }

  /**
   * Writes the header page when the dictionary file is empty, which happens when the database was killed
   * before the page reached disk. Must be called only after the component has been registered in the
   * schema: the write commits a transaction whose second phase resolves this file id, so an earlier call
   * fails with {@code SchemaException: File with id '0' was not found}.
   */
  public void createHeaderPageIfMissing() throws IOException {
    if (file.getSize() > 0)
      return;

    database.transaction(() -> {
      final MutablePage header = database.getTransaction().addPage(new PageId(database, file.getFileId(), 0), pageSize);
      updateCounters(header);
    });
  }
}
