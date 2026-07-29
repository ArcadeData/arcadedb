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
 * Maps every type name, property name and enumerated string value of a database to a small integer id, which is what records
 * carry instead of the name itself.
 * <br>
 * PAGE = [itemCount(int:4)][name(string)]* , where a name is a 1-3 byte varint length followed by its UTF-8 bytes. Every page
 * has the same shape, page 0 included, so a dictionary written before multi-page support (which is exactly one such page) loads
 * unchanged.
 * <br>
 * <b>An id is the ordinal of the name in page order</b>, and that id is written inside records on disk. The layout is therefore
 * strictly append-only across pages: names are only ever added to the LAST page, a page that has been left behind is never
 * revisited even when it still has room, and nothing is ever removed. Writing into an earlier page would renumber every name
 * after it and silently repoint every record that referenced them.
 * <br>
 * A name is never split over two pages, so the tail of a page is left unused when the next name does not fit: at identifier
 * lengths that is well under 1% of the file. The only hard limit left is a single name larger than one page.
 * <br>
 */
public class Dictionary extends PaginatedComponent {
  public static final String DICT_EXT = "dict";
  /**
   * Before multi-page support this was 65536 * 5, sized only to make the one available page hold as many names as possible.
   * Now that pages roll over, the page is sized like the rest of the engine instead: a new name dirties and eventually flushes
   * one page, so a smaller page is five times less write amplification per name. Existing databases keep the page size they
   * were created with, which is read back from the file name.
   */
  public static final  int    DEF_PAGE_SIZE          = 65_536;
  /**
   * v0 is single page, v1 rolls over. The reader handles both, the version only marks what wrote the file.
   */
  private static final int    CURRENT_VERSION        = 1;
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
    if (version > CURRENT_VERSION)
      // A NEWER LAYOUT WOULD BE READ AS IF IT WERE THIS ONE, SILENTLY. REFUSE INSTEAD OF GUESSING
      throw new DatabaseMetadataException(
          "Dictionary '" + filePath + "' was written with format version " + version + ", which this version of ArcadeDB cannot "
              + "read (latest known is " + CURRENT_VERSION + ")");
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

    // VALIDATED BEFORE ANYTHING IS MUTATED: THE REWRITE BELOW EDITS THE IN-RAM VIEW FIRST, AND A newName TOO BIG FOR A PAGE
    // WOULD OTHERWISE LEAVE IT RENAMED BUT UNWRITTEN UNTIL THE CALLER'S ROLLBACK HAPPENED TO REPAIR IT. EVERY OTHER NAME IS
    // ALREADY STORED ON A PAGE OF THIS SIZE, SO newName IS THE ONLY ONE THAT CAN FAIL
    checkNameFitsAPage(newName, spaceRequiredBy(newName.getBytes(DatabaseFactory.getDefaultCharset())));

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

      // REWRITE THE WHOLE DICTIONARY FROM PAGE 0 IN THE SAME ORDER, SO NO ID MOVES. THE TOTAL SIZE CAN SHRINK OR GROW, SO PAGES
      // ARE ADDED AS NEEDED AND THE ONES THE NEW CONTENT NO LONGER REACHES ARE EMPTIED: reload() WALKS EVERY COMMITTED PAGE, AND
      // A STALE TAIL PAGE LEFT BEHIND WOULD RE-ADD ITS OLD NAMES ON THE NEXT LOAD.
      int pageNumber = 0;
      MutablePage page = resetPageForRewrite(pageNumber);

      for (final String d : dictionary) {
        final byte[] property = d.getBytes(DatabaseFactory.getDefaultCharset());
        final int required = spaceRequiredBy(property);
        // newName WAS ALREADY CHECKED ABOVE AND THE OTHERS FIT BY CONSTRUCTION: THIS ONLY CATCHES A CORRUPT PAGE, AND SAYS SO
        // INSTEAD OF LETTING writeString() FAIL WITH A RAW PAGE-BOUNDARY ERROR
        checkNameFitsAPage(d, required);

        if (freeSpaceIn(page) < required)
          page = resetPageForRewrite(++pageNumber);

        page.writeString(page.getContentSize(), d);
      }

      for (int stale = pageNumber + 1; stale < getTotalPages(); ++stale)
        resetPageForRewrite(stale);

      final Integer newIndex = dictionaryMap.get(newName);
      if (newIndex == null)
        dictionaryMap.putIfAbsent(newName, oldIndexes.getFirst()); // IF ALREADY PRESENT, USE THE PREVIOUS KEY INDEX

    } catch (final IOException e) {
      try {
        reload();
      } catch (final IOException ioException) {
        LogManager.instance().log(this, Level.SEVERE, "Error on reloading dictionary", ioException);
      }
      throw new SchemaException("Error on updating name in dictionary", e);
    }
  }

  /**
   * Empties the given page, creating it when the rewrite needs more pages than the dictionary currently has, and leaves it with
   * only the legacy counter so it is ready to receive names.
   */
  private MutablePage resetPageForRewrite(final int pageNumber) throws IOException {
    if (pageNumber >= getTotalPages())
      return addPage(pageNumber);

    final MutablePage page = database.getTransaction()
        .getPageToModify(new PageId(database, file.getFileId(), pageNumber), pageSize, false);
    page.clearContent();
    updateCounters(page);
    return page;
  }

  private void addItemToPage(final String propertyName) {
    if (!database.isTransactionActive())
      throw new SchemaException("Error on adding new item to the database schema dictionary because no transaction was active");

    final byte[] property = propertyName.getBytes(DatabaseFactory.getDefaultCharset());
    final int required = spaceRequiredBy(property);
    checkNameFitsAPage(propertyName, required);

    try {
      final int totalPages = getTotalPages();
      final MutablePage target;

      if (totalPages == 0)
        // NO PAGE AT ALL, WHICH HAPPENS WHEN THE DATABASE WAS KILLED BEFORE THE HEADER PAGE REACHED DISK
        target = addPage(0);
      else {
        // ONLY THE LAST PAGE IS EVER APPENDED TO, EVEN WHEN AN EARLIER ONE STILL HAS ROOM: SEE THE CLASS JAVADOC ON WHY
        // FILLING A GAP WOULD RENUMBER EVERY NAME AFTER IT.
        final int lastPageNumber = totalPages - 1;
        final PageId lastPageId = new PageId(database, file.getFileId(), lastPageNumber);

        // READ BEFORE DECIDING: getPageToModify() WOULD ENLIST THE PAGE AS MODIFIED EVEN WHEN THE NAME DOES NOT FIT, BUMPING ITS
        // VERSION AND REWRITING IT AT COMMIT FOR NOTHING, AND MAKING IT FALSE-CONFLICT WITH CONCURRENT TRANSACTIONS.
        final BasePage lastPage = database.getTransaction().getPage(lastPageId, pageSize);

        target = freeSpaceIn(lastPage) >= required ?
            database.getTransaction().getPageToModify(lastPageId, pageSize, false) :
            addPage(lastPageNumber + 1);
      }

      target.writeString(target.getContentSize(), propertyName);

    } catch (final IOException e) {
      throw new SchemaException("Error on adding new item to the database schema dictionary", e);
    }
  }

  /**
   * Appends an empty page to the dictionary, initialised with the legacy counter every page carries.
   */
  private MutablePage addPage(final int pageNumber) {
    final MutablePage page = database.getTransaction().addPage(new PageId(database, file.getFileId(), pageNumber), pageSize);
    updateCounters(page);
    return page;
  }

  /**
   * What one name occupies on a page: the varint length prefix that {@link MutablePage#writeString} emits, plus the UTF-8 bytes.
   */
  private static int spaceRequiredBy(final byte[] name) {
    return Binary.getUnsignedNumberSpace(name.length) + name.length;
  }

  /**
   * Free bytes left on a page. {@link BasePage#getMaxContentSize()} already excludes the page header, unlike
   * {@link BasePage#getAvailableContentSize()} which over-reports by exactly that header.
   */
  private static int freeSpaceIn(final BasePage page) {
    return page.getMaxContentSize() - page.getContentSize();
  }

  /**
   * Names never span pages, so one that does not fit an empty page can never be stored, no matter how many pages there are.
   */
  private void checkNameFitsAPage(final String name, final int required) {
    final int usable = pageSize - BasePage.PAGE_HEADER_SIZE - DICTIONARY_HEADER_SIZE;
    if (required > usable)
      throw new DatabaseMetadataException(
          "Dictionary item '" + (name.length() > 64 ? name.substring(0, 64) + "..." : name) + "' needs " + required
              + " bytes and cannot fit in a dictionary page of " + pageSize + " bytes (usable " + usable
              + "): a name is never split across pages");
  }

  private void updateCounters(final MutablePage header) {
    // THIS IS LEGACY CODE CONTAINING THE NUMBER OF ITEMS. NOW THE ITEMS ARE DIRECTLY READ FORM THE PAGE. IT IS STILL WRITTEN ON
    // EVERY PAGE, PAGE 0 INCLUDED, SO THAT ALL PAGES HAVE ONE SHAPE AND A PRE-MULTI-PAGE FILE NEEDS NO SPECIAL CASE
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
      // LOAD THE DICTIONARY IN RAM. THE NAMES ARE COLLECTED IN AN ARRAYLIST AND THE COPY-ON-WRITE LIST IS BUILT ONCE FROM IT:
      // APPENDING TO A CopyOnWriteArrayList COPIES THE WHOLE BACKING ARRAY PER ITEM, MAKING THIS QUADRATIC ON THE ENTRY COUNT
      // (~150ms FOR 48K ENTRIES, PAID ON EVERY OPEN, EVERY ROLLBACK OF A DICTIONARY TX AND EVERY REPLICATED DICTIONARY CHANGE).
      final List<String> loaded = new ArrayList<>();

      // THE COMMITTED PAGE COUNT, NOT getTotalPages(): reload() REBUILDS THE IN-RAM VIEW FROM DURABLE STATE, AND
      // TransactionContext.rollback() CALLS IT WHILE THE TRANSACTION'S OWN PAGE COUNTER STILL COUNTS PAGES THAT ARE BEING
      // THROWN AWAY. THE FLOOR OF 1 KEEPS THE PRE-MULTI-PAGE BEHAVIOUR OF ALWAYS READING PAGE 0 OF A NON-EMPTY FILE.
      final int totalPages = Math.max(1, pageCount.get());

      for (int pageNumber = 0; pageNumber < totalPages; ++pageNumber) {
        final BasePage page = database.getTransaction().getPage(new PageId(database, file.getFileId(), pageNumber), pageSize);

        page.setBufferPosition(DICTIONARY_HEADER_SIZE);
        while (page.getBufferPosition() < page.getContentSize())
          loaded.add(page.readString());
      }

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
