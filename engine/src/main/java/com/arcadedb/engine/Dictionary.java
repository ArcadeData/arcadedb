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
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.logging.Level;

/**
 * Maps names to small integer ids, which is what records carry instead of the name itself.
 * <br>
 * Only identifiers are ever ADDED here: type names and property names, the callers that pass {@code create=true} to
 * {@link #getIdByName}. A string VALUE is only ever looked up with {@code create=false}, and is stored as a reference when it
 * happens to match an entry already present (see {@code BinarySerializer.serializeProperties}). User data therefore never
 * enters the dictionary, and the per-name size limit below only ever applies to identifiers.
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
 * A name is never split over two pages, so the tail of a page is left unused when the next name does not fit: on the append path,
 * at identifier lengths, that is well under 1% of the file. {@link #updateName} is looser - a rewrite that shrinks the content
 * leaves the pages it no longer reaches empty, and appends resume on the last of them, so the pages in between stay empty for
 * good. They cost one zero-entry read each on load and nothing else, and no production code calls that method. The only hard
 * limit left is a single name larger than one page, which caps one
 * identifier at {@code pageSize - BasePage.PAGE_HEADER_SIZE - DICTIONARY_HEADER_SIZE} bytes: ~65Kb on a dictionary created with
 * the current {@link #DEF_PAGE_SIZE}, against ~327Kb on one created before it was reduced. Existing databases keep their page
 * size, so only new ones see the lower cap, and no realistic identifier comes close to either.
 * <br>
 * <b>Upgrade order in a cluster:</b> a follower still running a build without multi-page support reads page 0 only, so once a
 * database has rolled over, replicated pages beyond the first leave that follower reporting "Dictionary item with id N is not
 * valid". Followers have to be upgraded before, or together with, the leader. See {@code docs/5560-dictionary-multipage.md}.
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
   * <br>
   * It is stamped at creation and never changed, so a v0 database that later rolls over keeps saying v0. That is deliberate:
   * this number lives in the FILE NAME, so bumping it at runtime means renaming a live component file, which
   * {@link PaginatedComponent#rename} can only do behind a full flush barrier and which would race replication shipping pages
   * by file id. It would also buy nothing, because a build old enough to be at risk does not validate this field at all - it
   * would open a v1 file just as happily. The signal that a database has rolled over is the INFO logged when page 1 is created.
   */
  private static final int    CURRENT_VERSION        = 1;
  // THIS IS LEGACY BECAUSE THE NUMBER OF ITEMS WAS STORED IN THE HEADER. NOW THE DICTIONARY IS POPULATED FROM THE ACTUAL CONTENT IN THE PAGES
  private static final int    DICTIONARY_HEADER_SIZE = Binary.INT_SERIALIZED_SIZE;

  /**
   * The names and the name-&gt;id map are replaced together by {@link #reload()}, which runs on threads that do not hold this
   * component's monitor (transaction rollback and replication apply). Publishing them as one immutable pair through a single
   * volatile field is what makes a reader either see both halves of the old snapshot or both halves of the new one, and is
   * what gives the reader a happens-before edge with the rebuild at all.
   * <br>
   * {@code names} is an array with spare capacity rather than a {@code CopyOnWriteArrayList}, and {@code size} is how much of it
   * is live. A COW list copies its whole backing array on every append, so growing a dictionary of N names cost N^2/2 element
   * copies: ~150ms and 4.6Gb of copying at the 48K names that used to be the hard ceiling, and quadratically worse without one.
   * Removing that ceiling is exactly what this class now does, so the append had to stop being O(n).
   * <br>
   * An append writes {@code names[size]} and then publishes a new {@code Entries} with {@code size + 1}. Mutating the shared
   * array before the volatile write is safe both ways round: a reader holding the older snapshot has the smaller size and never
   * looks at that slot, and a reader that sees the new snapshot sees the slot through the volatile write's happens-before edge.
   * Growth doubles the array, so appends stay amortised O(1) while reads stay a volatile read plus an array index.
   */
  private record Entries(String[] names, int size, ConcurrentMap<String, Integer> ids) {
  }

  private volatile Entries entries = new Entries(new String[64], 0, new ConcurrentHashMap<>(1024));

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

    if (pageCount.get() > 1)
      // RESTATED ON EVERY OPEN, NOT ONLY WHEN IT HAPPENS. THE ROLLOVER ITSELF IS LOGGED ONCE, WHICH IS NO USE TO SOMEONE LOOKING
      // AT A DATABASE AFTERWARDS OR PLANNING A ROLLBACK; THIS IS THE SAME FACT AS CURRENT STATE RATHER THAN AS AN EVENT. IT IS
      // DERIVED FROM THE PAGE COUNT, SO IT CANNOT DRIFT THE WAY A SEPARATE PERSISTED MARKER COULD
      LogManager.instance().log(this, Level.INFO,
          "Database '%s' schema dictionary spans %d pages. It cannot be read by a build without multi-page dictionary support, "
              + "so replicas and any rollback target have to be on this version or newer", null, database.getName(),
          pageCount.get());
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
            newPos.set(entries.size());
            addItemToPage(name);
          }, false);

          // THE COMMIT ABOVE CAN HAVE SWAPPED THE SNAPSHOT (RELOAD ON ROLLBACK/RETRY): RE-READ IT
          current = entries;

          // THE NAMES ARE PUBLISHED BEFORE THE MAP, AND THE ORDER MATTERS. THE TWO CANNOT BE UPDATED ATOMICALLY TOGETHER, SO ONE
          // OF THEM LEADS BY AN INSTANT AND A CONCURRENT READER CAN LAND IN BETWEEN. LEADING WITH THE MAP MEANS THAT READER
          // RESOLVES THE NAME TO AN ID THAT getNameById() DOES NOT YET ACCEPT, WHICH THROWS. LEADING WITH THE NAMES MEANS IT
          // FINDS THE NAME NOT PRESENT YET AND -1 COMES BACK, WHICH EVERY create=false CALLER ALREADY HANDLES (BinarySerializer
          // JUST SKIPS THE COMPRESSION) AND WHICH A create=true CALLER RESOLVES BY BLOCKING ON THIS MONITOR. NOBODY CAN HOLD THE
          // NEW ID BEFORE THE MAP PUBLISHES IT, BECAUSE IT ONLY REACHES A RECORD AFTER THIS RETURNS. A MISS COSTS NOTHING, A
          // THROW COSTS A REQUEST, SO THE MISS IS THE ONE TO PREFER
          if (!current.ids().containsKey(name)) {
            final int appended = appendName(current, name);
            if (appended != newPos.get() + 1) {
              try {
                reload();
              } catch (final IOException e) {
                // IGNORE IT
              }
              throw new SchemaException("Error on updating dictionary for key '" + name + "'");
            }
            current.ids().putIfAbsent(name, newPos.get());
          }
          pos = current.ids().get(name);
        }
      }
    }

    if (pos == null)
      return -1;

    return pos;
  }

  /**
   * Appends one name to the in-RAM view and publishes it, returning the new total. Amortised O(1): see {@link Entries} for why
   * the slot is written before the volatile publish and why that is safe for readers holding either snapshot.
   * <p>
   * Callers hold this component's monitor.
   */
  private int appendName(final Entries current, final String name) {
    String[] names = current.names();
    final int size = current.size();

    if (size == names.length)
      names = Arrays.copyOf(names, names.length * 2);

    names[size] = name;
    entries = new Entries(names, size + 1, current.ids());
    return size + 1;
  }

  public String getNameById(final int nameId) {
    final Entries current = entries;
    if (nameId < 0 || nameId >= current.size())
      throw new IllegalArgumentException("Dictionary item with id " + nameId + " is not valid (total=" + current.size() + ")");

    final String itemName = current.names()[nameId];
    if (itemName == null)
      throw new IllegalArgumentException("Dictionary item with id " + nameId + " was not found");

    return itemName;
  }

  public Map<String, Integer> getDictionaryMap() {
    return CollectionUtils.immutableMap(entries.ids());
  }

  /**
   * Updates a name. The update will impact the entire database with both properties and values (if used as ENUM). The update is valid only if the name has not been used as type name.
   * <br>
   * <b>Cost:</b> the whole dictionary is re-laid out from page 0, so this dirties every dictionary page and puts them all in the
   * caller's transaction. The WAL commit therefore grows with the size of the dictionary, not with the one name that changed. It
   * used to touch page 0 alone, when page 0 was the whole dictionary.
   * <br>
   * <b>Concurrency:</b> synchronized on the same monitor as the {@link #getIdByName} create path, because both mutate the shared
   * {@code entries} snapshot and the same pages. That serialises the in-RAM edit, which is the part that has no other protection:
   * a concurrent append would otherwise read a half-renamed list. It does NOT make the two atomic against each other on disk -
   * this method writes inside the CALLER's transaction and does not commit, so an append that commits in between is resolved the
   * usual way, by the page version check raising {@link com.arcadedb.exception.ConcurrentModificationException} on whichever
   * commits second. There is no production caller today; anything wiring a live rename should hold a schema-level lock too.
   *
   * @param oldName The old name to rename. Must be already present in the schema dictionary
   * @param newName The new name. Can be already present in the schema dictionary
   */
  public synchronized void updateName(final String oldName, final String newName) {
    if (!database.isTransactionActive())
      throw new SchemaException("Error on adding new item to the database schema dictionary because no transaction was active");

    if (oldName == null)
      throw new IllegalArgumentException("Dictionary old item name was null");

    if (newName == null)
      throw new IllegalArgumentException("Dictionary new item name was null");

    if (oldName.equals(newName))
      // NOTHING TO DO. WITHOUT THIS THE LOOP BELOW WOULD NEVER FIND ITS TERMINATION CONDITION
      return;

    // EVERY VALIDATION RUNS BEFORE ANY MUTATION, AND NONE OF THEM NEEDS THE MUTATION TO HAVE HAPPENED. THIS ORDER IS THE WHOLE
    // POINT: THESE THROW IllegalArgumentException / DatabaseMetadataException, WHICH THE catch BELOW DOES NOT HANDLE (IT ONLY
    // CATCHES IOException), AND NO DICTIONARY PAGE HAS BEEN TOUCHED YET, SO TransactionContext.rollback() DOES NOT ARM ITS
    // REPAIRING reload() EITHER. MUTATING FIRST WOULD LEAVE THE IN-RAM VIEW RENAMED IN THE LIST AND MISSING FROM THE MAP UNTIL
    // SOME UNRELATED RELOAD CAME ALONG.

    // newName IS THE ONLY NAME THAT CAN BE TOO BIG: EVERY OTHER ONE IS ALREADY STORED ON A PAGE OF THIS SIZE
    checkNameFitsAPage(newName, spaceRequiredBy(newName.getBytes(DatabaseFactory.getDefaultCharset())));

    // CHEAPEST FIRST, AND IT DEPENDS ONLY ON oldName, SO IT DOES NOT EVEN NEED THE SCAN BELOW
    for (final DocumentType t : database.getSchema().getTypes())
      if (oldName.equals(t.getName()))
        throw new IllegalArgumentException(
            "Cannot rename the item '" + oldName + "' in the dictionary because it has been used as a type name");

    final Entries current = entries;
    final int total = current.size();
    final ConcurrentMap<String, Integer> dictionaryMap = current.ids();

    // READ-ONLY SCAN. ONE PASS: THE OLD indexOf()-UNTIL-GONE LOOP RE-SCANNED THE WHOLE LIST PER OCCURRENCE, AND ONLY TERMINATED
    // BECAUSE THE NAMES DIFFER, SINCE WITH oldName EQUAL TO newName EVERY SCAN FOUND WHAT THE PREVIOUS set() HAD JUST WRITTEN.
    // THE EARLY RETURN ABOVE STILL COVERS THAT CASE, BUT SCANNING BY INDEX MAKES IT IMPOSSIBLE BY CONSTRUCTION
    final List<Integer> oldIndexes = new ArrayList<>();
    for (int i = 0; i < total; ++i)
      if (oldName.equals(current.names()[i]))
        oldIndexes.add(i);

    if (oldIndexes.isEmpty())
      throw new IllegalArgumentException("Item '" + oldName + "' not found in the dictionary");

    try {
      // VALIDATION IS OVER: FROM HERE ON THE ONLY WAY OUT IS AN IOException, WHICH THE catch REPAIRS WITH A reload()
      //
      // THE RENAME GOES ONTO A COPY RATHER THAN INTO THE LIVE ARRAY. AN APPEND ONLY EVER WRITES A SLOT PAST THE PUBLISHED SIZE,
      // SO IT CAN SHARE ITS ARRAY WITH OLDER READERS; A RENAME REWRITES A SLOT THEY ARE ALREADY READING, WHICH WOULD NEED A
      // HAPPENS-BEFORE EDGE THEY DO NOT HAVE. ONE COPY PER RENAME IS NOTHING NEXT TO THE FULL PAGE REWRITE BELOW.
      final String[] renamed = Arrays.copyOf(current.names(), current.names().length);
      for (final int oldIndex : oldIndexes)
        renamed[oldIndex] = newName;

      dictionaryMap.remove(oldName);
      entries = new Entries(renamed, total, dictionaryMap);

      // REWRITE THE WHOLE DICTIONARY FROM PAGE 0 IN THE SAME ORDER, SO NO ID MOVES. THE TOTAL SIZE CAN SHRINK OR GROW, SO PAGES
      // ARE ADDED AS NEEDED AND THE ONES THE NEW CONTENT NO LONGER REACHES ARE EMPTIED: reload() WALKS EVERY COMMITTED PAGE, AND
      // A STALE TAIL PAGE LEFT BEHIND WOULD RE-ADD ITS OLD NAMES ON THE NEXT LOAD.
      int pageNumber = 0;
      MutablePage page = resetPageForRewrite(pageNumber);

      // BOUNDED BY total, NOT BY renamed.length: THE ARRAY CARRIES SPARE CAPACITY FOR FUTURE APPENDS AND THOSE SLOTS ARE NULL
      for (int i = 0; i < total; ++i) {
        final String d = renamed[i];
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

    if (pageNumber == 1)
      // THE ONE MOMENT IN A DATABASE'S LIFE WHEN IT STOPS BEING READABLE BY A BUILD WITHOUT MULTI-PAGE SUPPORT. THE BREADCRUMB
      // IS WHAT LETS AN OPERATOR CORRELATE A FOLLOWER LATER REPORTING "Dictionary item with id N is not valid" WITH THIS EVENT.
      //
      // ON COMMIT, NOT HERE: THIS RUNS INSIDE A TRANSACTION THAT CAN STILL ROLL BACK, AND updateName GROWS INSIDE THE CALLER'S
      // TRANSACTION, SO LOGGING EAGERLY WOULD ANNOUNCE A ROLLOVER THAT NEVER HAPPENED. A BREADCRUMB THAT LIES IS WORSE THAN NO
      // BREADCRUMB. THE KEY MAKES IT ONE LINE PER TRANSACTION EVEN IF SEVERAL PAGES ARE ADDED, AND ROLLBACK DISCARDS IT.
      //
      // INFO, NOT WARNING: NOTHING IS WRONG, AND WARNINGS ON HEALTHY GROWTH ARE HOW LOGS STOP BEING READ
      database.getTransaction().addAfterCommitCallbackIfAbsent("dictionaryRolledOver",
          () -> LogManager.instance().log(this, Level.INFO,
              "Database '%s' schema dictionary grew beyond its first page. Any replica or tool running a build without "
                  + "multi-page dictionary support can no longer read this database: upgrade followers before, or together "
                  + "with, the leader", null, database.getName()));

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
      // LOAD THE DICTIONARY IN RAM. COLLECTED IN AN ARRAYLIST BECAUSE THE ENTRY COUNT IS NOT KNOWN UNTIL THE LAST PAGE HAS BEEN
      // READ, THEN COPIED INTO THE PUBLISHED ARRAY ONCE. THIS USED TO APPEND STRAIGHT INTO A CopyOnWriteArrayList, WHICH COPIED
      // THE WHOLE BACKING ARRAY PER ENTRY AND MADE THE LOAD QUADRATIC: ~150ms FOR 48K ENTRIES, PAID ON EVERY OPEN, EVERY
      // ROLLBACK OF A DICTIONARY TRANSACTION AND EVERY REPLICATED DICTIONARY CHANGE.
      final List<String> loaded = new ArrayList<>();

      // COMMITTED STATE ON BOTH COUNTS, WHICH IS THE WHOLE CONTRACT OF THIS METHOD: EVERY CALLER (LOAD, ROLLBACK, AND THE TWO
      // POST-COMMIT REPLICATION PATHS) WANTS WHAT IS DURABLE, NEVER WHAT A TRANSACTION IS CURRENTLY HOLDING.
      //
      // getTotalPages() WOULD COUNT PAGES A ROLLING-BACK TRANSACTION IS THROWING AWAY, AND TransactionContext.getPage() WOULD
      // RETURN THAT TRANSACTION'S OWN DIRTY COPY (IT CHECKS modifiedPages FIRST), SO A ROLLBACK WOULD REBUILD THE DICTIONARY
      // FROM EXACTLY THE CONTENT BEING DISCARDED. READ THROUGH THE PAGE MANAGER INSTEAD, AND COUNT WITH pageCount, WHICH ONLY
      // ADVANCES ON COMMIT. THE FLOOR OF 1 KEEPS THE PRE-MULTI-PAGE BEHAVIOUR OF ALWAYS READING PAGE 0 OF A NON-EMPTY FILE.
      final int totalPages = Math.max(1, pageCount.get());

      for (int pageNumber = 0; pageNumber < totalPages; ++pageNumber) {
        // createIfNotExists ONLY FOR PAGE 0, WHERE IT IS LOAD BEARING: PAIRED WITH THE FLOOR ABOVE IT MATERIALISES PAGE 0 OF A
        // FILE SHORTER THAN ONE PAGE (KILLED MID-WRITE), WHICH IS WHAT THE SINGLE-PAGE READER DID.
        //
        // FOR EVERY LATER PAGE IT IS OFF, SO A PAGE THAT pageCount CLAIMS BUT THAT IS NOT THERE FAILS INSTEAD OF BEING INVENTED.
        // AN INVENTED PAGE IS NOT MERELY MASKED CORRUPTION: IT CONTRIBUTES ZERO NAMES, WHICH SHIFTS EVERY NAME AFTER IT DOWN BY
        // AS MANY IDS AS THE MISSING PAGE HELD, AND IDS ARE EMBEDDED IN RECORDS. SILENTLY RENUMBERING IS THE ONE OUTCOME THIS
        // CLASS EXISTS TO PREVENT, SO A GAP HAS TO BE LOUD.
        final BasePage page;
        try {
          page = database.getPageManager()
              .getImmutablePage(new PageId(database, file.getFileId(), pageNumber), pageSize, false, pageNumber == 0);
        } catch (final IllegalArgumentException e) {
          throw new DatabaseMetadataException(
              "Schema dictionary of database '" + database.getName() + "' is truncated: page " + pageNumber + " of " + totalPages
                  + " is missing. Reading on would silently renumber every name stored after it", e);
        }

        page.setBufferPosition(DICTIONARY_HEADER_SIZE);
        while (page.getBufferPosition() < page.getContentSize())
          loaded.add(page.readString());
      }

      final int size = loaded.size();

      final ConcurrentMap<String, Integer> newDictionaryMap = new ConcurrentHashMap<>(size + (size / 3) + 1);
      for (int i = 0; i < size; ++i)
        newDictionaryMap.putIfAbsent(loaded.get(i), i);

      // SPARE CAPACITY SO THE FIRST APPEND AFTER A RELOAD DOES NOT IMMEDIATELY HAVE TO GROW THE ARRAY. reload() RUNS ON THE
      // ROLLBACK AND REPLICATION PATHS, SO APPENDS RIGHT AFTER ONE ARE THE NORMAL CASE RATHER THAN THE EXCEPTION
      final String[] names = new String[Math.max(64, size + (size >> 2))];
      for (int i = 0; i < size; ++i)
        names[i] = loaded.get(i);

      this.entries = new Entries(names, size, newDictionaryMap);
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
