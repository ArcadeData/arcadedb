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
package com.arcadedb.index;

import com.arcadedb.TestHelper;
import com.arcadedb.database.Database;
import com.arcadedb.database.MutableDocument;
import com.arcadedb.index.lsm.LSMTreeFullTextIndex;
import com.arcadedb.index.lsm.LSMTreeIndexAbstract;
import com.arcadedb.log.LogManager;
import com.arcadedb.schema.DocumentType;
import com.arcadedb.schema.Schema;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.logging.Level;

public class LSMTreeFullTextIndexTest extends TestHelper {
  private static final int    TOT       = 10000;
  private static final String TYPE_NAME = "V";
  private static final int    PAGE_SIZE = LSMTreeIndexAbstract.DEF_PAGE_SIZE;

  @Test
  public void testIndexing() {
    database.transaction(new Database.TransactionScope() {
      @Override
      public void execute() {
        Assertions.assertFalse(database.getSchema().existsType(TYPE_NAME));

        final DocumentType type = database.getSchema().createDocumentType(TYPE_NAME, 1);
        type.createProperty("text", String.class);
        Index typeIndex = database.getSchema().createTypeIndex(Schema.INDEX_TYPE.FULL_TEXT, false, TYPE_NAME, new String[] { "text" }, PAGE_SIZE);

        Assertions.assertTrue(database.getSchema().existsType(TYPE_NAME));

        final String text =
            "Jay Glenn Miner (May 31, 1932 – June 20, 1994) was an American integrated circuit designer, known primarily for developing multimedia chips for the Atari 2600 and Atari 8-bit family and as the \"father of the Amiga\". He received a BS in EECS from UC Berkeley in 1959.[2]\n"
                + "\n" + "Miner started in the electronics industry with a number of designs in the medical world, including a remote-control pacemaker.\n"
                + "\n"
                + "He moved to Atari, Inc. in the late 1970s. One of his first successes was to combine an entire breadboard of components into a single chip, known as the TIA. The TIA was the display hardware for the Atari 2600, which would go on to sell millions. After working on the TIA he headed up the design of the follow-on chip set known as ANTIC and CTIA for which he held a patent.[3] These chips would be used for the Atari 8-bit family of home computers and the Atari 5200 video game system.\n"
                + "\n"
                + "In the early 1980s, Jay, along with other Atari staffers, had become fed up with management and decamped. They set up another chipset project under a new company in Santa Clara, called Hi-Toro (later renamed to Amiga Corporation), where they could have creative freedom. There, they started to create a new Motorola 68000-based games console, codenamed Lorraine that could be upgraded to a computer. To raise money for the Lorraine project, Amiga Corp. designed and sold joysticks and game cartridges for popular game consoles such as the Atari 2600 and ColecoVision, as well as an odd input device called the Joyboard, essentially a joystick the player stood on. Atari continued to be interested in the team's efforts throughout this period, and funded them with $500,000 in capital in return for first use of their resulting chipset.\n"
                + "\n"
                + "Also in the early 1980s, Jay worked on a project with Intermedics, Inc. to create their first microprocessor-based cardiac pacemaker. The microprocessor was called Lazarus and the pacemaker was eventually called Cosmos. Jay was listed co-inventor on two patents. US patent 4390022, Richard V. Calfee & Jay Miner, \"Implantable device with microprocessor control\", issued 1983-06-28, assigned to Intermedics, Inc. US patent 4404972, Pat L. Gordon; Richard V. Calfee & Jay Miner, \"Implantable device with microprocessor control\", issued 1983-06-28, assigned to Intermedics, Inc.\n"
                + "\n"
                + "The Amiga crew, having continuing serious financial problems, had sought more monetary support from investors that entire Spring. Amiga entered into discussions with Commodore. The discussions ultimately led to Commodore wanting to purchase Amiga outright, which would (from Commodore's viewpoint) cancel any outstanding contracts - including Atari Inc.'s. So instead of Amiga delivering the chipset, Commodore delivered a check of $500,000 to Atari on Amiga's behalf, in effect returning the funds invested into Amiga for completion of the Lorraine chipset.\n"
                + "\n" + "\n" + "The original Amiga (1985)\n"
                + "Jay worked at Commodore-Amiga for several years, in Los Gatos, California. They made good progress at the beginning, but as Commodore management changed, they became marginalised and the original Amiga staff was fired or left out on a one-by-one basis, until the entire Los Gatos office was closed. Miner later worked as a consultant for Commodore until it went bankrupt. He was known as the 'Padre' (father) of the Amiga among Amiga users.\n"
                + "\n"
                + "Jay always took his dog \"Mitchy\" (a cockapoo) with him wherever he went. While he worked at Atari, Mitchy even had her own ID-badge, and an embossing of Mitchy's paw print is visible on the inside of the Amiga 1000 top cover, alongside the signatures of the engineers who worked on it.\n"
                + "\n"
                + "Jay endured kidney problems for most of his life, according to his wife, and relied on dialysis. His sister donated one of her own. Miner died due to complications from kidney failure at the age of 62, just two months after Commodore declared bankruptcy.";

        LogManager.instance().log(this, Level.FINE, "Indexing %d documents...", null, TOT);

        for (int i = 0; i < TOT; ++i) {
          final MutableDocument v = database.newDocument(TYPE_NAME);
          v.set("id", i);
          v.set("text", text);
          v.save();
        }

        LogManager.instance().log(this, Level.FINE, "Done, committing...");

        database.commit();

        LogManager.instance().log(this, Level.FINE, "Committed");

        final List<String> keywords = ((LSMTreeFullTextIndex) ((TypeIndex) typeIndex).getIndexesOnBuckets()[0]).analyzeText(
            ((LSMTreeFullTextIndex) ((TypeIndex) typeIndex).getIndexesOnBuckets()[0]).getAnalyzer(), new Object[] { text });
        Assertions.assertFalse(keywords.isEmpty());

        LogManager.instance().log(this, Level.FINE, "Checking keywords...");

        for (String k : keywords) {
          int totalPerKeyword = 0;

          for (Index idx : ((TypeIndex) typeIndex).getIndexesOnBuckets()) {
            if (idx instanceof TypeIndex)
              continue;

            final IndexCursor result = idx.get(new Object[] { k });

            int totalPerIndex = 0;
            while (result.hasNext()) {
              result.next();
              ++totalPerIndex;
            }

            Assertions.assertEquals(result.size(), totalPerIndex);

            totalPerKeyword += totalPerIndex;
          }
          Assertions.assertEquals(TOT, totalPerKeyword);
        }

        LogManager.instance().log(this, Level.FINE, "Check completed");

        database.begin();
      }
    });
  }
}
