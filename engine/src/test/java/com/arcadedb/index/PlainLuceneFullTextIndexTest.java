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

import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.standard.StandardAnalyzer;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.StringField;
import org.apache.lucene.document.TextField;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.queryparser.classic.ParseException;
import org.apache.lucene.queryparser.classic.QueryParser;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.ScoreDoc;
import org.apache.lucene.search.Sort;
import org.apache.lucene.store.FSDirectory;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.stream.IntStream;

public class PlainLuceneFullTextIndexTest {

  @Test
  void rename_me() throws IOException, ParseException {

    final Path path = Paths.get("./target/databases/testIndex");

    try {
      final String text =
          "Jay Glenn Miner (May 31, 1932 – June 20, 1994) was an American integrated circuit designer, known primarily for developing multimedia chips for the Atari 2600 and Atari 8-bit family and as the \"father of the Amiga\". He received a BS in EECS from UC Berkeley in 1959.[2]\n"
              + "\n" + "Miner started in the electronics industry with a number of designs in the medical world, including a remote-control pacemaker.\n" + "\n"
              + "He moved to Atari, Inc. in the late 1970s. One of his first successes was to combine an entire breadboard of components into a single chip, known as the TIA. The TIA was the display hardware for the Atari 2600, which would go on to sell millions. After working on the TIA he headed up the design of the follow-on chip set known as ANTIC and CTIA for which he held a patent.[3] These chips would be used for the Atari 8-bit family of home computers and the Atari 5200 video game system.\n"
              + "\n"
              + "In the early 1980s, Jay, along with other Atari staffers, had become fed up with management and decamped. They set up another chipset project under a new company in Santa Clara, called Hi-Toro (later renamed to Amiga Corporation), where they could have creative freedom. There, they started to create a new Motorola 68000-based games console, codenamed Lorraine that could be upgraded to a computer. To raise money for the Lorraine project, Amiga Corp. designed and sold joysticks and game cartridges for popular game consoles such as the Atari 2600 and ColecoVision, as well as an odd input device called the Joyboard, essentially a joystick the player stood on. Atari continued to be interested in the team's efforts throughout this period, and funded them with $500,000 in capital in return for first use of their resulting chipset.\n"
              + "\n"
              + "Also in the early 1980s, Jay worked on a project with Intermedics, Inc. to create their first microprocessor-based cardiac pacemaker. The microprocessor was called Lazarus and the pacemaker was eventually called Cosmos. Jay was listed co-inventor on two patents. US patent 4390022, Richard V. Calfee & Jay Miner, \"Implantable device with microprocessor control\", issued 1983-06-28, assigned to Intermedics, Inc. US patent 4404972, Pat L. Gordon; Richard V. Calfee & Jay Miner, \"Implantable device with microprocessor control\", issued 1983-06-28, assigned to Intermedics, Inc.\n";

      final String text2 =
          "The Amiga crew, having continuing serious financial problems, had sought more monetary support from investors that entire Spring. Amiga entered into discussions with Commodore. The discussions ultimately led to Commodore wanting to purchase Amiga outright, which would (from Commodore's viewpoint) cancel any outstanding contracts - including Atari Inc.'s. So instead of Amiga delivering the chipset, Commodore delivered a check of $500,000 to Atari on Amiga's behalf, in effect returning the funds invested into Amiga for completion of the Lorraine chipset.\n"
              + "\n" + "\n" + "The original Amiga (1985)\n"
              + "Jay worked at Commodore-Amiga for several years, in Los Gatos, California. They made good progress at the beginning, but as Commodore management changed, they became marginalised and the original Amiga staff was fired or left out on a one-by-one basis, until the entire Los Gatos office was closed. Miner later worked as a consultant for Commodore until it went bankrupt. He was known as the 'Padre' (father) of the Amiga among Amiga users.\n"
              + "\n"
              + "Jay always took his dog \"Mitchy\" (a cockapoo) with him wherever he went. While he worked at Atari, Mitchy even had her own ID-badge, and an embossing of Mitchy's paw print is visible on the inside of the Amiga 1000 top cover, alongside the signatures of the engineers who worked on it.\n"
              + "\n"
              + "Jay endured kidney problems for most of his life, according to his wife, and relied on dialysis. His sister donated one of her own. Miner died due to complications from kidney failure at the age of 62, just two months after Commodore declared bankruptcy.";

      //System.out.println("path = " + path.toAbsolutePath());
      final FSDirectory directory = FSDirectory.open(path);

      Analyzer analyzer = new StandardAnalyzer();

      //re-create index
      IndexWriterConfig config = new IndexWriterConfig(analyzer);
      config.setOpenMode(IndexWriterConfig.OpenMode.CREATE);

      //write documents
      IndexWriter writer = new IndexWriter(directory, config);

      IntStream.rangeClosed(0, 1000).forEach(i -> {
        try {
          Document doc = new Document();
          doc.add(new StringField("id", "" + i, Field.Store.YES));

          if (i % 2 == 0)
            doc.add(new TextField("text", text, Field.Store.NO));
          else
            doc.add(new TextField("text", text2, Field.Store.NO));

          writer.addDocument(doc);
        } catch (IOException e) {
          e.printStackTrace();
        }
      });

      writer.close();

      //search
      final DirectoryReader reader = DirectoryReader.open(directory);

      IndexSearcher searcher = new IndexSearcher(reader);

      QueryParser parser = new QueryParser("text", analyzer);
      Query query = parser.parse("elec*");
      ScoreDoc[] hits = searcher.search(query, 1000, Sort.RELEVANCE).scoreDocs;

      Assertions.assertEquals(501, hits.length);
      // Iterate through the results:
      for (int i = 0; i < hits.length; i++) {
        Document hitDoc = searcher.doc(hits[i].doc);
        //System.out.print(hitDoc.get("id") + " - ");
      }

      reader.close();
      directory.close();
    } finally {
      path.toFile().delete();
    }
  }
}
