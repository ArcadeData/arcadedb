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

package performance;

import com.arcadedb.database.*;
import com.arcadedb.engine.Bucket;
import com.arcadedb.graph.Vertex;
import com.arcadedb.index.IndexCursor;
import com.arcadedb.log.LogManager;

import java.io.IOException;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.logging.Level;

/**
 * Imports the POKEC relationships (https://snap.stanford.edu/data/soc-pokec.html)
 */
public class PokecBenchmark {

  private static final String DB_PATH = "target/databases/pokec";

  private static final String[] IDS = new String[] { "P572469", "P933630", "P132198", "P204695", "P187002", "P1454566", "P1117610", "P1383745", "P1458636",
      "P649626", "P972244", "P1460241", "P588649", "P1108907", "P434722", "P1158434", "P1371145", "P49822", "P614248", "P402303", "P75768", "P493589",
      "P740602", "P380953", "P810049", "P1246934", "P287052", "P344172", "P660681", "P828832", "P887132", "P1046904", "P1479699", "P1407833", "P1350929",
      "P855276", "P1246404", "P1231124", "P526524", "P23981", "P404980", "P1243623", "P1163825", "P1492742", "P1170012", "P40084", "P186531", "P194596",
      "P1260111", "P1010258", "P1001598", "P1625974", "P408222", "P520726", "P613180", "P1069970", "P966486", "P1095935", "P1186346", "P1270053", "P1109025",
      "P1118547", "P629251", "P539998", "P472789", "P146669", "P312250", "P1248822", "P1381254", "P1433896", "P1047197", "P264079", "P488600", "P1281107",
      "P668473", "P1567037", "P362853", "P1219894", "P1290284", "P1323738", "P1485525", "P1276152", "P741845", "P371124", "P409700", "P476466", "P74694",
      "P610539", "P1094498", "P621542", "P1335844", "P541555", "P1013583", "P1066525", "P136524", "P861594", "P1510861", "P1539044", "P748859", "P311977",
      "P192229", "P1427713", "P1335574", "P1025903", "P861864", "P1432265", "P864683", "P209156", "P1020473", "P641818", "P179284", "P1084359", "P717426",
      "P380020", "P1222250", "P812633", "P335036", "P234683", "P1001506", "P1065631", "P1030535", "P462344", "P614291", "P1379169", "P1257994", "P1623346",
      "P988395", "P90033", "P725259", "P1114648", "P1370978", "P1307107", "P402603", "P1501451", "P638327", "P1280439", "P1299694", "P273744", "P573576",
      "P291232", "P193707", "P678706", "P1019939", "P611833", "P1355949", "P1499765", "P19040", "P273854", "P524857", "P923514", "P1120321", "P207931",
      "P1206082", "P351525", "P149676", "P601523", "P716414", "P911159", "P999682", "P973793", "P1592016", "P1270086", "P554310", "P61352", "P560377",
      "P1467570", "P1595211", "P539941", "P1000424", "P721633", "P1110353", "P1455175", "P1254723", "P1418108", "P337742", "P1013513", "P984678", "P1361850",
      "P1453064", "P1528927", "P960550", "P546028", "P1373222", "P1502337", "P1161558", "P750844", "P1469833", "P1510776", "P1277087", "P290714", "P1530358",
      "P641886", "P1234720", "P1045045", "P6331", "P22025", "P37707", "P71205", "P143291", "P820176", "P760384", "P186815", "P1356404", "P1628745", "P521042",
      "P1522406", "P37817", "P1163211", "P926783", "P1594472", "P150988", "P555223", "P486617", "P1180701", "P498574", "P1553262", "P1179660", "P1169894",
      "P1258174", "P1543787", "P415058", "P517241", "P1189390", "P198102", "P1540820", "P904039", "P29435", "P38933", "P803317", "P39316", "P1610999",
      "P1630453", "P923582", "P620463", "P1391340", "P256064", "P986103", "P685204", "P950810", "P1081101", "P885758", "P223257", "P424098", "P671529",
      "P427767", "P1160933", "P1037360", "P312602", "P1181263", "P1516064", "P488276", "P1552740", "P1475287", "P1448074", "P560834", "P1157072", "P676429",
      "P973746", "P1012117", "P608336", "P1402861", "P864859", "P1519253", "P953388", "P1250379", "P503379", "P397055", "P797785", "P187818", "P437795",
      "P162363", "P430758", "P409489", "P596300", "P139517", "P711444", "P502", "P311095", "P553975", "P701227", "P1587914", "P1020359", "P534123", "P225511",
      "P1432164", "P1610905", "P131766", "P1220491", "P965308", "P201752", "P1222379", "P1196866", "P825403", "P967414", "P1301614", "P101765", "P1604307",
      "P289126", "P1001489", "P49992", "P217725", "P1359986", "P356043", "P68411", "P333212", "P1507641", "P344222", "P454065", "P646252", "P626705", "P855126",
      "P421917", "P933993", "P821922", "P36353", "P175674", "P552960", "P1280550", "P447623", "P1467849", "P1475277", "P1157088", "P1501578", "P40754",
      "P805829", "P41860", "P1031968", "P427798", "P774172", "P679545", "P1508493", "P911276", "P630253", "P1274296", "P514895", "P354944", "P323826",
      "P509358", "P1390804", "P1593439", "P838555", "P1281077", "P435503", "P109168", "P984711", "P1350466", "P647770", "P84816", "P1610078", "P550648",
      "P837655", "P1069710", "P1161677", "P1235553", "P1253162", "P1308431", "P1460650", "P1485495", "P955104", "P641909", "P885556", "P521430", "P1383816",
      "P1566958", "P989250", "P167194", "P739010", "P926333", "P820053", "P583192", "P831795", "P1483825", "P1485965", "P1607559", "P765222", "P677524",
      "P1315007", "P1490732", "P993264", "P1092702", "P1335404", "P882232", "P179334", "P443634", "P380190", "P190620", "P717596", "P342974", "P816038",
      "P1072106", "P1558387", "P198089", "P418298", "P60920", "P234773", "P571184", "P342444", "P365122", "P1462378", "P1068731", "P843607", "P1176536",
      "P511032", "P941326", "P1028316", "P729010", "P852054", "P925851", "P1387306", "P1441606", "P1583099", "P1299724", "P307570", "P495153", "P1007740",
      "P226965", "P367488", "P1051939", "P596932", "P1477966", "P1541530", "P1627738", "P265430", "P577188", "P703342", "P476873", "P1407601", "P343963",
      "P1141405", "P1150923", "P923954", "P999772", "P973043", "P61002", "P70522", "P1458919", "P514844", "P676858", "P236134", "P354995", "P1521866",
      "P317562", "P1264315", "P950", "P757663", "P337492", "P504017", "P1440328", "P603761", "P448077", "P1502287", "P148450", "P232751", "P900841", "P1117596",
      "P1122856", "P1045395", "P206605", "P266352", "P552798", "P303139", "P1531931", "P571112", "P1467697", "P131463", "P1001039", "P355198", "P1389126",
      "P666462", "P1370393", "P974644", "P556517", "P493065", "P744404", "P1101218", "P1441690", "P1583005", "P1140947", "P13372", "P284480", "P1186609",
      "P1292125", "P1198560", "P326477", "P413042", "P198072", "P830610", "P1280102", "P1558378", "P607611", "P1278821", "P522793", "P1121044", "P1606569",
      "P788082", "P535044", "P81445", "P1118165", "P1240096", "P968225", "P1300774", "P886223", "P594264", "P601278", "P582041", "P1470876", "P779808",
      "P275413", "P427437", "P1568396", "P577276", "P1000957", "P975366", "P1394143", "P492907", "P748617", "P759955", "P1190411", "P1396007", "P1058726",
      "P1184354", "P499886", "P1038388", "P814158", "P1281995", "P485610", "P1188266", "P144631", "P394362", "P1195724", "P122168", "P652199", "P383741",
      "P1594081", "P174382", "P22376", "P874562", "P773462", "P734652", "P66366", "P188798", "P1134103", "P1583978", "P1438486", "P913849", "P232788",
      "P259525", "P1540493", "P669484", "P880951", "P1268776", "P396955", "P1630846", "P377014", "P496037", "P537176", "P1046102", "P1092689", "P1070801",
      "P1142251", "P1336573", "P980235", "P1008716", "P515607", "P758002", "P1559058", "P1173456", "P355141", "P31551", "P1196916", "P816191", "P515917",
      "P403108", "P511484", "P1382069", "P15583", "P127935", "P68741", "P1010936", "P1312220", "P525729", "P492997", "P1233655", "P1372461", "P788057",
      "P41489", "P307499", "P286772", "P1101119", "P1599806", "P601283", "P1431496", "P137391", "P226514", "P1568369", "P1000982", "P572667", "P679435",
      "P356663", "P1215762", "P364047", "P564962", "P142889", "P343828", "P975399", "P1366967", "P1523205", "P1536367", "P1463534", "P1274146", "P1566849",
      "P1017304", "P672344", "P232808", "P401747", "P410025", "P89482", "P1002464", "P1134193", "P1388882", "P972482", "P1607446", "P866663", "P1348420",
      "P1445282", "P1371307", "P652140", "P38739", "P165110", "P1117276", "P1301263", "P505438", "P1458250", "P920924", "P287214", "P1624147", "P715361",
      "P1241440", "P996817", "P816169", "P839022", "P1194060", "P11697", "P677494", "P127046", "P357022", "P1303780", "P1267036", "P1092672", "P1189412",
      "P961853", "P944242", "P411017", "P607261", "P1372499", "P1173483", "P41471", "P1546738", "P77697", "P1558457", "P366175", "P1599890", "P15556",
      "P342534", "P1381092", "P955483", "P665342", "P877271", "P686805", "P1068861", "P1118264", "P1439232", "P419266", "P231334", "P802006", "P1282997",
      "P323455", "P187252", "P881185", "P114041", "P424800", "P42795", "P1377478", "P304916", "P1177768", "P1443897", "P627158", "P745028", "P765871",
      "P257371", "P936183", "P226815", "P139638", "P375692", "P397882", "P98954", "P947960", "P1550340", "P604943", "P1302275", "P572698", "P703232", "P132329",
      "P1407551", "P733132", "P144758", "P210658", "P287282", "P292362", "P199778", "P1150563", "P1437544", "P1089110", "P317632", "P1283902", "P1334721",
      "P1613256", "P348602", "P1222757", "P191505", "P232401", "P1101309", "P629933", "P918845", "P634053", "P1148065", "P1541091", "P591615", "P237447",
      "P804995", "P1090337", "P855429", "P1379408", "P576296", "P405668", "P626883", "P629403", "P638121", "P834298", "P1130816", "P273506", "P702714",
      "P1211122", "P1518024", "P560722", "P877289", "P1496073", "P719740", "P21509", "P763675", "P615935", "P974914", "P1048138", "P709365", "P1254975",
      "P565457", "P344784", "P200443", "P728459", "P246739", "P313666", "P602879", "P1067160", "P1605034", "P50384", "P212127", "P1278771", "P1242761",
      "P1510323", "P751617", "P1026144", "P788132", "P673360", "P930699", "P947996", "P1525338", "P1494172", "P750183", "P455370", "P716072", "P806348",
      "P1296559", "P1566716", "P641743", "P1484285", "P865973", "P1413632", "P1000607", "P151135", "P142902", "P397857", "P674986", "P975036", "P623511",
      "P748767", "P1302936", "P1502674", "P1453482", "P98985", "P32464", "P1058696", "P1132352", "P1101892", "P759188", "P700285", "P292843", "P1533563",
      "P831337", "P144781", "P1195694", "P35724", "P1049823", "P1155394", "P1594131", "P1497549", "P666091", "P1473110", "P112303", "P882658", "P897118",
      "P1294477", "P605607", "P21595", "P498992", "P523684", "P744137", "P1174105", "P1248787", "P669574", "P1583538", "P400969", "P357169", "P1303409",
      "P1397920", "P1592246", "P1630716", "P1306626", "P480963", "P182138", "P1624822", "P592552", "P1430400", "P198329", "P229833", "P238128", "P693234",
      "P1037166", "P758352", "P1526562", "P41598", "P930661", "P470184", "P280923", "P786844", "P1016976", "P15433", "P708448", "P1357058", "P445458",
      "P1484275", "P280633", "P419163", "P1193305", "P919231", "P1078144", "P1345848", "P1080441", "P1326898", "P1386471", "P1029007", "P267856", "P805048",
      "P257218", "P108784", "P448758", "P420464", "P1202729", "P547345", "P433439", "P619080", "P1093578", "P1221279", "P1232355", "P604848", "P797466",
      "P1215432", "P1204936", "P575532", "P135043", "P188948", "P1548238", "P734909", "P1228446", "P1294487", "P1017054", "P139171", "P410195", "P1619440",
      "P514710", "P1551260", "P506813", "P634423", "P447875", "P1517430", "P1019077", "P991652", "P611171", "P710316", "P165060", "P1546979", "P213883",
      "P1301313", "P7273", "P1211594", "P422764", "P646014", "P1379381", "P922266", "P23897", "P269144", "P977156", "P477468", "P104345", "P357192", "P1436768",
      "P392789", "P214641", "P719449", "P686015", "P1013804", "P354685", "P799844", "P1029097", "P1465084", "P272528", "P412489", "P509196", "P86097",
      "P209782", "P844237", "P1015767", "P406918", "P1357081", "P774598", "P1587434", "P1281275", "P831927", "P1532172", "P871871", "P881655", "P460116",
      "P210749", "P1260994", "P1022857", "P1080498", "P1162752", "P1056229", "P1333606", "P936073", "P692880", "P583354", "P1291", "P745893", "P1165001",
      "P1350080", "P98604", "P178716", "P506883", "P765024", "P920597", "P1571959", "P333972", "P281177", "P400793", "P759601", "P1632413", "P26534", "P380831",
      "P830902", "P1624214", "P1070181", "P422792", "P1119196", "P1379379", "P958751", "P1437694", "P1487162", "P314124", "P1285718", "P33104", "P568611",
      "P53006", "P882577", "P47123", "P211350", "P1001238", "P1010338", "P573876", "P424293", "P360247" };

  public static void main(String[] args) throws Exception {
    new PokecBenchmark();
  }

  private PokecBenchmark() throws Exception {
    final Database db = new DatabaseFactory(DB_PATH).open();
    db.begin();

    try {
      //warmup(db);
//      aggregate(db);
      for (int i = 0; i < 10; ++i) {
        neighbors2(db);
      }
//      displayVertices(db);

    } finally {
      db.close();
    }
  }

  private void neighbors2(Database db) {
    LogManager.instance().log(this, Level.INFO, "Traversing the entire graph...");

    final long begin = System.currentTimeMillis();

    final AtomicInteger rootTraversed = new AtomicInteger();
    final AtomicLong totalTraversed = new AtomicLong();

    for (int i = 0; i < IDS.length; ++i) {
      final IndexCursor result = db.lookupByKey("V", new String[] { "id" }, new Object[] { Integer.parseInt(IDS[i].substring(1)) });

      final Vertex v = (Vertex) result.next().getRecord();

      rootTraversed.incrementAndGet();

      for (final Iterator<Vertex> neighbors = v.getVertices(Vertex.DIRECTION.OUT).iterator(); neighbors.hasNext(); ) {
        final Vertex neighbor = neighbors.next();

        totalTraversed.incrementAndGet();

        for (final Iterator<Vertex> neighbors2 = neighbor.getVertices(Vertex.DIRECTION.OUT).iterator(); neighbors2.hasNext(); ) {
          final Vertex neighbor2 = neighbors2.next();

          totalTraversed.incrementAndGet();
        }
      }

//      if (rootTraversed.get() % 1000 == 0) {
//        PLogManager.instance().log(this, Level.INFO, "- traversed %d roots - %d total", rootTraversed.get(), totalTraversed.get());
//      }
    }

    LogManager.instance()
        .log(this, Level.INFO, "- elapsed: " + (System.currentTimeMillis() - begin) + "traversed %d roots - %d total", null, rootTraversed.get(),
            totalTraversed.get());
  }

  private void aggregate(Database db) {
    LogManager.instance().log(this, Level.INFO, "Aggregating by age...");

    for (int i = 0; i < 3; ++i) {
      final long begin = System.currentTimeMillis();

      final Map<String, AtomicInteger> aggregate = new HashMap<>();
      db.scanType("V", true, new DocumentCallback() {
        @Override
        public boolean onRecord(final Document record) {
          String age = (String) record.get("age");

          AtomicInteger counter = aggregate.get(age);
          if (counter == null) {
            counter = new AtomicInteger(1);
            aggregate.put(age, counter);
          } else
            counter.incrementAndGet();

          return true;
        }
      });

      LogManager.instance().log(this, Level.INFO, "- elapsed: " + (System.currentTimeMillis() - begin));
    }
  }

  private void displayVertices(Database db) {
    LogManager.instance().log(this, Level.INFO, "Display vertices...");

    final long begin = System.currentTimeMillis();

    final Map<String, AtomicInteger> aggregate = new HashMap<>();
    db.scanType("V", true, new DocumentCallback() {
      @Override
      public boolean onRecord(final Document record) {
        final Vertex v = (Vertex) record;
        final long countOut = v.countEdges(Vertex.DIRECTION.OUT, "E");
        final long countIn = v.countEdges(Vertex.DIRECTION.IN, "E");

        LogManager.instance().log(this, Level.INFO, "- %s out=%d in=%d", null, v.getIdentity(), countOut, countIn);

        return true;
      }
    });

    LogManager.instance().log(this, Level.INFO, "- elapsed: " + (System.currentTimeMillis() - begin));
  }

  private void warmup(DatabaseInternal db) throws IOException {
    LogManager.instance().log(this, Level.INFO, "Warming up...");

    final long begin = System.currentTimeMillis();

    for (Bucket bucket : db.getSchema().getBuckets()) {
      LogManager.instance().log(this, Level.INFO, "Loading bucket %s in RAM...", null, bucket);
      db.getPageManager().preloadFile(bucket.getId());
      LogManager.instance().log(this, Level.INFO, "- done, elapsed so far " + (System.currentTimeMillis() - begin));
    }

    LogManager.instance().log(this, Level.INFO, "- elapsed: " + (System.currentTimeMillis() - begin));
  }
}
