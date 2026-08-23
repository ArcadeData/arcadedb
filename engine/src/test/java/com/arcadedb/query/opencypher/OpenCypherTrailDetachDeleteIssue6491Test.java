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
package com.arcadedb.query.opencypher;

import com.arcadedb.database.Database;
import com.arcadedb.database.DatabaseFactory;
import com.arcadedb.query.sql.executor.ResultSet;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;

/**
 * Regression test for issue #6491: a {@code DETACH DELETE} inside a {@code TRAIL}/self-loop fan-out
 * threw {@code RecordNotFoundException} because a later output row of the same MATCH kept
 * dereferencing a node already removed by an earlier row's DELETE within the same statement.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
public class OpenCypherTrailDetachDeleteIssue6491Test {
  private Database database;

  @AfterEach
  void tearDown() {
    if (database != null) {
      if (database.isOpen())
        database.drop();
      database = null;
    }
  }

  @Test
  void detachDeleteAfterTrailSelfLoopFanOutDoesNotThrow() {
    database = new DatabaseFactory("./target/databases/testopencypher-issue6491").create();

    database.transaction(() -> {
      database.command("opencypher",
          "CREATE (n0 :l2{k1 : true, k2 : '7Y', k3 : false, k4 : 978957757, k5 : 'un', k11 : 1063739587, id : 0, k6 : 'lT', k10 : 'J', k7 : -1774603358, k9 : -541988996, klist : ['QnMSUwb', 'L', 'C', 'y2ODSSGN']})");
      database.command("opencypher",
          "CREATE (n0 :l11:l1:l6:l8:l4:l2:l3:l9:l0:l5{k0 : false, k1 : false, k2 : 'b', k4 : -588521116, k5 : 'ZjmG', id : 1, k10 : 'Au24Ocj', k7 : -1364487066, k8 : false})");
      database.command("opencypher",
          "CREATE (n0 :l11:l1:l6:l8:l4:l2:l3:l9:l0:l5{k0 : false, k1 : false, k2 : 'b', k4 : -588521116, k5 : 'ZjmG', id : 2, k10 : 'Au24Ocj', k7 : -1364487066, k8 : false})");
      database.command("opencypher",
          "CREATE (n0 :l8:l9:l2{k0 : false, k1 : true, k2 : 'X0cdZ5', k5 : 'L', k11 : -43316742, id : 13, k6 : '0w4ohOc4', k10 : 'h', k7 : -969787684, k8 : true, k9 : -256415028, klist : []})");
      database.command("opencypher",
          "CREATE (n0 :l3:l2:l7:l10:l6:l9:l1:l5:l4{k3 : false, k5 : 'cmK6ZYnaQ', k11 : 532250155, id : 32, k6 : 'V0', k10 : '1', k7 : -1303523236, k8 : true, k9 : 1178986})");
      database.command("opencypher",
          "CREATE (n0 :l1:l7:l5:l0:l2:l8{k1 : true, k2 : 'SkA', k3 : true, k5 : 'l', k11 : 356654659, id : 47, k6 : 'U', k10 : '7', k7 : 1035784699})");
      database.command("opencypher",
          "CREATE (n0 :l8:l6:l3:l10:l1:l4:l7:l11:l0:l5:l9:l2{k1 : true, k2 : 'sJ1GAA', k5 : 'B', id : 50, k6 : 'idYrroYId', k10 : 'b', k7 : 365435696, k8 : true, k9 : 1538248021, klist : [-1273064944, 713469903, -1390606580]})");
      database.command("opencypher",
          "CREATE (n0 :l9:l8:l11:l2:l3:l6:l5:l0:l7:l4:l1:l10{k2 : 'i', k3 : false, k5 : 'd6w', k11 : 493487259, id : 61, k10 : 'Szx3', k7 : -1652611899, k8 : true, k9 : 919062054})");
      database.command("opencypher",
          "CREATE (n0 :l7:l11:l2:l6:l10:l8:l0:l9:l5:l1{k0 : true, k1 : false, k3 : false, k4 : -332818474, k5 : 'D', id : 63, k6 : 'laiZUfa', k10 : '8d', k7 : 632373400, k8 : true, k9 : 1952376764, klist : ['m']})");
      database.command("opencypher",
          "CREATE (n0 :l4:l10:l1:l2:l6{k0 : true, k1 : false, k2 : 'MNbY', k3 : true, k5 : 'vozL0OEbN', k11 : 1637873211, id : 95, k10 : 'U2MN', k7 : 345683480, k8 : true, k9 : 761767703, klist : ['B']})");
      database.command("opencypher",
          "CREATE (n0 :l2:l0:l9:l5:l11:l8{k0 : true, k2 : 'uuiO', k3 : true, k4 : -1709840903, k5 : 'O', k11 : -1438114993, id : 109, k10 : '8de3', k8 : true, klist : [539008409]})");
      database.command("opencypher",
          "CREATE (n0 :l7:l0:l8:l4:l3{k0 : false, k1 : true, k3 : true, k4 : 158759993, k11 : 1785296671, id : 112, k6 : 'HvYp', k10 : '9', k7 : -1511332071, k8 : true, k9 : 1368099125})");

      database.command("opencypher",
          "MATCH (n0 {id : 47}), (n1 {id : 47}) MERGE(n0)-[r :rt4{k0 : false, k1 : true, k2 : 'yDP', k3 : true, k4 : 1936048494, k5 : 'o', k11 : 749576131, k6 : 't', k10 : 'O', k7 : -1103132776, k8 : false, k9 : 10937250, id : 138, klist : [37587925]}]->(n1)");
      database.command("opencypher",
          "MATCH (n0 {id : 47}), (n1 {id : 1}) MERGE(n0)-[r :rt10{k0 : false, k1 : true, k2 : 'p', k3 : true, k4 : -2075496847, k5 : '6', k11 : -300042034, k6 : 'b', k10 : 'ZpH0pBy', k7 : 1875550045, k8 : false, k9 : -1625857092, id : 19, klist : []}]->(n1)");
      database.command("opencypher",
          "MATCH (n0 {id : 47}), (n1 {id : 1}) MERGE(n0)-[r :rt10{k0 : true, k1 : true, k2 : 'Dp8tnldwu', k3 : true, k4 : 27842061, k5 : 'F', k11 : -1697848320, k6 : 'qd', k10 : '5ZKeNPh3L', k7 : -1436456444, k8 : false, k9 : -191933183, id : 46, klist : ['nyGziiK']}]->(n1)");
      database.command("opencypher",
          "MATCH (n0 {id : 47}), (n1 {id : 1}) MERGE(n0)-[r :rt5{k0 : true, k1 : false, k2 : '4', k3 : true, k4 : 1541568696, k5 : 'MdKBkKP', k11 : 547910532, k6 : 'b3', k10 : 'Q', k7 : -2031011752, k8 : false, k9 : 21533447, id : 55, klist : [590450264, -1929005694, -2055187623, -2062795327]}]->(n1)");
      database.command("opencypher",
          "MATCH (n0 {id : 47}), (n1 {id : 1}) MERGE(n0)-[r :rt5{k0 : true, k1 : true, k2 : 'GyD', k3 : true, k4 : -255406390, k5 : '2', k11 : 1645842322, k6 : 'VSA9ee', k10 : 'x', k7 : -2111616258, k8 : false, k9 : 188621682, id : 56, klist : []}]->(n1)");
      database.command("opencypher",
          "MATCH (n0 {id : 47}), (n1 {id : 1}) MERGE(n0)-[r :rt6{k0 : false, k1 : true, k3 : true, k4 : -1889675071, k5 : '8', k11 : 1141094652, k6 : 'FFHQFwaW2', k10 : 'X7', k7 : -1843587940, k8 : false, k9 : 1863700587, id : 69, klist : ['oBsuDcSq']}]->(n1)");
      database.command("opencypher",
          "MATCH (n0 {id : 47}), (n1 {id : 1}) MERGE(n0)-[r :rt4{k0 : false, k1 : true, k2 : 'y3Pt2FUh', k3 : true, k4 : -106802386, k5 : 't2', k11 : -330024054, k6 : 'RuHnyODkT', k10 : 'Too', k7 : -520742564, k8 : false, k9 : -1614064212, id : 74, klist : ['kzlDrbO']}]->(n1)");
      database.command("opencypher",
          "MATCH (n0 {id : 47}), (n1 {id : 1}) MERGE(n0)-[r :rt2{k0 : false, k1 : false, k2 : 'QutJN', k3 : false, k4 : -1837965990, k5 : 'D', k11 : -2077008664, k6 : 'Q4MJLkrXA', k10 : 'sVrN', k7 : -1546692995, k8 : false, k9 : -1524530346, id : 83, klist : []}]->(n1)");
      database.command("opencypher",
          "MATCH (n0 {id : 47}), (n1 {id : 1}) MERGE(n0)-[r :rt9{k0 : false, k1 : true, k2 : '4', k3 : true, k4 : 1547513299, k5 : 'mC2ayLtuK', k11 : 838169207, k6 : 'f', k10 : 'a', k7 : -215542939, k8 : false, k9 : 495708701, id : 153, klist : ['0', 'pC', 'rLD5XaV', 'k']}]->(n1)");
      database.command("opencypher",
          "MATCH (n0 {id : 47}), (n1 {id : 1}) MERGE(n0)-[r :rt3{k0 : false, k1 : false, k2 : 'u', k3 : true, k4 : -89168607, k5 : 'b', k11 : -254711332, k6 : 'A', k10 : '3F', k7 : 410016732, k8 : true, k9 : -1927530826, id : 201, klist : []}]->(n1)");
      database.command("opencypher",
          "MATCH (n0 {id : 47}), (n1 {id : 1}) MERGE(n0)-[r :rt6{k0 : false, k1 : true, k2 : 'z', k3 : false, k4 : -1462472899, k5 : 'uJb', k11 : 1358210785, k6 : 'c', k10 : 'd', k7 : 1827613636, k8 : false, k9 : -241887831, id : 230, klist : [648291860]}]->(n1)");
      database.command("opencypher",
          "MATCH (n0 {id : 47}), (n1 {id : 1}) MERGE(n0)-[r :rt1{k1 : true, k2 : '7o3jPys1', k3 : true, k4 : 1467868090, k5 : 'cfKB8P7', k11 : -905004356, id : 236, k6 : 'pa', k10 : 'N2zpd', k7 : 1814944569, k8 : true, k9 : -1736442959}]->(n1)");
      database.command("opencypher",
          "MATCH (n0 {id : 47}), (n1 {id : 1}) MERGE(n0)-[r :rt4{k0 : true, k1 : true, k3 : true, k4 : -1694381881, k5 : 'qkp', id : 250, k6 : 'rPP0KBQ8', k10 : 'phJ', k7 : -2056605696, k9 : 1788066541, klist : [-1677069883, -1722628107, -1673976951, 748899562]}]->(n1)");
      database.command("opencypher",
          "MATCH (n0 {id : 47}), (n1 {id : 1}) MERGE(n0)-[r :rt3{k0 : false, k1 : true, k2 : 'V5ZF', k3 : false, k4 : -1780203615, k5 : 'VfdKJ7op', k11 : 1189208871, k6 : '3', k10 : 'vF70a', k7 : 2089566395, k8 : true, id : 268, klist : ['95Q']}]->(n1)");
      database.command("opencypher",
          "MATCH (n0 {id : 2}), (n1 {id : 2}) MERGE(n0)-[r :rt0{k0 : true, k1 : false, k2 : '7OoC', k3 : true, k4 : 59440778, k11 : 2015320013, k6 : 'q', k10 : 'z', k7 : -1536231599, k8 : true, k9 : -1649816513, id : 97, klist : [-386765274, -1568757710]}]->(n1)");
      database.command("opencypher",
          "MATCH (n0 {id : 13}), (n1 {id : 13}) MERGE(n0)-[r :rt6{k0 : true, k1 : false, k2 : 'x', k3 : false, k4 : 1979180036, k5 : 'j', k11 : 155142787, k6 : 'x9C', k10 : 'O', k7 : -428124377, k8 : false, k9 : 327127700, id : 92, klist : [206052898]}]->(n1)");
      database.command("opencypher",
          "MATCH (n0 {id : 13}), (n1 {id : 13}) MERGE(n0)-[r :rt9{k0 : true, k1 : true, k2 : 'TM', k3 : false, k4 : -1302352519, k5 : 'U', k11 : 33528952, k6 : 'F', k10 : 'fU7', k7 : 2092880591, k8 : false, k9 : -1290035815, id : 14, klist : ['g']}]->(n1)");
      database.command("opencypher",
          "MATCH (n0 {id : 32}), (n1 {id : 32}) MERGE(n0)-[r :rt4{k0 : false, k1 : false, k2 : 'j', k3 : false, k4 : 1597072507, k5 : '0', k11 : -1097047702, k6 : 'ZZ', k10 : 'VT7', k7 : 1258300374, k8 : true, k9 : -29719996, id : 199, klist : []}]->(n1)");
      database.command("opencypher",
          "MATCH (n0 {id : 13}), (n1 {id : 13}) MERGE(n0)-[r :rt4{k0 : true, k1 : false, k2 : '72X', k3 : true, k4 : -2086435140, k5 : 'C', k11 : -230480237, k6 : 'a', k10 : 'my', k7 : -238408429, k8 : true, id : 311, klist : ['e', 'O57brRbf6', 'xq']}]->(n1)");
      database.command("opencypher",
          "MATCH (n0 {id : 50}), (n1 {id : 50}) MERGE(n0)-[r :rt7{k0 : true, k2 : 'c', k3 : true, k4 : -557450797, k5 : 'O85xCp', id : 195, k6 : 'xd', k10 : 'bxMmfGUO', k7 : -1567972939, k8 : false, k9 : 1720602863, klist : []}]->(n1)");
      database.command("opencypher",
          "MATCH (n0 {id : 61}), (n1 {id : 61}) MERGE(n0)-[r :rt9{k1 : true, k2 : 'IVmSM', k3 : true, k4 : -1714577447, k5 : 'U2mZH6p', k11 : -1766616315, id : 278, k6 : '0e', k10 : 'qX', k7 : 1709764385, k8 : false, klist : []}]->(n1)");
      database.command("opencypher",
          "MATCH (n0 {id : 13}), (n1 {id : 13}) MERGE(n0)-[r :rt7{k0 : true, k1 : true, k2 : 'G', k3 : true, k5 : 'w', k11 : -697703689, k6 : 'sRQNxF', k10 : 'X', k7 : 966245917, k8 : true, k9 : -1010958398, id : 8, klist : ['t', '2']}]->(n1)");
      database.command("opencypher",
          "MATCH (n0 {id : 63}), (n1 {id : 63}) MERGE(n0)-[r :rt0{k0 : false, k1 : false, k2 : 'Hm7b', k3 : false, k4 : -1057743736, k5 : '3O4x4CDxp', k11 : -451886211, k6 : 'A', k10 : 'm', k7 : 1092242399, k8 : false, k9 : 765266953, id : 318, klist : []}]->(n1)");
      database.command("opencypher",
          "MATCH (n0 {id : 13}), (n1 {id : 13}) MERGE(n0)-[r :rt7{k0 : true, k1 : true, k2 : 'tjp', k3 : true, k4 : -579164724, k5 : 'E', k11 : -1686189897, k6 : '3hZQ', k10 : '0', k7 : -711265195, k8 : true, k9 : 902842914, id : 292, klist : [-1966698877, 1018824523]}]->(n1)");
      database.command("opencypher",
          "MATCH (n0 {id : 13}), (n1 {id : 13}) MERGE(n0)-[r :rt0{k0 : true, k1 : true, k2 : 'cEN', k3 : false, k5 : 'lI1QM', k11 : -252567918, k6 : 'G', k10 : 'W6qT', k7 : 1688471691, k8 : false, k9 : 1079029165, id : 207, klist : [-1868901604, 415590082]}]->(n1)");
      database.command("opencypher",
          "MATCH (n0 {id : 95}), (n1 {id : 95}) MERGE(n0)-[r :rt7{k0 : true, k1 : false, k2 : 'Dk6r72Lfz', k3 : false, k4 : -1677906800, k5 : '1imJey', k11 : -1311170651, k6 : 'OI', k10 : 'b8', k7 : 1889726638, k8 : true, k9 : 1848089882, klist : ['sE9P6zo']}]->(n1)");
      database.command("opencypher",
          "MATCH (n0 {id : 109}), (n1 {id : 109}) MERGE(n0)-[r :rt0{k0 : false, k1 : false, k2 : '6', k3 : false, k4 : 1878859223, k5 : 'YU', k11 : -219308369, k6 : 'L', k10 : 'C', k7 : -171893969, k8 : false, k9 : 269961804, id : 68, klist : []}]->(n1)");
      database.command("opencypher",
          "MATCH (n0 {id : 112}), (n1 {id : 112}) MERGE(n0)-[r :rt9{k0 : true, k1 : false, k2 : 'lHbCNYrs', k3 : false, k4 : 1726190317, k5 : 'BWWQFCxM', k11 : -328652841, k6 : 'ak', k7 : -992473017, k8 : false, k9 : -426361853, id : 114, klist : [243328600, -1308341480, -177158674, -786443605]}]->(n1)");
      database.command("opencypher",
          "MATCH (n0 {id : 13}), (n1 {id : 13}) MERGE(n0)-[r :rt8{k0 : false, k1 : false, k2 : 'H', k3 : true, k4 : -1306531872, k5 : 'p2Twu', k11 : 1500583550, k6 : 'z', k10 : 'E', k7 : 1557384097, k8 : false, k9 : 1842757, id : 58, klist : ['AFAX', 'qlJL8', 'NiFqSmr', 'Vt']}]->(n1)");
      database.command("opencypher",
          "MATCH (n0 {id : 13}), (n1 {id : 13}) MERGE(n0)-[r :rt4{k0 : true, k1 : false, k2 : '48', k3 : false, k4 : 76911865, k5 : '5', k11 : -897296510, k6 : '9G', k10 : 'wwp', k7 : 678651546, k8 : false, k9 : 1443357371, id : 98, klist : [566155798, 2000312671, -2026941845, -1562380162]}]->(n1)");
      database.command("opencypher",
          "MATCH (n0 {id : 13}), (n1 {id : 13}) MERGE(n0)-[r :rt11{k0 : true, k1 : false, k2 : '3', k3 : false, k4 : -1928708168, k5 : 'JHS', k6 : 'M', k10 : '5qA', k7 : 871516401, k8 : false, k9 : 575976348, id : 176, klist : ['CG']}]->(n1)");
      database.command("opencypher",
          "MATCH (n0 {id : 13}), (n1 {id : 13}) MERGE(n0)-[r :rt10{k0 : false, k1 : false, k2 : 'NMa', k3 : true, k4 : -1211871831, k5 : 'N0M1', k11 : -1146503336, k6 : 'JxZisXHCJ', k10 : 'W', k7 : -774707099, k8 : false, k9 : -177462808, klist : ['1', 'Z', 'w', 'l']}]->(n1)");
      database.command("opencypher",
          "MATCH (n0 {id : 13}), (n1 {id : 13}) MERGE(n0)-[r :rt6{k1 : false, k2 : 'K', k3 : false, k4 : -1518580216, k5 : 'Lmv2l4t4L', k11 : 2118744307, id : 169, k6 : 'ZGpNb', k10 : 's', k7 : 1008566405, k9 : 1106634875, klist : []}]->(n1)");
      database.command("opencypher",
          "MATCH (n0 {id : 2}), (n1 {id : 2}) MERGE(n0)-[r :rt4{k0 : false, k1 : false, k2 : 'g', k3 : true, k4 : 1764863636, k5 : '8PUrE0w', k11 : -631347247, k6 : 'P', k10 : 'v', k7 : -1474713031, k8 : true, k9 : 625521074, id : 182, klist : []}]->(n1)");
      database.command("opencypher",
          "MATCH (n0 {id : 13}), (n1 {id : 13}) MERGE(n0)-[r :rt8{k0 : true, k1 : true, k2 : '4', k3 : true, k4 : 839983172, k5 : 'ossSzaNG', k11 : -1289445578, k10 : 'Nsy', k7 : -722207244, k8 : false, k9 : 690077828, id : 303, klist : ['djHqz', 'k2GH0']}]->(n1)");
      database.command("opencypher",
          "UNWIND range(0,53) AS i MATCH (a {id: 0}), (b {id: 1}) CREATE (a)-[:FILL {k6: 'f', i: i}]->(b)");
    });

    final String query = """
        OPTIONAL MATCH (n1 {k8: false, id: 116}) <-[r1:rt7 {id: 76, k6: "I6"}]- ( {k6: "Dm"}) <-[r0 {k8: false}]- (n0:l7:l0:l4 {id: 66}) -[ {k8: true, k1: true}]-> (:l7:l10:l0 {k0: true}), ( {k11: -43316742}) <-[:rt9]- (n2:l0:l7) WHERE  (r1.k10 IS NULL) WITH *\s
        CALL () { CALL db.schema.visualization() YIELD nodes AS alias0, relationships AS alias1 MATCH TRAIL (:l8:l7 {k11: 356654659, k6: "U"}) <-[]- (:l0:l7:l1:l2:l8) -[r2]-> (), (n3) <-[]- (n3), p0 = (n3) WHERE  ((toStringOrNull(n3.k10) STARTS WITH 'KO') XOR (toStringOrNull(n3.k10) <= toStringOrNull(r2.k6))) DETACH DELETE n3 WITH sum(toFloatOrNull(1)) AS alias2\s
        CALL () { UNWIND [] AS alias4 RETURN alias4 AS alias5 SKIP 0}
         RETURN collect(toStringOrNull(alias2)) AS alias7 SKIP 0
        UNION ALL
        CALL db.schema() YIELD nodes AS alias9, relationships AS alias10 UNWIND [-1328417065,1617464201,1678765992] AS alias11 CREATE (:l5:l6:l4:l1:l7:l10:l9:l3 {k0: true, k3: false, k4: 1256807501, k11: -2128059805, id: 133, k6: "B", k10: "Oz", k7: -958726819, k8: true, k9: -1188316991}) FOREACH (elem704 IN ['A', 'B', 'C'] | CREATE (:l4 {k1: elem704})) WITH count(DISTINCT toFloatOrNull(1)) AS alias19 FOREACH (elem7731 IN range(4, 6) | CREATE (:l8 {k10: elem7731})) RETURN null AS alias7 SKIP 3}
         WITH alias7, n0, n1, n2, r0, r1 WHERE r1 IS NOT NULL AND r0 IS NOT NULL AND n0 IS NOT NULL SET n0.k3 = TRUE, r0.k9 = -1701215515, r1.k2 = NULL RETURN n0, r0, alias7, n2.k5 AS alias24, toIntegerList(keys(n0)) AS alias25, valueType(r1.k7) AS alias26
        """;

    assertThatCode(() -> database.transaction(() -> {
      try (ResultSet result = database.command("opencypher", query)) {
        while (result.hasNext())
          result.next();
      }
    })).doesNotThrowAnyException();
  }

  /**
   * Smaller, hand-written shape of a disconnected-pattern MATCH feeding a DETACH DELETE - an unrelated
   * "fan-out" pattern ({@code (o:Other)}, three rows) cross-joined with a self-loop pattern
   * ({@code (n:Loop)<-[:SELF]-(n)}, one row) - without the TRAIL/CALL/UNION scaffolding of the
   * fuzzer-generated repro above.
   * <p>
   * This shape alone is not sufficient to reproduce issue #6491 end-to-end: the cost-based optimizer
   * plans it as a {@code CartesianProduct} that materializes the smaller (self-loop) side once and
   * reuses it, which never observes a mid-iteration delete. Reproducing the original hazard requires
   * the traditional (non-optimized) execution path, which only a disqualifying construct - such as the
   * leading {@code CALL db.schema.visualization()} in the repro above - forces; a hand-written query
   * that adds one for the sole purpose of forcing that path stops being "minimal" in any way that
   * earns its keep over the real repro. This test is kept instead as a correctness check of the new
   * {@code DeleteStep} eager-materialization path itself (it does get exercised here, since
   * {@code MatchClause#hasDisconnectedPathPatterns()} is true for this MATCH) on a case the optimizer
   * plans safely without it: it must still delete exactly the one self-loop node once and return all
   * three fan-out rows unchanged.
   * <p>
   * This is also the only test that exercises the {@code DeleteStep} construction site inside
   * {@code CypherExecutionPlan.buildExecutionStepsWithOptimizer()} with a disconnected-pattern MATCH -
   * the other two construction sites (the traditional ordered-clause build and the legacy fixed-order
   * build) are covered by the fuzzer-derived repro above, which is routed to the traditional path by
   * its leading {@code CALL} clause.
   */
  @Test
  void detachDeleteOfSelfLoopCrossJoinedWithUnrelatedPatternDeletesOnceAndReturnsAllRows() {
    database = new DatabaseFactory("./target/databases/testopencypher-issue6491-minimal").create();

    database.transaction(() -> {
      database.command("opencypher", "CREATE (:Loop {tag: null})");
      database.command("opencypher", "CREATE (:Other {id: 1})");
      database.command("opencypher", "CREATE (:Other {id: 2})");
      database.command("opencypher", "CREATE (:Other {id: 3})");
      database.command("opencypher", "MATCH (n:Loop) CREATE (n)-[:SELF]->(n)");
    });

    final List<Object> ids = new ArrayList<>();
    database.transaction(() -> {
      try (ResultSet result = database.command("opencypher",
          "MATCH (o:Other), (n:Loop)<-[:SELF]-(n) WHERE n.tag IS NULL DETACH DELETE n RETURN o.id AS id")) {
        while (result.hasNext())
          ids.add(result.next().getProperty("id"));
      }
    });

    assertThat(ids).containsExactlyInAnyOrder(1, 2, 3);

    try (ResultSet remainingLoops = database.query("opencypher", "MATCH (n:Loop) RETURN n")) {
      assertThat(remainingLoops.hasNext()).isFalse();
    }
  }

  /**
   * Same disconnected-pattern shape as
   * {@link #detachDeleteOfSelfLoopCrossJoinedWithUnrelatedPatternDeletesOnceAndReturnsAllRows()}, but
   * spelled as two separate {@code MATCH} keywords instead of one {@code MATCH} with a comma. A code
   * review on this fix pointed out that {@code MatchClause.hasDisconnectedPathPatterns()} originally
   * only unioned node variables within a single MATCH clause, so this exact shape - which the execution
   * plan builders chain onto the same step chain as the comma form (see
   * {@code CypherExecutionPlan.matchClausesHaveDisconnectedPatterns()}) - went undetected. Kept as its
   * own test (rather than folded into the one above) so a future regression in the cross-clause
   * overload fails here specifically, pointing straight at the two-clause spelling.
   */
  @Test
  void detachDeleteOfSelfLoopSpelledAsTwoSeparateMatchClausesDeletesOnceAndReturnsAllRows() {
    database = new DatabaseFactory("./target/databases/testopencypher-issue6491-twomatch").create();

    database.transaction(() -> {
      database.command("opencypher", "CREATE (:Loop {tag: null})");
      database.command("opencypher", "CREATE (:Other {id: 1})");
      database.command("opencypher", "CREATE (:Other {id: 2})");
      database.command("opencypher", "CREATE (:Other {id: 3})");
      database.command("opencypher", "MATCH (n:Loop) CREATE (n)-[:SELF]->(n)");
    });

    final List<Object> ids = new ArrayList<>();
    database.transaction(() -> {
      try (ResultSet result = database.command("opencypher",
          "MATCH (n:Loop)<-[:SELF]-(n) MATCH (o:Other) WHERE n.tag IS NULL DETACH DELETE n RETURN o.id AS id")) {
        while (result.hasNext())
          ids.add(result.next().getProperty("id"));
      }
    });

    assertThat(ids).containsExactlyInAnyOrder(1, 2, 3);

    try (ResultSet remainingLoops = database.query("opencypher", "MATCH (n:Loop) RETURN n")) {
      assertThat(remainingLoops.hasNext()).isFalse();
    }
  }

  /**
   * Same disconnected-pattern shape again, but the DETACH DELETE runs inside a FOREACH body instead of
   * directly after the MATCH. A code review on this fix pointed out that {@code ForeachStep} streams
   * its own outer input exactly the way the pre-fix {@code DeleteStep} did, so the same disconnected
   * MATCH re-enumeration hazard applies one level up - each of the three fan-out rows binds the very
   * same self-loop node, and FOREACH runs its body (hence the DELETE) once per row.
   * <p>
   * This shape surfaces two distinct bugs, both fixed together:
   * <ul>
   *   <li>The read hazard from issue #6491 itself: {@code ForeachStep} now eagerly materializes its
   *   upstream row set before running any iteration whose body contains a DELETE, mirroring
   *   {@code DeleteStep}'s own fix (see {@code ForeachStep.eagerMaterialize} and
   *   {@code ForeachClause#containsDelete()}).</li>
   *   <li>A second, write-side bug this shape exposed: {@code DeleteStep.flushDeferredDeletes} rebuilds
   *   its de-duplication set fresh on every call (one call per FOREACH outer row), so it cannot
   *   remember a vertex an earlier row already deleted - deleting the same node bound by three
   *   different rows threw on the second and third attempts. Neo4j treats deleting an already-deleted
   *   node as a no-op, so {@code flushDeferredDeletes} now catches that case instead of propagating it.</li>
   * </ul>
   */
  @Test
  void detachDeleteInsideForeachOfSelfLoopCrossJoinedWithUnrelatedPatternDeletesOnceAndReturnsAllRows() {
    database = new DatabaseFactory("./target/databases/testopencypher-issue6491-foreach").create();

    database.transaction(() -> {
      database.command("opencypher", "CREATE (:Loop {tag: null})");
      database.command("opencypher", "CREATE (:Other {id: 1})");
      database.command("opencypher", "CREATE (:Other {id: 2})");
      database.command("opencypher", "CREATE (:Other {id: 3})");
      database.command("opencypher", "MATCH (n:Loop) CREATE (n)-[:SELF]->(n)");
    });

    final List<Object> ids = new ArrayList<>();
    database.transaction(() -> {
      try (ResultSet result = database.command("opencypher",
          "MATCH (o:Other), (n:Loop)<-[:SELF]-(n) WHERE n.tag IS NULL "
              + "FOREACH (x IN [1] | DETACH DELETE n) RETURN o.id AS id")) {
        while (result.hasNext())
          ids.add(result.next().getProperty("id"));
      }
    });

    assertThat(ids).containsExactlyInAnyOrder(1, 2, 3);

    try (ResultSet remainingLoops = database.query("opencypher", "MATCH (n:Loop) RETURN n")) {
      assertThat(remainingLoops.hasNext()).isFalse();
    }
  }
}
