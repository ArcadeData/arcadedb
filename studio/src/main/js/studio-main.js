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
// ArcadeDB Studio Main Entry Point
// This file imports and initializes all the required dependencies

// Import CSS dependencies
import 'bootstrap/dist/css/bootstrap.min.css';
import 'datatables.net-bs5/css/dataTables.bootstrap5.min.css';
import 'datatables.net-buttons-bs5/css/buttons.bootstrap5.min.css';
import 'datatables.net-responsive-bs5/css/responsive.bootstrap5.min.css';
import 'datatables.net-select-bs5/css/select.bootstrap5.min.css';
import 'codemirror/lib/codemirror.css';
import 'codemirror/theme/neo.css';
import '@fortawesome/fontawesome-free/css/all.min.css';
import 'sweetalert2/dist/sweetalert2.min.css';
import 'apexcharts/dist/apexcharts.css';

// Import JavaScript dependencies
import $ from 'jquery';
window.jQuery = window.$ = $;

import 'bootstrap';
import 'datatables.net';
import 'datatables.net-bs5';
import 'datatables.net-buttons';
import 'datatables.net-buttons-bs5';
import 'datatables.net-responsive';
import 'datatables.net-responsive-bs5';
import 'datatables.net-select';
import 'datatables.net-select-bs5';

import Swal from 'sweetalert2';
window.Swal = Swal;

import cytoscape from 'cytoscape';
import fcose from 'cytoscape-fcose';
import cxtmenu from 'cytoscape-cxtmenu';
import nodeHtmlLabel from 'cytoscape-node-html-label';

// Register Cytoscape extensions
cytoscape.use(fcose);
cytoscape.use(cxtmenu);
cytoscape.use(nodeHtmlLabel);
window.cytoscape = cytoscape;

import ApexCharts from 'apexcharts';
window.ApexCharts = ApexCharts;

import CodeMirror from 'codemirror';
import 'codemirror/mode/sql/sql';
import 'codemirror/mode/javascript/javascript';
import 'codemirror/addon/hint/show-hint';
import 'codemirror/addon/hint/sql-hint';
window.CodeMirror = CodeMirror;

// Import custom studio modules (these will be refactored from existing files)
import './studio-utils';
import './studio-database';
import './studio-server';
import './studio-cluster';
import './studio-table';
import './studio-graph';
import './studio-graph-widget';
import './studio-graph-functions';

console.log('ArcadeDB Studio initialized with modern dependency management');
