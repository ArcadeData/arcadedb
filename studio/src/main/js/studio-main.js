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
import 'notyf/notyf.min.css';
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

import { Notyf } from 'notyf';
window.Notyf = Notyf;

import ClipboardJS from 'clipboard';
window.ClipboardJS = ClipboardJS;

import cytoscape from 'cytoscape';
import cola from 'cytoscape-cola';
import cxtmenu from 'cytoscape-cxtmenu';
import nodeHtmlLabel from 'cytoscape-node-html-label';

// Register Cytoscape extensions
cytoscape.use(cola);
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
