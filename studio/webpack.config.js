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
const path = require('path');
const CopyWebpackPlugin = require('copy-webpack-plugin');

module.exports = {
  entry: {
    'vendor-libs': './src/main/js/vendor-libs.js'
  },
  output: {
    path: path.resolve(__dirname, 'src/main/resources/static/dist'),
    filename: 'js/[name].bundle.js',
    clean: true,
  },
  mode: 'production',
  module: {
    rules: [
      {
        test: /\.css$/i,
        use: ['style-loader', 'css-loader'],
      },
      {
        test: /\.(png|svg|jpg|jpeg|gif|ico)$/i,
        type: 'asset/resource',
        generator: {
          filename: 'images/[name][ext]',
        },
      },
      {
        test: /\.(woff|woff2|eot|ttf|otf)$/i,
        type: 'asset/resource',
        generator: {
          filename: 'fonts/[name][ext]',
        },
      },
    ],
  },
  plugins: [
    new CopyWebpackPlugin({
      patterns: [
        {
          from: 'node_modules/jquery/dist/jquery.min.js',
          to: 'js/jquery.min.js',
        },
        {
          from: 'node_modules/bootstrap/dist/js/bootstrap.bundle.min.js',
          to: 'js/bootstrap.bundle.min.js',
        },
        {
          from: 'node_modules/bootstrap/dist/css/bootstrap.min.css',
          to: 'css/bootstrap.min.css',
        },
        {
          from: 'node_modules/datatables.net/js/dataTables.min.js',
          to: 'js/dataTables.min.js',
        },
        {
          from: 'node_modules/datatables.net-bs5/js/dataTables.bootstrap5.min.js',
          to: 'js/dataTables.bootstrap5.min.js',
        },
        {
          from: 'node_modules/datatables.net-bs5/css/dataTables.bootstrap5.min.css',
          to: 'css/dataTables.bootstrap5.min.css',
        },
        // DataTables Extensions
        {
          from: 'node_modules/datatables.net-buttons/js/dataTables.buttons.min.js',
          to: 'js/dataTables.buttons.min.js',
        },
        {
          from: 'node_modules/datatables.net-buttons-bs5/js/buttons.bootstrap5.min.js',
          to: 'js/buttons.bootstrap5.min.js',
        },
        {
          from: 'node_modules/datatables.net-buttons-bs5/css/buttons.bootstrap5.min.css',
          to: 'css/buttons.bootstrap5.min.css',
        },
        {
          from: 'node_modules/datatables.net-responsive/js/dataTables.responsive.min.js',
          to: 'js/dataTables.responsive.min.js',
        },
        {
          from: 'node_modules/datatables.net-responsive-bs5/js/responsive.bootstrap5.min.js',
          to: 'js/responsive.bootstrap5.min.js',
        },
        {
          from: 'node_modules/datatables.net-responsive-bs5/css/responsive.bootstrap5.min.css',
          to: 'css/responsive.bootstrap5.min.css',
        },
        {
          from: 'node_modules/datatables.net-select/js/dataTables.select.min.js',
          to: 'js/dataTables.select.min.js',
        },
        {
          from: 'node_modules/datatables.net-select-bs5/js/select.bootstrap5.min.js',
          to: 'js/select.bootstrap5.min.js',
        },
        {
          from: 'node_modules/datatables.net-select-bs5/css/select.bootstrap5.min.css',
          to: 'css/select.bootstrap5.min.css',
        },
        // DataTables Buttons Extension - Additional Files
        {
          from: 'node_modules/datatables.net-buttons/js/buttons.html5.min.js',
          to: 'js/buttons.html5.min.js',
        },
        {
          from: 'node_modules/datatables.net-buttons/js/buttons.print.min.js',
          to: 'js/buttons.print.min.js',
        },
        // Excel/PDF Export Dependencies for DataTables
        {
          from: 'node_modules/jszip/dist/jszip.min.js',
          to: 'js/jszip.min.js',
        },
        {
          from: 'node_modules/pdfmake/build/pdfmake.min.js',
          to: 'js/pdfmake.min.js',
        },
        {
          from: 'node_modules/pdfmake/build/vfs_fonts.js',
          to: 'js/vfs_fonts.js',
        },
        {
          from: 'node_modules/sweetalert2/dist/sweetalert2.all.min.js',
          to: 'js/sweetalert2.all.min.js',
        },
        {
          from: 'node_modules/sweetalert2/dist/sweetalert2.min.css',
          to: 'css/sweetalert2.min.css',
        },
        {
          from: 'node_modules/cytoscape/dist/cytoscape.min.js',
          to: 'js/cytoscape.min.js',
        },
        {
          from: 'node_modules/apexcharts/dist/apexcharts.min.js',
          to: 'js/apexcharts.min.js',
        },
        {
          from: 'node_modules/apexcharts/dist/apexcharts.css',
          to: 'css/apexcharts.css',
        },
        {
          from: 'node_modules/codemirror/lib/codemirror.js',
          to: 'js/codemirror.js',
        },
        {
          from: 'node_modules/codemirror/lib/codemirror.css',
          to: 'css/codemirror.css',
        },
        {
          from: 'node_modules/codemirror/theme/neo.css',
          to: 'css/neo.css',
        },
        {
          from: 'node_modules/codemirror/mode/cypher/cypher.js',
          to: 'js/cypher.js',
        },
        {
          from: 'node_modules/codemirror/mode/javascript/javascript.js',
          to: 'js/javascript.js',
        },
        {
          from: 'node_modules/codemirror/mode/sql/sql.js',
          to: 'js/sql.js',
        },
        {
          from: 'node_modules/codemirror/addon/hint/show-hint.js',
          to: 'js/show-hint.js',
        },
        {
          from: 'node_modules/codemirror/addon/hint/show-hint.css',
          to: 'css/show-hint.css',
        },
        {
          from: 'node_modules/codemirror/addon/hint/sql-hint.js',
          to: 'js/sql-hint.js',
        },
        {
          from: 'node_modules/cytoscape-cxtmenu/cytoscape-cxtmenu.js',
          to: 'js/cytoscape-cxtmenu.js',
        },
        // fcose layout and its dependencies (must load in order: layout-base -> cose-base -> fcose)
        {
          from: 'node_modules/layout-base/layout-base.js',
          to: 'js/layout-base.js',
        },
        {
          from: 'node_modules/cose-base/cose-base.js',
          to: 'js/cose-base.js',
        },
        {
          from: 'node_modules/cytoscape-fcose/cytoscape-fcose.js',
          to: 'js/cytoscape-fcose.js',
        },
        {
          from: 'node_modules/cytoscape-graphml/cytoscape-graphml.js',
          to: 'js/cytoscape-graphml.js',
        },
        {
          from: 'node_modules/cytoscape-node-html-label/dist/cytoscape-node-html-label.js',
          to: 'js/cytoscape-node-html-label.min.js',
        },
        {
          from: 'node_modules/marked/marked.min.js',
          to: 'js/marked.min.js',
        },
        {
          from: 'node_modules/@fortawesome/fontawesome-free/css/all.min.css',
          to: 'css/fontawesome.min.css',
        },
        {
          from: 'node_modules/@fortawesome/fontawesome-free/webfonts',
          to: 'webfonts',
        },
      ],
    }),
  ],
  optimization: {
    minimize: true,
  },
};
