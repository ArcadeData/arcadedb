const path = require('path');
const CopyWebpackPlugin = require('copy-webpack-plugin');

module.exports = {
  entry: {
    'vendor-libs': './src/main/resources/static/js/vendor-libs.js'
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
          from: 'node_modules/notyf/notyf.min.js',
          to: 'js/notyf.min.js',
        },
        {
          from: 'node_modules/notyf/notyf.min.css',
          to: 'css/notyf.min.css',
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
        // CodeMirror v6 JavaScript is bundled via vendor-libs.js
        // CodeMirror v6 CSS is managed separately
        {
          from: 'src/main/resources/static/css/codemirror-v6.css',
          to: 'css/codemirror-v6.css',
        },
        {
          from: 'node_modules/cytoscape-cola/cytoscape-cola.js',
          to: 'js/cytoscape-cola.js',
        },
        {
          from: 'node_modules/cytoscape-cxtmenu/cytoscape-cxtmenu.js',
          to: 'js/cytoscape-cxtmenu.js',
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
          from: 'node_modules/webcola/WebCola/cola.min.js',
          to: 'js/cola.min.js',
        },
        {
          from: 'node_modules/clipboard/dist/clipboard.min.js',
          to: 'js/clipboard.min.js',
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
