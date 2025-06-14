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
          from: 'node_modules/datatables.net/js/jquery.dataTables.min.js',
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
