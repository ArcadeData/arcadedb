// ArcadeDB Studio Vendor Libraries Entry Point
// This file serves as the entry point for bundling vendor dependencies

// Import CodeMirror v6 and create global bundle
import { EditorView, basicSetup } from 'codemirror';
import { EditorState } from '@codemirror/state';
import { javascript } from '@codemirror/lang-javascript';
import { sql } from '@codemirror/lang-sql';
import { oneDark } from '@codemirror/theme-one-dark';
import { autocompletion } from '@codemirror/autocomplete';
import { searchKeymap, highlightSelectionMatches } from '@codemirror/search';
import { keymap } from '@codemirror/view';
import { indentWithTab } from '@codemirror/commands';
import { LanguageSupport, StreamLanguage } from '@codemirror/language';

// Inline Cypher language support
function createCypherLanguage() {
  const cypherKeywords = [
    'MATCH', 'CREATE', 'RETURN', 'WHERE', 'WITH', 'DELETE', 'SET', 'REMOVE',
    'MERGE', 'OPTIONAL', 'UNION', 'ALL', 'DISTINCT', 'ORDER', 'BY', 'SKIP',
    'LIMIT', 'AS', 'AND', 'OR', 'NOT', 'XOR', 'IN', 'STARTS', 'ENDS', 'CONTAINS',
    'NULL', 'TRUE', 'FALSE', 'ASC', 'DESC', 'ASCENDING', 'DESCENDING',
    'CASE', 'WHEN', 'THEN', 'ELSE', 'END'
  ].map(k => k.toLowerCase());

  const cypherFunctions = [
    'COUNT', 'SUM', 'AVG', 'MIN', 'MAX', 'COLLECT', 'DISTINCT',
    'LENGTH', 'SIZE', 'TYPE', 'ID', 'KEYS', 'LABELS', 'NODES', 'RELATIONSHIPS',
    'STARTNODE', 'ENDNODE', 'HEAD', 'LAST', 'TAIL', 'RANGE', 'REDUCE', 'EXTRACT',
    'FILTER', 'ANY', 'ALL', 'NONE', 'SINGLE'
  ].map(f => f.toLowerCase());

  const cypherParser = {
    name: 'cypher',
    startState: function() {
      return { inString: false, stringDelim: null };
    },
    token: function(stream, state) {
      if (state.inString) {
        if (stream.match(state.stringDelim)) {
          state.inString = false;
          state.stringDelim = null;
          return 'string';
        }
        stream.next();
        return 'string';
      }

      if (stream.match(/["']/)) {
        state.inString = true;
        state.stringDelim = stream.current();
        return 'string';
      }

      if (stream.match('//')) {
        stream.skipToEnd();
        return 'comment';
      }

      if (stream.match(/\d+(\.\d+)?/)) {
        return 'number';
      }

      if (stream.match(/\w+/)) {
        const word = stream.current().toLowerCase();
        if (cypherKeywords.includes(word)) {
          return 'keyword';
        }
        if (cypherFunctions.includes(word)) {
          return 'builtin';
        }
        return 'variable';
      }

      if (stream.match(/[+\-*\/=<>!&|:()\[\]{},;.]/)) {
        return 'operator';
      }

      stream.next();
      return null;
    },
    languageData: {
      commentTokens: { line: '//' },
      closeBrackets: { brackets: ['(', '[', '{', '"', "'"] }
    }
  };

  return new LanguageSupport(StreamLanguage.define(cypherParser));
}

// Custom Cypher language support will be defined inline
// import { cypher } from './codemirror-lang-cypher.js';

// Create global CodeMirror v6 bundle for compatibility
window.CodeMirrorV6 = {
  EditorView,
  EditorState,
  basicSetup,
  javascript,
  sql,
  cypher: createCypherLanguage,
  oneDark,
  autocompletion,
  searchKeymap,
  highlightSelectionMatches,
  keymap,
  indentWithTab
};

// Legacy CodeMirror v5 compatibility layer
const CodeMirrorCompat = {
  fromTextArea: function(textarea, options = {}) {
    const extensions = [basicSetup];

    // Add language support based on mode
    if (options.mode) {
      if (options.mode === 'text/x-sql' || options.mode === 'sql') {
        extensions.push(sql());
      } else if (options.mode === 'javascript') {
        extensions.push(javascript());
      } else if (options.mode === 'application/x-cypher-query') {
        extensions.push(createCypherLanguage());
      }
    }

    // Add theme support
    if (options.theme === 'neo' || options.theme === 'dark') {
      extensions.push(oneDark);
    }

    // Add key bindings
    const keyBindings = [];
    if (options.extraKeys) {
      Object.keys(options.extraKeys).forEach(key => {
        const callback = options.extraKeys[key];
        keyBindings.push({
          key,
          run: () => { callback(); return true; }
        });
      });
    }
    if (keyBindings.length > 0) {
      extensions.push(keymap.of(keyBindings));
    }

    // Add indentation with tab
    extensions.push(keymap.of([indentWithTab]));

    // Create editor state
    const state = EditorState.create({
      doc: textarea.value,
      extensions
    });

    // Create editor view
    const view = new EditorView({
      state,
      parent: textarea.parentNode
    });

    // Hide original textarea
    textarea.style.display = 'none';

    // Create compatibility object
    const editor = {
      view,
      state,

      // v5 compatibility methods
      getValue: () => view.state.doc.toString(),
      setValue: (value) => {
        view.dispatch({
          changes: {
            from: 0,
            to: view.state.doc.length,
            insert: value
          }
        });
      },
      refresh: () => {
        // In v6, this is usually not needed as the editor auto-updates
        setTimeout(() => {
          view.requestMeasure();
        }, 1);
      },
      focus: () => view.focus(),
      setOption: (option, value) => {
        if (option === 'mode') {
          // Handle mode changes by recreating the editor with new language
          const newExtensions = [basicSetup];
          if (value === 'text/x-sql' || value === 'sql') {
            newExtensions.push(sql());
          } else if (value === 'javascript') {
            newExtensions.push(javascript());
          } else if (value === 'application/x-cypher-query') {
            newExtensions.push(createCypherLanguage());
          }

          view.dispatch({
            effects: EditorState.reconfigure.of(newExtensions)
          });
        }
      },
      addKeyMap: (keyMap) => {
        const keyBindings = [];
        Object.keys(keyMap).forEach(key => {
          const callback = keyMap[key];
          keyBindings.push({
            key,
            run: () => { callback(editor); return true; }
          });
        });

        view.dispatch({
          effects: EditorState.reconfigure.of([
            ...view.state.extensions,
            keymap.of(keyBindings)
          ])
        });
      }
    };

    return editor;
  }
};

// Expose compatibility layer globally
window.CodeMirror = CodeMirrorCompat;

console.log('ArcadeDB Studio - CodeMirror v6 compatibility layer initialized');

// This file exists primarily for webpack entry point
// The actual libraries are copied via CopyWebpackPlugin
