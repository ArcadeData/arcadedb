// ArcadeDB Studio Vendor Libraries with CodeMirror v6 Compatibility Layer
// Provides v5 API compatibility for CodeMirror v6

// Import CodeMirror v6 dependencies
import { EditorState } from '@codemirror/state'
import { EditorView, keymap, placeholder } from '@codemirror/view'
import { defaultKeymap, history, historyKeymap } from '@codemirror/commands'
import { searchKeymap, highlightSelectionMatches } from '@codemirror/search'
import { autocompletion, completionKeymap, closeBrackets, closeBracketsKeymap } from '@codemirror/autocomplete'
import { foldGutter, indentOnInput, indentUnit, bracketMatching, foldKeymap } from '@codemirror/language'
import { highlightActiveLineGutter, lineNumbers, highlightActiveLine } from '@codemirror/view'
import { javascript } from '@codemirror/lang-javascript'
import { sql } from '@codemirror/lang-sql'

// Cypher language support (basic)
function cypher() {
  return sql() // Use SQL as fallback for Cypher
}

// Global CodeMirror object for v5 compatibility
window.CodeMirror = {
  // Main fromTextArea function for v5 compatibility
  fromTextArea: function(textarea, options = {}) {
    console.log('CodeMirror v6 compatibility: fromTextArea called', { textarea, options })

    const mode = options.mode || 'text/plain'
    let languageSupport = []

    // Map v5 modes to v6 language extensions
    switch(mode) {
      case 'application/x-cypher-query':
      case 'cypher':
        languageSupport = [cypher()]
        break
      case 'application/x-sql':
      case 'text/x-sql':
      case 'sql':
        languageSupport = [sql()]
        break
      case 'application/javascript':
      case 'text/javascript':
      case 'javascript':
        languageSupport = [javascript()]
        break
      default:
        languageSupport = [] // Plain text
    }

    // Base extensions
    const extensions = [
      lineNumbers(),
      highlightActiveLineGutter(),
      history(),
      foldGutter(),
      indentOnInput(),
      bracketMatching(),
      closeBrackets(),
      autocompletion(),
      highlightSelectionMatches(),
      highlightActiveLine(),
      keymap.of([
        ...closeBracketsKeymap,
        ...defaultKeymap,
        ...searchKeymap,
        ...historyKeymap,
        ...foldKeymap,
        ...completionKeymap,
      ]),
      ...languageSupport
    ]

    if (options.placeholder) {
      extensions.push(placeholder(options.placeholder))
    }

    if (options.indentUnit) {
      extensions.push(indentUnit.of(' '.repeat(options.indentUnit)))
    }

    // Create editor state
    const state = EditorState.create({
      doc: textarea.value,
      extensions: extensions
    })

    // Create editor view
    const view = new EditorView({
      state,
      parent: textarea.parentNode
    })

    // Create wrapper div for v5 compatibility
    const wrapper = document.createElement('div')
    wrapper.className = 'CodeMirror'
    wrapper.appendChild(view.dom)
    textarea.parentNode.insertBefore(wrapper, textarea)

    // Hide original textarea
    textarea.style.display = 'none'

    // Create compatibility object
    const compatibilityEditor = {
      // Core v5 API methods
      getValue: function() {
        return view.state.doc.toString()
      },

      setValue: function(value) {
        const strValue = String(value || '');
        view.dispatch({
          changes: {
            from: 0,
            to: view.state.doc.length,
            insert: strValue
          }
        })
        // Update textarea value for form compatibility
        textarea.value = strValue;
        // Trigger change event
        const event = new Event('change', { bubbles: true });
        textarea.dispatchEvent(event);
      },

      // Cursor and selection methods
      getCursor: function() {
        const selection = view.state.selection.main;
        const line = view.state.doc.lineAt(selection.head);
        return {
          line: line.number - 1,
          ch: selection.head - line.from
        };
      },

      setCursor: function(pos) {
        const line = typeof pos === 'object' ? pos.line : 0;
        const ch = typeof pos === 'object' ? pos.ch : pos || 0;
        const docLine = view.state.doc.line(line + 1);
        const offset = Math.min(docLine.from + ch, docLine.to);
        view.dispatch({
          selection: { anchor: offset, head: offset }
        });
      },

      getSelection: function() {
        const selection = view.state.selection.main;
        return view.state.doc.sliceString(selection.from, selection.to);
      },

      setSelection: function(from, to) {
        const fromLine = view.state.doc.line(from.line + 1);
        const fromOffset = fromLine.from + from.ch;
        const toLine = view.state.doc.line(to.line + 1);
        const toOffset = toLine.from + to.ch;
        view.dispatch({
          selection: { anchor: fromOffset, head: toOffset }
        });
      },

      replaceRange: function(replacement, from, to) {
        if (!to) to = from;
        const fromLine = view.state.doc.line(from.line + 1);
        const fromOffset = fromLine.from + from.ch;
        const toLine = view.state.doc.line(to.line + 1);
        const toOffset = toLine.from + to.ch;

        view.dispatch({
          changes: {
            from: fromOffset,
            to: toOffset,
            insert: replacement
          }
        });
        // Sync with textarea
        textarea.value = view.state.doc.toString();
      },

      // Line operations
      lineCount: function() {
        return view.state.doc.lines;
      },

      getLine: function(n) {
        const line = view.state.doc.line(n + 1);
        return line.text;
      },

      // History operations
      undo: function() {
        // Import undo from commands
        import('@codemirror/commands').then(({ undo }) => {
          undo(view);
        });
      },

      redo: function() {
        // Import redo from commands
        import('@codemirror/commands').then(({ redo }) => {
          redo(view);
        });
      },

      // Options support
      getOption: function(name) {
        const options = {
          mode: mode,
          theme: 'neo',
          lineNumbers: false,
          lineWrapping: true,
          matchBrackets: true,
          viewportMargin: Infinity
        };
        return options[name];
      },

      setOption: function(name, value) {
        if (name === 'mode') {
          // Handle mode change - would need to reconfigure extensions
          console.log('CodeMirror v6 compatibility: setOption mode change to', value);
        }
      },

      // Focus and interaction
      hasFocus: function() {
        return view.hasFocus;
      },

      getTextArea: function() {
        return textarea;
      },

      addKeyMap: function(keyMap) {
        // Basic keymap support for Ctrl+Enter/Cmd+Enter
        console.log('CodeMirror v6 compatibility: addKeyMap called', keyMap);
      },

      getDoc: function() {
        return {
          getValue: () => view.state.doc.toString(),
          setValue: (value) => {
            view.dispatch({
              changes: {
                from: 0,
                to: view.state.doc.length,
                insert: value
              }
            })
            textarea.value = value
          }
        }
      },

      refresh: function() {
        // No-op in v6
      },

      focus: function() {
        view.focus()
      },

      on: function(event, callback) {
        // Basic event simulation
        if (event === 'change') {
          view.dom.addEventListener('input', callback)
        }
      },

      // v6 editor reference for advanced usage
      _view: view,
      _state: state
    }

    // Sync changes back to textarea for form submission
    // Use the simpler event listener approach for now
    view.dom.addEventListener('input', function() {
      const newValue = view.state.doc.toString();
      textarea.value = newValue;
      // Dispatch change event on textarea
      const event = new Event('change', { bubbles: true });
      textarea.dispatchEvent(event);
    })

    console.log('CodeMirror v6 compatibility: Editor created successfully')
    return compatibilityEditor
  },

  // Static methods for v5 compatibility
  defineMode: function(name, modeFunc) {
    console.log('CodeMirror v6 compatibility: defineMode called for', name)
    // No-op for v6 compatibility
  },

  defineMIME: function(mime, mode) {
    console.log('CodeMirror v6 compatibility: defineMIME called', mime, mode)
    // No-op for v6 compatibility
  },

  version: '6.0.2-compat'
}

console.log('ArcadeDB Studio - CodeMirror v6 with v5 compatibility initialized')
