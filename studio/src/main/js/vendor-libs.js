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

    // Hide original textarea
    textarea.style.display = 'none'

    // Create wrapper div for v5 compatibility
    const wrapper = document.createElement('div')
    wrapper.className = 'CodeMirror'
    wrapper.appendChild(view.dom)
    textarea.parentNode.insertBefore(wrapper, view.dom)

    // Create compatibility object
    const compatibilityEditor = {
      // Core v5 API methods
      getValue: function() {
        return view.state.doc.toString()
      },

      setValue: function(value) {
        view.dispatch({
          changes: {
            from: 0,
            to: view.state.doc.length,
            insert: value
          }
        })
        // Update textarea value for form compatibility
        textarea.value = value
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
    view.dom.addEventListener('input', function() {
      textarea.value = view.state.doc.toString()
      // Dispatch change event on textarea
      const event = new Event('change', { bubbles: true })
      textarea.dispatchEvent(event)
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
