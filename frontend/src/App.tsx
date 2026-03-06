import { useState, useEffect, useReducer, FormEvent } from 'react'
import './App.css'
// Import the Dashboard component we created previously
import Dashboard from './Dashboard'

const STORAGE_KEY = 'api_key'

// Define available views
type View = 'items' | 'dashboard'

interface Item {
  id: number
  type: string
  title: string
  created_at: string
}

type FetchState =
  | { status: 'idle' }
  | { status: 'loading' }
  | { status: 'success'; items: Item[] }
  | { status: 'error'; message: string }

type FetchAction =
  | { type: 'fetch_start' }
  | { type: 'fetch_success'; data: Item[] }
  | { type: 'fetch_error'; message: string }

function fetchReducer(_state: FetchState, action: FetchAction): FetchState {
  switch (action.type) {
    case 'fetch_start':
      return { status: 'loading' }
    case 'fetch_success':
      return { status: 'success', items: action.data }
    case 'fetch_error':
      return { status: 'error', message: action.message }
  }
}

function App() {
  const [token, setToken] = useState(
    () => localStorage.getItem(STORAGE_KEY) ?? '',
  )
  const [draft, setDraft] = useState('')
  const [fetchState, dispatch] = useReducer(fetchReducer, { status: 'idle' })

  // New state to manage navigation between views
  const [currentView, setCurrentView] = useState<View>('items')

  useEffect(() => {
    // Only fetch items if we are on the items view and have a token
    if (!token || currentView !== 'items') return

    dispatch({ type: 'fetch_start' })

    fetch('/items/', {
      headers: { Authorization: `Bearer ${token}` },
    })
      .then((res) => {
        if (!res.ok) throw new Error(`HTTP ${res.status}`)
        return res.json()
      })
      .then((data: Item[]) => dispatch({ type: 'fetch_success', data }))
      .catch((err: Error) =>
        dispatch({ type: 'fetch_error', message: err.message }),
      )
  }, [token, currentView])

  function handleConnect(e: FormEvent) {
    e.preventDefault()
    const trimmed = draft.trim()
    if (!trimmed) return
    localStorage.setItem(STORAGE_KEY, trimmed)
    setToken(trimmed)
  }

  function handleDisconnect() {
    localStorage.removeItem(STORAGE_KEY)
    setToken('')
    setDraft('')
    setCurrentView('items') // Reset to items view on disconnect
  }

  // Render Login Form if no token
  if (!token) {
    return (
      <form className="token-form" onSubmit={handleConnect}>
        <h1>API Key</h1>
        <p>Enter your API key to connect.</p>
        <input
          type="password"
          placeholder="Token"
          value={draft}
          onChange={(e) => setDraft(e.target.value)}
        />
        <button type="submit">Connect</button>
      </form>
    )
  }

  // Main Application Layout with Navigation
  return (
    <div>
      <header className="app-header" style={{ display: 'flex', justifyContent: 'space-between', alignItems: 'center', marginBottom: '20px' }}>
        <h1 style={{ margin: 0 }}>{currentView === 'items' ? 'Items' : 'Dashboard'}</h1>

        <nav style={{ display: 'flex', gap: '10px', alignItems: 'center' }}>
          {/* Navigation Buttons */}
          <button
            onClick={() => setCurrentView('items')}
            style={{
              padding: '8px 16px',
              cursor: 'pointer',
              backgroundColor: currentView === 'items' ? '#007bff' : '#e9ecef',
              color: currentView === 'items' ? 'white' : 'black',
              border: 'none',
              borderRadius: '4px'
            }}
          >
            Items
          </button>

          <button
            onClick={() => setCurrentView('dashboard')}
            style={{
              padding: '8px 16px',
              cursor: 'pointer',
              backgroundColor: currentView === 'dashboard' ? '#007bff' : '#e9ecef',
              color: currentView === 'dashboard' ? 'white' : 'black',
              border: 'none',
              borderRadius: '4px'
            }}
          >
            Dashboard
          </button>

          <div style={{ width: '1px', height: '20px', backgroundColor: '#ccc', margin: '0 10px' }}></div>

          <button className="btn-disconnect" onClick={handleDisconnect}>
            Disconnect
          </button>
        </nav>
      </header>

      {/* Conditional Rendering based on currentView */}
      {currentView === 'items' && (
        <>
          {fetchState.status === 'loading' && <p>Loading items...</p>}
          {fetchState.status === 'error' && <p style={{ color: 'red' }}>Error: {fetchState.message}</p>}

          {fetchState.status === 'success' && (
            <table style={{ width: '100%', borderCollapse: 'collapse' }}>
              <thead>
                <tr style={{ borderBottom: '2px solid #ddd', textAlign: 'left' }}>
                  <th style={{ padding: '8px' }}>ID</th>
                  <th style={{ padding: '8px' }}>ItemType</th>
                  <th style={{ padding: '8px' }}>Title</th>
                  <th style={{ padding: '8px' }}>Created at</th>
                </tr>
              </thead>
              <tbody>
                {fetchState.items.map((item) => (
                  <tr key={item.id} style={{ borderBottom: '1px solid #eee' }}>
                    <td style={{ padding: '8px' }}>{item.id}</td>
                    <td style={{ padding: '8px' }}>{item.type}</td>
                    <td style={{ padding: '8px' }}>{item.title}</td>
                    <td style={{ padding: '8px' }}>{item.created_at}</td>
                  </tr>
                ))}
              </tbody>
            </table>
          )}
        </>
      )}

      {currentView === 'dashboard' && (
        <Dashboard />
      )}
    </div>
  )
}

export default App