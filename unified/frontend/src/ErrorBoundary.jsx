import React from "react";

// Catches render/lifecycle errors in the subtree so a single tab crash shows a
// recoverable panel instead of white-screening the whole app. Keyed by route in
// App.jsx so navigating to another tab clears the error automatically.
export default class ErrorBoundary extends React.Component {
  constructor(props) {
    super(props);
    this.state = { error: null };
  }

  static getDerivedStateFromError(error) {
    return { error };
  }

  componentDidCatch(error, info) {
    // Surface for debugging; a real deployment would ship this to a logger.
    console.error("[ErrorBoundary]", error, info?.componentStack);
  }

  render() {
    if (!this.state.error) return this.props.children;
    return (
      <div
        style={{
          margin: "40px auto",
          maxWidth: 640,
          padding: 24,
          background: "#1e293b",
          border: "1px solid #7f1d1d",
          borderRadius: 12,
          color: "#f1f5f9",
        }}
      >
        <div style={{ fontSize: 16, fontWeight: 700, marginBottom: 8 }}>
          This tab hit an error
        </div>
        <div style={{ fontSize: 13, color: "#94a3b8", marginBottom: 16 }}>
          The rest of the app is still running — switch tabs, or retry.
        </div>
        <pre
          style={{
            fontSize: 12,
            color: "#fca5a5",
            background: "#0f172a",
            padding: 12,
            borderRadius: 8,
            overflowX: "auto",
            marginBottom: 16,
          }}
        >
          {String(this.state.error?.message || this.state.error)}
        </pre>
        <button
          onClick={() => this.setState({ error: null })}
          style={{
            padding: "8px 16px",
            background: "#334155",
            color: "#f1f5f9",
            border: "none",
            borderRadius: 8,
            cursor: "pointer",
            fontSize: 13,
          }}
        >
          Retry
        </button>
      </div>
    );
  }
}
