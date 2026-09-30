import React from "react";
import { Alert, Button } from "./ui/components.jsx";

// Catches render/lifecycle errors in the subtree so a single page crash shows a
// recoverable panel instead of white-screening the whole app. Keyed by route in
// App.jsx so navigating to another page clears the error automatically.
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
      <div style={{ maxWidth: 640, margin: "40px auto" }}>
        <Alert
          tone="bad"
          title="This page hit an error"
          action={<Button size="sm" onClick={() => this.setState({ error: null })}>Retry</Button>}
        >
          The rest of the app is still running — switch pages, or retry.
          <pre style={{
            marginTop: 10, padding: 10, borderRadius: 8, overflowX: "auto",
            background: "var(--surface)", border: "1px solid var(--border)", color: "var(--bad)", fontSize: 12,
          }}>
            {String(this.state.error?.message || this.state.error)}
          </pre>
        </Alert>
      </div>
    );
  }
}
