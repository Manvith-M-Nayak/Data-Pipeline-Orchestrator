import { useCallback, useEffect, useState } from "react";

// A translucent version of any colour — including CSS variables, which the old
// `color + "22"` hex-alpha trick could not handle.
export function tint(color, pct = 13) {
  return `color-mix(in srgb, ${color} ${pct}%, transparent)`;
}

// Status tones used across badges, alerts and dots.
export const TONES = {
  neutral: { fg: "var(--text-3)", bg: "var(--surface-2)",  line: "var(--border)" },
  accent:  { fg: "var(--accent)", bg: "var(--accent-soft)", line: "var(--accent-line)" },
  ok:      { fg: "var(--ok)",     bg: "var(--ok-soft)",     line: "var(--ok-line)" },
  warn:    { fg: "var(--warn)",   bg: "var(--warn-soft)",   line: "var(--warn-line)" },
  bad:     { fg: "var(--bad)",    bg: "var(--bad-soft)",    line: "var(--bad-line)" },
  violet:  { fg: "var(--violet)", bg: "var(--violet-soft)", line: "var(--violet-line)" },
};

function systemTheme() {
  return window.matchMedia?.("(prefers-color-scheme: dark)").matches ? "dark" : "light";
}

function savedTheme() {
  try {
    const t = localStorage.getItem("theme");
    return t === "light" || t === "dark" ? t : null;
  } catch {
    return null;
  }
}

// Current theme + toggle. The choice is stored; with no stored choice the app
// follows the OS and keeps following it if the OS setting changes.
export function useTheme() {
  const [choice, setChoice] = useState(savedTheme);
  const [system, setSystem] = useState(systemTheme);

  useEffect(() => {
    const mq = window.matchMedia?.("(prefers-color-scheme: dark)");
    if (!mq) return undefined;
    const onChange = () => setSystem(mq.matches ? "dark" : "light");
    mq.addEventListener("change", onChange);
    return () => mq.removeEventListener("change", onChange);
  }, []);

  useEffect(() => {
    const root = document.documentElement;
    if (choice) root.dataset.theme = choice;
    else delete root.dataset.theme;
  }, [choice]);

  const theme = choice || system;
  const toggle = useCallback(() => {
    const next = theme === "dark" ? "light" : "dark";
    setChoice(next);
    try { localStorage.setItem("theme", next); } catch { /* not persisted */ }
  }, [theme]);

  return { theme, toggle };
}
