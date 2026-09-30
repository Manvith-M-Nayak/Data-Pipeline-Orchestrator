import React from "react";
import { AlertTriangle, CheckCircle2, Info, XCircle } from "lucide-react";
import { TONES } from "./theme.js";

const cx = (...c) => c.filter(Boolean).join(" ");

export function Button({ variant = "secondary", size, icon: Icon, loading, className, children, ...rest }) {
  return (
    <button
      className={cx("btn", variant !== "secondary" && `btn-${variant}`, size && `btn-${size}`, className)}
      disabled={loading || rest.disabled}
      {...rest}
    >
      {loading ? <Spinner size={13} /> : Icon ? <Icon size={size === "sm" ? 13 : 15} strokeWidth={1.8} /> : null}
      {children}
    </button>
  );
}

export function Spinner({ size = 14 }) {
  return <span className="spinner" style={{ width: size, height: size }} aria-label="Loading" />;
}

export function Card({ title, subtitle, icon: Icon, actions, pad = true, className, style, children }) {
  return (
    <section className={cx("card", className)} style={style}>
      {(title || actions) && (
        <header className="card-head">
          <div style={{ minWidth: 0 }}>
            {title && (
              <div className="card-title">
                {Icon && <Icon size={15} strokeWidth={1.8} style={{ color: "var(--text-3)" }} />}
                {title}
              </div>
            )}
            {subtitle && <div className="card-sub">{subtitle}</div>}
          </div>
          {actions && <div style={{ marginLeft: "auto", display: "flex", gap: 8, alignItems: "center" }}>{actions}</div>}
        </header>
      )}
      {pad ? <div className="card-body">{children}</div> : children}
    </section>
  );
}

export function PageHeader({ eyebrow, icon: Icon, title, description, actions }) {
  return (
    <div className="page-head">
      <div style={{ minWidth: 0 }}>
        {eyebrow && (
          <div className="page-eyebrow">
            {Icon && <Icon size={13} strokeWidth={2} />}
            {eyebrow}
          </div>
        )}
        <h1 className="page-title">{title}</h1>
        {description && <p className="page-desc">{description}</p>}
      </div>
      {actions && <div className="page-actions">{actions}</div>}
    </div>
  );
}

export function Badge({ tone = "neutral", dot, children, style }) {
  const t = TONES[tone] || TONES.neutral;
  return (
    <span className="badge" style={{ color: t.fg, background: t.bg, borderColor: t.line, ...style }}>
      {dot && <span className="dot" style={{ background: t.fg }} />}
      {children}
    </span>
  );
}

export function Dot({ tone = "neutral", live }) {
  const t = TONES[tone] || TONES.neutral;
  return <span className={cx("dot", live && "dot-live")} style={{ background: t.fg }} />;
}

const ALERT_ICON = { ok: CheckCircle2, warn: AlertTriangle, bad: XCircle, accent: Info, neutral: Info };

export function Alert({ tone = "warn", title, children, action, style }) {
  const t = TONES[tone] || TONES.neutral;
  const Icon = ALERT_ICON[tone] || Info;
  return (
    <div className="alert" role={tone === "bad" ? "alert" : "status"}
      style={{ color: "var(--text-2)", background: t.bg, borderColor: t.line, ...style }}>
      <Icon size={16} strokeWidth={1.9} style={{ color: t.fg, flexShrink: 0, marginTop: 1 }} />
      <div style={{ flex: 1, minWidth: 0 }}>
        {title && <div style={{ fontWeight: 600, color: "var(--text)", marginBottom: children ? 2 : 0 }}>{title}</div>}
        {children && <div style={{ wordBreak: "break-word" }}>{children}</div>}
      </div>
      {action}
    </div>
  );
}

export function Stat({ label, value, sub, icon: Icon, tone }) {
  const color = tone ? (TONES[tone] || TONES.neutral).fg : undefined;
  return (
    <div className="card card-pad">
      <div className="stat-label">
        {Icon && <Icon size={14} strokeWidth={1.8} />}
        {label}
      </div>
      <div className="stat-value" style={color ? { color } : undefined}>{value}</div>
      {sub && <div className="stat-sub">{sub}</div>}
    </div>
  );
}

export function Empty({ icon: Icon = Info, title, children, action }) {
  return (
    <div className="empty">
      <div className="empty-icon"><Icon size={18} strokeWidth={1.8} /></div>
      {title && <div className="empty-title">{title}</div>}
      {children && <div className="empty-desc">{children}</div>}
      {action && <div style={{ marginTop: 14 }}>{action}</div>}
    </div>
  );
}

export function Segmented({ value, onChange, options }) {
  return (
    <div className="segmented" role="tablist">
      {options.map((o) => (
        <button key={o.value} role="tab" aria-selected={value === o.value}
          className={value === o.value ? "on" : ""} onClick={() => onChange(o.value)} title={o.hint}>
          {o.label}
        </button>
      ))}
    </div>
  );
}

export function Tabs({ value, onChange, options }) {
  return (
    <div className="tabs" role="tablist">
      {options.map((o) => (
        <button key={o.value} role="tab" aria-selected={value === o.value}
          className={value === o.value ? "on" : ""} onClick={() => onChange(o.value)}>
          {o.label}
        </button>
      ))}
    </div>
  );
}

export function Field({ label, hint, children }) {
  return (
    <label style={{ display: "block" }}>
      {label && <span className="field-label">{label}</span>}
      {children}
      {hint && <div className="field-hint">{hint}</div>}
    </label>
  );
}

// Key/value list (definition list) — for dense metric panels.
export function KV({ items }) {
  return (
    <dl className="kv">
      {items.filter(Boolean).map(([k, v], i) => (
        <React.Fragment key={i}>
          <dt>{k}</dt>
          <dd>{v ?? "—"}</dd>
        </React.Fragment>
      ))}
    </dl>
  );
}
