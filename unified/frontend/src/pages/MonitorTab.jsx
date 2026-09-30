import React from "react";
import { Activity } from "lucide-react";
import { useAppContext } from "../AppContext.jsx";
import { PageHeader, Tabs } from "../ui/components.jsx";
import LiveDashboard   from "./LiveDashboard.jsx";
import LogsPage        from "./LogsPage.jsx";
import AnomaliesPage   from "./AnomaliesPage.jsx";
import PredictionsPage from "./PredictionsPage.jsx";

const TABS = [
  { value: "live",        label: "Live" },
  { value: "logs",        label: "Run logs" },
  { value: "anomalies",   label: "Anomalies" },
  { value: "predictions", label: "Runtime predictions" },
];

export default function MonitorTab() {
  const { monitorTab: active, setMonitorTab: setActive } = useAppContext();

  return (
    <div>
      <PageHeader
        eyebrow="Observe" icon={Activity}
        title="Monitor"
        description="Polls ADF every 20 seconds, writes an AI analysis for every finished run, and flags anomalies."
      />
      <Tabs value={active} onChange={setActive} options={TABS} />
      {active === "live"        && <LiveDashboard />}
      {active === "logs"        && <LogsPage />}
      {active === "anomalies"   && <AnomaliesPage />}
      {active === "predictions" && <PredictionsPage />}
    </div>
  );
}
