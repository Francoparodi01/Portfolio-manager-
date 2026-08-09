import {
  BarChart3,
  Bot,
  ClipboardList,
  Database,
  FileSearch,
  LayoutDashboard,
  LineChart,
  ListChecks,
  LogOut,
  RefreshCw,
  Search,
  ShieldCheck,
  WalletCards,
} from "lucide-react";
import type { ReactNode } from "react";
import { useIsFetching } from "@tanstack/react-query";
import { NavLink } from "react-router-dom";
import { useSession } from "../../app/session";
import { useHealthQuery, useRefreshAll } from "../../hooks/useMonitorData";
import { getBoolean, getRecord, getString, nestedNumber } from "../../utils/data";
import { ageLabel } from "../../utils/format";
import { StatusBadge } from "../ui/StatusBadge";

const navItems = [
  { icon: LayoutDashboard, label: "Resumen", to: "/" },
  { icon: WalletCards, label: "Cartera", to: "/cartera" },
  { icon: Search, label: "Análisis", to: "/analisis" },
  { icon: LineChart, label: "Oportunidades", to: "/oportunidades" },
  { icon: ListChecks, label: "Decisiones", to: "/decisiones" },
  { icon: BarChart3, label: "Performance", to: "/performance" },
  { icon: Bot, label: "Bot vs humano", to: "/bot-vs-humano" },
  { icon: FileSearch, label: "Auditoría", to: "/auditoria" },
  { icon: Database, label: "Datos", to: "/datos" },
] as const;

export function AppShell({ children }: { children: ReactNode }) {
  const { logout, session } = useSession();
  const health = useHealthQuery();
  const refreshAll = useRefreshAll();
  const fetching = useIsFetching({ queryKey: ["monitor"] });
  const scheduler = getRecord(health.data?.services, "scheduler");
  const dbOk = getBoolean(health.data?.database, "ok");
  const redisOk = getBoolean(health.data?.redis, "ok");
  const schedulerAlive = getBoolean(scheduler, "alive");
  const latestPortfolioAt = getString(health.data?.database, "latest_portfolio_at");
  const systemOk = Boolean(health.data?.ok && dbOk && redisOk && schedulerAlive);
  const schedulerAge = nestedNumber(health.data?.services, ["scheduler", "heartbeat_age_seconds"]);

  return (
    <div className="app-shell">
      <a className="skip-link" href="#main">Saltar al contenido</a>
      <header className="top-shell">
        <div className="brand-lockup" aria-label="Quantia">
          <span className="brand-mark">Q</span>
          <div>
            <strong>Quantia</strong>
            <span>monitor cuantitativo</span>
          </div>
        </div>
        <div className="system-strip" aria-live="polite">
          <StatusBadge tone={session?.mode === "demo" ? "theoretical" : systemOk ? "real" : "warning"}>
            {session?.mode === "demo" ? "Modo demostración" : systemOk ? "Sistema operativo" : "Revisar sistema"}
          </StatusBadge>
          <span>{latestPortfolioAt ? `Portfolio ${ageLabel(latestPortfolioAt)}` : "Portfolio sin fecha"}</span>
          <span>{schedulerAge === null ? "Scheduler sin pulso" : `Scheduler ${Math.round(schedulerAge)}s`}</span>
        </div>
        <div className="top-actions">
          <button className="icon-button" type="button" onClick={refreshAll} disabled={fetching > 0}>
            <RefreshCw size={17} aria-hidden="true" />
            <span>{fetching > 0 ? "Actualizando" : "Actualizar"}</span>
          </button>
          <button className="icon-button quiet" type="button" onClick={logout}>
            <LogOut size={17} aria-hidden="true" />
            <span>Salir</span>
          </button>
        </div>
      </header>
      <nav className="main-nav" aria-label="Navegación principal">
        {navItems.map((item) => {
          const Icon = item.icon;
          return (
            <NavLink className={({ isActive }) => (isActive ? "active" : undefined)} end={item.to === "/"} key={item.to} to={item.to}>
              <Icon size={16} aria-hidden="true" />
              <span>{item.label}</span>
            </NavLink>
          );
        })}
      </nav>
      <main id="main" className="workspace">{children}</main>
      <footer className="app-footer">
        <ShieldCheck size={15} aria-hidden="true" />
        <span>Solo lectura. La métrica principal usa ejecuciones reales validadas.</span>
        <ClipboardList size={15} aria-hidden="true" />
        <span>{session?.apiBase}</span>
      </footer>
    </div>
  );
}
