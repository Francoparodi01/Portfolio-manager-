import { lazy, Suspense } from "react";
import { Navigate, Route, Routes } from "react-router-dom";
import { useSession } from "./session";
import { AppShell } from "../components/layout/AppShell";
import { LoadingState } from "../components/feedback/States";
import LoginPage from "../pages/LoginPage";

const OverviewPage = lazy(() => import("../pages/OverviewPage"));
const PortfolioPage = lazy(() => import("../pages/PortfolioPage"));
const AnalysisPage = lazy(() => import("../pages/AnalysisPage"));
const OpportunitiesPage = lazy(() => import("../pages/OpportunitiesPage"));
const DecisionsPage = lazy(() => import("../pages/DecisionsPage"));
const PerformancePage = lazy(() => import("../pages/PerformancePage"));
const HumanBenchmarkPage = lazy(() => import("../pages/HumanBenchmarkPage"));
const AuditPage = lazy(() => import("../pages/AuditPage"));
const DataPage = lazy(() => import("../pages/DataPage"));

export default function App() {
  const { session } = useSession();
  if (!session) return <LoginPage />;

  return (
    <AppShell>
      <Suspense fallback={<LoadingState label="Cargando ruta" />}>
        <Routes>
          <Route element={<OverviewPage />} path="/" />
          <Route element={<PortfolioPage />} path="/cartera" />
          <Route element={<AnalysisPage />} path="/analisis" />
          <Route element={<OpportunitiesPage />} path="/oportunidades" />
          <Route element={<DecisionsPage />} path="/decisiones" />
          <Route element={<PerformancePage />} path="/performance" />
          <Route element={<HumanBenchmarkPage />} path="/bot-vs-humano" />
          <Route element={<AuditPage />} path="/auditoria" />
          <Route element={<DataPage />} path="/datos" />
          <Route element={<Navigate replace to="/" />} path="*" />
        </Routes>
      </Suspense>
    </AppShell>
  );
}
