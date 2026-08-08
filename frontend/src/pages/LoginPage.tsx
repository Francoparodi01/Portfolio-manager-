import { useQuery } from "@tanstack/react-query";
import { AlertTriangle, Database, KeyRound, PlayCircle } from "lucide-react";
import { useSession } from "../app/session";
import { StatusBadge } from "../components/ui/StatusBadge";
import { monitorApi } from "../services/monitorApi";
import { getBoolean } from "../utils/data";

export default function LoginPage() {
  const {
    apiBase,
    authError,
    login,
    loginBusy,
    loginDemo,
    setApiBase,
    setToken,
    setTotp,
    token,
    totp,
  } = useSession();

  const authStatus = useQuery({
    queryFn: () => monitorApi.authStatus(apiBase),
    queryKey: ["auth-status", apiBase],
    retry: 1,
  });
  const totpRequired = getBoolean(authStatus.data?.auth, "totp_required");

  return (
    <main className="login-shell">
      <section className="login-panel" aria-labelledby="login-title">
        <div className="brand-lockup login-brand">
          <span className="brand-mark">Q</span>
          <div>
            <strong id="login-title">Quantia</strong>
            <span>acceso read-only</span>
          </div>
        </div>

        <form className="login-form" onSubmit={login}>
          <label>
            <span>API monitor</span>
            <input
              autoComplete="url"
              onChange={(event) => setApiBase(event.target.value)}
              placeholder="http://localhost:8010"
              value={apiBase}
            />
          </label>
          <label>
            <span>Token</span>
            <input
              autoComplete="current-password"
              onChange={(event) => setToken(event.target.value)}
              type="password"
              value={token}
            />
          </label>
          <label>
            <span>TOTP {totpRequired ? "requerido" : "opcional"}</span>
            <input
              autoComplete="one-time-code"
              inputMode="numeric"
              onChange={(event) => setTotp(event.target.value)}
              value={totp}
            />
          </label>

          {authError || authStatus.isError ? (
            <div className="inline-alert" role="alert">
              <AlertTriangle size={17} aria-hidden="true" />
              <span>{authError || "No se pudo leer /api/auth/status"}</span>
            </div>
          ) : null}

          <button className="primary-command" disabled={loginBusy} type="submit">
            <KeyRound size={18} aria-hidden="true" />
            <span>{loginBusy ? "Validando" : "Entrar"}</span>
          </button>
          <button className="secondary-command" onClick={loginDemo} type="button">
            <PlayCircle size={18} aria-hidden="true" />
            <span>Ver datos demostrativos</span>
          </button>
        </form>
      </section>

      <aside className="login-context" aria-label="Estado de conexión">
        <StatusBadge tone={authStatus.data?.ok ? "real" : "warning"}>
          {authStatus.data?.ok ? "API detectada" : "API pendiente"}
        </StatusBadge>
        <div className="login-context-panel">
          <Database size={24} aria-hidden="true" />
          <h2>Datos separados por origen</h2>
          <p>El modo demostración está marcado en toda la interfaz y no se mezcla con métricas operativas.</p>
        </div>
      </aside>
    </main>
  );
}
