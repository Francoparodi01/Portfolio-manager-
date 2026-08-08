import {
  createContext,
  useCallback,
  useContext,
  useMemo,
  useState,
  type FormEvent,
  type ReactNode,
} from "react";
import type { ApiSession } from "../types/api";
import { defaultApiBase, normalizeApiBase } from "../services/apiClient";
import { monitorApi } from "../services/monitorApi";

const STORAGE_KEY = "quantia:frontend:session";

type SessionContextValue = {
  session: ApiSession | null;
  apiBase: string;
  token: string;
  totp: string;
  authError: string;
  loginBusy: boolean;
  setApiBase: (value: string) => void;
  setToken: (value: string) => void;
  setTotp: (value: string) => void;
  login: (event: FormEvent<HTMLFormElement>) => Promise<void>;
  loginDemo: () => void;
  logout: () => void;
};

const SessionContext = createContext<SessionContextValue | null>(null);

function readStoredSession(): ApiSession | null {
  try {
    const raw = window.sessionStorage.getItem(STORAGE_KEY);
    if (!raw) return null;
    const parsed = JSON.parse(raw) as Partial<ApiSession>;
    if (parsed.mode === "demo") {
      return { apiBase: normalizeApiBase(parsed.apiBase || ""), mode: "demo", token: "", totp: "" };
    }
    if (parsed.mode === "api" && parsed.token && parsed.apiBase) {
      return {
        apiBase: normalizeApiBase(parsed.apiBase),
        mode: "api",
        token: parsed.token,
        totp: parsed.totp || "",
      };
    }
    return null;
  } catch {
    return null;
  }
}

function storeSession(session: ApiSession): void {
  window.sessionStorage.setItem(
    STORAGE_KEY,
    JSON.stringify({
      apiBase: normalizeApiBase(session.apiBase),
      mode: session.mode,
      token: session.token,
      totp: session.totp,
    }),
  );
}

export function SessionProvider({ children }: { children: ReactNode }) {
  const [session, setSession] = useState<ApiSession | null>(() => readStoredSession());
  const [apiBase, setApiBase] = useState(() => session?.apiBase || defaultApiBase());
  const [token, setToken] = useState(() => session?.token || "");
  const [totp, setTotp] = useState(() => session?.totp || "");
  const [authError, setAuthError] = useState("");
  const [loginBusy, setLoginBusy] = useState(false);

  const login = useCallback(
    async (event: FormEvent<HTMLFormElement>) => {
      event.preventDefault();
      const candidate: ApiSession = {
        apiBase: normalizeApiBase(apiBase),
        mode: "api",
        token: token.trim(),
        totp: totp.trim().replace(/\s+/g, ""),
      };
      if (!candidate.token) {
        setAuthError("Token requerido");
        return;
      }
      setLoginBusy(true);
      setAuthError("");
      try {
        await monitorApi.health(candidate);
        storeSession(candidate);
        setSession(candidate);
      } catch (error) {
        setAuthError(error instanceof Error ? error.message : "No se pudo validar el acceso");
      } finally {
        setLoginBusy(false);
      }
    },
    [apiBase, token, totp],
  );

  const loginDemo = useCallback(() => {
    const demoSession: ApiSession = {
      apiBase: normalizeApiBase(apiBase) || defaultApiBase(),
      mode: "demo",
      token: "",
      totp: "",
    };
    storeSession(demoSession);
    setSession(demoSession);
    setAuthError("");
  }, [apiBase]);

  const logout = useCallback(() => {
    window.sessionStorage.removeItem(STORAGE_KEY);
    setSession(null);
    setTotp("");
  }, []);

  const value = useMemo(
    () => ({
      apiBase,
      authError,
      login,
      loginBusy,
      loginDemo,
      logout,
      session,
      setApiBase,
      setToken,
      setTotp,
      token,
      totp,
    }),
    [apiBase, authError, login, loginBusy, loginDemo, logout, session, token, totp],
  );

  return <SessionContext.Provider value={value}>{children}</SessionContext.Provider>;
}

export function useSession(): SessionContextValue {
  const value = useContext(SessionContext);
  if (!value) throw new Error("useSession debe usarse dentro de SessionProvider");
  return value;
}
