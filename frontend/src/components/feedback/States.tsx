import { AlertTriangle, Loader2, SearchX } from "lucide-react";

export function LoadingState({ label = "Cargando datos" }: { label?: string }) {
  return (
    <div className="state-box loading-state" role="status" aria-live="polite">
      <Loader2 size={18} aria-hidden="true" />
      <span>{label}</span>
    </div>
  );
}

export function ErrorState({ message, onRetry }: { message: string; onRetry?: () => void }) {
  return (
    <div className="state-box error-state" role="alert">
      <AlertTriangle size={18} aria-hidden="true" />
      <span>{message}</span>
      {onRetry ? (
        <button type="button" onClick={onRetry}>
          Reintentar
        </button>
      ) : null}
    </div>
  );
}

export function EmptyState({ label = "Sin datos para mostrar", detail }: { label?: string; detail?: string }) {
  return (
    <div className="state-box empty-state">
      <SearchX size={18} aria-hidden="true" />
      <span>{label}</span>
      {detail ? <small>{detail}</small> : null}
    </div>
  );
}

export function SkeletonBlock({ lines = 3 }: { lines?: number }) {
  return (
    <div className="skeleton-block" aria-hidden="true">
      {Array.from({ length: lines }).map((_, index) => (
        <span key={index} />
      ))}
    </div>
  );
}
