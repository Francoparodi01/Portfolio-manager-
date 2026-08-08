import type { ReactNode } from "react";
import type { Tone } from "../../types/api";

export function StatusBadge({ children, tone = "neutral" }: { children: ReactNode; tone?: Tone }) {
  return <span className={`status-badge ${tone}`}>{children}</span>;
}
