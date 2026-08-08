import type { ReactNode } from "react";

export function Panel({
  children,
  kicker,
  title,
  action,
  className = "",
}: {
  children: ReactNode;
  kicker?: string;
  title: string;
  action?: ReactNode;
  className?: string;
}) {
  return (
    <section className={`panel ${className}`}>
      <header className="panel-header">
        <div>
          {kicker ? <p>{kicker}</p> : null}
          <h2>{title}</h2>
        </div>
        {action}
      </header>
      {children}
    </section>
  );
}
