import { useEffect, useLayoutEffect, useRef, useState } from "react";

import { editHistoryValue, recordPresentationEdit, mergePresentationChanges, normalizePresentation, writeLocalPresentation } from "./presentation-state.js";

export function usePresentationHistory(presentation, enabled, restore) {
  const value = editHistoryValue(presentation);
  const [history, setHistory] = useState(() => ({ entries: [value], index: 0 }));
  useLayoutEffect(() => {
    setHistory((current) => recordPresentationEdit(current, value, enabled));
  }, [value, enabled]);
  const step = (direction) => {
    const index = history.index + direction;
    if (!enabled || index < 0 || index >= history.entries.length) return;
    setHistory({ ...history, index });
    restore(JSON.parse(history.entries[index]));
  };
  useEffect(() => {
    if (!enabled) return undefined;
    const onKeyDown = (event) => {
      if (event.defaultPrevented || !(event.metaKey || event.ctrlKey) || event.altKey
        || event.target?.closest?.('input, textarea, select, [contenteditable="true"], [role="dialog"]')) return;
      const key = event.key.toLowerCase();
      if (key !== "z" && key !== "y") return;
      event.preventDefault();
      step(key === "y" || event.shiftKey ? 1 : -1);
    };
    window.addEventListener("keydown", onKeyDown);
    return () => window.removeEventListener("keydown", onKeyDown);
  });
  return { canUndo: enabled && history.index > 0, canRedo: enabled && history.index < history.entries.length - 1,
    undo: () => step(-1), redo: () => step(1) };
}

export function usePresentationPersistence({
  snapshot, hosted, presentation, personalPresentation = {}, initialPresentation,
  initialRevision = 0, canEdit = true, endpoint = "/api/presentation", verificationAction = null,
  onAcknowledged, onError,
}) {
  const [status, setStatus] = useState("idle");
  const sharedInitial = Object.fromEntries(Object.entries(normalizePresentation(initialPresentation))
    .filter(([field]) => Object.hasOwn(presentation, field)));
  const latest = useRef(normalizePresentation({ ...presentation, ...sharedInitial }));
  const revision = useRef(initialRevision);
  const serialized = JSON.stringify(normalizePresentation(presentation));
  const personal = JSON.stringify(normalizePresentation(personalPresentation));
  const latestPersonal = useRef(personal);
  latestPersonal.current = personal;
  const previous = useRef(serialized);
  const saveGeneration = useRef(0);
  const acknowledgedGeneration = useRef(0);

  useEffect(() => {
    writeLocalPresentation(snapshot, { ...JSON.parse(serialized), ...JSON.parse(personal) });
  }, [personal, serialized, snapshot]);

  useEffect(() => {
    if (serialized === previous.current && !verificationAction) return undefined;
    const baseline = JSON.parse(previous.current);
    if (!canEdit) {
      previous.current = serialized;
      return undefined;
    }
    const current = JSON.parse(serialized);
    if (!hosted) {
      previous.current = serialized;
      setStatus("saved");
      return undefined;
    }
    setStatus("saving");

    const save = async () => {
      const generation = ++saveGeneration.current;
      try {
        let next = mergePresentationChanges(baseline, current, latest.current);
        for (let attempt = 0; attempt < 2; attempt += 1) {
          const response = await fetch(endpoint, {
            method: "PUT", headers: { "content-type": "application/json" },
            body: JSON.stringify({
              presentation: next,
              revision: revision.current,
              ...(verificationAction ? { verificationAction } : {}),
            }),
          });
          const result = await response.json();
          if (generation < acknowledgedGeneration.current) return;
          if (response.status === 409 && attempt === 0) {
            if (result.revision < revision.current) return;
            next = mergePresentationChanges(baseline, current, result.presentation);
            revision.current = result.revision;
            continue;
          }
          if (!response.ok) throw new Error(result.error || "Unable to save Data app changes.");
          if (result.revision < revision.current) return;
          const authoritative = normalizePresentation(result.presentation ?? next);
          const acknowledged = normalizePresentation(current);
          if (authoritative.verification) acknowledged.verification = authoritative.verification;
          else delete acknowledged.verification;
          acknowledgedGeneration.current = generation;
          latest.current = authoritative;
          revision.current = result.revision;
          previous.current = JSON.stringify(acknowledged);
          writeLocalPresentation(snapshot, { ...authoritative, ...JSON.parse(latestPersonal.current) });
          setStatus("saved");
          onAcknowledged?.(authoritative, verificationAction);
          return;
        }
      } catch (error) {
        if (generation < acknowledgedGeneration.current) return;
        setStatus("error");
        onError?.(error instanceof Error ? error.message : "Unable to save Data app changes.", verificationAction);
      }
    };

    const timer = setTimeout(save, 300);
    return () => clearTimeout(timer);
  }, [canEdit, endpoint, hosted, onAcknowledged, onError, serialized, snapshot, verificationAction]);

  return status;
}
