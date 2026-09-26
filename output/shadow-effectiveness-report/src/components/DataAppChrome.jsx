import * as Dropdown from "@radix-ui/react-dropdown-menu";
import React, { useEffect, useLayoutEffect, useMemo, useRef, useState, useSyncExternalStore } from "react";

import { dataAppChromeColors } from "../chrome-contrast.js";
import { dataAppChromeLayout } from "../chrome-layout.js";
import { dataAppScheduleDays, normalizeDataAppRefreshSchedule } from "../data-app-schedule.js";
import { dataAppPromptTarget } from "../runtime-environment.js";
import { reportDateMetadata } from "../report-date.js";
import { dataAppThemePalette, dataAppThemes, dataAppThemeToken } from "../theme-presets.js";
import convertGoogleDocsIcon from "./icons/convert-icon-google-docs.svg";
import convertGoogleSlidesIcon from "./icons/convert-icon-google-slides.svg";
import convertHtmlIcon from "./icons/convert-icon-html.svg";
import convertJupyterIcon from "./icons/convert-icon-jupyter.svg";
import convertPdfIcon from "./icons/convert-icon-pdf.svg";
import convertPowerpointIcon from "./icons/convert-icon-powerpoint.svg";
import convertWordIcon from "./icons/convert-icon-word.svg";
import { useDashboardAsk } from "./DashboardAsk.jsx";
import { DashboardTabs } from "./DashboardTabs.jsx";
import { Icon } from "./Icon.jsx";
import { Dialog, Menu, MenuGroup, MenuItem, MenuSeparator, Select, Tooltip } from "./ui.jsx";

const convertOptionGroups = [
  [
    { action: "pdf", label: "PDF", iconSrc: convertPdfIcon },
    { action: "word", label: "Word document", iconSrc: convertWordIcon },
    {
      action: "powerpoint",
      label: "PowerPoint",
      iconSrc: convertPowerpointIcon,
    },
  ],
  [
    {
      action: "google-docs",
      label: "Google Docs",
      iconSrc: convertGoogleDocsIcon,
    },
    {
      action: "google-slides",
      label: "Google Slides",
      iconSrc: convertGoogleSlidesIcon,
    },
  ],
  [
    {
      action: "jupyter-notebook",
      label: "Jupyter Notebook",
      iconSrc: convertJupyterIcon,
    },
    { action: "html", label: "HTML", iconSrc: convertHtmlIcon },
  ],
];

const refreshFrequencyOptions = [
  { value: "weekdays", label: "Every weekday" },
  { value: "daily", label: "Every day" },
  { value: "weekly", label: "Every week" },
  { value: "custom", label: "Custom days" },
];

const refreshTimeOptions = Array.from({ length: 96 }, (_, index) => {
  const hour = Math.floor(index / 4);
  const minute = String((index % 4) * 15).padStart(2, "0");
  return {
    value: `${String(hour).padStart(2, "0")}:${minute}`,
    label: `${hour % 12 || 12}:${minute} ${hour < 12 ? "AM" : "PM"}`,
  };
});

function formatReviewedAt(value, part = "date") {
  const date = new Date(value);
  if (Number.isNaN(date.valueOf())) return "";
  const options =
    part === "time"
      ? { hour: "numeric", minute: "2-digit" }
      : { month: "short", day: "numeric", ...(part === "compact" ? {} : { year: "numeric" }) };
  return new Intl.DateTimeFormat(undefined, options).format(date);
}

function ReviewedFreshness({ generatedAt }) {
  return (
    <>
      <span className="dashboard-freshness-full">
        Updated {formatReviewedAt(generatedAt)}
        <span className="dashboard-freshness-time">, {formatReviewedAt(generatedAt, "time")}</span>
      </span>
      <span className="dashboard-freshness-compact">{formatReviewedAt(generatedAt, "compact")}</span>
    </>
  );
}

function DataAppVerificationBadge({ verification, canEdit, onVerificationChange }) {
  if (!verification && !canEdit) return null;

  const tooltipId = "dashboard-verification-tooltip";
  const verifiedAt = verification
    ? new Intl.DateTimeFormat(undefined, {
        month: "short",
        day: "numeric",
        year: "numeric",
        hour: "numeric",
        minute: "2-digit",
      }).format(new Date(verification.verifiedAt))
    : "";
  const triggerProps = {
    className: "dashboard-verification-trigger",
    "aria-describedby": tooltipId,
  };

  return (
    <span className={`dashboard-verification${verification ? " is-verified" : ""}`}>
      {canEdit ? (
        <button
          {...triggerProps}
          type="button"
          aria-label={verification ? "Remove dashboard verification" : "Mark dashboard as verified"}
          onClick={() => onVerificationChange?.(!verification)}
        >
          <Icon name="shieldCheck" size={18} />
        </button>
      ) : (
        <span {...triggerProps} role="img" tabIndex={0} aria-label="Verified dashboard">
          <Icon name="shieldCheck" size={18} />
        </span>
      )}
      <Tooltip id={tooltipId} className="dashboard-verification-tooltip">
        {verification ? (
          <>
            <strong>Verified at:</strong>
            <span>{verifiedAt}</span>
            <strong>Verified by:</strong>
            <span>{verification.verifiedBy}</span>
          </>
        ) : (
          "Mark as verified"
        )}
      </Tooltip>
    </span>
  );
}

function DataAppFreshnessLabel({ generatedAt }) {
  return (
    <span className="freshness freshness-label">
      <ReviewedFreshness generatedAt={generatedAt} />
    </span>
  );
}

function ReportDateLabel({ asOf, generatedAt }) {
  const metadata = reportDateMetadata({ asOf, generatedAt });
  if (!metadata) return null;
  const formatted = new Intl.DateTimeFormat(undefined, {
    month: "short", day: "numeric", year: "numeric", timeZone: metadata.timeZone,
  }).format(metadata.date);
  return <span className="freshness freshness-label">
    {metadata.label} <time dateTime={metadata.value}>{formatted}</time>
  </span>;
}

function dataAppActionLink(getActionHref, action, options) {
  const href = getActionHref(action, options);
  return { href, target: dataAppPromptTarget(href), rel: "noopener noreferrer" };
}

function DataAppConvertItems({ onAction, getActionHref }) {
  return convertOptionGroups.flat().map(({ action, label, iconSrc }) => (
    <MenuItem
      key={action}
      className="dashboard-convert-menu-item"
      leading={<img className="dashboard-convert-icon" src={iconSrc} alt="" aria-hidden="true" />}
      {...(action === "pdf"
        ? { onSelect: () => onAction?.(action) }
        : dataAppActionLink(getActionHref, action))}
    >
      {label}
    </MenuItem>
  ));
}

function DataAppPublishButton({ published, getActionHref, surface }) {
  const [accessMode, setAccessMode] = useState("custom");
  const noun = surface === "report" ? "report" : "dashboard";
  const href = published ? getActionHref("sites") : getActionHref("sites", { accessMode });
  const target = dataAppPromptTarget(href);

  if (published)
    return (
      <a className="dashboard-publish-button" href={href} target={target} rel="noopener noreferrer">
        Publish changes
      </a>
    );

  return (
    <Menu
      label={`Publish ${noun}`}
      trigger={
        <button type="button" className="dashboard-publish-button">
          Publish
        </button>
      }
    >
      <span className="dashboard-publish-heading">Who can access</span>
      <Dropdown.RadioGroup value={accessMode} onValueChange={setAccessMode}>
        <Dropdown.RadioItem
          className="menu-item dashboard-publish-access"
          value="custom"
          onSelect={(event) => event.preventDefault()}
        >
          <Icon name="lock" />
          <span className="menu-item-label">Only those invited</span>
          <Dropdown.ItemIndicator className="menu-check">
            <Icon name="check" />
          </Dropdown.ItemIndicator>
        </Dropdown.RadioItem>
        <Dropdown.RadioItem
          className="menu-item dashboard-publish-access"
          value="workspace_all"
          onSelect={(event) => event.preventDefault()}
        >
          <Icon name="building" />
          <span className="menu-item-label">Anyone in this workspace with the link</span>
          <Dropdown.ItemIndicator className="menu-check">
            <Icon name="check" />
          </Dropdown.ItemIndicator>
        </Dropdown.RadioItem>
      </Dropdown.RadioGroup>
      <MenuSeparator />
      <Dropdown.Item asChild onSelect={(event) => event.preventDefault()}>
        <a
          className="dashboard-publish-confirm"
          href={href}
          target={target}
          rel="noopener noreferrer"
          onKeyDownCapture={(event) => {
            if (event.key === "Enter") event.stopPropagation();
          }}
        >
          Publish
        </a>
      </Dropdown.Item>
    </Menu>
  );
}

function DataAppRefreshControl({ generatedAt, getActionHref }) {
  const [open, setOpen] = useState(false);
  const [frequency, setFrequency] = useState("weekdays");
  const [time, setTime] = useState("09:00");
  const [days, setDays] = useState(["MO", "WE", "FR"]);
  const schedule = normalizeDataAppRefreshSchedule({
    frequency,
    time,
    ...(frequency === "custom" ? { days } : frequency === "weekly" ? { days: [days[0]] } : {}),
  });
  const scheduleLink = open && schedule ? dataAppActionLink(getActionHref, "schedule-refresh", { schedule }) : null;

  function openSchedule() {
    setFrequency("weekdays");
    setTime("09:00");
    setDays(["MO", "WE", "FR"]);
    setOpen(true);
  }

  return (
    <>
      <Menu
        label="Refresh data"
        align="start"
        contentClassName="dashboard-refresh-menu"
        trigger={
          <button
            type="button"
            className="freshness freshness-button dashboard-refresh-trigger"
            aria-label="Refresh data"
            title="Refresh data or configure a recurring schedule"
          >
            <span>
              <ReviewedFreshness generatedAt={generatedAt} />
            </span>
            <Icon name="chevronDown" size={18} className="dashboard-refresh-trigger-chevron" />
          </button>
        }
      >
        <MenuItem icon="refresh" {...dataAppActionLink(getActionHref, "refresh")}>
          Refresh now
        </MenuItem>
        <MenuItem icon="calendar" onSelect={openSchedule}>
          Schedule refresh
        </MenuItem>
      </Menu>
      {open && (
        <Dialog
          title="Schedule refresh"
          className="dashboard-schedule-dialog"
          titleInfo="Creates a new automation for this dashboard, or replaces its existing automation."
          initialFocusSelector='[aria-label="Repeat schedule"]'
          onClose={() => setOpen(false)}
        >
          <form className="dashboard-schedule-form" onSubmit={(event) => event.preventDefault()}>
            <div className="dashboard-schedule-field">
              <span>Repeat</span>
              <Select
                label="Repeat schedule"
                value={frequency}
                choices={refreshFrequencyOptions.map(({ value }) => value)}
                onChange={(value) => {
                  setFrequency(value);
                  if (value === "weekly") setDays((current) => [current[0] ?? "MO"]);
                }}
                formatChoice={(value) => refreshFrequencyOptions.find((option) => option.value === value)?.label}
                triggerClassName="dashboard-schedule-select"
                contentClassName="dashboard-schedule-select-menu"
                align="end"
                modal={false}
              />
            </div>
            {(frequency === "custom" || frequency === "weekly") && (
              <div className="dashboard-schedule-days" role="group" aria-label="Repeat on">
                {dataAppScheduleDays.map(({ value, label, short }) => (
                  <button
                    type="button"
                    key={value}
                    aria-label={label}
                    aria-pressed={days.includes(value)}
                    onClick={() =>
                      setDays((current) =>
                        frequency === "weekly"
                          ? [value]
                          : current.includes(value)
                            ? current.filter((day) => day !== value)
                            : [...current, value],
                      )
                    }
                  >
                    {short}
                  </button>
                ))}
              </div>
            )}
            <div className="dashboard-schedule-field">
              <span>Time</span>
              <Select
                label="Refresh time"
                value={time}
                choices={refreshTimeOptions.map(({ value }) => value)}
                onChange={setTime}
                formatChoice={(value) => refreshTimeOptions.find((option) => option.value === value)?.label}
                triggerClassName="dashboard-schedule-select dashboard-schedule-time"
                contentClassName="dashboard-schedule-select-menu dashboard-schedule-time-menu"
                align="end"
                modal={false}
                scrollToSelected
              />
            </div>
            {schedule && !scheduleLink?.href && (
              <p>Open this app's HTML file directly or use its published URL to schedule refresh.</p>
            )}
            <footer className="dashboard-schedule-actions">
              {scheduleLink?.href ? (
                <a
                  className="dashboard-schedule-submit"
                  role="button"
                  {...scheduleLink}
                >
                  Schedule refresh
                </a>
              ) : (
                <button type="button" className="dashboard-schedule-submit" disabled>
                  Schedule refresh
                </button>
              )}
            </footer>
          </form>
        </Dialog>
      )}
    </>
  );
}

const HeaderActionButton = React.forwardRef(function HeaderActionButton(
  { label, compactLabel, className = "", ...props },
  ref,
) {
  return (
    <button
      {...props}
      ref={ref}
      type="button"
      aria-label={label}
      className={["dashboard-header-action-button", className].filter(Boolean).join(" ")}
    >
      <span className="dashboard-header-action-label dashboard-header-action-label-full">{label}</span>
      {compactLabel && (
        <span className="dashboard-header-action-label dashboard-header-action-label-compact">{compactLabel}</span>
      )}
    </button>
  );
});

const HeaderOverflowButton = React.forwardRef(function HeaderOverflowButton(props, ref) {
  return (
    <button
      {...props}
      ref={ref}
      type="button"
      aria-label="More"
      className="dashboard-header-action-button dashboard-header-overflow-button"
    >
      <Icon name="more" size={18} />
    </button>
  );
});

function DataAppOverflowMenu({
  canEdit,
  compactAskMenu,
  getActionHref,
  hiddenCount,
  mode,
  onAction,
  onOpenAsk,
  onOpenThemes,
  onRestoreHidden,
  published,
}) {
  const triggerRef = useRef(null);
  const hasUtilityActions = published || canEdit;
  return (
    <Menu
      label="More"
      contentClassName="dashboard-header-action-menu"
      trigger={<HeaderOverflowButton ref={triggerRef} />}
    >
      {compactAskMenu && (
        <>
          <MenuItem
            icon="chatgpt"
            className="dashboard-action-menu-item"
            onSelect={() => window.setTimeout(() => onOpenAsk?.(triggerRef.current), 0)}
          >
            Ask ChatGPT
          </MenuItem>
          <MenuSeparator />
        </>
      )}
      {published && (
        <MenuItem
          icon="link"
          className="dashboard-action-menu-item"
          onSelect={() => onAction?.("copy-link")}
        >
          Copy link
        </MenuItem>
      )}
      {canEdit && (
        <MenuItem icon="palette" className="dashboard-action-menu-item" onSelect={onOpenThemes}>
          Edit theme
        </MenuItem>
      )}
      {canEdit && mode === "edit" && hiddenCount > 0 && (
        <MenuItem icon="eye" className="dashboard-action-menu-item" onSelect={onRestoreHidden}>
          Restore hidden ({hiddenCount})
        </MenuItem>
      )}
      {hasUtilityActions && <MenuSeparator />}
      <MenuGroup label="Export">
        <DataAppConvertItems onAction={onAction} getActionHref={getActionHref} />
      </MenuGroup>
    </Menu>
  );
}

const promptActionDetails = {
  "share-summary": {
    label: "Draft a team update",
    subtext: "Share the highlights in a message",
    icon: "summaryBubble",
  },
  "refresh-document": {
    label: "Refresh a document",
    subtext: "Update a file with fresh data",
    icon: "documentRefresh",
  },
  "alert-changes": {
    label: "Set up an alert",
    subtext: "Notify you of important changes",
    icon: "alertBell",
  },
};

function dataAppAskSuggestions({
  canEdit,
  hiddenCount,
  mode,
  onAction,
  getActionHref,
  onRestoreHidden,
  published,
  surface,
}) {
  const suggestions = Object.entries(promptActionDetails).map(([action, details]) => ({
    action,
    ...details,
    ...dataAppActionLink(getActionHref, action),
  })).filter(({ href }) => href);
  if (published) {
    suggestions.push({
      action: "copy-link",
      icon: "link",
      label: "Copy link",
      onSelect: () => onAction?.("copy-link"),
    });
  }
  if (surface !== "report") {
    suggestions.push({
      action: "duplicate",
      icon: "remix",
      label: "Remix this dashboard",
      ...dataAppActionLink(getActionHref, "duplicate"),
    });
  }
  if (canEdit && mode === "edit" && hiddenCount > 0) {
    suggestions.push({
      action: "restore-hidden",
      icon: "eye",
      label: `Restore hidden (${hiddenCount})`,
      onSelect: onRestoreHidden,
    });
  }
  return suggestions;
}

function useCompactAskMenu() {
  const query = "(max-width: 599px)";
  const [compact, setCompact] = useState(() => globalThis.matchMedia?.(query)?.matches ?? false);
  useEffect(() => {
    const media = globalThis.matchMedia?.(query);
    if (!media) return undefined;
    const update = () => setCompact(media.matches);
    update();
    media.addEventListener("change", update);
    return () => media.removeEventListener("change", update);
  }, []);
  return compact;
}

export function DataAppTopbar({
  title,
  generatedAt,
  reportAsOf,
  mode,
  onModeChange,
  editHistory,
  onTitleChange,
  onAction,
  getActionHref,
  onOpenThemes,
  surface = "dashboard",
  published = false,
  canEdit = true,
  verification,
  onVerificationChange,
  saveStatus = "idle",
  pendingCodeChanges = false,
  className = "",
  tabs = [],
  activeTabId,
  onTabChange,
  onReorderTabs,
  hiddenCount = 0,
  onRestoreHidden,
}) {
  const noun = surface === "report" ? "report" : "dashboard";
  const label = noun[0].toUpperCase() + noun.slice(1);
  const showTabs = surface !== "report" && tabs.length > 1;
  const [tabsShownForScroll, setTabsShownForScroll] = useState(true);
  const [chromeColors, setChromeColors] = useState({});
  const [chromeLayout, setChromeLayout] = useState({});
  const topbar = useRef(null);
  const tabsShownForScrollRef = useRef(true);
  const tabsScrollLockUntil = useRef(0);
  const askButtonRef = useRef(null);
  const compactAskMenu = useCompactAskMenu();
  const { openDashboardComposer } = useDashboardAsk();
  const askSuggestions = dataAppAskSuggestions({
    canEdit,
    hiddenCount,
    mode,
    onAction,
    getActionHref,
    onRestoreHidden,
    published,
    surface,
  });
  function openAskComposer(anchor = askButtonRef.current) {
    openDashboardComposer(anchor, askSuggestions);
  }
  useEffect(() => {
    const header = topbar.current;
    if (!header) return undefined;
    header.setAttribute("data-tabs-motion-ready", "");
    return () => header.removeAttribute("data-tabs-motion-ready");
  }, []);
  useLayoutEffect(() => {
    if (!topbar.current || typeof getComputedStyle !== "function") return undefined;
    function updateChromeColors() {
      const header = topbar.current;
      if (!header) return;
      const rootStyles = getComputedStyle(document.documentElement);
      const headerStyles = getComputedStyle(header);
      const next = dataAppChromeColors({
        background: headerStyles.backgroundColor,
        underlay: getComputedStyle(document.body).backgroundColor,
        foreground: headerStyles.color,
        secondary: rootStyles.getPropertyValue("--secondary").trim(),
        positive: rootStyles.getPropertyValue("--positive").trim(),
      });
      setChromeColors((current) => (Object.keys(next).every((key) => current[key] === next[key]) ? current : next));
    }
    updateChromeColors();
    const observer = new MutationObserver(updateChromeColors);
    observer.observe(document.documentElement, {
      attributes: true,
      attributeFilter: ["style", "data-app-theme", "data-color-scheme", "data-app-appearance"],
    });
    return () => observer.disconnect();
  }, []);
  useLayoutEffect(() => {
    const main = document.querySelector(`main[data-data-app-content="${surface}"]`);
    if (!main || typeof getComputedStyle !== "function" || typeof ResizeObserver !== "function") return undefined;
    let frame = 0;
    const sizes = new ResizeObserver(scheduleLayout);

    function scheduleLayout() {
      if (!frame) frame = requestAnimationFrame(updateChromeLayout);
    }

    function updateChromeLayout() {
      frame = 0;
      const styles = getComputedStyle(main);
      const next = dataAppChromeLayout({
        viewportWidth: window.innerWidth,
        paddingLeft: Number.parseFloat(styles.paddingLeft),
        paddingRight: Number.parseFloat(styles.paddingRight),
      });
      setChromeLayout((current) => (Object.keys(next).every((key) => current[key] === next[key]) ? current : next));
    }

    sizes.observe(main);
    window.addEventListener("resize", scheduleLayout);
    updateChromeLayout();
    return () => {
      cancelAnimationFrame(frame);
      sizes.disconnect();
      window.removeEventListener("resize", scheduleLayout);
    };
  }, [surface]);
  useEffect(() => {
    if (!showTabs) {
      tabsShownForScrollRef.current = true;
      setTabsShownForScroll(true);
      return undefined;
    }
    let previousY = window.scrollY;
    function updateTabsShown(next) {
      if (tabsShownForScrollRef.current === next) return;
      tabsShownForScrollRef.current = next;
      tabsScrollLockUntil.current = performance.now() + 220;
      setTabsShownForScroll(next);
    }
    function handleScroll() {
      const currentY = window.scrollY;
      const distance = currentY - previousY;
      const isSmallLayoutShift = performance.now() < tabsScrollLockUntil.current && Math.abs(distance) <= 48;
      if (currentY <= 12) updateTabsShown(true);
      else if (!isSmallLayoutShift && distance > 4) updateTabsShown(false);
      else if (!isSmallLayoutShift && distance < -4) updateTabsShown(true);
      previousY = currentY;
    }
    window.addEventListener("scroll", handleScroll, { passive: true });
    return () => window.removeEventListener("scroll", handleScroll);
  }, [showTabs]);
  const tabsExpanded = showTabs && tabsShownForScroll;
  return (
    <header
      ref={topbar}
      style={{ ...chromeColors, ...chromeLayout }}
      className={["dashboard-topbar", className].filter(Boolean).join(" ")}
      data-data-app-chrome="topbar"
      data-mode={mode}
    >
      <div className="dashboard-chrome-inner dashboard-topbar-inner">
        <div className="dashboard-topbar-copy">
          {published && surface !== "report" && (verification || (canEdit && onVerificationChange)) && (
            <DataAppVerificationBadge
              verification={verification}
              canEdit={canEdit && Boolean(onVerificationChange)}
              onVerificationChange={onVerificationChange}
            />
          )}
          <strong
            className="dashboard-topbar-title"
            contentEditable={mode === "edit"}
            suppressContentEditableWarning
            aria-label={mode === "edit" ? `Edit ${noun} title` : undefined}
            onDoubleClick={
              canEdit
                ? (event) => {
                    const target = event.currentTarget;
                    if (mode !== "edit") onModeChange?.("edit");
                    requestAnimationFrame(() =>
                      requestAnimationFrame(() => {
                        if (target.isConnected && target.isContentEditable) target.focus();
                      }),
                    );
                  }
                : undefined
            }
            onBlur={
              mode === "edit"
                ? (event) => {
                    const next = event.currentTarget.textContent.trim();
                    if (next) onTitleChange?.(next);
                    else event.currentTarget.textContent = title;
                  }
                : undefined
            }
            onKeyDown={
              mode === "edit"
                ? (event) => {
                    if (event.key === "Enter") {
                      event.preventDefault();
                      event.currentTarget.blur();
                    }
                  }
                : undefined
            }
          >
            {title}
          </strong>
          {generatedAt && canEdit && surface !== "report" && (
            <DataAppRefreshControl generatedAt={generatedAt} getActionHref={getActionHref} />
          )}
          {surface === "report" && <ReportDateLabel asOf={reportAsOf} generatedAt={generatedAt} />}
          {generatedAt && !canEdit && surface !== "report" && (
            <span className="freshness freshness-label">
              <ReviewedFreshness generatedAt={generatedAt} />
            </span>
          )}
        </div>

        <div className="dashboard-topbar-actions">
          {canEdit && mode === "edit" && editHistory && (
            <div className="dashboard-edit-history" role="group" aria-label={`${label} edit history`}>
              <span className="history-tooltip-trigger">
                <button type="button" className="icon-button" aria-label={`Undo ${noun} change`}
                  disabled={!editHistory.canUndo} onClick={editHistory.undo}>
                  <Icon name="undo" size={18} />
                </button>
                <Tooltip className="topbar-mode-tooltip">Undo</Tooltip>
              </span>
              <span className="history-tooltip-trigger">
                <button type="button" className="icon-button" aria-label={`Redo ${noun} change`}
                  disabled={!editHistory.canRedo} onClick={editHistory.redo}>
                  <Icon name="undo" size={18} className="chart-editor-redo-icon" />
                </button>
                <Tooltip className="topbar-mode-tooltip">Redo</Tooltip>
              </span>
            </div>
          )}
          {canEdit && mode === "edit" && saveStatus !== "idle" && (
            <span
              className={`dashboard-save-status${saveStatus === "error" ? " has-error" : ""}`}
              data-status={saveStatus}
              role="status"
              aria-live="polite"
            >
              {saveStatus === "saving" ? "Saving…" : saveStatus === "error" ? "Couldn’t save" : "Saved"}
            </span>
          )}
          {canEdit && (
            <div className="topbar-mode-switcher" aria-label={`${label} mode`} role="group">
              <span
                className="topbar-mode-indicator"
                aria-hidden="true"
                style={{
                  transform: `translateX(${mode === "edit" ? 40 : 0}px)`,
                }}
              />
              <button
                type="button"
                aria-label="View mode"
                aria-pressed={mode === "view"}
                onClick={() => onModeChange?.("view")}
              >
                <Icon name="eye" size={18} />
                <Tooltip className="topbar-mode-tooltip">View</Tooltip>
              </button>
              <button
                type="button"
                aria-label="Edit mode"
                aria-pressed={mode === "edit"}
                onClick={() => onModeChange?.("edit")}
              >
                <Icon name="edit" size={18} />
                <Tooltip className="topbar-mode-tooltip">Edit</Tooltip>
              </button>
            </div>
          )}
          {!compactAskMenu && (
            <HeaderActionButton
              ref={askButtonRef}
              label="Ask ChatGPT"
              compactLabel="Ask"
              className="dashboard-ask-button"
              onClick={() => openAskComposer()}
            />
          )}
          {canEdit && (!published || pendingCodeChanges) && (
            <DataAppPublishButton published={published} getActionHref={getActionHref} surface={surface} />
          )}
          <DataAppOverflowMenu
            canEdit={canEdit}
            compactAskMenu={compactAskMenu}
            getActionHref={getActionHref}
            hiddenCount={hiddenCount}
            mode={mode}
            onAction={onAction}
            onOpenAsk={openAskComposer}
            onOpenThemes={onOpenThemes}
            onRestoreHidden={onRestoreHidden}
            published={published}
          />
        </div>
      </div>
      <div
        className={`dashboard-tabs-collapse${tabsExpanded ? " is-open" : ""}`}
        aria-hidden={!tabsExpanded}
        inert={!tabsExpanded}
      >
        <div className="dashboard-tabs-collapse-inner">
          <DashboardTabs
            tabs={tabs}
            activeTabId={activeTabId}
            editMode={mode === "edit"}
            onChange={onTabChange}
            onReorder={onReorderTabs}
          />
        </div>
      </div>
    </header>
  );
}

function normalizedTokens(values) {
  if (typeof document === "undefined" || typeof getComputedStyle !== "function" || !document.body) {
    return values.map((value) => String(value));
  }
  const probe = document.createElement("i");
  probe.style.cssText = "position:absolute;visibility:hidden;pointer-events:none";
  document.body.append(probe);
  try {
    return values.map((value, index) => {
      if (index <= 20) {
        probe.style.color = value;
        return getComputedStyle(probe).color;
      }
      if (index <= 23) return String(Number.parseFloat(value));
      if (index <= 25) {
        probe.style.boxShadow = value;
        return getComputedStyle(probe).boxShadow;
      }
      probe.style.fontFamily = value;
      return getComputedStyle(probe).fontFamily;
    });
  } finally {
    probe.remove();
  }
}

function matchingPreset(originalTokens) {
  const original = normalizedTokens(originalTokens);
  const scheme = globalThis.document?.documentElement?.dataset.colorScheme ?? "light";
  return dataAppThemes.find((theme) => {
    const tokens = dataAppThemePalette(theme, scheme);
    return tokens && normalizedTokens(tokens).every((value, index) => value === original[index]);
  });
}

function fontLabel(font) {
  if (/mono|courier|menlo/iu.test(font)) return "Monospace";
  if (/rounded/iu.test(font)) return "Rounded sans";
  if (/georgia|times/iu.test(font)) return "Serif";
  return "Sans serif";
}

function subscribeColorScheme(onChange) {
  const root = globalThis.document?.documentElement;
  if (!root || typeof MutationObserver === "undefined") return () => {};
  const observer = new MutationObserver(onChange);
  observer.observe(root, {
    attributes: true,
    attributeFilter: ["data-color-scheme"],
  });
  return () => observer.disconnect();
}

function currentColorScheme() {
  return globalThis.document?.documentElement?.dataset.colorScheme ?? "light";
}

function ThemePreview({ theme, originalTokens, colorScheme }) {
  const tokens = dataAppThemePalette(theme, colorScheme) ?? originalTokens;
  const background = dataAppThemeToken(tokens, "background", "var(--background)");
  const surface = dataAppThemeToken(tokens, "surface", "var(--surface)");
  const text = dataAppThemeToken(tokens, "text", "var(--text)");
  const accent = dataAppThemeToken(tokens, "accent", "var(--accent)");
  const secondary = dataAppThemeToken(tokens, "secondary", "var(--secondary)");
  const border = dataAppThemeToken(tokens, "border", "var(--border)");
  const control = dataAppThemeToken(tokens, "control", "var(--control)");
  const controlRadius = Number.parseFloat(dataAppThemeToken(tokens, "control-radius", "8px"));
  const radius = controlRadius >= 16 ? "999px" : controlRadius > 0 ? "5px" : "0px";
  const font = dataAppThemeToken(tokens, "font-sans", "ui-sans-serif, system-ui, sans-serif");
  const swatches = Array.from({ length: 5 }, (_, index) => dataAppThemeToken(tokens, `chart-${index + 1}`, accent));

  return (
    <span
      className="theme-preview"
      aria-hidden="true"
      style={{
        "--preview-background": background,
        "--preview-surface": surface,
        "--preview-text": text,
        "--preview-secondary": secondary,
        "--preview-accent": accent,
        "--preview-border": border,
        "--preview-control": control,
        "--preview-radius": radius,
        "--preview-font": font,
      }}
    >
      <span className="theme-preview-type">
        <strong>Aa</strong>
        <span>{fontLabel(font)}</span>
      </span>
      <span className="theme-preview-swatches">
        {swatches.map((swatch, index) => (
          <i key={`${swatch}:${index}`} style={{ background: swatch }} />
        ))}
      </span>
      <span className="theme-preview-geometry">
        <i />
      </span>
    </span>
  );
}

export function DataAppThemeDrawer({
  activeTheme,
  open,
  onApply,
  onClose,
  onPreview,
  onPreviewEnd,
  originalTokens,
  appearance = "system",
  onAppearanceChange,
}) {
  const list = useRef(null);
  const colorScheme = useSyncExternalStore(subscribeColorScheme, currentColorScheme, currentColorScheme);
  const equivalentPreset = useMemo(() => matchingPreset(originalTokens), [originalTokens]);
  const themes = equivalentPreset ? dataAppThemes.filter(({ id }) => id !== "original") : dataAppThemes;
  const selected = activeTheme === "original" && equivalentPreset ? equivalentPreset.id : activeTheme;

  useEffect(() => {
    if (open && list.current) list.current.scrollLeft = 0;
  }, [open]);

  return (
    <section
      className={`theme-drawer${open ? " is-open" : ""}`}
      aria-hidden={!open}
      inert={!open}
      aria-label="Theme picker"
    >
      <div className="dashboard-chrome-inner theme-drawer-content">
        <header className="theme-drawer-header">
          <h2>Theme</h2>
          <div className="theme-drawer-actions">
            {onAppearanceChange && (
              <Menu
                label="Dashboard appearance"
                align="end"
                trigger={
                  <button
                    type="button"
                    className="theme-appearance-trigger"
                    aria-label="Appearance"
                    tabIndex={open ? 0 : -1}
                  >
                    <Icon name={appearance === "light" ? "sun" : appearance === "dark" ? "moon" : "monitor"} />
                    <span>{appearance[0].toUpperCase() + appearance.slice(1)}</span>
                    <Icon name="chevronDown" />
                  </button>
                }
              >
                <Dropdown.RadioGroup value={appearance} onValueChange={onAppearanceChange}>
                  {[
                    { value: "light", icon: "sun", label: "Light" },
                    { value: "dark", icon: "moon", label: "Dark" },
                    { value: "system", icon: "monitor", label: "System" },
                  ].map(({ value, icon, label }) => (
                    <Dropdown.RadioItem key={value} className="menu-item" value={value}>
                      <Icon name={icon} />
                      <span className="menu-item-label">{label}</span>
                      <Dropdown.ItemIndicator className="menu-check">
                        <Icon name="check" />
                      </Dropdown.ItemIndicator>
                    </Dropdown.RadioItem>
                  ))}
                </Dropdown.RadioGroup>
              </Menu>
            )}
            <button
              type="button"
              className="icon-button theme-drawer-close"
              aria-label="Close theme picker"
              tabIndex={open ? 0 : -1}
              onClick={onClose}
            >
              <Icon name="cross" size={20} />
            </button>
          </div>
        </header>
      </div>
      <div ref={list} className="theme-card-list">
        {themes.map((theme) => (
          <button
            type="button"
            key={theme.id}
            className={`theme-card${selected === theme.id ? " is-active" : ""}`}
            aria-pressed={selected === theme.id}
            aria-label={`Apply ${theme.label}`}
            tabIndex={open ? 0 : -1}
            onClick={() => onApply(theme.id)}
            onPointerMove={() => {
              if (document.documentElement.dataset.appTheme !== theme.id) onPreview?.(theme.id);
            }}
            onPointerLeave={onPreviewEnd}
            onFocus={() => onPreview?.(theme.id)}
            onBlur={onPreviewEnd}
          >
            <ThemePreview theme={theme} originalTokens={originalTokens} colorScheme={colorScheme} />
            <span className="theme-card-label">{theme.label}</span>
          </button>
        ))}
      </div>
    </section>
  );
}
