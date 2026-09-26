function copyComputedStyles(source, destination) {
  const computed = getComputedStyle(source);
  for (const property of computed) {
    destination.style.setProperty(property, computed.getPropertyValue(property), computed.getPropertyPriority(property));
  }
  const sourceChildren = source.children;
  const destinationChildren = destination.children;
  for (let index = 0; index < sourceChildren.length; index += 1) {
    if (destinationChildren[index]) copyComputedStyles(sourceChildren[index], destinationChildren[index]);
  }
}

async function imageFromSvg(svg) {
  const markup = new XMLSerializer().serializeToString(svg);
  const url = URL.createObjectURL(new Blob([markup], { type: "image/svg+xml;charset=utf-8" }));
  try {
    const image = new Image();
    image.decoding = "async";
    await new Promise((resolve, reject) => {
      image.onload = resolve;
      image.onerror = () => reject(new Error("The chart image could not be rendered."));
      image.src = url;
    });
    return image;
  } finally {
    URL.revokeObjectURL(url);
  }
}

function canvasFont(element, fallbackSize = 14) {
  const style = getComputedStyle(element);
  return `${style.fontWeight} ${style.fontSize || `${fallbackSize}px`} ${style.fontFamily}`;
}

export function wrapChartImageText(context, text, maxWidth) {
  const lines = [];
  let line = "";
  const words = text.trim().split(/\s+/u).flatMap((word) => {
    if (context.measureText(word).width <= maxWidth) return [word];
    const pieces = [];
    let piece = "";
    for (const character of word) {
      if (piece && context.measureText(piece + character).width > maxWidth) {
        pieces.push(piece); piece = "";
      }
      piece += character;
    }
    if (piece) pieces.push(piece);
    return pieces;
  });
  for (const word of words) {
    const next = line ? `${line} ${word}` : word;
    if (line && context.measureText(next).width > maxWidth) {
      lines.push(line);
      line = word;
    } else line = next;
  }
  if (line) lines.push(line);
  return lines;
}

const wrapText = wrapChartImageText;

function legendRows(context, component, maxWidth) {
  const markerWidth = 16;
  const markerGap = 8;
  const itemGap = 22;
  const entries = [...component.querySelectorAll(".chart-legend-button")].map((button) => {
    const mark = button.querySelector(".chart-legend-mark");
    const text = button.querySelector("span:last-child")?.textContent.trim();
    if (!mark || !text) return null;
    return {
      text,
      color: getComputedStyle(mark).backgroundColor,
      visible: button.getAttribute("aria-pressed") !== "false",
      line: mark.classList.contains("line"),
      width: markerWidth + markerGap + context.measureText(text).width,
    };
  }).filter(Boolean);
  const rows = [];
  let row = [];
  let width = 0;
  for (const entry of entries) {
    const next = width + (row.length ? itemGap : 0) + entry.width;
    if (row.length && next > maxWidth) {
      rows.push({ entries: row, width });
      row = [];
      width = 0;
    }
    width += (row.length ? itemGap : 0) + entry.width;
    row.push(entry);
  }
  if (row.length) rows.push({ entries: row, width });
  return rows;
}

const svgNamespace = "http://www.w3.org/2000/svg";

function svgElement(name, attributes = {}) {
  const element = document.createElementNS(svgNamespace, name);
  for (const [key, value] of Object.entries(attributes)) element.setAttribute(key, String(value));
  return element;
}

// Use the browser's line positions instead of rewrapping labels differently in
// the exported image. SVG text avoids foreignObject's canvas security limits.
function appendFunnelText(svg, element, origin) {
  if (!element) return;
  const style = getComputedStyle(element);
  const lines = [];
  const walker = document.createTreeWalker(element, NodeFilter.SHOW_TEXT);
  const range = document.createRange();
  while (walker.nextNode()) {
    const node = walker.currentNode;
    let offset = 0;
    for (const character of node.textContent) {
      range.setStart(node, offset);
      offset += character.length;
      range.setEnd(node, offset);
      const bounds = range.getBoundingClientRect();
      if (!bounds.height || !bounds.width && /\s/u.test(character)) continue;
      let line = lines.find((entry) => Math.abs(entry.top - bounds.top) < .5);
      if (!line) {
        line = { text: "", top: bounds.top, left: bounds.left, right: bounds.right };
        lines.push(line);
      }
      line.text += character;
      line.left = Math.min(line.left, bounds.left);
      line.right = Math.max(line.right, bounds.right);
    }
  }
  for (const line of lines) {
    const text = svgElement("text", {
      x: (style.direction === "rtl" ? line.right : line.left) - origin.left,
      y: line.top - origin.top,
      fill: style.color,
      "dominant-baseline": "text-before-edge",
      "font-family": style.fontFamily,
      "font-size": style.fontSize,
      "font-weight": style.fontWeight,
      "font-style": style.fontStyle,
      "letter-spacing": style.letterSpacing,
      direction: style.direction,
    });
    text.style.whiteSpace = "pre";
    text.style.fontVariantNumeric = style.fontVariantNumeric;
    text.style.fontFeatureSettings = style.fontFeatureSettings;
    text.textContent = line.text;
    svg.append(text);
  }
}

function funnelImage(component) {
  const funnel = component.querySelector(".chart-funnel");
  const canvas = funnel?.querySelector(".chart-funnel-canvas");
  const ribbon = funnel?.querySelector(".chart-funnel-ribbon");
  if (!canvas) return null;
  const bounds = canvas.getBoundingClientRect();
  const ribbonBounds = ribbon?.getBoundingClientRect();
  const width = Math.ceil(bounds.width);
  const height = Math.ceil(bounds.height);
  const svg = svgElement("svg", { xmlns: svgNamespace, width, height, viewBox: `0 0 ${width} ${height}` });
  // Compact and wide funnels paint the ribbon; the vertical layout instead
  // paints per-stage bars. Never export a CSS-hidden ribbon's zero-size path.
  if (ribbonBounds?.width > 0 && ribbonBounds.height > 0 && getComputedStyle(ribbon).visibility !== "hidden") {
    const clone = ribbon.cloneNode(true);
    copyComputedStyles(ribbon, clone);
    const viewBox = ribbon.viewBox.baseVal;
    const band = svgElement("g", {
      transform: `translate(${ribbonBounds.left - bounds.left} ${ribbonBounds.top - bounds.top})`
        + ` scale(${ribbonBounds.width / viewBox.width} ${ribbonBounds.height / viewBox.height})`
        + ` translate(${-viewBox.x} ${-viewBox.y})`,
    });
    band.append(...clone.childNodes);
    svg.append(band);
  }
  for (const stage of canvas.querySelectorAll(".chart-funnel-stages > li")) {
    const stageBounds = stage.getBoundingClientRect();
    const stageStyle = getComputedStyle(stage);
    const borderWidths = Object.fromEntries(["Left", "Right", "Top", "Bottom"].map((side) =>
      [side, Number.parseFloat(stageStyle[`border${side}Width`]) || 0]));
    for (const [side, thickness] of Object.entries(borderWidths)) {
      if (!thickness) continue;
      const vertical = side === "Left" || side === "Right";
      // Vertical borders already paint the corners. Inset horizontal borders
      // so translucent borders are not painted twice at their intersections.
      const borderWidth = vertical ? thickness : Math.max(0, stageBounds.width - borderWidths.Left - borderWidths.Right);
      if (!borderWidth || !stageBounds.height) continue;
      svg.append(svgElement("rect", {
        x: (vertical ? side === "Left" ? stageBounds.left : stageBounds.right - thickness
          : stageBounds.left + borderWidths.Left) - bounds.left,
        y: (side === "Bottom" ? stageBounds.bottom - thickness : stageBounds.top) - bounds.top,
        width: borderWidth, height: vertical ? stageBounds.height : thickness,
        fill: stageStyle[`border${side}Color`],
      }));
    }
    const pill = stage.querySelector(".chart-funnel-stage-share > span");
    if (pill) {
      const pillBounds = pill.getBoundingClientRect();
      const pillStyle = getComputedStyle(pill);
      svg.append(svgElement("rect", {
        x: pillBounds.left - bounds.left, y: pillBounds.top - bounds.top,
        width: pillBounds.width, height: pillBounds.height,
        rx: Math.min(Number.parseFloat(pillStyle.borderTopLeftRadius) || 0, pillBounds.width / 2, pillBounds.height / 2),
        fill: pillStyle.backgroundColor,
      }));
    }
    const bar = stage.querySelector(".chart-funnel-stage-bar-fill");
    if (bar) {
      const barBounds = bar.getBoundingClientRect();
      const barStyle = getComputedStyle(bar);
      // Missing/zero stages have zero-width fills. Do not invent a minimum
      // painted width, or include bars hidden by the horizontal layouts.
      if (barBounds.width > 0 && barBounds.height > 0 && barStyle.visibility !== "hidden") {
        svg.append(svgElement("rect", {
          x: barBounds.left - bounds.left, y: barBounds.top - bounds.top,
          width: barBounds.width, height: barBounds.height,
          rx: Math.min(Number.parseFloat(barStyle.borderTopLeftRadius) || 0, barBounds.width / 2, barBounds.height / 2),
          fill: barStyle.backgroundColor,
        }));
      }
    }
    for (const element of stage.querySelectorAll(".chart-funnel-stage-name, .chart-funnel-stage-value, .chart-funnel-stage-share > span")) {
      appendFunnelText(svg, element, bounds);
    }
  }
  return { svg, width, height };
}

export function canDownloadChartImage(componentId) {
  if (typeof document === "undefined" || typeof URL === "undefined" || typeof URL.createObjectURL !== "function") {
    return false;
  }
  const component = document.querySelector(`[data-component-id="${CSS.escape(componentId)}"]`);
  return Boolean(
    component?.querySelector(".recharts-wrapper svg")
      || component?.querySelector(".chart-funnel .chart-funnel-canvas"),
  );
}

async function renderChartImageBlob(componentId, { description } = {}) {
  const component = document.querySelector(`[data-component-id="${CSS.escape(componentId)}"]`);
  const funnel = component && funnelImage(component);
  const source = component?.querySelector(".recharts-wrapper svg");
  if (!source && !funnel) throw new Error("This component does not contain a chart image.");

  const bounds = funnel ?? source.getBoundingClientRect();
  const componentBounds = component.getBoundingClientRect();
  const title = component.querySelector(".component-title-text");
  const axis = component.querySelector(".chart-axis-label");
  const legend = component.querySelector(".chart-legend");
  const footer = component.querySelector(".chart-footer");
  const svg = funnel?.svg ?? source.cloneNode(true);
  svg.setAttribute("xmlns", "http://www.w3.org/2000/svg");
  svg.setAttribute("width", String(Math.ceil(bounds.width)));
  svg.setAttribute("height", String(Math.ceil(bounds.height)));
  if (!funnel) copyComputedStyles(source, svg);

  const canvas = document.createElement("canvas");
  const context = canvas.getContext("2d");
  if (!context) throw new Error("This browser cannot render chart images.");
  const padding = 20;
  const width = Math.ceil(Math.max(componentBounds.width, bounds.width) + padding * 2);
  const availableWidth = width - padding * 2;
  const titleFont = title ? canvasFont(title) : canvasFont(component);
  const secondaryFont = axis ? canvasFont(axis, 12) : `12px ${getComputedStyle(component).fontFamily}`;
  context.font = secondaryFont;
  const descriptionLines = description ? wrapText(context, description, availableWidth) : [];
  const legendFont = legend ? canvasFont(legend.querySelector(".chart-legend-button") ?? legend, 12)
    : secondaryFont;
  const footerBounds = footer?.getBoundingClientRect();
  const footerWidth = footerBounds?.width || availableWidth;
  const footerCenter = footerBounds
    ? padding + footerBounds.left - componentBounds.left + footerBounds.width / 2
    : width / 2;
  context.font = legendFont;
  const rows = legend ? legendRows(context, component, Math.min(availableWidth, footerWidth)) : [];
  const titleHeight = title ? 28 : 0;
  const descriptionHeight = descriptionLines.length ? descriptionLines.length * 18 + 8 : 0;
  const axisHeight = axis ? 24 : 0;
  const legendHeight = rows.length ? rows.length * 24 + 10 : 0;
  context.font = secondaryFont;
  const annotationNotes = [...component.querySelectorAll(".chart-annotation-note")];
  const annotationColor = annotationNotes.length ? getComputedStyle(annotationNotes[0]).color : null;
  const annotationLines = annotationNotes.flatMap((note) =>
    wrapText(context, note.textContent, availableWidth));
  const annotationHeight = annotationLines.length ? annotationLines.length * 18 + 10 : 0;
  const height = Math.ceil(padding * 2 + titleHeight + descriptionHeight + bounds.height + axisHeight + legendHeight + annotationHeight);
  const scale = Math.min(Math.max(devicePixelRatio || 1, 2), 3);
  canvas.width = Math.ceil(width * scale);
  canvas.height = Math.ceil(height * scale);
  context.scale(scale, scale);
  const cardSurface = component.querySelector(":scope > .smooth-card-surface[data-ready] > path");
  context.fillStyle = cardSurface ? getComputedStyle(cardSurface).fill : getComputedStyle(component).backgroundColor;
  if (context.fillStyle === "rgba(0, 0, 0, 0)") context.fillStyle = getComputedStyle(document.body).backgroundColor;
  context.fillRect(0, 0, width, height);
  context.textBaseline = "top";
  let top = padding;
  if (title) {
    context.font = titleFont;
    context.fillStyle = getComputedStyle(title).color;
    context.fillText(title.textContent.trim(), padding, top);
    top += titleHeight;
  }
  if (descriptionLines.length) {
    context.font = secondaryFont;
    context.fillStyle = getComputedStyle(component).getPropertyValue("--secondary").trim()
      || getComputedStyle(component).color;
    for (const line of descriptionLines) {
      context.fillText(line, padding, top);
      top += 18;
    }
    top += 8;
  }
  context.drawImage(await imageFromSvg(svg), padding, top, bounds.width, bounds.height);
  top += bounds.height;
  if (axis) {
    context.font = secondaryFont;
    context.fillStyle = getComputedStyle(axis).color;
    const label = axis.textContent.trim();
    context.fillText(label, footerCenter - context.measureText(label).width / 2, top + 5);
    top += axisHeight;
  }
  if (rows.length) {
    context.font = legendFont;
    context.textBaseline = "middle";
    top += 6;
    for (const row of rows) {
      let left = footerCenter - row.width / 2;
      const center = top + 11;
      for (const entry of row.entries) {
        context.globalAlpha = entry.visible ? 1 : 0.42;
        context.fillStyle = entry.color;
        if (entry.line) context.fillRect(left, center - 1, 16, 2);
        else {
          context.beginPath();
          context.roundRect(left + 4, center - 4, 8, 8, 2);
          context.fill();
        }
        context.fillStyle = getComputedStyle(legend).color;
        context.fillText(entry.text, left + 24, center);
        context.globalAlpha = 1;
        left += entry.width + 22;
      }
      top += 24;
    }
    context.textBaseline = "top";
  }
  if (annotationLines.length) {
    top += 10;
    context.font = secondaryFont;
    context.textBaseline = "top";
    context.fillStyle = annotationColor;
    for (const line of annotationLines) { context.fillText(line, padding, top); top += 18; }
  }
  const blob = await new Promise((resolve) => canvas.toBlob(resolve, "image/png"));
  if (!blob) throw new Error("The chart image could not be encoded.");
  return blob;
}

export async function copyChartImage(componentId, options = {}) {
  if (!navigator.clipboard?.write || typeof ClipboardItem !== "function") {
    throw new Error("Copying chart images is unavailable in this browser.");
  }
  const blob = await renderChartImageBlob(componentId, options);
  await navigator.clipboard.write([new ClipboardItem({ "image/png": blob })]);
}

function chartImageFilename(title) {
  const stem = String(title || "chart")
    .normalize("NFKD")
    .replace(/\p{Mark}+/gu, "")
    .replace(/[^a-z0-9]+/giu, "-")
    .replace(/^-+|-+$/gu, "")
    .toLowerCase()
    .slice(0, 80) || "chart";
  return `${stem}.png`;
}

export async function downloadChartImage(componentId, { description, title } = {}) {
  if (typeof URL === "undefined" || typeof URL.createObjectURL !== "function") {
    throw new Error("Downloading chart images is unavailable in this browser.");
  }
  const blob = await renderChartImageBlob(componentId, { description });
  const componentTitle = document.querySelector(
    `[data-component-id="${CSS.escape(componentId)}"] .component-title-text`,
  )?.textContent.trim();
  const link = document.createElement("a");
  const url = URL.createObjectURL(blob);
  link.href = url;
  link.download = chartImageFilename(title || componentTitle);
  link.style.display = "none";
  document.body.appendChild(link);
  try {
    link.click();
  } finally {
    link.remove();
    setTimeout(() => URL.revokeObjectURL(url), 0);
  }
  return link.download;
}
