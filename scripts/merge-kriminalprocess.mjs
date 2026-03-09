import fs from "node:fs/promises";
import path from "node:path";
import process from "node:process";
import { createHash } from "node:crypto";
import { spawn } from "node:child_process";
import { fileURLToPath } from "node:url";
import * as cheerio from "cheerio";
import ExcelJS from "exceljs";

const __filename = fileURLToPath(import.meta.url);
const __dirname = path.dirname(__filename);
const PROJECT_ROOT = path.resolve(__dirname, "..");
const DATA_DIR = path.join(PROJECT_ROOT, "data");
const CACHE_DIR = path.join(PROJECT_ROOT, "cache");
const LAW_CACHE_DIR = path.join(CACHE_DIR, "laws");
const LAW_SOURCES_FILE = path.join(DATA_DIR, "law-sources.json");
const SUMMARY_CACHE_FILE = path.join(CACHE_DIR, "law-summaries.json");
const MLX_SUMMARIZER = path.join(__dirname, "mlx_summarize.py");
const MLX_MODEL_ID = "mlx-community/Qwen3.5-0.8B-OptiQ-4bit";
const MAX_EXCEL_CELL_LENGTH = 32000;

const KRIMINALPROCESS_GROUP = "Kriminālprocess";
const SHARED_DETAIL_FIELDS = [
  "Procesa veids",
  "Tiesas sēdes laiks",
  "Lietas numurs",
  "Arhīva numurs",
  "Tiesas sēdes veids",
  "Tiesnesis",
];
const PRIMARY_PARTY_FIELDS = [
  "Apsūdzētais",
  "Prasītājs",
  "Pieteicējs",
  "Persona, kuru sauc pie administratīvās atbildības",
];
const SECONDARY_PARTY_FIELDS = ["Atbildētājs"];
const SUBJECT_FIELDS = ["Būtība", "Prasījums"];
const CRIMINAL_CITATION_FIELD = "Apsūdzības panti";
const EXCLUDED_SUMMARY_GROUPS = new Set(["Tiesa"]);

const EXCEL_COLUMNS = [
  { header: "Procesa grupa", key: "processGroup", width: 24 },
  { header: "City", key: "city", width: 14 },
  { header: "Sēdes datums", key: "date", width: 14 },
  { header: "Sēdes laiks", key: "time", width: 9 },
  { header: "Sēdes veids", key: "tiesasSedesVeids", width: 14 },
  { header: "Procesa veids", key: "procesaVeids", width: 14 },
  { header: "Court", key: "courtName", width: 28 },
  {
    header: "Pirmā puse / pieteicējs / prasītājs / apsūdzētais",
    key: "primaryParty",
    width: 28,
  },
  { header: "Otrā puse / atbildētājs", key: "secondaryParty", width: 28 },
  { header: "Lietas būtība / prasījums", key: "caseSubject", width: 36 },
  { header: "Lietas rakstura kopsavilkums", key: "caseNatureSummary", width: 38 },
  { header: "Lietas numurs", key: "lietasNumurs", width: 16 },
  { header: "Arhīva numurs", key: "arhivaNumurs", width: 16 },
  {
    header: "Apsūdzības panti (deciphered)",
    key: "apsudzibasPantiDeciphered",
    width: 24,
  },
  { header: "Citēto normu teksts", key: "citedNormText", width: 30 },
  { header: "Instance", key: "instanceLabel", width: 16 },
  { header: "Tiesnesis", key: "tiesnesis", width: 18 },
  { header: "Tiesas sēdes laiks", key: "tiesasSedesLaiks", width: 18 },
  { header: "Seriousness rank (1-5)", key: "seriousnessRank", width: 16 },
  { header: "Source", key: "detailUrl", width: 22 },
];

const SERIOUSNESS_KEYWORDS = [
  { pattern: /slepkav|nogalin|teror|sprādzien|uzbruk/, weight: 9 },
  { pattern: /ielas|saimniec|naudas|krāpš|sagroz/, weight: 6 },
  { pattern: /zādz|sveš|laupa|nark|tabak|alkoh|ieroču|šaujam/, weight: 4 },
  { pattern: /krāpt|viltus|krāp|krimin|apzag|haker/, weight: 3 },
];

const SERIOUSNESS_MAX_RANK = 5;
const SERIOUSNESS_MIN_RANK = 1;

function estimateSeriousnessScore(entry) {
  const citations = collectUniqueCitations(
    decipherLawText(entry.fields["Apsūdzības panti"]),
  );
  let score = 0;

  for (const citation of citations) {
    score += 3;
    const parsedCitation = parseCitationLine(citation);
    if (!parsedCitation) {
      continue;
    }
    if (parsedCitation.part) {
      score += 2;
    }
    if (parsedCitation.point) {
      score += 1;
    }
  }

  const normalizedSummary = normalizeText(entry.caseNatureSummary).toLowerCase();
  for (const { pattern, weight } of SERIOUSNESS_KEYWORDS) {
    if (pattern.test(normalizedSummary)) {
      score += weight;
    }
  }

  if (score === 0) {
    return 1;
  }
  return score;
}

function assignSeriousnessRanks(entries) {
  const ranked = entries
    .map((entry, index) => ({ entry, index }))
    .filter(({ entry }) => isCriminalEntry(entry))
    .map(({ entry, index }) => ({
      entry,
      score: estimateSeriousnessScore(entry),
      index,
    }));

  for (const entry of entries) {
    if (!isCriminalEntry(entry)) {
      entry.seriousnessRank = "";
    }
  }

  ranked.sort((a, b) => {
    if (b.score !== a.score) {
      return b.score - a.score;
    }
    return a.index - b.index;
  });

  const count = ranked.length;
  for (let rankIndex = 0; rankIndex < count; rankIndex += 1) {
    const computedRank =
      count <= 1
        ? SERIOUSNESS_MAX_RANK
        : Math.max(
            SERIOUSNESS_MIN_RANK,
            SERIOUSNESS_MAX_RANK -
              Math.floor((rankIndex * (SERIOUSNESS_MAX_RANK - 1)) / (count - 1)),
          );

    ranked[rankIndex].entry.seriousnessRank = String(computedRank);
  }

  return entries;
}

function usage() {
  console.error(
    "Usage: node scripts/merge-kriminalprocess.mjs <monthly-index-url> [output-path]",
  );
}

function normalizeText(value) {
  return String(value ?? "")
    .replace(/\u00a0/g, " ")
    .replace(/\s+/g, " ")
    .trim();
}

function normalizeMultilineText(value) {
  return String(value ?? "")
    .replace(/\u00a0/g, " ")
    .replace(/\r\n/g, "\n")
    .replace(/[ \t]+\n/g, "\n")
    .replace(/\n{3,}/g, "\n\n")
    .trim();
}

function makeSafeFilePart(value) {
  return value
    .normalize("NFKD")
    .replace(/[^\w.-]+/g, "-")
    .replace(/-+/g, "-")
    .replace(/^-|-$/g, "")
    .toLowerCase();
}

function defaultOutputPath(indexUrl) {
  const url = new URL(indexUrl);
  const lastSegment = url.pathname.split("/").filter(Boolean).at(-1) ?? "output";
  const withoutExtension = lastSegment.replace(/\.[^.]+$/, "");
  return `court-calendar-${makeSafeFilePart(withoutExtension)}.xlsx`;
}

function canonicalizeNumericId(value) {
  return String(value ?? "")
    .replace(/\s+/g, "")
    .replace(/\.\-(?=\d)/g, "-")
    .replace(/[._]/g, "-")
    .replace(/--+/g, "-")
    .replace(/^-|-$/g, "");
}

function escapeRegex(value) {
  return value.replace(/[.*+?^${}()|[\]\\]/g, "\\$&");
}

function excelColumnLetter(index) {
  let current = index;
  let result = "";
  while (current > 0) {
    const remainder = (current - 1) % 26;
    result = String.fromCharCode(65 + remainder) + result;
    current = Math.floor((current - 1) / 26);
  }
  return result;
}

function truncateForExcelCell(value) {
  const normalized = String(value ?? "");
  if (normalized.length <= MAX_EXCEL_CELL_LENGTH) {
    return normalized;
  }

  return (
    normalized.slice(0, MAX_EXCEL_CELL_LENGTH - 30).trimEnd() +
    "\n\n[Saīsināts Excel šūnas limita dēļ]"
  );
}

function singleLineCellText(value) {
  return truncateForExcelCell(normalizeMultilineText(value).replace(/\n+/g, " | "));
}

function hashText(value) {
  return createHash("sha256").update(value).digest("hex");
}

async function readJsonFile(filePath, fallbackValue) {
  try {
    const raw = await fs.readFile(filePath, "utf8");
    return JSON.parse(raw);
  } catch (error) {
    if (error.code === "ENOENT") {
      return fallbackValue;
    }
    throw error;
  }
}

async function writeJsonFile(filePath, value) {
  await fs.mkdir(path.dirname(filePath), { recursive: true });
  await fs.writeFile(filePath, `${JSON.stringify(value, null, 2)}\n`, "utf8");
}

async function fetchDecoded(url) {
  const response = await fetch(url);
  if (!response.ok) {
    throw new Error(`Request failed for ${url}: ${response.status} ${response.statusText}`);
  }

  const bytes = new Uint8Array(await response.arrayBuffer());
  return new TextDecoder("windows-1257").decode(bytes);
}

async function fetchUtf8(url) {
  const response = await fetch(url);
  if (!response.ok) {
    throw new Error(`Request failed for ${url}: ${response.status} ${response.statusText}`);
  }

  return response.text();
}

function getCalendarTable($) {
  const table = $("table.calendar").first();
  if (!table.length) {
    throw new Error("Could not find table.calendar on the page.");
  }
  return table;
}

function buildGroupColumns($table) {
  const headerRows = $table.find("tr").slice(0, 2);
  if (headerRows.length < 2) {
    throw new Error("Expected two header rows in the summary table.");
  }

  const topHeaders = headerRows.eq(0).find("th");
  const secondHeaders = headerRows.eq(1).find("th");
  if (!topHeaders.length || !secondHeaders.length) {
    throw new Error("Could not read summary table headers.");
  }

  const columns = [];
  let secondHeaderIndex = 0;

  topHeaders.each((_, element) => {
    const $element = $table.find(element);
    const text = normalizeText($element.text());
    const colspan = Number.parseInt($element.attr("colspan") ?? "1", 10);
    const rowspan = Number.parseInt($element.attr("rowspan") ?? "1", 10);

    if (rowspan > 1) {
      columns.push({ group: text, subcolumn: text });
      return;
    }

    for (let index = 0; index < colspan; index += 1) {
      const subcolumn = normalizeText(secondHeaders.eq(secondHeaderIndex).text());
      columns.push({ group: text, subcolumn });
      secondHeaderIndex += 1;
    }
  });

  return columns;
}

function collectCalendarTargets(indexHtml, indexUrl) {
  const $ = cheerio.load(indexHtml);
  const $table = getCalendarTable($);
  const columns = buildGroupColumns($table);
  const dataColumns = columns
    .map((column, index) => ({ ...column, index }))
    .filter((column) => !EXCLUDED_SUMMARY_GROUPS.has(column.group));

  if (!dataColumns.length) {
    throw new Error("No process columns were found in the monthly summary table.");
  }

  const targets = [];
  const rows = $table.find("tr").slice(2);

  rows.each((rowIndex, rowElement) => {
    const $cells = $(rowElement).children("td");
    if (!$cells.length) {
      return;
    }

    const courtName = normalizeText($cells.eq(0).text());
    if (!courtName) {
      return;
    }

    dataColumns.forEach((column) => {
      const $cell = $cells.eq(column.index);
      if (!$cell.length) {
        return;
      }

      const cellText = normalizeText($cell.text());
      const href = $cell.find("a").attr("href");
      if (!href || !cellText || cellText === "-") {
        return;
      }

      targets.push({
        courtName,
        processGroup: column.group,
        instanceLabel: column.subcolumn,
        detailUrl: new URL(href, indexUrl).href,
        rowOrder: rowIndex,
      });
    });
  });

  return targets;
}

function expandCells($row) {
  const cells = [];

  $row.children("td").each((_, cell) => {
    const $cell = $row.find(cell);
    const colspan = Number.parseInt($cell.attr("colspan") ?? "1", 10);
    const text = normalizeText($cell.text());

    for (let index = 0; index < colspan; index += 1) {
      cells.push(text);
    }
  });

  return cells;
}

function extractCourtInfoLines($) {
  const courtInfo = $("div.courtinfo").first();
  if (!courtInfo.length) {
    return [];
  }

  const lines = [];
  let currentLine = "";

  courtInfo.contents().each((_, node) => {
    if (node.type === "tag" && node.name === "br") {
      const line = normalizeText(currentLine);
      if (line) {
        lines.push(line);
      }
      currentLine = "";
      return;
    }

    const text = normalizeText($(node).text());
    if (!text) {
      return;
    }

    currentLine = currentLine ? `${currentLine} ${text}` : text;
  });

  const trailingLine = normalizeText(currentLine);
  if (trailingLine) {
    lines.push(trailingLine);
  }

  return lines;
}

function cityFromAddress(addressLine) {
  const parts = addressLine
    .split(",")
    .map((part) => normalizeText(part))
    .filter(Boolean);

  if (parts.length < 2) {
    return "";
  }

  for (let index = parts.length - 2; index >= 0; index -= 1) {
    const part = parts[index];
    if (!/^LV-\d+$/i.test(part)) {
      return part;
    }
  }

  return "";
}

function cityFromLabel(label) {
  const cleaned = normalizeText(label).replace(/\([^)]*\)/g, "").trim();
  const patterns = [
    /\b(?:tiesa|tiesu nams)\s+(.+)$/i,
    /\b(?:apgabaltiesa)\s+(.+)$/i,
  ];

  for (const pattern of patterns) {
    const match = cleaned.match(pattern);
    if (match?.[1]) {
      return normalizeText(match[1]);
    }
  }

  return "";
}

function extractDetailMetadata($, fallbackMeta) {
  const lines = extractCourtInfoLines($);
  const rawCourtName = lines[0] || fallbackMeta.courtName;
  const addressLine = lines.find((line) => line.includes(",")) || "";

  const city =
    cityFromAddress(addressLine) ||
    cityFromLabel(lines.at(-1) ?? "") ||
    cityFromLabel(rawCourtName);

  const courtName = addressLine
    ? `${rawCourtName} | ${addressLine}`
    : rawCourtName;

  return {
    courtName,
    processGroup: fallbackMeta.processGroup,
    instanceLabel: fallbackMeta.instanceLabel,
    city,
  };
}

function headerSetMatches(headers, requiredHeaders) {
  return requiredHeaders.every((header) => headers.includes(header));
}

function validateDetailHeaders(headers, detailUrl) {
  const supportedHeaderSets = [
    [...SHARED_DETAIL_FIELDS, "Prasītājs", "Atbildētājs", "Būtība"],
    [...SHARED_DETAIL_FIELDS, "Pieteicējs", "Atbildētājs", "Prasījums"],
    [
      "Procesa veids",
      "Tiesas sēdes laiks",
      "Lietas numurs",
      "Arhīva numurs",
      "Apsūdzētais",
      CRIMINAL_CITATION_FIELD,
      "Tiesas sēdes veids",
      "Tiesnesis",
    ],
    [
      "Procesa veids",
      "Tiesas sēdes laiks",
      "Lietas numurs",
      "Persona, kuru sauc pie administratīvās atbildības",
      CRIMINAL_CITATION_FIELD,
      "Tiesas sēdes veids",
      "Tiesnesis",
    ],
  ];

  if (supportedHeaderSets.some((requiredHeaders) => headerSetMatches(headers, requiredHeaders))) {
    return;
  }

  throw new Error(
    `Unexpected detail headers for ${detailUrl}: ${headers.join(", ") || "none"}.`,
  );
}

function parseDetailPage(detailHtml, detailUrl, fallbackMeta) {
  const $ = cheerio.load(detailHtml);
  const $table = getCalendarTable($);
  const $rows = $table.find("tr");

  if ($rows.length < 1) {
    throw new Error(`No rows found in detail page: ${detailUrl}`);
  }

  const headers = $rows
    .eq(0)
    .find("th")
    .toArray()
    .map((element) => normalizeText($(element).text()));

  validateDetailHeaders(headers, detailUrl);

  const detailMeta = extractDetailMetadata($, fallbackMeta);
  const hearings = [];

  $rows.slice(1).each((hearingIndex, rowElement) => {
    const $row = $(rowElement);
    const cells = expandCells($row);
    if (!cells.length) {
      return;
    }

    if (cells.length !== headers.length) {
      throw new Error(
        `Unexpected detail row width for ${detailUrl} at row ${hearingIndex + 1}: expected ${headers.length}, got ${cells.length}.`,
      );
    }

    const fields = Object.fromEntries(headers.map((field, index) => [field, cells[index] ?? ""]));

    hearings.push({
      ...detailMeta,
      detailUrl,
      fields,
    });
  });

  return hearings;
}

function parseDateParts(rawDateTime) {
  const match = normalizeText(rawDateTime).match(
    /^(\d{2})\.(\d{2})\.(\d{4})(?:\s+(\d{2}):(\d{2}))?$/,
  );

  if (!match) {
    throw new Error(`Could not parse hearing date/time: '${rawDateTime}'`);
  }

  const [, day, month, year, hour = "", minute = ""] = match;
  const dateValue = new Date(Number(year), Number(month) - 1, Number(day), 12, 0, 0, 0);
  const timeValue = hour && minute ? `${hour}:${minute}` : "";

  return { dateValue, timeValue };
}

function decipherLawText(rawLawText) {
  const normalized = normalizeText(rawLawText);
  if (!normalized || normalized === "Informācija nav izpaužama") {
    return normalized;
  }

  const likelyLawCodePattern = /[A-ZĀČĒĢĪĶĻŅŠŪŽ]{2,}\s*\d/;
  if (!likelyLawCodePattern.test(normalized)) {
    return normalized;
  }

  const splitMatches = [
    ...normalized.matchAll(
      /([A-ZĀČĒĢĪĶĻŅŠŪŽ]{2,}\s+.*?)(?=(?:[A-ZĀČĒĢĪĶĻŅŠŪŽ]{2,}\s+\d)|$)/g,
    ),
  ]
    .map((match) => normalizeText(match[1]))
    .filter(Boolean);

  if (splitMatches.length > 1) {
    return splitMatches.join("\n");
  }

  const boundarySplit = normalized
    .replace(/(\.)(?=[A-ZĀČĒĢĪĶĻŅŠŪŽ]{2,}\s*\d)/g, "$1\n")
    .split("\n")
    .map((part) => normalizeText(part))
    .filter(Boolean);

  return boundarySplit.length > 1 ? boundarySplit.join("\n") : normalized;
}

function parseCitationLine(line) {
  const normalized = normalizeText(line);
  if (!normalized || normalized === "Informācija nav izpaužama") {
    return null;
  }

  const lawMatch = normalized.match(/^([A-ZĀČĒĢĪĶĻŅŠŪŽ]{2,})\s+(.+)$/u);
  if (!lawMatch) {
    return null;
  }

  const [, code, restRaw] = lawMatch;
  const rest = restRaw.replace(/\s+/g, "").replace(/\.\-(?=\d)/g, "-");
  const segments = [...rest.matchAll(/(\d+(?:[._-]\d+)*)\.?(p|d|pkt)\.?/gi)];

  let article = null;
  let part = null;
  let point = null;

  for (const [, number, kind] of segments) {
    const normalizedNumber = canonicalizeNumericId(number);
    if (kind === "p" && !article) {
      article = normalizedNumber;
      continue;
    }
    if (kind === "d" && !part) {
      part = normalizedNumber;
      continue;
    }
    if (kind === "pkt" && !point) {
      point = normalizedNumber;
    }
  }

  if (!article) {
    return null;
  }

  return {
    raw: normalized,
    code,
    article,
    part,
    point,
  };
}

function citationLabel(parsedCitation) {
  const segments = [`${parsedCitation.article}. pants`];
  if (parsedCitation.part) {
    segments.push(`${parsedCitation.part}. daļa`);
  }
  if (parsedCitation.point) {
    segments.push(`${parsedCitation.point}. punkts`);
  }
  return segments.join(", ");
}

function normalizeLawHtmlSnippet(html) {
  return html.replace(/<sup\b[^>]*>(.*?)<\/sup>/gi, "-$1");
}

function extractParagraphTextFromHtml(html) {
  const normalizedHtml = normalizeLawHtmlSnippet(html);
  const $ = cheerio.load(`<div>${normalizedHtml}</div>`);
  return normalizeMultilineText($("div").text());
}

function parseLawTitle($, fallbackTitle) {
  const pageTitle = normalizeText($("title").first().text());
  if (pageTitle) {
    return pageTitle.replace(/^Likumi\.lv\s*-\s*/i, "").trim() || fallbackTitle;
  }
  return fallbackTitle;
}

function cleanLawHeading(heading) {
  return normalizeText(heading)
    .replace(/^\d+(?:[._-]\d+)?\.?\s*pants?\.?\s*/iu, "")
    .replace(/^\d+(?:[._-]\d+)?\s*/u, "")
    .trim();
}

function stripPenaltyTail(text) {
  return normalizeText(text)
    .replace(/\s*[—-]\s*soda ar[\s\S]*$/iu, "")
    .replace(/\s*,\s*paredzot[\s\S]*$/iu, "")
    .trim();
}

function extractTopicFromNormText(text) {
  const normalized = stripPenaltyTail(text);
  if (!normalized) {
    return "";
  }

  const parMatch = normalized.match(/^Par\s+(.+)$/iu);
  if (!parMatch?.[1]) {
    return normalized;
  }

  return normalizeText(
    parMatch[1]
      .replace(/,\s*ja\s+.+$/iu, "")
      .replace(/\s+vai\s+par\s+šā panta.+$/iu, "")
      .replace(/\s+un\s+par\s+šā panta.+$/iu, ""),
  );
}

function compressSummaryTopic(topic) {
  const normalized = normalizeText(topic);
  const replacements = [
    [
      /^Dokumenta, zīmoga un spiedoga viltošana.+$/iu,
      "dokumentu, zīmogu un spiedogu viltošana un izmantošana",
    ],
    [
      /^Alkoholisko dzērienu un tabakas izstrādājumu.+$/iu,
      "nelikumīga alkohola un tabakas izstrādājumu aprite",
    ],
    [
      /^Narkotisko un psihotropo vielu neatļauta .+$/iu,
      "neatļauta narkotisko un psihotropo vielu aprite",
    ],
    [
      /^Datu, programmatūras un iekārtu iegūšana.+$/iu,
      "datu, programmatūras un iekārtu izmantošana nelikumīgām darbībām ar finanšu instrumentiem",
    ],
    [
      /^Preču un vielu, kuru aprite ir aizliegta vai speciāli reglamentēta.+$/iu,
      "aizliegtu vai reglamentētu preču un vielu pārvietošana pāri robežai",
    ],
    [
      /^Šaujamieroču.+$/iu,
      "ieroču, munīcijas un sprāgstvielu neatļauta aprite",
    ],
    [
      /^Izvairīšanās no nodokļu.+$/iu,
      "nodokļu un tiem pielīdzināto maksājumu nenomaksa",
    ],
  ];

  for (const [pattern, replacement] of replacements) {
    if (pattern.test(normalized)) {
      return replacement;
    }
  }

  return normalized;
}

function extractSummaryTopic(article, text) {
  const cleanedHeading = cleanLawHeading(article.heading);
  if (cleanedHeading) {
    return compressSummaryTopic(cleanedHeading);
  }
  return compressSummaryTopic(extractTopicFromNormText(text));
}

function extractPointFromText(text, pointId) {
  if (!text) {
    return "";
  }

  const pattern = new RegExp(
    `(?:^|\\n|\\s)${escapeRegex(pointId)}\\)\\s*([\\s\\S]*?)(?=(?:\\n|\\s)\\d+(?:[._-]\\d+)*\\)\\s|$)`,
  );
  const match = text.match(pattern);
  return match ? normalizeMultilineText(match[1]) : "";
}

function parseLawPage(code, registryEntry, html) {
  const $ = cheerio.load(html);
  const title = parseLawTitle($, registryEntry.title);
  const articles = {};

  $("div.TV213[data-pfx='p']").each((_, element) => {
    const $element = $(element);
    const rawArticleId = $element.attr("data-num");
    if (!rawArticleId) {
      return;
    }

    const articleId = canonicalizeNumericId(rawArticleId);
    const heading = normalizeText($element.children("p.TV213.TVP").first().text());
    const bodyLines = [];
    const articlePoints = {};
    const parts = {};
    let currentPartId = null;

    $element.children("p.TV213").each((_, paragraph) => {
      const $paragraph = $(paragraph);
      if ($paragraph.hasClass("TVP") || $paragraph.hasClass("labojumu_pamats")) {
        return;
      }

      const text = extractParagraphTextFromHtml($.html(paragraph) ?? "");
      if (!text) {
        return;
      }

      const partMatch = text.match(/^\(([^)]+)\)\s*(.+)$/);
      if (partMatch) {
        currentPartId = canonicalizeNumericId(partMatch[1]);
        parts[currentPartId] = parts[currentPartId] ?? { textLines: [], points: {} };
        parts[currentPartId].textLines.push(normalizeText(partMatch[2]));
        return;
      }

      const pointMatch = text.match(/^(\d+(?:[._-]\d+)*)\)\s*(.+)$/);
      if (pointMatch) {
        const pointId = canonicalizeNumericId(pointMatch[1]);
        const pointText = normalizeText(pointMatch[2]);
        if (currentPartId && parts[currentPartId]) {
          parts[currentPartId].points[pointId] = pointText;
          parts[currentPartId].textLines.push(`${pointId}) ${pointText}`);
          return;
        }
        articlePoints[pointId] = pointText;
        bodyLines.push(`${pointId}) ${pointText}`);
        return;
      }

      if (currentPartId && parts[currentPartId]) {
        parts[currentPartId].textLines.push(text);
        return;
      }

      bodyLines.push(text);
    });

    const normalizedParts = Object.fromEntries(
      Object.entries(parts).map(([partId, partData]) => [
        partId,
        {
          text: normalizeMultilineText(partData.textLines.join("\n")),
          points: partData.points,
        },
      ]),
    );

    articles[articleId] = {
      articleId,
      heading,
      body: normalizeMultilineText(bodyLines.join("\n")),
      points: articlePoints,
      parts: normalizedParts,
    };
  });

  return {
    code,
    title,
    source_url: registryEntry.sourceUrl,
    fetched_at: new Date().toISOString(),
    articles,
  };
}

async function loadLawRegistry() {
  const registry = await readJsonFile(LAW_SOURCES_FILE, null);
  if (!registry) {
    throw new Error(`Missing law registry: ${LAW_SOURCES_FILE}`);
  }
  return registry;
}

function lawCachePathForCode(code) {
  return path.join(LAW_CACHE_DIR, `${makeSafeFilePart(code)}.json`);
}

async function ensureLawCache(code, registryEntry, stats) {
  const cachePath = lawCachePathForCode(code);
  const cachedLaw = await readJsonFile(cachePath, null);
  if (cachedLaw) {
    stats.lawsReused += 1;
    return cachedLaw;
  }

  const html = await fetchUtf8(registryEntry.sourceUrl);
  const parsedLaw = parseLawPage(code, registryEntry, html);
  await writeJsonFile(cachePath, parsedLaw);
  stats.lawsFetched += 1;
  return parsedLaw;
}

async function ensureLawLibrary(codes, registry, stats) {
  const lawLibrary = new Map();

  for (const code of codes) {
    const registryEntry = registry[code];
    if (!registryEntry?.sourceUrl) {
      throw new Error(
        `Unknown law code '${code}'. Add it to ${LAW_SOURCES_FILE} before running the export.`,
      );
    }

    const cachedLaw = await ensureLawCache(code, registryEntry, stats);
    lawLibrary.set(code, cachedLaw);
  }

  return lawLibrary;
}

function resolveCitation(parsedCitation, lawLibrary) {
  const law = lawLibrary.get(parsedCitation.code);
  if (!law) {
    return {
      found: false,
      message: `Nav atrasts oficiāls teksts atsaucei: ${parsedCitation.raw}`,
    };
  }

  const article = law.articles[parsedCitation.article];
  if (!article) {
    return {
      found: false,
      message: `Nav atrasts oficiāls teksts atsaucei: ${parsedCitation.raw}`,
    };
  }

  let text = "";

  if (parsedCitation.part) {
    const part = article.parts?.[parsedCitation.part];
    if (!part) {
      return {
        found: false,
        message: `Nav atrasts oficiāls teksts atsaucei: ${parsedCitation.raw}`,
      };
    }

    if (parsedCitation.point) {
      text = part.points?.[parsedCitation.point] || extractPointFromText(part.text, parsedCitation.point);
      if (!text) {
        return {
          found: false,
          message: `Nav atrasts oficiāls teksts atsaucei: ${parsedCitation.raw}`,
        };
      }
    } else {
      text = part.text;
    }
  } else if (parsedCitation.point) {
    text =
      article.points?.[parsedCitation.point] || extractPointFromText(article.body, parsedCitation.point);
    if (!text) {
      return {
        found: false,
        message: `Nav atrasts oficiāls teksts atsaucei: ${parsedCitation.raw}`,
      };
    }
  } else {
    text = normalizeMultilineText([article.heading, article.body].filter(Boolean).join("\n"));
  }

  const label = citationLabel(parsedCitation);
  return {
    found: true,
    citation: parsedCitation.raw,
    block: `${parsedCitation.raw} — ${law.title}, ${label}: ${text}`,
    summaryTopic: extractSummaryTopic(article, text),
  };
}

function collectUniqueCitations(decipheredText) {
  const seen = new Set();
  const citations = [];

  for (const line of normalizeMultilineText(decipheredText).split("\n")) {
    const normalizedLine = normalizeText(line);
    if (!normalizedLine || seen.has(normalizedLine)) {
      continue;
    }
    seen.add(normalizedLine);
    citations.push(normalizedLine);
  }

  return citations;
}

function firstNonEmptyField(fields, fieldNames) {
  for (const fieldName of fieldNames) {
    const value = normalizeMultilineText(fields[fieldName]);
    if (value) {
      return value;
    }
  }
  return "";
}

function hasCriminalCitationField(entry) {
  return Object.prototype.hasOwnProperty.call(entry.fields, CRIMINAL_CITATION_FIELD);
}

function isCriminalEntry(entry) {
  return entry.processGroup === KRIMINALPROCESS_GROUP && hasCriminalCitationField(entry);
}

function summarizeWithoutModel() {
  return "Publiskajā rindā nav pietiekamu normu datu lietas rakstura kopsavilkumam.";
}

function collectUniqueLines(multilineText) {
  const seen = new Set();
  const values = [];

  for (const line of normalizeMultilineText(multilineText).split("\n")) {
    const normalizedLine = normalizeText(line);
    if (!normalizedLine || seen.has(normalizedLine)) {
      continue;
    }
    seen.add(normalizedLine);
    values.push(normalizedLine);
  }

  return values;
}

function joinLatvianList(values) {
  if (values.length === 0) {
    return "";
  }
  if (values.length === 1) {
    return values[0];
  }
  if (values.length === 2) {
    return `${values[0]} un ${values[1]}`;
  }
  return `${values.slice(0, -1).join(", ")} un ${values.at(-1)}`;
}

function fallbackSummaryFromTopics(summarySourceText) {
  const topics = collectUniqueLines(summarySourceText);
  if (!topics.length) {
    return summarizeWithoutModel();
  }
  if (topics.length === 1) {
    return `${topics[0]}.`;
  }
  if (topics.length === 2) {
    return `${joinLatvianList(topics)}.`;
  }
  return `${topics[0]}, ${topics[1]} un citi saistīti nodarījumi.`;
}

const SUMMARY_BOILERPLATE_STEMS = new Set([
  "apsūd",
  "minēt",
  "norma",
  "aptve",
  "tēmas",
]);

function textStems(value) {
  return (normalizeText(value).toLowerCase().match(/[a-zā-ž]{5,}/gu) ?? []).map((word) =>
    word.slice(0, 5),
  );
}

function isLowQualitySummary(summary, summarySourceText) {
  const normalized = normalizeText(summary);
  if (!normalized) {
    return true;
  }

  const letterCount = (normalized.match(/[A-Za-zĀ-ž]/gu) ?? []).length;
  if (letterCount < 20) {
    return true;
  }

  const words = normalized.split(/\s+/);
  if (words.length < 5) {
    return true;
  }

  if (normalized.length > 160) {
    return true;
  }

  if (/^(?:Krimināllikuma|KL|LKK)\b/i.test(normalized)) {
    return true;
  }

  if (/\d/.test(normalized)) {
    return true;
  }

  const lastWord = normalized
    .replace(/[.!?]+$/g, "")
    .split(/\s+/)
    .at(-1)
    ?.toLowerCase();
  if (!lastWord || lastWord.length < 4 || ["bez", "jeb", "kas", "par", "pie", "un", "vai"].includes(lastWord)) {
    return true;
  }

  const sourceStems = new Set(textStems(summarySourceText));
  const summaryStems = textStems(normalized).filter((stem) => !SUMMARY_BOILERPLATE_STEMS.has(stem));
  if (summaryStems.length === 0) {
    return true;
  }

  const overlappingStems = summaryStems.filter((stem) => sourceStems.has(stem)).length;
  if (overlappingStems / summaryStems.length < 0.35) {
    return true;
  }

  return false;
}

function enrichEntryWithLawContext(entry, lawLibrary, stats) {
  if (!isCriminalEntry(entry)) {
    return {
      ...entry,
      apsudzibasPantiDeciphered: "",
      citedNormText: "",
      summarySourceText: "",
      caseNatureSummary: "",
      seriousnessRank: "",
    };
  }

  const deciphered = decipherLawText(entry.fields[CRIMINAL_CITATION_FIELD]);
  const citationLines = collectUniqueCitations(deciphered);
  const dumpBlocks = [];
  const summaryTopics = [];

  for (const citationLine of citationLines) {
    const parsedCitation = parseCitationLine(citationLine);
    if (!parsedCitation) {
      if (citationLine === "Informācija nav izpaužama") {
        dumpBlocks.push(citationLine);
      } else {
        dumpBlocks.push(`Nav izdevies parsēt atsauci: ${citationLine}`);
      }
      continue;
    }

    const resolvedCitation = resolveCitation(parsedCitation, lawLibrary);
    if (!resolvedCitation.found) {
      dumpBlocks.push(resolvedCitation.message);
      stats.unresolvedCitations += 1;
      continue;
    }

    dumpBlocks.push(resolvedCitation.block);
    if (resolvedCitation.summaryTopic) {
      summaryTopics.push(resolvedCitation.summaryTopic);
    }
  }

  const citedNormText = truncateForExcelCell(
    normalizeMultilineText(dumpBlocks.join("\n\n")) || deciphered,
  );
  const summarySourceText = normalizeMultilineText(collectUniqueLines(summaryTopics.join("\n")).join("\n"));

  return {
    ...entry,
    apsudzibasPantiDeciphered: deciphered,
    citedNormText,
    summarySourceText,
    caseNatureSummary: summarySourceText ? "" : summarizeWithoutModel(),
  };
}

async function runPythonSummarizer(items) {
  const payload = JSON.stringify({
    model: MLX_MODEL_ID,
    items,
  });

  return new Promise((resolve, reject) => {
    const child = spawn("python3", [MLX_SUMMARIZER], {
      cwd: PROJECT_ROOT,
      stdio: ["pipe", "pipe", "pipe"],
    });

    let stdout = "";
    let stderr = "";
    let settled = false;

    function finishWithError(error) {
      if (settled) {
        return;
      }
      settled = true;
      reject(error);
    }

    function finishWithSuccess(value) {
      if (settled) {
        return;
      }
      settled = true;
      resolve(value);
    }

    child.stdout.on("data", (chunk) => {
      stdout += chunk.toString();
    });

    child.stderr.on("data", (chunk) => {
      stderr += chunk.toString();
    });

    child.on("error", (error) => {
      finishWithError(error);
    });

    child.stdin.on("error", (error) => {
      if (error.code === "EPIPE") {
        return;
      }
      finishWithError(error);
    });

    child.on("close", (code) => {
      if (code !== 0) {
        finishWithError(
          new Error((stderr || stdout).trim() || `Python summarizer exited with code ${code}`),
        );
        return;
      }

      try {
        finishWithSuccess(JSON.parse(stdout));
      } catch (error) {
        finishWithError(new Error(`Could not parse Python summarizer output: ${error.message}`));
      }
    });

    child.stdin.end(payload);
  });
}

async function enrichSummaries(entries, stats) {
  await fs.mkdir(CACHE_DIR, { recursive: true });
  const summaryCache = await readJsonFile(SUMMARY_CACHE_FILE, {});
  const pending = [];

  for (const entry of entries) {
    if (!entry.summarySourceText) {
      continue;
    }

    const summaryKey = hashText(entry.summarySourceText);
    entry.summaryKey = summaryKey;

    if (summaryCache[summaryKey]) {
      entry.caseNatureSummary = summaryCache[summaryKey];
      stats.summaryCacheHits += 1;
      continue;
    }

    pending.push({
      id: summaryKey,
      text: entry.summarySourceText,
    });
  }

  const uniquePending = Array.from(
    new Map(pending.map((item) => [item.id, item])).values(),
  );

  if (uniquePending.length > 0) {
    stats.summaryCacheMisses += uniquePending.length;
    const result = await runPythonSummarizer(uniquePending);
    const summaries = result?.summaries ?? {};

    for (const item of uniquePending) {
      const summary = normalizeText(summaries[item.id]);
      if (!summary) {
        throw new Error(`Missing MLX summary for item ${item.id}`);
      }
      summaryCache[item.id] = summary;
    }

    await writeJsonFile(SUMMARY_CACHE_FILE, summaryCache);
  }

  for (const entry of entries) {
    if (!entry.summarySourceText) {
      entry.caseNatureSummary = isCriminalEntry(entry) ? summarizeWithoutModel() : "";
      continue;
    }
    const cachedSummary = summaryCache[entry.summaryKey];
    const fallbackSummary = fallbackSummaryFromTopics(entry.summarySourceText);
    entry.caseNatureSummary =
      fallbackSummary.length <= 140 || isLowQualitySummary(cachedSummary, entry.summarySourceText)
        ? fallbackSummary
        : cachedSummary;
  }
}

function toWorksheetRow(entry) {
  const rawDateTime = entry.fields["Tiesas sēdes laiks"] ?? "";
  const { dateValue, timeValue } = parseDateParts(rawDateTime);

  return {
    processGroup: singleLineCellText(entry.processGroup),
    city: singleLineCellText(entry.city),
    date: dateValue,
    time: singleLineCellText(timeValue),
    tiesasSedesVeids: singleLineCellText(entry.fields["Tiesas sēdes veids"]),
    procesaVeids: singleLineCellText(entry.fields["Procesa veids"]),
    courtName: singleLineCellText(entry.courtName),
    primaryParty: singleLineCellText(firstNonEmptyField(entry.fields, PRIMARY_PARTY_FIELDS)),
    secondaryParty: singleLineCellText(firstNonEmptyField(entry.fields, SECONDARY_PARTY_FIELDS)),
    caseSubject: singleLineCellText(firstNonEmptyField(entry.fields, SUBJECT_FIELDS)),
    instanceLabel: singleLineCellText(entry.instanceLabel),
    lietasNumurs: singleLineCellText(entry.fields["Lietas numurs"]),
    arhivaNumurs: singleLineCellText(entry.fields["Arhīva numurs"]),
    apsudzibasPantiDeciphered: singleLineCellText(entry.apsudzibasPantiDeciphered),
    citedNormText: singleLineCellText(entry.citedNormText),
    caseNatureSummary: truncateForExcelCell(entry.caseNatureSummary),
    seriousnessRank: String(entry.seriousnessRank ?? ""),
    tiesnesis: singleLineCellText(entry.fields["Tiesnesis"]),
    tiesasSedesLaiks: singleLineCellText(rawDateTime),
    detailUrl: singleLineCellText(entry.detailUrl),
  };
}

async function writeWorkbook(outputPath, entries) {
  const workbook = new ExcelJS.Workbook();
  const worksheet = workbook.addWorksheet("Court Calendar", {
    views: [{ state: "frozen", ySplit: 1 }],
  });

  worksheet.columns = EXCEL_COLUMNS;
  worksheet.autoFilter = {
    from: "A1",
    to: `${excelColumnLetter(EXCEL_COLUMNS.length)}1`,
  };

  for (const entry of entries) {
    const row = worksheet.addRow(toWorksheetRow(entry));
    row.getCell("date").numFmt = "dd.mm.yyyy";
  }

  worksheet.getRow(1).font = { bold: true };
  worksheet.getColumn("date").alignment = { horizontal: "left" };
  worksheet.getColumn("time").alignment = { horizontal: "left" };
  for (const columnKey of [
    "processGroup",
    "city",
    "time",
    "tiesasSedesVeids",
    "procesaVeids",
    "courtName",
    "primaryParty",
    "secondaryParty",
    "caseSubject",
    "instanceLabel",
    "lietasNumurs",
    "arhivaNumurs",
    "apsudzibasPantiDeciphered",
    "citedNormText",
    "tiesnesis",
    "tiesasSedesLaiks",
    "seriousnessRank",
    "detailUrl",
  ]) {
    worksheet.getColumn(columnKey).alignment = {
      vertical: "top",
      wrapText: false,
    };
  }
  for (const columnKey of ["caseSubject", "caseNatureSummary"]) {
    worksheet.getColumn(columnKey).alignment = {
      vertical: "top",
      wrapText: true,
    };
  }

  await fs.mkdir(path.dirname(outputPath), { recursive: true });
  await workbook.xlsx.writeFile(outputPath);
}

async function main() {
  const [, , indexUrlArg, outputPathArg] = process.argv;
  if (!indexUrlArg) {
    usage();
    process.exitCode = 1;
    return;
  }

  let indexUrl;
  try {
    indexUrl = new URL(indexUrlArg).href;
  } catch {
    throw new Error(`Invalid URL: ${indexUrlArg}`);
  }

  const stats = {
    lawsFetched: 0,
    lawsReused: 0,
    unresolvedCitations: 0,
    summaryCacheHits: 0,
    summaryCacheMisses: 0,
  };

  const outputPath = outputPathArg
    ? path.resolve(outputPathArg)
    : path.resolve(defaultOutputPath(indexUrl));

  const indexHtml = await fetchDecoded(indexUrl);
  const targets = collectCalendarTargets(indexHtml, indexUrl);
  if (!targets.length) {
    throw new Error(`No detail links were found on ${indexUrl}.`);
  }

  const rawEntries = [];
  for (const target of targets) {
    const detailHtml = await fetchDecoded(target.detailUrl);
    const hearings = parseDetailPage(detailHtml, target.detailUrl, {
      courtName: target.courtName,
      processGroup: target.processGroup,
      instanceLabel: target.instanceLabel,
    });
    rawEntries.push(...hearings);
  }

  const registry = await loadLawRegistry();
  const requiredLawCodes = Array.from(
    new Set(
      rawEntries.flatMap((entry) =>
        !isCriminalEntry(entry)
          ? []
          : collectUniqueCitations(decipherLawText(entry.fields[CRIMINAL_CITATION_FIELD]))
          .map(parseCitationLine)
          .filter(Boolean)
          .map((citation) => citation.code),
      ),
    ),
  ).sort();

  const lawLibrary = await ensureLawLibrary(requiredLawCodes, registry, stats);
  const enrichedEntries = rawEntries.map((entry) =>
    enrichEntryWithLawContext(entry, lawLibrary, stats),
  );

  await enrichSummaries(enrichedEntries, stats);
  const rankedEntries = assignSeriousnessRanks(enrichedEntries);
  await writeWorkbook(outputPath, rankedEntries);

  console.log(`Fetched detail pages: ${targets.length}`);
  console.log(`Wrote hearing rows: ${enrichedEntries.length}`);
  console.log(`Law codes required: ${requiredLawCodes.join(", ")}`);
  console.log(`Law cache fetched: ${stats.lawsFetched}`);
  console.log(`Law cache reused: ${stats.lawsReused}`);
  console.log(`Unresolved citations: ${stats.unresolvedCitations}`);
  console.log(`Summary cache hits: ${stats.summaryCacheHits}`);
  console.log(`Summary cache misses: ${stats.summaryCacheMisses}`);
  console.log(`Output: ${outputPath}`);
}

const isMain =
  process.argv[1] &&
  path.resolve(process.argv[1]) === fileURLToPath(import.meta.url);

if (isMain) {
  main().catch((error) => {
    console.error(`Error: ${error.message}`);
    process.exitCode = 1;
  });
}
