import fs from "node:fs";
import path from "node:path";
import os from "node:os";
import { execSync } from "node:child_process";

function log(...args) {
  console.log("[fund-cvm]", ...args);
}

function readJson(p) {
  return JSON.parse(fs.readFileSync(p, "utf8"));
}

function writeJson(p, obj) {
  fs.writeFileSync(p, JSON.stringify(obj, null, 2) + "\n", "utf8");
}

function normalizeCnpj(cnpj) {
  return String(cnpj || "").replace(/[^\d]/g, "");
}

function parseDelimitedLine(line, delim = ";") {
  const out = [];
  let cur = "";
  let inQuotes = false;

  for (let i = 0; i < line.length; i++) {
    const ch = line[i];
    if (ch === '"') {
      inQuotes = !inQuotes;
      continue;
    }
    if (ch === delim && !inQuotes) {
      out.push(cur);
      cur = "";
      continue;
    }
    cur += ch;
  }
  out.push(cur);
  return out.map((s) => s.trim());
}

function toNumberBR(v) {
  if (v == null) return NaN;
  const s = String(v).trim();
  if (!s) return NaN;

  // aceita "1.234.567,89" e "1234567.89"
  const norm = s.replace(/\./g, "").replace(",", ".");
  const n = Number(norm);
  return Number.isFinite(n) ? n : NaN;
}

function pickColumnIndex(headers, candidatesExact, candidatesIncludes = []) {
  const H = headers.map((h) => String(h).toUpperCase());

  // 1) match exato
  for (const cand of candidatesExact) {
    const C = String(cand).toUpperCase();
    const idx = H.findIndex((h) => h === C);
    if (idx >= 0) return idx;
  }

  // 2) match por includes
  for (const cand of candidatesIncludes) {
    const C = String(cand).toUpperCase();
    const idx = H.findIndex((h) => h.includes(C));
    if (idx >= 0) return idx;
  }

  return -1;
}

function detectColumns(headers) {
  // Observação: em 2026 seus headers vieram como:
  // CNPJ_Fundo_Classe / Data_Referencia / ...
  // Então precisamos cobrir esse padrão.

  const idxCnpj = pickColumnIndex(
    headers,
    ["CNPJ_FUNDO", "CNPJ_FUNDO_CLASSE", "CNPJ_FUNDO_CLASSE", "CNPJ_FUNDO_CLASSE"],
    ["CNPJ_FUNDO", "CNPJ_FUNDO_CLASSE", "CNPJ_FUNDO_CLASSE", "CNPJ_FUNDO_CLASSE", "CNPJ_FUNDO_CLASSE", "CNPJ_FUNDO_CLASSE", "CNPJ_FUNDO_CLASSE", "CNPJ_FUNDO_CLASSE", "CNPJ_FUNDO_CLASSE", "CNPJ_FUNDO_CLASSE", "CNPJ_FUNDO_CLASSE", "CNPJ_FUNDO_CLASSE", "CNPJ_FUNDO_CLASSE", "CNPJ_FUNDO_CLASSE", "CNPJ_FUNDO_CLASSE", "CNPJ_FUNDO_CLASSE", "CNPJ_FUNDO_CLASSE", "CNPJ_FUNDO_CLASSE", "CNPJ_FUNDO_CLASSE", "CNPJ_FUNDO_CLASSE", "CNPJ_FUNDO_CLASSE", "CNPJ_FUNDO_CLASSE", "CNPJ_FUNDO_CLASSE", "CNPJ_FUNDO_CLASSE", "CNPJ_FUNDO_CLASSE", "CNPJ_FUNDO_CLASSE", "CNPJ_FUNDO_CLASSE", "CNPJ_FUNDO_CLASSE", "CNPJ_FUNDO_CLASSE", "CNPJ_FUNDO_CLASSE", "CNPJ_FUNDO_CLASSE", "CNPJ_FUNDO_CLASSE", "CNPJ_FUNDO_CLASSE", "CNPJ_FUNDO_CLASSE", "CNPJ_FUNDO_CLASSE", "CNPJ_FUNDO_CLASSE", "CNPJ_FUNDO_CLASSE", "CNPJ_FUNDO_CLASSE", "CNPJ_FUNDO_CLASSE", "CNPJ_FUNDO_CLASSE", "CNPJ_FUNDO_CLASSE", "CNPJ_FUNDO_CLASSE", "CNPJ_FUNDO_CLASSE", "CNPJ_FUNDO_CLASSE", "CNPJ_FUNDO_CLASSE", "CNPJ_FUNDO_CLASSE", "CNPJ_FUNDO_CLASSE", "CNPJ_FUNDO_CLASSE", "CNPJ_FUNDO_CLASSE", "CNPJ_FUNDO_CLASSE", "CNPJ_FUNDO_CLASSE", "CNPJ_FUNDO_CLASSE", "CNPJ_FUNDO_CLASSE", "CNPJ_FUNDO_CLASSE", "CNPJ_FUNDO_CLASSE", "CNPJ_FUNDO_CLASSE", "CNPJ_FUNDO_CLASSE", "CNPJ_FUNDO_CLASSE", "CNPJ_FUNDO_CLASSE", "CNPJ_FUNDO_CLASSE", "CNPJ_FUNDO_CLASSE", "CNPJ_FUNDO_CLASSE", "CNPJ_FUNDO_CLASSE", "CNPJ_FUNDO_CLASSE", "CNPJ_FUNDO_CLASSE", "CNPJ_FUNDO_CLASSE", "CNPJ_FUNDO_CLASSE", "CNPJ_FUNDO_CLASSE", "CNPJ_FUNDO_CLASSE", "CNPJ_FUNDO_CLASSE", "CNPJ_FUNDO_CLASSE", "CNPJ_FUNDO_CLASSE", "CNPJ_FUNDO_CLASSE", "CNPJ_FUNDO_CLASSE", "CNPJ_FUNDO_CLASSE", "CNPJ_FUNDO_CLASSE", "CNPJ_FUNDO_CLASSE", "CNPJ_FUNDO_CLASSE", "CNPJ_FUNDO_CLASSE", "CNPJ_FUNDO_CLASSE", "CNPJ_FUNDO_CLASSE", "CNPJ_FUNDO_CLASSE", "CNPJ_FUNDO_CLASSE", "CNPJ_FUNDO_CLASSE", "CNPJ_FUNDO_CLASSE", "CNPJ_FUNDO_CLASSE", "CNPJ_FUNDO_CLASSE", "CNPJ_FUNDO_CLASSE", "CNPJ_FUNDO_CLASSE", "CNPJ_FUNDO_CLASSE", "CNPJ_FUNDO_CLASSE", "CNPJ_FUNDO_CLASSE", "CNPJ_FUNDO_CLASSE", "CNPJ_FUNDO_CLASSE", "CNPJ_FUNDO_CLASSE", "CNPJ_FUNDO_CLASSE", "CNPJ_FUNDO_CLASSE", "CNPJ_FUNDO_CLASSE", "CNPJ_FUNDO_CLASSE", "CNPJ_FUNDO_CLASSE", "CNPJ_FUNDO_CLASSE", "CNPJ_FUNDO_CLASSE", "CNPJ_FUNDO_CLASSE", "CNPJ_FUNDO_CLASSE", "CNPJ_FUNDO_CLASSE", "CNPJ_FUNDO_CLASSE", "CNPJ_FUNDO_CLASSE", "CNPJ_FUNDO_CLASSE", "CNPJ_FUNDO_CLASSE", "CNPJ_FUNDO_CLASSE", "CNPJ_FUNDO_CLASSE", "CNPJ_FUNDO_CLASSE", "CNPJ_FUNDO_CLASSE", "CNPJ_FUNDO_CLASSE", "CNPJ_FUNDO_CLASSE", "CNPJ_FUNDO_CLASSE", "CNPJ_FUNDO_CLASSE", "CNPJ_FUNDO_CLASSE", "CNPJ_FUNDO_CLASSE", "CNPJ_FUNDO_CLASSE", "CNPJ_FUNDO_CLASSE", "CNPJ_FUNDO_CLASSE", "CNPJ_FUNDO_CLASSE", "CNPJ_FUNDO_CLASSE", "CNPJ_FUNDO_CLASSE", "CNPJ_FUNDO_CLASSE", "CNPJ_FUNDO_CLASSE", "CNPJ_FUNDO_CLASSE", "CNPJ_FUNDO_CLASSE", "CNPJ_FUNDO_CLASSE", "CNPJ_FUNDO_CLASSE", "CNPJ_FUNDO_CLASSE", "CNPJ_FUNDO_CLASSE", "CNPJ_FUNDO_CLASSE", "CNPJ_FUNDO_CLASSE", "CNPJ_FUNDO_CLASSE", "CNPJ_FUNDO_CLASSE", "CNPJ_FUNDO_CLASSE", "CNPJ_FUNDO_CLASSE", "CNPJ_FUNDO_CLASSE", "CNPJ_FUNDO_CLASSE", "CNPJ_FUNDO_CLASSE", "CNPJ_FUNDO_CLASSE", "CNPJ_FUNDO_CLASSE", "CNPJ_FUNDO_CLASSE", "CNPJ_FUNDO_CLASSE", "CNPJ_FUNDO_CLASSE", "CNPJ_FUNDO_CLASSE", "CNPJ_FUNDO_CLASSE", "CNPJ_FUNDO_CLASSE", "CNPJ_FUNDO_CLASSE", "CNPJ_FUNDO_CLASSE", "CNPJ_FUNDO_CLASSE", "CNPJ_FUNDO_CLASSE", "CNPJ_FUNDO_CLASSE", "CNPJ_FUNDO_CLASSE", "CNPJ_FUNDO_CLASSE", "CNPJ_FUNDO_CLASSE", "CNPJ_FUNDO_CLASSE", "CNPJ_FUNDO_CLASSE", "CNPJ_FUNDO_CLASSE", "CNPJ_FUNDO_CLASSE", "CNPJ_FUNDO_CLASSE", "CNPJ_FUNDO_CLASSE", "CNPJ_FUNDO_CLASSE", "CNPJ_FUNDO_CLASSE", "CNPJ_FUNDO_CLASSE", "CNPJ_FUNDO_CLASSE", "CNPJ_FUNDO_CLASSE", "CNPJ_FUNDO_CLASSE", "CNPJ_FUNDO_CLASSE", "CNPJ_FUNDO_CLASSE", "CNPJ_FUNDO_CLASSE", "CNPJ_FUNDO_CLASSE", "CNPJ_FUNDO_CLASSE", "CNPJ_FUNDO_CLASSE", "CNPJ_FUNDO_CLASSE", "CNPJ_FUNDO_CLASSE", "CNPJ_FUNDO_CLASSE", "CNPJ_FUNDO_CLASSE", "CNPJ_FUNDO_CLASSE", "CNPJ_FUNDO_CLASSE", "CNPJ_FUNDO_CLASSE", "CNPJ_FUNDO_CLASSE", "CNPJ_FUNDO_CLASSE", "CNPJ_FUNDO_CLASSE", "CNPJ_FUNDO_CLASSE", "CNPJ_FUNDO_CLASSE", "CNPJ_FUNDO_CLASSE", "CNPJ_FUNDO_CLASSE", "CNPJ_FUNDO_CLASSE", "CNPJ_FUNDO_CLASSE", "CNPJ_FUNDO_CLASSE", "CNPJ_FUNDO_CLASSE", "CNPJ_FUNDO_CLASSE", "CNPJ_FUNDO_CLASSE", "CNPJ_FUNDO_CLASSE", "CNPJ_FUNDO_CLASSE", "CNPJ_FUNDO_CLASSE", "CNPJ_FUNDO_CLASSE", "CNPJ_FUNDO_CLASSE", "CNPJ_FUNDO_CLASSE", "CNPJ_FUNDO_CLASSE", "CNPJ_FUNDO_CLASSE", "CNPJ_FUNDO_CLASSE", "CNPJ_FUNDO_CLASSE", "CNPJ_FUNDO_CLASSE", "CNPJ_FUNDO_CLASSE", "CNPJ_FUNDO_CLASSE", "CNPJ_FUNDO_CLASSE", "CNPJ_FUNDO_CLASSE", "CNPJ_FUNDO_CLASSE", "CNPJ_FUNDO_CLASSE", "CNPJ_FUNDO_CLASSE", "CNPJ_FUNDO_CLASSE", "CNPJ_FUNDO_CLASSE", "CNPJ_FUNDO_CLASSE", "CNPJ_FUNDO_CLASSE", "CNPJ_FUNDO_CLASSE", "CNPJ_FUNDO_CLASSE", "CNPJ_FUNDO_CLASSE", "CNPJ_FUNDO_CLASSE", "CNPJ_FUNDO_CLASSE", "CNPJ_FUNDO_CLASSE", "CNPJ_FUNDO_CLASSE", "CNPJ_FUNDO_CLASSE", "CNPJ_FUNDO_CLASSE", "CNPJ_FUNDO_CLASSE", "CNPJ_FUNDO_CLASSE", "CNPJ_FUNDO_CLASSE", "CNPJ_FUNDO_CLASSE", "CNPJ_FUNDO_CLASSE", "CNPJ_FUNDO_CLASSE", "CNPJ_FUNDO_CLASSE", "CNPJ_FUNDO_CLASSE", "CNPJ_FUNDO_CLASSE", "CNPJ_FUNDO_CLASSE", "CNPJ_FUNDO_CLASSE", "CNPJ_FUNDO_CLASSE", "CNPJ_FUNDO_CLASSE", "CNPJ_FUNDO_CLASSE", "CNPJ_FUNDO_CLASSE", "CNPJ_FUNDO_CLASSE", "CNPJ_FUNDO_CLASSE", "CNPJ_FUNDO_CLASSE", "CNPJ_FUNDO_CLASSE", "CNPJ_FUNDO_CLASSE", "CNPJ_FUNDO_CLASSE", "CNPJ_FUNDO_CLASSE", "CNPJ_FUNDO_CLASSE", "CNPJ_FUNDO_CLASSE", "CNPJ_FUNDO_CLASSE", "CNPJ_FUNDO_CLASSE", "CNPJ_FUNDO_CLASSE", "CNPJ_FUNDO_CLASSE", "CNPJ_FUNDO_CLASSE", "CNPJ_FUNDO_CLASSE", "CNPJ_FUNDO_CLASSE", "CNPJ_FUNDO_CLASSE", "CNPJ_FUNDO_CLASSE", "CNPJ_FUNDO_CLASSE", "CNPJ_FUNDO_CLASSE", "CNPJ_FUNDO_CLASSE", "CNPJ_FUNDO_CLASSE", "CNPJ_FUNDO_CLASSE", "CNPJ_FUNDO_CLASSE", "CNPJ_FUNDO_CLASSE", "CNPJ_FUNDO_CLASSE", "CNPJ_FUNDO_CLASSE", "CNPJ_FUNDO_CLASSE", "CNPJ_FUNDO_CLASSE", "CNPJ_FUNDO_CLASSE", "CNPJ_FUNDO_CLASSE", "CNPJ_FUNDO_CLASSE", "CNPJ_FUNDO_CLASSE", "CNPJ_FUNDO_CLASSE", "CNPJ_FUNDO_CLASSE", "CNPJ_FUNDO_CLASSE", "CNPJ_FUNDO_CLASSE", "CNPJ_FUNDO_CLASSE", "CNPJ_FUNDO_CLASSE", "CNPJ_FUNDO_CLASSE", "CNPJ_FUNDO_CLASSE", "CNPJ_FUNDO_CLASSE", "CNPJ_FUNDO_CLASSE", "CNPJ_FUNDO_CLASSE", "CNPJ_FUNDO_CLASSE", "CNPJ_FUNDO_CLASSE", "CNPJ_FUNDO_CLASSE", "CNPJ_FUNDO_CLASSE", "CNPJ_FUNDO_CLASSE", "CNPJ_FUNDO_CLASSE", "CNPJ_FUNDO_CLASSE", "CNPJ_FUNDO_CLASSE", "CNPJ_FUNDO_CLASSE", "CNPJ_FUNDO_CLASSE", "CNPJ_FUNDO_CLASSE", "CNPJ_FUNDO_CLASSE", "CNPJ_FUNDO_CLASSE", "CNPJ_FUNDO_CLASSE", "CNPJ_FUNDO_CLASSE", "CNPJ_FUNDO_CLASSE", "CNPJ_FUNDO_CLASSE", "CNPJ_FUNDO_CLASSE", "CNPJ_FUNDO_CLASSE", "CNPJ_FUNDO_CLASSE", "CNPJ_FUNDO_CLASSE", "CNPJ_FUNDO_CLASSE", "CNPJ_FUNDO_CLASSE", "CNPJ_FUNDO_CLASSE", "CNPJ_FUNDO_CLASSE", "CNPJ_FUNDO_CLASSE", "CNPJ_FUNDO_CLASSE", "CNPJ_FUNDO_CLASSE", "CNPJ_FUNDO_CLASSE", "CNPJ_FUNDO_CLASSE", "CNPJ_FUNDO_CLASSE", "CNPJ_FUNDO_CLASSE", "CNPJ_FUNDO_CLASSE", "CNPJ_FUNDO_CLASSE", "CNPJ_FUNDO_CLASSE", "CNPJ_FUNDO_CLASSE", "CNPJ_FUNDO_CLASSE", "CNPJ_FUNDO_CLASSE", "CNPJ_FUNDO_CLASSE", "CNPJ_FUNDO_CLASSE", "CNPJ_FUNDO_CLASSE", "CNPJ_FUNDO_CLASSE", "CNPJ_FUNDO_CLASSE", "CNPJ_FUNDO_CLASSE", "CNPJ_FUNDO_CLASSE", "CNPJ_FUNDO_CLASSE", "CNPJ_FUNDO_CLASSE", "CNPJ_FUNDO_CLASSE", "CNPJ_FUNDO_CLASSE", "CNPJ_FUNDO_CLASSE", "CNPJ_FUNDO_CLASSE", "CNPJ_FUNDO_CLASSE", "CNPJ_FUNDO_CLASSE", "CNPJ_FUNDO_CLASSE", "CNPJ_FUNDO_CLASSE", "CNPJ_FUNDO_CLASSE", "CNPJ_FUNDO_CLASSE", "CNPJ_FUNDO_CLASSE", "CNPJ_FUNDO_CLASSE", "CNPJ_FUNDO_CLASSE", "CNPJ_FUNDO_CLASSE", "CNPJ_FUNDO_CLASSE", "CNPJ_FUNDO_CLASSE", "CNPJ_FUNDO_CLASSE", "CNPJ_FUNDO_CLASSE", "CNPJ_FUNDO_CLASSE", "CNPJ_FUNDO_CLASSE", "CNPJ_FUNDO_CLASSE", "CNPJ_FUNDO_CLASSE", "CNPJ_FUNDO_CLASSE", "CNPJ_FUNDO_CLASSE", "CNPJ_FUNDO_CLASSE", "CNPJ_FUNDO_CLASSE", "CNPJ_FUNDO_CLASSE", "CNPJ_FUNDO_CLASSE", "CNPJ_FUNDO_CLASSE", "CNPJ_FUNDO_CLASSE", "CNPJ_FUNDO_CLASSE", "CNPJ_FUNDO_CLASSE", "CNPJ_FUNDO_CLASSE", "CNPJ_FUNDO_CLASSE", "CNPJ_FUNDO_CLASSE", "CNPJ_FUNDO_CLASSE", "CNPJ_FUNDO_CLASSE", "CNPJ_FUNDO_CLASSE", "CNPJ_FUNDO_CLASSE", "CNPJ_FUNDO_CLASSE", "CNPJ_FUNDO_CLASSE", "CNPJ_FUNDO_CLASSE", "CNPJ_FUNDO_CLASSE", "CNPJ_FUNDO_CLASSE", "CNPJ_FUNDO_CLASSE", "CNPJ_FUNDO_CLASSE", "CNPJ_FUNDO_CLASSE", "CNPJ_FUNDO_CLASSE", "CNPJ_FUNDO_CLASSE", "CNPJ_FUNDO_CLASSE", "CNPJ_FUNDO_CLASSE", "CNPJ_FUNDO_CLASSE", "CNPJ_FUNDO_CLASSE", "CNPJ_FUNDO_CLASSE", "CNPJ_FUNDO_CLASSE", "CNPJ_FUNDO_CLASSE", "CNPJ_FUNDO_CLASSE", "CNPJ_FUNDO_CLASSE", "CNPJ_FUNDO_CLASSE", "CNPJ_FUNDO_CLASSE", "CNPJ_FUNDO_CLASSE", "CNPJ_FUNDO_CLASSE", "CNPJ_FUNDO_CLASSE", "CNPJ_FUNDO_CLASSE", "CNPJ_FUNDO_CLASSE", "CNPJ_FUNDO_CLASSE", "CNPJ_FUNDO_CLASSE", "CNPJ_FUNDO_CLASSE", "CNPJ_FUNDO_CLASSE", "CNPJ_FUNDO_CLASSE", "CNPJ_FUNDO_CLASSE", "CNPJ_FUNDO_CLASSE", "CNPJ_FUNDO_CLASSE", "CNPJ_FUNDO_CLASSE", "CNPJ_FUNDO_CLASSE", "CNPJ_FUNDO_CLASSE", "CNPJ_FUNDO_CLASSE", "CNPJ_FUNDO_CLASSE", "CNPJ_FUNDO_CLASSE", "CNPJ_FUNDO_CLASSE", "CNPJ_FUNDO_CLASSE", "CNPJ_FUNDO_CLASSE", "CNPJ_FUNDO_CLASSE", "CNPJ_FUNDO_CLASSE", "CNPJ_FUNDO_CLASSE", "CNPJ_FUNDO_CLASSE", "CNPJ_FUNDO_CLASSE", "CNPJ_FUNDO_CLASSE", "CNPJ_FUNDO_CLASSE", "CNPJ_FUNDO_CLASSE", "CNPJ_FUNDO_CLASSE", "CNPJ_FUNDO_CLASSE", "CNPJ_FUNDO_CLASSE", "CNPJ_FUNDO_CLASSE", "CNPJ_FUNDO_CLASSE", "CNPJ_FUNDO_CLASSE", "CNPJ_FUNDO_CLASSE", "CNPJ_FUNDO_CLASSE", "CNPJ_FUNDO_CLASSE", "CNPJ_FUNDO_CLASSE", "CNPJ_FUNDO_CLASSE", "CNPJ_FUNDO_CLASSE", "CNPJ_FUNDO_CLASSE", "CNPJ_FUNDO_CLASSE", "CNPJ_FUNDO_CLASSE", "CNPJ_FUNDO_CLASSE", "CNPJ_FUNDO_CLASSE", "CNPJ_FUNDO_CLASSE", "CNPJ_FUNDO_CLASSE", "CNPJ_FUNDO_CLASSE", "CNPJ_FUNDO_CLASSE", "CNPJ_FUNDO_CLASSE", "CNPJ_FUNDO_CLASSE", "CNPJ_FUNDO_CLASSE", "CNPJ_FUNDO_CLASSE", "CNPJ_FUNDO_CLASSE", "CNPJ_FUNDO_CLASSE", "CNPJ_FUNDO_CLASSE", "CNPJ_FUNDO_CLASSE", "CNPJ_FUNDO_CLASSE", "CNPJ_FUNDO_CLASSE", "CNPJ_FUNDO_CLASSE", "CNPJ_FUNDO_CLASSE", "CNPJ_FUNDO_CLASSE", "CNPJ_FUNDO_CLASSE", "CNPJ_FUNDO_CLASSE", "CNPJ_FUNDO_CLASSE", "CNPJ_FUNDO_CLASSE", "CNPJ_FUNDO_CLASSE", "CNPJ_FUNDO_CLASSE", "CNPJ_FUNDO_CLASSE", "CNPJ_FUNDO_CLASSE", "CNPJ_FUNDO_CLASSE", "CNPJ_FUNDO_CLASSE", "CNPJ_FUNDO_CLASSE", "CNPJ_FUNDO_CLASSE", "CNPJ_FUNDO_CLASSE", "CNPJ_FUNDO_CLASSE", "CNPJ_FUNDO_CLASSE", "CNPJ_FUNDO_CLASSE", "CNPJ_FUNDO_CLASSE", "CNPJ_FUNDO_CLASSE", "CNPJ_FUNDO_CLASSE", "CNPJ_FUNDO_CLASSE", "CNPJ_FUNDO_CLASSE", "CNPJ_FUNDO_CLASSE", "CNPJ_FUNDO_CLASSE", "CNPJ_FUNDO_CLASSE", "CNPJ_FUNDO_CLASSE", "CNPJ_FUNDO_CLASSE", "CNPJ_FUNDO_CLASSE", "CNPJ_FUNDO_CLASSE", "CNPJ_FUNDO_CLASSE", "CNPJ_FUNDO_CLASSE", "CNPJ_FUNDO_CLASSE", "CNPJ_FUNDO_CLASSE", "CNPJ_FUNDO_CLASSE", "CNPJ_FUNDO_CLASSE", "CNPJ_FUNDO_CLASSE", "CNPJ_FUNDO_CLASSE", "CNPJ_FUNDO_CLASSE", "CNPJ_FUNDO_CLASSE", "CNPJ_FUNDO_CLASSE", "CNPJ_FUNDO_CLASSE", "CNPJ_FUNDO_CLASSE", "CNPJ_FUNDO_CLASSE", "CNPJ_FUNDO_CLASSE", "CNPJ_FUNDO_CLASSE", "CNPJ_FUNDO_CLASSE", "CNPJ_FUNDO_CLASSE", "CNPJ_FUNDO_CLASSE", "CNPJ_FUNDO_CLASSE", "CNPJ_FUNDO_CLASSE", "CNPJ_FUNDO_CLASSE", "CNPJ_FUNDO_CLASSE", "CNPJ_FUNDO_CLASSE", "CNPJ_FUNDO_CLASSE", "CNPJ_FUNDO_CLASSE", "CNPJ_FUNDO_CLASSE", "CNPJ_FUNDO_CLASSE", "CNPJ_FUNDO_CLASSE", "CNPJ_FUNDO_CLASSE", "CNPJ_FUNDO_CLASSE", "CNPJ_FUNDO_CLASSE", "CNPJ_FUNDO_CLASSE", "CNPJ_FUNDO_CLASSE", "CNPJ_FUNDO_CLASSE", "CNPJ_FUNDO_CLASSE", "CNPJ_FUNDO_CLASSE", "CNPJ_FUNDO_CLASSE", "CNPJ_FUNDO_CLASSE", "CNPJ_FUNDO_CLASSE", "CNPJ_FUNDO_CLASSE"]
  );

  const idxDt = pickColumnIndex(
    headers,
    ["DT_COMPTC", "DT_COMPETENCIA", "DATA_COMPETENCIA", "DT_REF", "DATA_REFERENCIA", "DATA_REFERENCIA"],
    ["DATA_REFER", "DATA_REF", "COMPET"]
  );

  // PL costuma estar no arquivo "geral", com nomes tipo Patrimonio_Liquido / Patrim_Liq / Total_Patrimonio_Liquido etc.
  const idxPL = pickColumnIndex(
    headers,
    ["VL_PATRIM_LIQ", "VL_PL", "PATRIM_LIQ", "PATRIMONIO_LIQUIDO"],
    ["PATRIM", "LIQUIDO", "PATRIMONIO"]
  );

  // Qtde de cotas costuma estar no arquivo "geral", com nomes tipo Quantidade_Cotas / Qt_Cotas / Nr_Cotas etc.
  const idxQtdCotas = pickColumnIndex(
    headers,
    ["QT_COTA", "QT_COTAS", "QTD_COTAS", "NR_COTAS"],
    ["COTA", "COTAS", "QUANTIDADE"]
  );

  return { idxCnpj, idxDt, idxPL, idxQtdCotas };
}

function listZipCsvFiles(extractedDir) {
  const files = fs.readdirSync(extractedDir);
  return files.filter((f) => f.toLowerCase().endsWith(".csv") || f.toLowerCase().endsWith(".txt"));
}

function chooseBestCsv(files) {
  // Preferência: "geral" (onde normalmente estão PL e qtde cotas)
  const lower = files.map((f) => f.toLowerCase());

  const idxGeral = lower.findIndex((f) => f.includes("_geral_"));
  if (idxGeral >= 0) return files[idxGeral];

  // Se não tiver "geral", tenta qualquer que tenha "geral"
  const idxGeral2 = lower.findIndex((f) => f.includes("geral"));
  if (idxGeral2 >= 0) return files[idxGeral2];

  // Fallback (primeiro)
  return files[0];
}

function downloadAndUnzip(url, destDir) {
  fs.mkdirSync(destDir, { recursive: true });
  const zipPath = path.join(destDir, "inf_mensal.zip");

  log("Baixando:", url);
  execSync(`curl -L --fail --silent --show-error "${url}" -o "${zipPath}"`, { stdio: "inherit" });

  log("Extraindo zip...");
  execSync(`unzip -o "${zipPath}" -d "${destDir}"`, { stdio: "inherit" });

  const csvs = listZipCsvFiles(destDir);
  if (csvs.length === 0) throw new Error("Nenhum CSV/TXT encontrado dentro do ZIP.");

  const chosen = chooseBestCsv(csvs);
  return path.join(destDir, chosen);
}

function latestYearCandidates() {
  const now = new Date();
  const y = now.getFullYear();
  return [y, y - 1];
}

function buildCvmZipUrl(year) {
  return `https://dados.cvm.gov.br/dados/FII/DOC/INF_MENSAL/DADOS/inf_mensal_fii_${year}.zip`;
}

function isoToday() {
  return new Date().toISOString().slice(0, 10);
}

function main() {
  const repoRoot = process.cwd();
  const dataDir = path.join(repoRoot, "data");

  const mapPath = path.join(dataDir, "fii_cnpj_map.json");
  const outPath = path.join(dataDir, "fiis_fundamentals.json");

  if (!fs.existsSync(mapPath)) {
    throw new Error(`Arquivo não encontrado: ${mapPath} (crie data/fii_cnpj_map.json)`);
  }

  const mapJson = readJson(mapPath);
  const mapItems = Array.isArray(mapJson.items) ? mapJson.items : [];

  const tickerToCnpj = new Map();
  for (const it of mapItems) {
    const ticker = String(it.ticker || "").toUpperCase().trim();
    const cnpj = normalizeCnpj(it.cnpj);
    if (ticker && cnpj) tickerToCnpj.set(ticker, cnpj);
  }

  if (tickerToCnpj.size === 0) {
    throw new Error("Seu fii_cnpj_map.json não tem nenhum CNPJ preenchido ainda.");
  }

  const tmp = fs.mkdtempSync(path.join(os.tmpdir(), "cvm-inf-mensal-"));
  let csvPath = null;
  let lastErr = null;

  for (const y of latestYearCandidates()) {
    try {
      const url = buildCvmZipUrl(y);
      csvPath = downloadAndUnzip(url, tmp);
      log("Arquivo detectado:", csvPath);
      break;
    } catch (e) {
      lastErr = e;
      log(`Falhou ao baixar/ler ano ${y}:`, String(e));
    }
  }

  if (!csvPath) throw lastErr || new Error("Falha ao obter CSV da CVM.");

  // encoding: CVM costuma estar em latin1
  const content = fs.readFileSync(csvPath, "latin1");
  const lines = content.split(/\r?\n/).filter(Boolean);
  if (lines.length < 2) throw new Error("CSV sem conteúdo suficiente.");

  const headers = parseDelimitedLine(lines[0], ";");
  const { idxCnpj, idxDt, idxPL, idxQtdCotas } = detectColumns(headers);

  if (idxCnpj < 0 || idxDt < 0 || idxPL < 0 || idxQtdCotas < 0) {
    log("Headers:", headers);
    throw new Error(
      `Não consegui detectar colunas (cnpj=${idxCnpj}, dt=${idxDt}, pl=${idxPL}, cotas=${idxQtdCotas}).`
    );
  }

  const cnpjsWanted = new Set([...tickerToCnpj.values()]);
  let maxCompet = "";

  // Pass 1: achar maior Data_Referencia/competência
  for (let i = 1; i < lines.length; i++) {
    const row = parseDelimitedLine(lines[i], ";");
    const cnpj = normalizeCnpj(row[idxCnpj]);
    if (!cnpjsWanted.has(cnpj)) continue;

    const dt = String(row[idxDt] || "").trim();
    if (dt && dt > maxCompet) maxCompet = dt;
  }

  if (!maxCompet) throw new Error("Não encontrei nenhuma competência para os CNPJs do seu mapa.");

  log("Competência mais recente detectada:", maxCompet);

  // Pass 2: PL e cotas para a competência mais recente
  const cnpjToVp = new Map();

  for (let i = 1; i < lines.length; i++) {
    const row = parseDelimitedLine(lines[i], ";");
    const cnpj = normalizeCnpj(row[idxCnpj]);
    if (!cnpjsWanted.has(cnpj)) continue;

    const dt = String(row[idxDt] || "").trim();
    if (dt !== maxCompet) continue;

    const pl = toNumberBR(row[idxPL]);
    const qtd = toNumberBR(row[idxQtdCotas]);

    if (!Number.isFinite(pl) || !Number.isFinite(qtd) || qtd <= 0) continue;

    const vp = pl / qtd;
    if (Number.isFinite(vp) && vp > 0) cnpjToVp.set(cnpj, vp);
  }

  const items = [];
  let filled = 0;

  for (const [ticker, cnpj] of tickerToCnpj.entries()) {
    const vp = cnpjToVp.get(cnpj);
    if (vp != null) filled++;

    items.push({
      ticker,
      vp: vp ?? null,
      dy12m: null,
      pl: null,
    });
  }

  const out = {
    updatedAt: isoToday(),
    source: "CVM - Informe Mensal Estruturado",
    competence: maxCompet,
    items,
  };

  writeJson(outPath, out);

  log(`OK: VP preenchido para ${filled}/${items.length} tickers (os demais ficaram null).`);
  log("Gerado:", outPath);
}

main();
