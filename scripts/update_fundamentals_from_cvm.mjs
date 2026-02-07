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
  // CSV da CVM costuma ser simples (sem aspas complexas na maioria dos casos)
  // Se tiver aspas, ainda funciona para casos comuns.
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

function pickColumnIndex(headers, candidates) {
  const H = headers.map((h) => h.toUpperCase());
  for (const cand of candidates) {
    const idx = H.findIndex((h) => h === cand);
    if (idx >= 0) return idx;
  }
  // fallback por "includes"
  for (const cand of candidates) {
    const idx = H.findIndex((h) => h.includes(cand));
    if (idx >= 0) return idx;
  }
  return -1;
}

function detectColumns(headers) {
  // Heurística: o layout pode mudar, então tentamos várias opções.
  const idxCnpj = pickColumnIndex(headers, [
    "CNPJ_FUNDO",
    "CNPJ",
    "CNPJ_CLASSE",
    "CNPJ_FUNDO_CLASSE",
    "CNPJ_DO_FUNDO"
  ]);

  const idxDt = pickColumnIndex(headers, [
    "DT_COMPTC",
    "DT_COMPETENCIA",
    "DATA_COMPETENCIA",
    "DT_REF",
    "DATA_REFERENCIA"
  ]);

  const idxPL = pickColumnIndex(headers, [
    "VL_PATRIM_LIQ",
    "VL_PATRIMONIO_LIQUIDO",
    "PATRIM_LIQ",
    "PATRIMONIO_LIQUIDO",
    "VL_PL"
  ]);

  const idxQtdCotas = pickColumnIndex(headers, [
    "QT_COTA",
    "QT_COTAS",
    "QTD_COTAS",
    "NR_COTAS",
    "QT_COTAS_EMITIDAS",
    "QTD_COTAS_EMITIDAS"
  ]);

  return { idxCnpj, idxDt, idxPL, idxQtdCotas };
}

function listZipCsvFiles(extractedDir) {
  const files = fs.readdirSync(extractedDir);
  return files.filter((f) => f.toLowerCase().endsWith(".csv") || f.toLowerCase().endsWith(".txt"));
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
  // normalmente é 1 arquivo principal
  return path.join(destDir, csvs[0]);
}

function latestYearCandidates() {
  const now = new Date();
  const y = now.getFullYear();
  return [y, y - 1]; // segurança
}

function buildCvmZipUrl(year) {
  // URLs listadas no diretório oficial da CVM
  return `https://dados.cvm.gov.br/dados/FII/DOC/INF_MENSAL/DADOS/inf_mensal_fii_${year}.zip`;
}

function isoToday() {
  const d = new Date();
  return d.toISOString().slice(0, 10);
}

function main() {
  const repoRoot = process.cwd();
  const dataDir = path.join(repoRoot, "data");
  const scriptsDir = path.join(repoRoot, "scripts");

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

  // 1) Baixa o zip do ano atual (com fallback)
  const tmp = fs.mkdtempSync(path.join(os.tmpdir(), "cvm-inf-mensal-"));
  let csvPath = null;

  const years = latestYearCandidates();
  let lastErr = null;

  for (const y of years) {
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

  // 2) Lê linha a linha (stream)
  const content = fs.readFileSync(csvPath, "latin1"); // CVM costuma vir em latin1
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

  // 3) Primeiro pass: descobre a maior competência disponível por CNPJ mapeado
  const cnpjsWanted = new Set([...tickerToCnpj.values()]);
  let maxCompet = ""; // YYYY-MM-DD ou YYYYMM, etc.

  for (let i = 1; i < lines.length; i++) {
    const row = parseDelimitedLine(lines[i], ";");
    const cnpj = normalizeCnpj(row[idxCnpj]);
    if (!cnpjsWanted.has(cnpj)) continue;

    const dt = String(row[idxDt] || "").trim();
    // usamos comparação lexicográfica (funciona bem para ISO e YYYYMM)
    if (dt && dt > maxCompet) maxCompet = dt;
  }

  if (!maxCompet) throw new Error("Não encontrei nenhuma competência para os CNPJs do seu mapa.");

  log("Competência mais recente detectada:", maxCompet);

  // 4) Segundo pass: extrai PL e QtdCotas para essa competência
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
    if (Number.isFinite(vp) && vp > 0) {
      cnpjToVp.set(cnpj, vp);
    }
  }

  // 5) Monta output
  const items = [];
  let filled = 0;

  for (const [ticker, cnpj] of tickerToCnpj.entries()) {
    const vp = cnpjToVp.get(cnpj);
    if (vp != null) filled++;

    items.push({
      ticker,
      vp: vp ?? null,
      dy12m: null,
      pl: null
    });
  }

  const out = {
    updatedAt: isoToday(),
    source: "CVM - Informe Mensal Estruturado",
    competence: maxCompet,
    items
  };

  writeJson(outPath, out);

  log(`OK: VP preenchido para ${filled}/${items.length} tickers (os demais ficaram null).`);
  log("Gerado:", outPath);
}

main();
