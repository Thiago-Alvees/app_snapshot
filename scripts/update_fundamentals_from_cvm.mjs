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
  const norm = s.replace(/\./g, "").replace(",", ".");
  const n = Number(norm);
  return Number.isFinite(n) ? n : NaN;
}

function pickColumnIndex(headers, exact = [], includes = []) {
  const H = headers.map((h) => String(h).toUpperCase());
  for (const cand of exact) {
    const C = String(cand).toUpperCase();
    const idx = H.findIndex((h) => h === C);
    if (idx >= 0) return idx;
  }
  for (const cand of includes) {
    const C = String(cand).toUpperCase();
    const idx = H.findIndex((h) => h.includes(C));
    if (idx >= 0) return idx;
  }
  return -1;
}

function listZipCsvFiles(extractedDir) {
  const files = fs.readdirSync(extractedDir);
  return files.filter((f) => f.toLowerCase().endsWith(".csv") || f.toLowerCase().endsWith(".txt"));
}

function chooseFile(files, keyword) {
  const lower = files.map((f) => f.toLowerCase());
  const idx = lower.findIndex((f) => f.includes(keyword));
  if (idx >= 0) return files[idx];
  return null;
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

  const geral = chooseFile(csvs, "_geral_");
  const comp = chooseFile(csvs, "_complemento_");
  const ap = chooseFile(csvs, "_ativo_passivo_");

  if (!geral) throw new Error("Não encontrei arquivo *_geral_* no ZIP.");
  if (!comp && !ap) throw new Error("Não encontrei *_complemento_* nem *_ativo_passivo_* no ZIP.");

  return {
    geralPath: path.join(destDir, geral),
    compPath: comp ? path.join(destDir, comp) : null,
    apPath: ap ? path.join(destDir, ap) : null,
  };
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

function readCsvLines(filePath) {
  const content = fs.readFileSync(filePath, "latin1");
  return content.split(/\r?\n/).filter(Boolean);
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

  // baixa ZIP
  const tmp = fs.mkdtempSync(path.join(os.tmpdir(), "cvm-inf-mensal-"));
  let files = null;
  let lastErr = null;

  for (const y of latestYearCandidates()) {
    try {
      const url = buildCvmZipUrl(y);
      files = downloadAndUnzip(url, tmp);
      log("Arquivos detectados:", files);
      break;
    } catch (e) {
      lastErr = e;
      log(`Falhou ao baixar/ler ano ${y}:`, String(e));
    }
  }
  if (!files) throw lastErr || new Error("Falha ao obter arquivos da CVM.");

  // Lê "geral": tem CNPJ, Data_Referencia, Quantidade_Cotas_Emitidas
  const geralLines = readCsvLines(files.geralPath);
  const geralHeaders = parseDelimitedLine(geralLines[0], ";");

  const idxGeralCnpj = pickColumnIndex(geralHeaders, ["CNPJ_FUNDO_CLASSE", "CNPJ_FUNDO_CLASSE"], ["CNPJ_FUNDO_CLASSE", "CNPJ_FUNDO"]);
  const idxGeralDt = pickColumnIndex(geralHeaders, ["DATA_REFERENCIA"], ["DATA_REFER"]);
  const idxQtdCotas = pickColumnIndex(geralHeaders, ["QUANTIDADE_COTAS_EMITIDAS"], ["COTAS_EMITID", "QUANTIDADE_COTAS"]);

  if (idxGeralCnpj < 0 || idxGeralDt < 0 || idxQtdCotas < 0) {
    log("Headers geral:", geralHeaders);
    throw new Error(`Não detectei colunas no GERAL (cnpj=${idxGeralCnpj}, dt=${idxGeralDt}, cotas=${idxQtdCotas}).`);
  }

  // Vamos montar map (cnpj|dt) -> qtdCotas
  const qtdMap = new Map();
  const cnpjsWanted = new Set([...tickerToCnpj.values()]);
  let maxDt = "";

  for (let i = 1; i < geralLines.length; i++) {
    const row = parseDelimitedLine(geralLines[i], ";");
    const cnpj = normalizeCnpj(row[idxGeralCnpj]);
    if (!cnpjsWanted.has(cnpj)) continue;

    const dt = String(row[idxGeralDt] || "").trim();
    const qtd = toNumberBR(row[idxQtdCotas]);

    if (dt && dt > maxDt) maxDt = dt;
    if (dt && Number.isFinite(qtd) && qtd > 0) {
      qtdMap.set(`${cnpj}|${dt}`, qtd);
    }
  }

  if (!maxDt) throw new Error("Não encontrei Data_Referencia para os CNPJs do seu mapa no arquivo GERAL.");

  log("Data_Referencia mais recente (base GERAL):", maxDt);

  // Agora precisamos do PL. Ele NÃO está no geral.
  // Vamos procurar PL em complemento e/ou ativo_passivo:
  // Tentamos detectar uma coluna que pareça "Patrimonio_Liquido" / "VL_Patrim_Liq" / etc.
  function buildPlMapFromFile(filePath, label) {
    if (!filePath) return new Map();

    const lines = readCsvLines(filePath);
    const headers = parseDelimitedLine(lines[0], ";");

    const idxCnpj = pickColumnIndex(headers, ["CNPJ_FUNDO_CLASSE", "CNPJ_FUNDO_CLASSE"], ["CNPJ_FUNDO_CLASSE", "CNPJ_FUNDO"]);
    const idxDt = pickColumnIndex(headers, ["DATA_REFERENCIA"], ["DATA_REFER"]);
    const idxPL = pickColumnIndex(
      headers,
      ["VL_PATRIM_LIQ", "VL_PL", "PATRIMONIO_LIQUIDO", "PATRIM_LIQ"],
      ["PATRIM", "LIQUIDO"]
    );

    if (idxCnpj < 0 || idxDt < 0 || idxPL < 0) {
      log(`Headers ${label}:`, headers);
      // não explode aqui; apenas retorna vazio
      log(`Não detectei PL em ${label} (cnpj=${idxCnpj}, dt=${idxDt}, pl=${idxPL})`);
      return new Map();
    }

    const plMap = new Map();

    for (let i = 1; i < lines.length; i++) {
      const row = parseDelimitedLine(lines[i], ";");
      const cnpj = normalizeCnpj(row[idxCnpj]);
      if (!cnpjsWanted.has(cnpj)) continue;

      const dt = String(row[idxDt] || "").trim();
      if (dt !== maxDt) continue;

      const pl = toNumberBR(row[idxPL]);
      if (Number.isFinite(pl) && pl > 0) {
        plMap.set(`${cnpj}|${dt}`, pl);
      }
    }

    return plMap;
  }

  const plMapComp = buildPlMapFromFile(files.compPath, "COMPLEMENTO");
  const plMapAp = buildPlMapFromFile(files.apPath, "ATIVO_PASSIVO");

  // Preferência: complemento, senão ativo_passivo
  const plMap = plMapComp.size > 0 ? plMapComp : plMapAp;

  if (plMap.size === 0) {
    throw new Error(
      "Não consegui localizar a coluna de Patrimônio Líquido nem no COMPLEMENTO nem no ATIVO_PASSIVO. " +
      "Cole aqui os headers do COMPLEMENTO (primeira linha) que eu ajusto os candidatos."
    );
  }

  // Agora calculamos VP = PL / QtdCotas
  const cnpjToVp = new Map();

  for (const cnpj of cnpjsWanted) {
    const key = `${cnpj}|${maxDt}`;
    const pl = plMap.get(key);
    const qtd = qtdMap.get(key);

    if (!Number.isFinite(pl) || !Number.isFinite(qtd) || qtd <= 0) continue;

    const vp = pl / qtd;
    if (Number.isFinite(vp) && vp > 0) cnpjToVp.set(cnpj, vp);
  }

  // Monta saída
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
    source: "CVM - Informe Mensal Estruturado (join geral + complemento/ativo_passivo)",
    referenceDate: maxDt,
    items
  };

  writeJson(outPath, out);

  log(`OK: VP preenchido para ${filled}/${items.length} tickers (os demais ficaram null).`);
  log("Gerado:", outPath);
}

main();
