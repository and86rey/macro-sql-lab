// app.js (type="module")

// DuckDB-WASM via jsDelivr ESM bundle
import * as duckdb from "https://cdn.jsdelivr.net/npm/@duckdb/duckdb-wasm@1.30.0/+esm"; // version can be adjusted [web:2][web:8]

/**
 * CONFIGURATION: SQL Scenarios with Business Analytics Logic
 */
const SCENARIOS = {
  growth: {
    sql: `
      SELECT 
        date, 
        value, 
        LAG(value, 1) OVER (ORDER BY date) AS prev_year,
        ROUND(
          ((value - LAG(value, 1) OVER (ORDER BY date)) 
            / NULLIF(LAG(value, 1) OVER (ORDER BY date), 0)
          ) * 100, 2
        ) AS growth_pct
      FROM macro_data
      ORDER BY date DESC;
    `,
    insight:
      "<strong>Scenario: Liquidity Momentum.</strong> Calculates YoY growth using <code>LAG</code>. High growth indicates increasing monetary supply relative to GDP.",
    kpis: [{ col: "growth_pct", label: "YoY Growth %" }]
  },
  volatility: {
    sql: `
      SELECT 
        date, 
        value, 
        ROUND(
          AVG(value) OVER (
            ORDER BY date 
            ROWS BETWEEN 4 PRECEDING AND CURRENT ROW
          ), 2
        ) AS rolling_avg,
        ROUND(
          STDDEV_SAMP(value) OVER (
            ORDER BY date 
            ROWS BETWEEN 4 PRECEDING AND CURRENT ROW
          ), 2
        ) AS rolling_stddev
      FROM macro_data
      ORDER BY date DESC;
    `,
    insight:
      "<strong>Scenario: Risk Volatility.</strong> Uses <code>STDDEV_SAMP</code> over a 5-period window to detect regime shifts in monetary stability.",
    kpis: [{ col: "rolling_stddev", label: "5Y Rolling Volatility" }]
  },
  smoothing: {
    sql: `
      SELECT 
        date, 
        value, 
        ROUND(
          AVG(value) OVER (
            ORDER BY date 
            ROWS BETWEEN 4 PRECEDING AND CURRENT ROW
          ), 2
        ) AS short_term_ma,
        ROUND(
          AVG(value) OVER (
            ORDER BY date 
            ROWS BETWEEN 9 PRECEDING AND CURRENT ROW
          ), 2
        ) AS long_term_ma
      FROM macro_data
      ORDER BY date DESC;
    `,
    insight:
      "<strong>Scenario: Trend Convergence.</strong> Compares 5-year (Short) vs 10-year (Long) Moving Averages to identify structural trends.",
    kpis: [
      { col: "short_term_ma", label: "5Y Short-Term MA" },
      { col: "long_term_ma", label: "10Y Long-Term MA" }
    ]
  }
};

let db, conn, chart;

/**
 * World Bank API: Robust Pagination
 * indicator: e.g. "FM.LBL.MQMY.GD.ZS"
 * country:   e.g. "EMU"
 */

async function fetchAllWorldBankData(indicator, country) {
  let allData = [];
  let page = 1;
  let totalPages = 1;

  try {
    do {
      const url = `https://api.worldbank.org/v2/country/${country}/indicator/${indicator}?format=json&page=${page}&per_page=100`;
      console.log("WB URL:", url); // DEBUG

      const response = await fetch(url);
      console.log("WB status:", response.status); // DEBUG

      const json = await response.json();
      console.log("WB JSON page", page, ":", json); // DEBUG

      if (!json || !Array.isArray(json) || !json[1]) {
        break;
      }

      if (json[0] && typeof json[0].pages === "number") {
        totalPages = json[0].pages;
      } else {
        totalPages = 1;
      }

      const validObs = json[1]
        .filter(obs => obs && obs.value !== null && obs.date != null)
        .map(obs => ({
          date: parseInt(obs.date, 10),
          value: parseFloat(obs.value)
        }));

      allData = allData.concat(validObs);
      page++;
    } while (page <= totalPages);
  } catch (err) {
    console.error("World Bank error:", err);
    throw new Error(`World Bank API unreachable: ${err.message}`);
  }

  console.log("Total observations:", allData.length); // DEBUG
  return allData;
}

/**
 * DuckDB-WASM: Standard ESM Instantiation via jsDelivr
 */
async function initDuckDB() {
  const JSDELIVR_BUNDLES = duckdb.getJsDelivrBundles(); // [web:5][web:10]
  const bundle = await duckdb.selectBundle(JSDELIVR_BUNDLES);

  const worker_url = URL.createObjectURL(
    new Blob([`importScripts("${bundle.mainWorker}");`], {
      type: "text/javascript"
    })
  );

  const worker = new Worker(worker_url);
  const logger = new duckdb.ConsoleLogger();
  const db = new duckdb.AsyncDuckDB(logger, worker);

  await db.instantiate(bundle.mainModule, bundle.pthreadWorker);
  URL.revokeObjectURL(worker_url);

  return db;
}

/**
 * Initialization flow:
 * 1. Fetch macro data from World Bank
 * 2. Boot DuckDB-WASM
 * 3. Load data into DuckDB
 * 4. Run default scenario
 */
async function init() {
  try {
    updateStatus("FETCHING WORLD BANK DATA...");

    // EMU: Euro area aggregate, indicator: Money & quasi money (% of GDP) [web:38]
    const observations = await fetchAllWorldBankData(
      "FM.LBL.MQMY.GD.ZS",
      "EMU"
    );

    if (!observations || observations.length === 0) {
      updateStatus("EMPTY DATASET: NO OBSERVATIONS FOUND");
      console.warn("No observations returned from World Bank");
      return;
    }

    updateStatus("BOOTING DUCKDB-WASM...");
    db = await initDuckDB();
    conn = await db.connect();

    updateStatus("INGESTING DATA...");
    const fileName = "macro.json";
    await db.registerFileText(fileName, JSON.stringify(observations));

    await conn.query(`
      CREATE TABLE macro_data (date INTEGER, value DOUBLE);
      INSERT INTO macro_data (date, value)
      SELECT date::INTEGER, value::DOUBLE
      FROM read_json_auto('${fileName}');
    `);

    // Attach scenario runner to window for HTML buttons
    window.runScenario = async id => {
      if (!SCENARIOS[id]) {
        console.warn(`Invalid Scenario: ${id}`);
        return;
      }

      const scenario = SCENARIOS[id];
      updateStatus(`EXECUTING: ${id.toUpperCase()}...`);

      try {
        const result = await conn.query(scenario.sql);
        const data = result.toArray().map(r => r.toJSON());

        updateDOM("sql-display", scenario.sql.trim());
        updateDOM("insight-box", scenario.insight);

        renderChart(data, scenario.kpis);
        renderTable(data);

        updateStatus("SYSTEM ONLINE // IDLE");
      } catch (err) {
        updateStatus(`SQL ERROR: ${err.message.substring(0, 40)}...`);
        console.error("SQL Execution failed:", err);
      }
    };

    // Auto-run default scenario
    await window.runScenario("growth");
  } catch (error) {
    updateStatus(`CRITICAL ERROR: ${error.message.toUpperCase()}`);
    console.error("Initialization failed:", error);
  }
}

/**
 * DOM helpers
 */
function renderTable(data) {
  const head = document.getElementById("table-head");
  const body = document.getElementById("table-body");
  if (!head || !body || !data || data.length === 0) return;

  const cols = Object.keys(data[0]);

  head.innerHTML = `<tr>${cols
    .map(c => `<th>${c.toUpperCase()}</th>`)
    .join("")}</tr>`;

  body.innerHTML = data
    .slice(0, 20)
    .map(
      row =>
        `<tr>${cols
          .map(c => `<td>${row[c] !== null ? row[c] : "—"}</td>`)
          .join("")}</tr>`
    )
    .join("");
}

function renderChart(data, kpiDefinitions) {
  const canvas = document.getElementById("main-chart");
  if (!canvas || typeof Chart === "undefined") {
    console.warn("Chart.js not available or canvas missing");
    return;
  }

  const ctx = canvas.getContext("2d");
  if (chart) chart.destroy();

  const sortedData = [...data].sort((a, b) => a.date - b.date);
  const palette = ["#FF9900", "#00FF00", "#0099FF", "#FF3366"];

  const datasets = kpiDefinitions.map((kpi, i) => ({
    label: kpi.label,
    data: sortedData.map(row => row[kpi.col]),
    borderColor: palette[i % palette.length],
    backgroundColor: `${palette[i % palette.length]}33`,
    fill: kpiDefinitions.length === 1,
    tension: 0.3,
    pointRadius: 2,
    spanGaps: true
  }));

  chart = new Chart(ctx, {
    type: "line",
    data: {
      labels: sortedData.map(i => i.date),
      datasets
    },
    options: {
      responsive: true,
      maintainAspectRatio: false,
      interaction: { mode: "index", intersect: false },
      plugins: {
        legend: {
          display: true,
          labels: {
            color: "#888",
            font: { family: "monospace", size: 11 }
          }
        }
      },
      scales: {
        x: {
          ticks: { color: "#666" },
          grid: { display: false }
        },
        y: {
          ticks: { color: "#666" },
          grid: { color: "#222" }
        }
      }
    }
  });
}

function updateStatus(msg) {
  updateDOM("status-indicator", msg);
  console.log("[STATUS]:", msg);
}

function updateDOM(id, content) {
  const el = document.getElementById(id);
  if (el) el.innerHTML = content;
}

// Kick off
init();
