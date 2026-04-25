/* ===== App: Filter, Sort, ECharts, Theme ===== */
(function() {
  "use strict";

  const DATA = window.__CBOND_DATA__ || [];
  const BT = window.__BACKTEST__ || {};

  const state = { query: "", quick: "all", sortKey: null, sortDir: "none", theme: "" };

  const $ = s => document.querySelector(s);
  const $$ = s => [...document.querySelectorAll(s)];

  /* ---- Utils ---- */
  function toNum(v) { const n = parseFloat(v || 0); return Number.isFinite(n) ? n : 0; }
  function normQ(v) { return String(v||"").toLowerCase().replace(/[\s_\/()（）#`]+/g," ").replace(/\.(sh|sz|bj)\b/g,"").trim(); }

  /* ---- Filter ---- */
  function matchQuick(d) {
    if (state.quick === "all") return true;
    const p = toNum(d.price), c = toNum(d.conv), v = toNum(d.vol);
    if (state.quick === "highPrice") return p > 130;
    if (state.quick === "lowPrice") return p < 100;
    if (state.quick === "lowPremium") return c < 20;
    if (state.quick === "midPremium") return c >= 20 && c < 50;
    if (state.quick === "highPremium") return c >= 50;
    return true;
  }
  function matchQuery(d) {
    if (!state.query) return true;
    return (d.search_text || "").includes(state.query);
  }
  function matchTheme(d) {
    if (!state.theme) return true;
    return (d.themes || []).includes(state.theme);
  }

  /* ---- Sort ---- */
  function sortItems(items) {
    if (!state.sortKey || state.sortDir === "none") return items;
    const dir = state.sortDir === "asc" ? 1 : -1;
    return [...items].sort((a, b) => {
      const va = toNum(a[state.sortKey]);
      const vb = toNum(b[state.sortKey]);
      return (va - vb) * dir;
    });
  }

  /* ---- Render ---- */
  function render() {
    const filtered = DATA.filter(d => matchQuick(d) && matchQuery(d) && matchTheme(d));
    const sorted = sortItems(filtered);
    const visibleIdx = new Set(sorted.map(d => d.idx));

    // Toggle row visibility
    const rows = $$(".bond-row");
    rows.forEach(row => {
      const idx = parseInt(row.dataset.idx, 10);
      row.hidden = !visibleIdx.has(idx);
    });

    // Toggle groups
    $$(".group").forEach(g => {
      const hasVis = [...g.querySelectorAll(".bond-row")].some(r => !r.hidden);
      g.hidden = !hasVis;
    });

    // Toggle category dividers
    $$(".category-divider").forEach(div => {
      const catName = div.querySelector("h2").textContent;
      const catGroups = $$(`.group[data-category="${catName}"]`);
      const hasVis = [...catGroups].some(g => !g.hidden);
      div.hidden = !hasVis;
    });

    // Update count
    const countEl = $("#resultCount");
    if (countEl) countEl.textContent = String(sorted.length);
    const emptyEl = $("#empty");
    if (emptyEl) emptyEl.style.display = sorted.length ? "none" : "block";
  }

  /* ---- Column Sort ---- */
  function initColumnSort() {
    $$(".btable th[data-sort-key]").forEach(th => {
      th.addEventListener("click", () => {
        const key = th.dataset.sortKey;
        if (state.sortKey === key) {
          state.sortDir = state.sortDir === "none" ? "asc" : state.sortDir === "asc" ? "desc" : "none";
          if (state.sortDir === "none") state.sortKey = null;
        } else {
          state.sortKey = key;
          state.sortDir = "asc";
        }
        // Update sort indicators
        $$(".btable th").forEach(h => {
          h.classList.remove("sort-active");
          const icon = h.querySelector(".sort-icon");
          if (icon) icon.textContent = "";
        });
        if (state.sortKey) {
          // Add icons to all headers with same sort key
          $$(`.btable th[data-sort-key="${state.sortKey}"]`).forEach(h => {
            h.classList.add("sort-active");
            const icon = h.querySelector(".sort-icon");
            if (icon) icon.textContent = state.sortDir === "asc" ? "▲" : "▼";
          });
        }
        render();
      });
    });
  }

  /* ---- Theme Toggle ---- */
  function initTheme() {
    const saved = localStorage.getItem("cbond-theme") || "dark";
    document.documentElement.setAttribute("data-theme", saved);
    updateThemeIcon(saved);
  }
  function updateThemeIcon(theme) {
    const btn = $("#themeToggle");
    if (btn) btn.textContent = theme === "dark" ? "🌙" : "☀️";
  }
  function toggleTheme() {
    const html = document.documentElement;
    const next = html.getAttribute("data-theme") === "dark" ? "light" : "dark";
    html.setAttribute("data-theme", next);
    localStorage.setItem("cbond-theme", next);
    updateThemeIcon(next);
    renderCharts();
  }

  /* ---- ECharts: Equity Curve ---- */
  let equityChart = null;
  function initEquityCurve() {
    const el = $("#equity-chart");
    if (!el || !BT.equity_curve || BT.equity_curve.length < 2) return;
    if (typeof echarts === "undefined") {
      el.innerHTML = '<div style="padding:20px;color:var(--text-muted);text-align:center">图表加载需要网络连接</div>';
      return;
    }
    equityChart = echarts.init(el, null, { renderer: "canvas" });
    renderEquityCurve();
    window.addEventListener("resize", () => equityChart && equityChart.resize());
  }
  function renderEquityCurve() {
    if (!equityChart) return;
    const curve = dedupEquity(BT.equity_curve || []);
    if (curve.length < 2) return;

    const style = getComputedStyle(document.documentElement);
    const textMuted = style.getPropertyValue("--text-muted").trim() || "#6b7280";
    const border = style.getPropertyValue("--border").trim() || "#2d3140";
    const accent = style.getPropertyValue("--accent").trim() || "#3b82f6";
    const gold = style.getPropertyValue("--gold").trim() || "#eab308";

    const dates = curve.map(p => p.date);
    equityChart.setOption({
      backgroundColor: "transparent",
      grid: { left: 60, right: 50, top: 50, bottom: 55 },
      xAxis: {
        type: "category", data: dates, boundaryGap: false,
        axisLabel: { color: textMuted, fontSize: 10, formatter: v => v.slice(4) },
        axisLine: { lineStyle: { color: border } },
        axisTick: { show: false },
      },
      yAxis: {
        type: "value",
        axisLabel: { color: textMuted, fontSize: 10, formatter: v => (v * 100).toFixed(1) + "%" },
        axisLine: { show: false },
        splitLine: { lineStyle: { color: border, type: "dashed" } },
      },
      tooltip: {
        trigger: "axis",
        backgroundColor: "rgba(0,0,0,.8)", borderWidth: 0,
        textStyle: { color: "#e4e4e7", fontSize: 12 },
        valueFormatter: v => v != null ? (v * 100).toFixed(2) + "%" : "--",
      },
      legend: {
        top: 5, textStyle: { color: textMuted, fontSize: 11 },
        data: ["双低Top10", "分域双低Top10", "全市场等权"],
      },
      dataZoom: [{ type: "inside" }, { type: "slider", height: 20, bottom: 5, borderColor: border, fillerColor: "rgba(59,130,246,.15)", handleStyle: { color: accent } }],
      series: [
        { name: "双低Top10", type: "line", data: curve.map(p => p.cum_dl), smooth: true, symbol: "none", lineStyle: { width: 2, color: accent }, itemStyle: { color: accent }, areaStyle: { color: { type: "linear", x: 0, y: 0, x2: 0, y2: 1, colorStops: [{ offset: 0, color: "rgba(59,130,246,.15)" }, { offset: 1, color: "rgba(59,130,246,.01)" }] } } },
        { name: "分域双低Top10", type: "line", data: curve.map(p => p.cum_sn), smooth: true, symbol: "none", lineStyle: { width: 2, color: gold }, itemStyle: { color: gold } },
        { name: "全市场等权", type: "line", data: curve.map(p => p.cum_mkt), smooth: true, symbol: "none", lineStyle: { width: 1.5, type: "dashed", color: textMuted }, itemStyle: { color: textMuted } },
      ],
    }, true);
  }
  function dedupEquity(curve) {
    const seen = {};
    for (let i = 0; i < curve.length; i++) seen[curve[i].date] = i;
    return Object.values(seen).sort((a, b) => a - b).map(i => curve[i]);
  }

  function renderCharts() {
    renderEquityCurve();
  }

  /* ---- Group Collapse ---- */
  function initGroupToggle() {
    $$(".group-head").forEach(h => {
      h.addEventListener("click", e => {
        if (e.target.closest("th")) return; // don't toggle on sort clicks
        const body = h.nextElementSibling;
        body.classList.toggle("collapsed");
        const toggle = h.querySelector(".toggle");
        if (toggle) toggle.textContent = body.classList.contains("collapsed") ? "展开" : "收起";
      });
    });
  }

  /* ---- Copy / Export ---- */
  function initCopyExport() {
    const copyBtn = $("#copyCodes");
    const exportBtn = $("#exportCsv");
    const statusEl = $("#exportStatus");

    function getVisibleItems() {
      const visibleRows = $$(".bond-row").filter(r => !r.hidden);
      return visibleRows.map(r => {
        const idx = parseInt(r.dataset.idx, 10);
        return DATA.find(d => d.idx === idx);
      }).filter(Boolean);
    }

    if (copyBtn) {
      copyBtn.addEventListener("click", async () => {
        const items = getVisibleItems();
        const codes = items.map(d => d.bond_code).filter(Boolean);
        if (!codes.length) return;
        try {
          await navigator.clipboard.writeText(codes.join("\n"));
          if (statusEl) { statusEl.textContent = "已复制 " + codes.length + " 个代码"; setTimeout(() => statusEl.textContent = "", 2000); }
        } catch (_) {
          if (statusEl) { statusEl.textContent = "复制失败"; setTimeout(() => statusEl.textContent = "", 2000); }
        }
      });
    }

    if (exportBtn) {
      exportBtn.addEventListener("click", () => {
        const items = getVisibleItems();
        if (!items.length) return;
        const esc = v => { const s = String(v||""); return s.includes(",")||s.includes('"')||s.includes("\n") ? '"'+s.replace(/"/g,'""')+'"' : s; };
        const h = "bond_code,bond_name,stock_code,stock_name,price,day_chg,conv_prem,pure_prem,vol,pure_bond_ytm,relative_value,delta,balance,rating,maturity,strategy";
        const rows = items.map(d => [d.bond_code,d.bond_name,d.stock_code,d.stock_name,d.price,d.day_chg,d.conv,d.pure,d.vol,d.pure_bond_ytm,d.relative_value,d.delta,d.balance,d.rating,d.maturity,d.strategy].map(esc).join(","));
        const blob = new Blob(["﻿"+h+"\n"+rows.join("\n")], {type:"text/csv;charset=utf-8;"});
        const a = document.createElement("a"); a.href = URL.createObjectURL(blob); a.download = "cbond_"+Date.now()+".csv";
        document.body.appendChild(a); a.click(); a.remove(); URL.revokeObjectURL(a.href);
        if (statusEl) { statusEl.textContent = "已导出 " + items.length + " 条"; setTimeout(() => statusEl.textContent = "", 2000); }
      });
    }
  }

  /* ---- Controls ---- */
  function initControls() {
    const searchInput = $("#search");
    const themeSelect = $("#themeFilter");
    const quickButtons = $$("[data-quick]");

    if (searchInput) {
      searchInput.addEventListener("input", e => { state.query = normQ(e.target.value); render(); });
    }
    if (themeSelect) {
      themeSelect.addEventListener("change", e => { state.theme = e.target.value; render(); });
    }
    quickButtons.forEach(btn => {
      btn.addEventListener("click", () => {
        state.quick = btn.dataset.quick;
        quickButtons.forEach(b => b.classList.toggle("is-active", b === btn));
        render();
      });
    });
  }

  /* ---- Init ---- */
  document.addEventListener("DOMContentLoaded", () => {
    initTheme();
    initControls();
    initColumnSort();
    initGroupToggle();
    initCopyExport();
    initEquityCurve();
    render();
  });

  const themeBtn = $("#themeToggle");
  if (themeBtn) themeBtn.addEventListener("click", toggleTheme);
})();
