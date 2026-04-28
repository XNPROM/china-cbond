(function() {
  "use strict";

  const VIEW_MODEL = window.__VIEW_MODEL__ || {};
  const EXPLORER = VIEW_MODEL.explorer || {};
  const ITEMS = EXPLORER.items || [];
  const ITEM_MAP = new Map(ITEMS.map(item => [item.bond_code, item]));

  const state = {
    query: "",
    theme: "",
    category: "",
    quick: "all",
    view: "cards",
    density: "reading",
    sortKey: "relative_value",
    sortDir: "asc",
    selectedCode: null,
  };

  const $ = selector => document.querySelector(selector);
  const $$ = selector => Array.from(document.querySelectorAll(selector));

  let radarChart = null;

  const BACKTEST_SERIES = [
    { key: "cum_dl", label: "双低 Top10", color: "#1d4ed8", width: 3 },
    { key: "cum_equity", label: "偏股双低", color: "#dc2626", width: 2 },
    { key: "cum_balanced", label: "平衡双低", color: "#a16207", width: 2 },
    { key: "cum_debt", label: "偏债双低", color: "#059669", width: 2 },
    { key: "cum_rv", label: "低估 Top10", color: "#7c3aed", width: 2 },
    { key: "cum_bench", label: "中证转债", color: "#6b7280", width: 2, dash: "6 5" },
  ];

  function escapeHtml(value) {
    return String(value ?? "")
      .replace(/&/g, "&amp;")
      .replace(/</g, "&lt;")
      .replace(/>/g, "&gt;")
      .replace(/"/g, "&quot;")
      .replace(/'/g, "&#39;");
  }

  function highlightText(text, query) {
    if (!query || !text) return escapeHtml(text);
    const escaped = escapeHtml(text);
    const regex = new RegExp(`(${query.replace(/[.*+?^${}()|[\]\\]/g, "\\$&")})`, "gi");
    return escaped.replace(regex, '<mark class="search-highlight">$1</mark>');
  }

  function toNumber(metric) {
    if (metric == null) return null;
    if (typeof metric === "number") return Number.isFinite(metric) ? metric : null;
    if (typeof metric === "object" && "value" in metric) return toNumber(metric.value);
    const parsed = parseFloat(String(metric).replace("%", "").replace(/,/g, ""));
    return Number.isFinite(parsed) ? parsed : null;
  }

  function metricText(metric) {
    if (metric == null) return "--";
    if (typeof metric === "object" && "text" in metric) return metric.text || "--";
    return String(metric);
  }

  function formatBacktestDate(value) {
    const text = String(value || "");
    if (/^\d{8}$/.test(text)) return `${text.slice(4, 6)}-${text.slice(6, 8)}`;
    if (/^\d{4}-\d{2}-\d{2}$/.test(text)) return text.slice(5);
    return text;
  }

  function formatBacktestTooltipDate(value) {
    const text = String(value || "");
    if (/^\d{8}$/.test(text)) return `${text.slice(0, 4)}-${text.slice(4, 6)}-${text.slice(6, 8)}`;
    return text;
  }

  function formatPct(value, digits = 2) {
    const num = Number(value);
    if (!Number.isFinite(num)) return "--";
    return `${(num * 100).toFixed(digits)}%`;
  }

  function signedClass(metric) {
    const value = typeof metric === "object" ? metric.class_name : "";
    if (value === "up") return "is-positive";
    if (value === "down") return "is-negative";
    if (value) return value;
    const num = toNumber(metric);
    if (num == null || num === 0) return "";
    return num > 0 ? "is-positive" : "is-negative";
  }

  function stateClass(stateValue) {
    if (stateValue === "undervalued" || stateValue === "safe") return "safe";
    if (stateValue === "expensive" || stateValue === "danger") return "danger";
    if (stateValue === "warn") return "warn";
    return "note";
  }

  function sectorClass(sector) {
    if (sector === "偏股") return "pill-equity";
    if (sector === "平衡") return "pill-balanced";
    if (sector === "偏债") return "pill-debt";
    return "";
  }

  function matchesQuery(item) {
    if (!state.query) return true;
    return (item.search_text || "").includes(state.query);
  }

  function matchesTheme(item) {
    return !state.theme || (item.themes || []).includes(state.theme);
  }

  function matchesCategory(item) {
    return !state.category || item.category === state.category;
  }

  function parseCallDays(item) {
    const text = (item.call_status || {}).text || "";
    const m = text.match(/(\d+)\s*\/\s*\d+/);
    return m ? parseInt(m[1], 10) : null;
  }

  function matchesQuick(item) {
    if (state.quick === "all") return true;
    if (state.quick === "undervalued") return toNumber(item.relative_value) != null && toNumber(item.relative_value) < 1.0;
    if (state.quick === "lowPremium") return toNumber(item.conv) != null && toNumber(item.conv) < 20;
    if (state.quick === "highDelta") return toNumber(item.delta) != null && toNumber(item.delta) >= 0.75;
    if (state.quick === "callRisk") return ["warn", "danger"].includes((item.call_status || {}).state);
    if (state.quick === "callGt10") { const d = parseCallDays(item); return d != null && d > 10; }
    if (state.quick === "callLe10") { const d = parseCallDays(item); return d != null && d <= 10; }
    if (state.quick === "lowPrice") return toNumber(item.price) != null && toNumber(item.price) < 100;
    return true;
  }

  function comparableValue(item, key) {
    if (key === "bond_name") return item.bond_name || "";
    if (key === "theme_group") return item.theme_group || "";
    return toNumber(item[key]);
  }

  function sortItems(items) {
    const factor = state.sortDir === "desc" ? -1 : 1;
    return [...items].sort((left, right) => {
      const a = comparableValue(left, state.sortKey);
      const b = comparableValue(right, state.sortKey);
      if (typeof a === "string" || typeof b === "string") {
        return String(a).localeCompare(String(b), "zh-CN") * factor;
      }
      if (a == null && b == null) return 0;
      if (a == null) return 1;
      if (b == null) return -1;
      if (a === b) return 0;
      return (a - b) * factor;
    });
  }

  function filteredItems() {
    const list = ITEMS.filter(item =>
      matchesQuery(item) &&
      matchesTheme(item) &&
      matchesCategory(item) &&
      matchesQuick(item)
    );
    return sortItems(list);
  }

  function activeSummaryText() {
    const parts = [];
    if (state.quick !== "all") parts.push($(`.quick-chip[data-quick="${state.quick}"]`)?.textContent || state.quick);
    if (state.theme) parts.push(state.theme);
    if (state.category) parts.push(state.category);
    if (state.query) parts.push(`搜索:${state.query}`);
    return parts.length ? `· ${parts.join(" / ")}` : "· 全部标的";
  }

  function sparklineSvg(values, color) {
    if (!Array.isArray(values) || values.length < 2) return "";
    const width = 120;
    const height = 42;
    const min = Math.min(...values);
    const max = Math.max(...values);
    const range = Math.max(max - min, 1e-6);
    const points = values.map((value, index) => {
      const x = 6 + (index / (values.length - 1)) * (width - 12);
      const y = 6 + (1 - ((value - min) / range)) * (height - 12);
      return `${x.toFixed(1)},${y.toFixed(1)}`;
    });
    return [
      `<svg class="sparkline" viewBox="0 0 ${width} ${height}" preserveAspectRatio="none">`,
      `<polyline points="${points.join(" ")}" fill="none" stroke="${color}" stroke-width="3" stroke-linecap="round" stroke-linejoin="round"></polyline>`,
      `</svg>`,
    ].join("");
  }

  function renderCompactRow(item) {
    const chgClass = signedClass(item.day_chg);
    return `
      <div class="compact-row" data-open-code="${escapeHtml(item.bond_code)}">
        <span class="compact-row-name">${highlightText(item.bond_name, state.query)}</span>
        <span class="compact-row-code">${escapeHtml(item.bond_code)}</span>
        <span class="compact-row-item"><span class="compact-row-v ${chgClass}">${escapeHtml(metricText(item.price))}</span></span>
        <span class="compact-row-item"><span class="compact-row-v ${chgClass}">${escapeHtml(metricText(item.day_chg))}</span></span>
        <span class="compact-row-item"><span class="compact-row-v">${escapeHtml(metricText(item.conv))}</span></span>
        <span class="compact-row-item"><span class="compact-row-v">${escapeHtml(metricText(item.vol))}</span></span>
        <span class="compact-row-item"><span class="compact-row-v">${escapeHtml(metricText(item.relative_value))}</span></span>
        <span class="compact-row-item"><span class="compact-row-v">${escapeHtml(metricText(item.delta))}</span></span>
        <span class="compact-row-item"><span class="compact-row-v">${escapeHtml(metricText(item.balance))}</span></span>
        <span class="compact-row-item"><span class="compact-row-v">${escapeHtml(item.call_status.text || "—")}</span></span>
        <span class="compact-row-tags">${(item.themes || []).slice(0, 3).map(t => `<span class="compact-row-tag">${escapeHtml(t)}</span>`).join("")}</span>
      </div>`;
  }

  function renderCard(item) {
    return `
      <article class="bond-card" data-open-code="${escapeHtml(item.bond_code)}">
        <div class="bond-card-head">
          <div>
            <div class="bond-card-top">
              <span class="pill ${sectorClass(item.sector)}">${escapeHtml(item.sector || "偏债")}</span>
              <span class="pill">${escapeHtml(item.theme_group)}</span>
            </div>
            <h3>${highlightText(item.bond_name, state.query)}</h3>
            <p class="bond-card-sub">${highlightText(item.stock_name, state.query)} · ${escapeHtml(item.industry || "行业待补充")} · ${escapeHtml(item.bond_code)}</p>
          </div>
        </div>
        <div class="bond-card-stats">
          <div class="stat-box">
            <span class="stat-label">价格</span>
            <strong class="stat-value ${signedClass(item.day_chg)}">${escapeHtml(metricText(item.price))}</strong>
          </div>
          <div class="stat-box">
            <span class="stat-label">涨跌幅</span>
            <strong class="stat-value ${signedClass(item.day_chg)}">${escapeHtml(metricText(item.day_chg))}</strong>
          </div>
          <div class="stat-box">
            <span class="stat-label">转股溢价</span>
            <strong class="stat-value">${escapeHtml(metricText(item.conv))}</strong>
          </div>
          <div class="stat-box">
            <span class="stat-label">纯债溢价</span>
            <strong class="stat-value">${escapeHtml(metricText(item.pure))}</strong>
          </div>
          <div class="stat-box">
            <span class="stat-label">相对价值</span>
            <strong class="stat-value ${stateClass(item.relative_value.state) === "safe" ? "is-positive" : stateClass(item.relative_value.state) === "danger" ? "is-negative" : ""}">${escapeHtml(metricText(item.relative_value))}</strong>
          </div>
          <div class="stat-box">
            <span class="stat-label">Delta</span>
            <strong class="stat-value">${escapeHtml(metricText(item.delta))}</strong>
          </div>
        </div>
        <div class="bond-card-flags">
          ${item.call_status.text ? `<span class="status-pill ${stateClass(item.call_status.state)}">&#9888; ${escapeHtml(item.call_status.text)}</span>` : '<span class="status-pill safe">&#10003; 未触发强赎</span>'}
          ${item.down_status.text ? `<span class="status-pill ${stateClass(item.down_status.state)}">&#8595; ${escapeHtml(item.down_status.text)}</span>` : ""}
          ${item.strategy ? `<span class="status-pill note">&#9733; ${escapeHtml(item.strategy)}</span>` : ""}
        </div>
        <div class="bond-card-tags">
          ${(item.themes || []).slice(0, 4).map(theme => `<span class="theme-pill">${escapeHtml(theme)}</span>`).join("")}
        </div>
        <div class="bond-card-body">${escapeHtml(item.business || "暂无主营描述。")}</div>
      </article>
    `;
  }

  function renderTableRow(item) {
    return `
      <tr class="table-row" data-open-code="${escapeHtml(item.bond_code)}">
        <td>
          <strong>${highlightText(item.bond_name, state.query)}</strong>
          <div class="table-meta">${escapeHtml(item.bond_code)} · ${highlightText(item.stock_name, state.query)}</div>
        </td>
        <td><span class="table-value ${signedClass(item.day_chg)}">${escapeHtml(metricText(item.price))}</span></td>
        <td><span class="table-value ${signedClass(item.day_chg)}">${escapeHtml(metricText(item.day_chg))}</span></td>
        <td><span class="table-value">${escapeHtml(metricText(item.conv))}</span></td>
        <td><span class="table-value">${escapeHtml(metricText(item.pure))}</span></td>
        <td><span class="table-value ${stateClass(item.relative_value.state) === "safe" ? "is-positive" : stateClass(item.relative_value.state) === "danger" ? "is-negative" : ""}">${escapeHtml(metricText(item.relative_value))}</span></td>
        <td><span class="table-value">${escapeHtml(metricText(item.delta))}</span></td>
        <td><span class="table-value">${escapeHtml(metricText(item.balance))}</span></td>
        <td>${escapeHtml(item.theme_group)}</td>
        <td>
          <div class="bond-card-flags">
            ${item.call_status.text
              ? `<span class="status-pill ${stateClass(item.call_status.state)}">&#9888; ${escapeHtml(item.call_status.text)}</span>`
              : '<span class="status-pill safe">&#10003;</span>'}
          </div>
        </td>
      </tr>
    `;
  }

  function renderCards(items) {
    const view = $("#cardsView");
    if (!view) return;
    view.hidden = state.view !== "cards";
    if (state.view !== "cards") return;
    if (state.density === "compact") {
      const header = `<div class="compact-header">
        <span class="ch-name">转债</span>
        <span class="ch-code">代码</span>
        <span class="ch-item">价格</span>
        <span class="ch-item">涨跌</span>
        <span class="ch-item">溢价率</span>
        <span class="ch-item">波动率</span>
        <span class="ch-item">相对价值</span>
        <span class="ch-item">Delta</span>
        <span class="ch-item">余额</span>
        <span class="ch-item">强赎</span>
        <span class="ch-tags">题材</span>
      </div>`;
      view.innerHTML = header + items.map(renderCompactRow).join("");
      view.classList.add("is-compact");
    } else {
      view.innerHTML = items.map(renderCard).join("");
      view.classList.remove("is-compact");
    }
  }

  function renderTable(items) {
    const container = $("#tableView");
    const tbody = $("#tableBody");
    if (!container || !tbody) return;
    container.hidden = state.view !== "table";
    if (state.view !== "table") return;
    tbody.innerHTML = items.map(renderTableRow).join("");
  }

  function renderExplorer(items) {
    renderCards(items);
    renderTable(items);
    const empty = $("#emptyState");
    if (empty) empty.hidden = items.length > 0;
  }

  function updateSummary(items) {
    const countEl = $("#resultCount");
    const summaryEl = $("#activeSummary");
    if (countEl) countEl.textContent = String(items.length);
    if (summaryEl) summaryEl.textContent = activeSummaryText();
  }

  function detailHtml(item) {
    return `
      <div class="drawer-top">
        <div>
          <h2 class="drawer-title">${escapeHtml(item.detail.bond_name)}</h2>
          <p class="drawer-subtitle">${escapeHtml(item.bond_code)}</p>
          <p class="drawer-copy">${escapeHtml(item.detail.business || "暂无主营描述。")}</p>
        </div>
      </div>
      <div class="drawer-section">
        <div class="drawer-section-head">
          <h4>核心数据</h4>
        </div>
        <dl class="drawer-kv-grid">
          <div class="drawer-kv-row">
            <dt>正股</dt>
            <dd>${escapeHtml(item.detail.stock_name)}</dd>
          </div>
          <div class="drawer-kv-row">
            <dt>正股代码</dt>
            <dd>${escapeHtml(item.detail.stock_code)}</dd>
          </div>
          <div class="drawer-kv-row">
            <dt>行业</dt>
            <dd>${escapeHtml(item.detail.industry || "--")}${item.detail.sw_l2 ? " · " + escapeHtml(item.detail.sw_l2) : ""}</dd>
          </div>
          <div class="drawer-kv-row">
            <dt>价格</dt>
            <dd class="${signedClass(item.day_chg)}">${escapeHtml(metricText(item.price))}</dd>
          </div>
          <div class="drawer-kv-row">
            <dt>涨跌幅</dt>
            <dd class="${signedClass(item.day_chg)}">${escapeHtml(metricText(item.day_chg))}</dd>
          </div>
          <div class="drawer-kv-row">
            <dt>转股溢价率</dt>
            <dd>${escapeHtml(metricText(item.conv))}</dd>
          </div>
          <div class="drawer-kv-row">
            <dt>纯债溢价率</dt>
            <dd>${escapeHtml(metricText(item.pure))}</dd>
          </div>
          <div class="drawer-kv-row">
            <dt>20日年化&sigma;</dt>
            <dd>${escapeHtml(metricText(item.vol))}</dd>
          </div>
          <div class="drawer-kv-row">
            <dt>相对价值</dt>
            <dd class="${stateClass(item.relative_value.state) === "safe" ? "is-positive" : stateClass(item.relative_value.state) === "danger" ? "is-negative" : ""}">${escapeHtml(metricText(item.relative_value))}</dd>
          </div>
          <div class="drawer-kv-row">
            <dt>Delta</dt>
            <dd>${escapeHtml(metricText(item.delta))}</dd>
          </div>
          <div class="drawer-kv-row">
            <dt>纯债YTM</dt>
            <dd>${escapeHtml(metricText(item.pure_bond_ytm))}</dd>
          </div>
          <div class="drawer-kv-row">
            <dt>剩余年限</dt>
            <dd>${escapeHtml(metricText(item.surplus_years))}</dd>
          </div>
          <div class="drawer-kv-row">
            <dt>余额(亿)</dt>
            <dd>${escapeHtml(metricText(item.balance))}</dd>
          </div>
        </dl>
      </div>
      <div class="drawer-section">
        <div class="drawer-section-head">
          <h4>条款与分类</h4>
        </div>
        <dl class="drawer-kv-grid">
          <div class="drawer-kv-row">
            <dt>强赎</dt>
            <dd>${item.call_status.text
              ? `<span class="status-pill ${stateClass(item.call_status.state)}">&#9888; ${escapeHtml(item.call_status.text)}</span>`
              : '<span class="status-pill safe">&#10003; 未触发</span>'}</dd>
          </div>
          ${item.down_status.text ? `<div class="drawer-kv-row"><dt>下修</dt><dd><span class="status-pill ${stateClass(item.down_status.state)}">&#8595; ${escapeHtml(item.down_status.text)}</span></dd></div>` : ""}
          <div class="drawer-kv-row">
            <dt>评级</dt>
            <dd>${escapeHtml(item.rating || "--")}</dd>
          </div>
          <div class="drawer-kv-row">
            <dt>到期</dt>
            <dd>${escapeHtml(item.maturity || "--")}</dd>
          </div>
          <div class="drawer-kv-row">
            <dt>主分组</dt>
            <dd><span class="pill ${sectorClass(item.sector)}">${escapeHtml(item.sector || "未分域")}</span> ${escapeHtml(item.theme_group)}</dd>
          </div>
          ${item.detail.strategy ? `<div class="drawer-kv-row"><dt>策略</dt><dd><span class="status-pill note">${escapeHtml(item.detail.strategy)}</span></dd></div>` : ""}
        </dl>
      </div>
      <div class="drawer-section">
        <div class="drawer-section-head">
          <h4>题材</h4>
        </div>
        <div class="bond-card-tags">
          ${(item.detail.themes || []).map(theme => `<span class="theme-pill">${escapeHtml(theme)}</span>`).join("")}
        </div>
      </div>
    `;
  }

  function openDetail(code) {
    const item = ITEM_MAP.get(code);
    if (!item) return;
    state.selectedCode = code;
    const content = $("#detailContent");
    const drawer = $("#detailDrawer");
    const scrim = $("#drawerScrim");
    if (content) content.innerHTML = detailHtml(item);
    if (drawer) {
      drawer.classList.add("is-open");
      drawer.setAttribute("aria-hidden", "false");
    }
    if (scrim) scrim.hidden = false;
  }

  function closeDetail() {
    state.selectedCode = null;
    const drawer = $("#detailDrawer");
    const scrim = $("#drawerScrim");
    if (drawer) {
      drawer.classList.remove("is-open");
      drawer.setAttribute("aria-hidden", "true");
    }
    if (scrim) scrim.hidden = true;
  }

  function renderRadar(items) {
    const el = $("#marketRadarChart");
    if (!el) return;
    if (typeof echarts === "undefined") {
      el.innerHTML = '<div class="empty-state"><h3>图表加载失败</h3><p>当前环境无法加载 ECharts。</p></div>';
      return;
    }
    if (!radarChart) {
      radarChart = echarts.init(el, null, { renderer: "canvas" });
      radarChart.on("click", params => {
        if (params.data && params.data.bondCode) openDetail(params.data.bondCode);
      });
      window.addEventListener("resize", () => radarChart && radarChart.resize());
    }

    const groups = {
      undervalued: [],
      fair: [],
      expensive: [],
    };
    items.forEach(item => {
      const bucket = item.relative_value.state === "undervalued"
        ? "undervalued"
        : item.relative_value.state === "expensive"
          ? "expensive"
          : "fair";
      groups[bucket].push({
        value: [
          toNumber(item.conv) || 0,
          toNumber(item.price) || 0,
          toNumber(item.relative_value) || 0,
        ],
        bondCode: item.bond_code,
        bondName: item.bond_name,
        themeGroup: item.theme_group,
        sector: item.sector,
        priceText: metricText(item.price),
        convText: metricText(item.conv),
        rvText: metricText(item.relative_value),
        deltaText: metricText(item.delta),
      });
    });

    // Find lowest-RV bond for annotation
    const allScatter = [...groups.undervalued, ...groups.fair, ...groups.expensive];
    const lowestRv = allScatter.filter(d => d.value[2] > 0).sort((a, b) => a.value[2] - b.value[2])[0];
    const markPointData = lowestRv ? [{
      coord: [lowestRv.value[0], lowestRv.value[1]],
      symbol: "pin",
      symbolSize: 40,
      itemStyle: { color: "#1d7a46" },
      label: { show: true, formatter: lowestRv.bondName, color: "#1d7a46", fontSize: 11, position: "top", distance: 8 },
    }] : [];

    radarChart.setOption({
      animationDuration: 300,
      grid: { left: 52, right: 22, top: 36, bottom: 42 },
      tooltip: {
        trigger: "item",
        backgroundColor: "rgba(255,255,255,0.96)",
        borderColor: "rgba(139,124,94,0.18)",
        borderWidth: 1,
        textStyle: { color: "#1f2937" },
        formatter: params => {
          const data = params.data;
          return [
            `<strong>${escapeHtml(data.bondName)}</strong>`,
            `${escapeHtml(data.themeGroup)}${data.sector ? " · " + escapeHtml(data.sector) : ""}`,
            `价格 ${escapeHtml(data.priceText)} / 转股溢价 ${escapeHtml(data.convText)}`,
            `RV ${escapeHtml(data.rvText)} / Delta ${escapeHtml(data.deltaText)}`,
          ].join("<br>");
        },
      },
      xAxis: {
        type: "value",
        name: "转股溢价率(%)",
        nameTextStyle: { color: "#536072" },
        axisLabel: { color: "#536072" },
        splitLine: { lineStyle: { color: "rgba(139,124,94,0.14)" } },
      },
      yAxis: {
        type: "value",
        name: "价格",
        nameTextStyle: { color: "#536072" },
        axisLabel: { color: "#536072" },
        splitLine: { lineStyle: { color: "rgba(139,124,94,0.14)" } },
      },
      series: [
        {
          name: "低估",
          type: "scatter",
          data: groups.undervalued,
          symbolSize: 10,
          itemStyle: { color: "#1d7a46", opacity: 0.86 },
          markPoint: { data: markPointData, animation: false },
        },
        {
          name: "合理",
          type: "scatter",
          data: groups.fair,
          symbolSize: 10,
          itemStyle: { color: "#4b5563", opacity: 0.74 },
        },
        {
          name: "偏贵",
          type: "scatter",
          data: groups.expensive,
          symbolSize: 10,
          itemStyle: { color: "#c2410c", opacity: 0.8 },
        },
      ],
    });
  }

  function renderBacktest() {
    const chartEl = $("#equityChart");
    const payload = VIEW_MODEL.backtest;
    if (!chartEl || !payload || !payload.equity_curve || payload.equity_curve.length < 2) return;
    const curve = payload.equity_curve;

    const width = 980;
    const height = 340;
    const margin = { top: 58, right: 24, bottom: 42, left: 58 };
    const plotW = width - margin.left - margin.right;
    const plotH = height - margin.top - margin.bottom;
    const values = [];
    curve.forEach(point => {
      BACKTEST_SERIES.forEach(series => {
        const value = Number(point[series.key]);
        if (Number.isFinite(value)) values.push(value);
      });
    });
    if (!values.length) {
      chartEl.innerHTML = '<div class="empty-state"><h3>暂无回测曲线</h3><p>当前回测数据没有可绘制的净值序列。</p></div>';
      return;
    }

    let minY = Math.min(0, ...values);
    let maxY = Math.max(0, ...values);
    const span = maxY - minY || 0.02;
    minY -= span * 0.12;
    maxY += span * 0.12;

    const xAt = index => margin.left + (curve.length === 1 ? 0 : (index / (curve.length - 1)) * plotW);
    const yAt = value => margin.top + ((maxY - value) / (maxY - minY)) * plotH;
    const yTicks = Array.from({ length: 5 }, (_, i) => minY + ((maxY - minY) * i) / 4);
    const xStep = Math.max(1, Math.ceil(curve.length / 6));
    const xTicks = curve
      .map((point, index) => ({ point, index }))
      .filter(({ index }) => index === 0 || index === curve.length - 1 || index % xStep === 0);

    const pathFor = series => {
      const commands = [];
      let drawing = false;
      curve.forEach((point, index) => {
        const value = Number(point[series.key]);
        if (!Number.isFinite(value)) {
          drawing = false;
          return;
        }
        commands.push(`${drawing ? "L" : "M"}${xAt(index).toFixed(2)},${yAt(value).toFixed(2)}`);
        drawing = true;
      });
      return commands.join(" ");
    };

    const yGrid = yTicks.map(value => {
      const y = yAt(value);
      return `
        <line class="backtest-grid" x1="${margin.left}" y1="${y.toFixed(2)}" x2="${width - margin.right}" y2="${y.toFixed(2)}"></line>
        <text class="backtest-axis-label" x="${margin.left - 10}" y="${(y + 4).toFixed(2)}" text-anchor="end">${formatPct(value, 1)}</text>
      `;
    }).join("");
    const xLabels = xTicks.map(({ point, index }) => `
      <text class="backtest-axis-label" x="${xAt(index).toFixed(2)}" y="${height - 14}" text-anchor="middle">${escapeHtml(formatBacktestDate(point.date))}</text>
    `).join("");
    const legend = BACKTEST_SERIES.map((series, index) => {
      const col = index % 3;
      const row = Math.floor(index / 3);
      const x = margin.left + col * 150;
      const y = 20 + row * 22;
      return `
        <g class="backtest-legend-item" transform="translate(${x},${y})">
          <line x1="0" y1="0" x2="26" y2="0" stroke="${series.color}" stroke-width="${series.width}" stroke-dasharray="${series.dash || ""}" stroke-linecap="round"></line>
          <text x="34" y="4">${escapeHtml(series.label)}</text>
        </g>
      `;
    }).join("");
    const paths = BACKTEST_SERIES.map(series => `
      <path class="backtest-line" d="${pathFor(series)}" fill="none" stroke="${series.color}" stroke-width="${series.width}" stroke-dasharray="${series.dash || ""}" stroke-linecap="round" stroke-linejoin="round"></path>
    `).join("");
    const hoverCircles = BACKTEST_SERIES.map(series => `
      <circle class="backtest-hover-point" data-hover-key="${series.key}" r="4.5" fill="${series.color}"></circle>
    `).join("");

    chartEl.innerHTML = `
      <svg class="backtest-svg" viewBox="0 0 ${width} ${height}" role="img" aria-label="策略回测净值曲线">
        <g>${yGrid}</g>
        <line class="backtest-axis" x1="${margin.left}" y1="${height - margin.bottom}" x2="${width - margin.right}" y2="${height - margin.bottom}"></line>
        <line class="backtest-axis" x1="${margin.left}" y1="${margin.top}" x2="${margin.left}" y2="${height - margin.bottom}"></line>
        ${xLabels}
        <line class="backtest-zero-line" x1="${margin.left}" y1="${yAt(0).toFixed(2)}" x2="${width - margin.right}" y2="${yAt(0).toFixed(2)}"></line>
        <g>${legend}</g>
        <g>${paths}</g>
        <g class="backtest-hover-layer" hidden>
          <line class="backtest-hover-line" x1="0" y1="${margin.top}" x2="0" y2="${height - margin.bottom}"></line>
          ${hoverCircles}
        </g>
        <rect class="backtest-hit-area" x="${margin.left}" y="${margin.top}" width="${plotW}" height="${plotH}"></rect>
      </svg>
      <div class="backtest-tooltip" hidden></div>
    `;

    const svg = chartEl.querySelector("svg");
    const hitArea = chartEl.querySelector(".backtest-hit-area");
    const hoverLayer = chartEl.querySelector(".backtest-hover-layer");
    const hoverLine = chartEl.querySelector(".backtest-hover-line");
    const tooltip = chartEl.querySelector(".backtest-tooltip");
    const circles = new Map(Array.from(chartEl.querySelectorAll(".backtest-hover-point")).map(circle => [circle.dataset.hoverKey, circle]));

    hitArea.addEventListener("mousemove", event => {
      const rect = svg.getBoundingClientRect();
      const svgX = ((event.clientX - rect.left) / rect.width) * width;
      const index = Math.max(0, Math.min(curve.length - 1, Math.round(((svgX - margin.left) / plotW) * (curve.length - 1))));
      const point = curve[index];
      const x = xAt(index);
      hoverLayer.removeAttribute("hidden");
      hoverLine.setAttribute("x1", x.toFixed(2));
      hoverLine.setAttribute("x2", x.toFixed(2));
      BACKTEST_SERIES.forEach(series => {
        const circle = circles.get(series.key);
        const value = Number(point[series.key]);
        if (!circle || !Number.isFinite(value)) {
          if (circle) circle.setAttribute("display", "none");
          return;
        }
        circle.removeAttribute("display");
        circle.setAttribute("cx", x.toFixed(2));
        circle.setAttribute("cy", yAt(value).toFixed(2));
      });
      const lines = BACKTEST_SERIES.map(series => {
        const value = Number(point[series.key]);
        return `
          <div class="backtest-tooltip-row">
            <span><i style="background:${series.color}"></i>${escapeHtml(series.label)}</span>
            <strong>${formatPct(value)}</strong>
          </div>
        `;
      }).join("");
      tooltip.hidden = false;
      tooltip.innerHTML = `<div class="backtest-tooltip-date">${escapeHtml(formatBacktestTooltipDate(point.date))}</div>${lines}`;
      const cssX = (x / width) * rect.width;
      const maxTooltipLeft = Math.max(12, rect.width - 190);
      tooltip.style.left = `${Math.min(maxTooltipLeft, Math.max(12, cssX + 14))}px`;
      tooltip.style.top = `${Math.max(12, event.clientY - rect.top - 20)}px`;
    });
    hitArea.addEventListener("mouseleave", () => {
      hoverLayer.setAttribute("hidden", "");
      tooltip.hidden = true;
    });
  }

  function setSort(key) {
    if (state.sortKey === key) {
      state.sortDir = state.sortDir === "asc" ? "desc" : "asc";
    } else {
      state.sortKey = key;
      state.sortDir = "asc";
    }
    $$(".bond-table th[data-sort-key]").forEach(th => {
      th.classList.toggle("is-sorted", th.dataset.sortKey === state.sortKey);
    });
    const sel = $("#sortSelect");
    if (sel) {
      const combo = `${state.sortKey}:${state.sortDir}`;
      if ([...sel.options].some(o => o.value === combo)) {
        sel.value = combo;
      }
    }
    render();
  }

  function render() {
    const items = filteredItems();
    renderExplorer(items);
    updateSummary(items);
    renderRadar(items.length ? items : ITEMS);
  }

  function visibleItems() {
    return filteredItems();
  }

  function exportCsv() {
    const items = visibleItems();
    if (!items.length) return;
    const header = [
      "bond_code", "bond_name", "stock_code", "stock_name", "price", "day_chg",
      "conv_prem", "relative_value", "delta", "balance", "theme_group", "industry",
    ];
    const lines = items.map(item => [
      item.bond_code,
      item.bond_name,
      item.stock_code,
      item.stock_name,
      metricText(item.price),
      metricText(item.day_chg),
      metricText(item.conv),
      metricText(item.relative_value),
      metricText(item.delta),
      metricText(item.balance),
      item.theme_group,
      item.industry,
    ].map(value => {
      const text = String(value ?? "");
      return /[,"\n]/.test(text) ? `"${text.replace(/"/g, '""')}"` : text;
    }).join(","));
    const blob = new Blob([`\ufeff${header.join(",")}\n${lines.join("\n")}`], { type: "text/csv;charset=utf-8;" });
    const link = document.createElement("a");
    link.href = URL.createObjectURL(blob);
    link.download = `cbond_${Date.now()}.csv`;
    document.body.appendChild(link);
    link.click();
    link.remove();
    URL.revokeObjectURL(link.href);
    const status = $("#exportStatus");
    if (status) {
      status.textContent = `已导出 ${items.length} 条`;
      window.setTimeout(() => { status.textContent = ""; }, 2000);
    }
  }

  async function copyCodes() {
    const codes = visibleItems().map(item => item.bond_code).filter(Boolean);
    if (!codes.length) return;
    const status = $("#exportStatus");
    try {
      await navigator.clipboard.writeText(codes.join("\n"));
      if (status) {
        status.textContent = `已复制 ${codes.length} 个代码`;
        window.setTimeout(() => { status.textContent = ""; }, 2000);
      }
    } catch (error) {
      if (status) {
        status.textContent = "复制失败";
        window.setTimeout(() => { status.textContent = ""; }, 2000);
      }
    }
  }

  function scrollToTop() {
    window.scrollTo({ top: 0, behavior: "smooth" });
  }

  function initScrollToTopButton() {
    const btn = document.createElement("button");
    btn.className = "scroll-top-btn";
    btn.innerHTML = "&uarr;";
    btn.title = "回到顶部 (快捷键: t)";
    btn.addEventListener("click", scrollToTop);
    document.body.appendChild(btn);

    window.addEventListener("scroll", () => {
      btn.classList.toggle("visible", window.scrollY > 400);
    }, { passive: true });
  }

  function initControls() {
    $("#searchInput")?.addEventListener("input", event => {
      state.query = String(event.target.value || "")
        .toLowerCase()
        .replace(/\.(sh|sz|bj)\b/g, "")
        .trim();
      render();
    });

    $("#themeFilter")?.addEventListener("change", event => {
      state.theme = event.target.value;
      render();
    });

    $("#categoryFilter")?.addEventListener("change", event => {
      state.category = event.target.value;
      render();
    });

    $("#sortSelect")?.addEventListener("change", event => {
      const val = event.target.value;
      const [key, dir] = val.split(":");
      state.sortKey = key;
      state.sortDir = dir || "asc";
      render();
    });

    $$(".quick-chip").forEach(button => {
      button.addEventListener("click", () => {
        state.quick = button.dataset.quick || "all";
        $$(".quick-chip").forEach(node => node.classList.toggle("is-active", node === button));
        render();
      });
    });

    $$(".view-btn").forEach(button => {
      button.addEventListener("click", () => {
        state.view = button.dataset.view || "cards";
        $$(".view-btn").forEach(node => node.classList.toggle("is-active", node === button));
        render();
      });
    });

    $$(".density-btn").forEach(button => {
      button.addEventListener("click", () => {
        state.density = button.dataset.density || "reading";
        $$(".density-btn").forEach(node => node.classList.toggle("is-active", node === button));
        render();
      });
    });

    $("#copyCodes")?.addEventListener("click", copyCodes);
    $("#exportCsv")?.addEventListener("click", exportCsv);
    $("#detailClose")?.addEventListener("click", closeDetail);
    $("#drawerScrim")?.addEventListener("click", closeDetail);

    document.addEventListener("click", event => {
      const openTarget = event.target.closest("[data-open-code]");
      if (openTarget) {
        openDetail(openTarget.dataset.openCode);
      }
    });

    document.addEventListener("keydown", event => {
      if (event.key === "Escape") closeDetail();
      // Keyboard shortcuts
      if (event.key === "/" && !event.target.matches("input, textarea, select")) {
        event.preventDefault();
        $("#searchInput")?.focus();
      }
      if (event.key === "t" && !event.target.matches("input, textarea, select")) {
        scrollToTop();
      }
    });

    $$(".bond-table th[data-sort-key]").forEach(th => {
      th.addEventListener("click", () => setSort(th.dataset.sortKey));
    });
    $$(".bond-table th[data-sort-key]").forEach(th => {
      th.classList.toggle("is-sorted", th.dataset.sortKey === state.sortKey);
    });
  }

  document.addEventListener("DOMContentLoaded", () => {
    initControls();
    initScrollToTopButton();
    renderBacktest();
    render();
  });
})();
