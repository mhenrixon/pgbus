// Charts module for the Pgbus Insights page.
// Loaded only on the insights page via an inline <script type="module">.

function getThemeColors() {
  const isDark = document.documentElement.classList.contains("dark");
  return {
    isDark,
    text: isDark ? "#9ca3af" : "#6b7280",
    grid: isDark ? "#374151" : "#e5e7eb",
    tooltip: isDark ? "dark" : "light",
    dataLabel: isDark ? "#fff" : "#000",
  };
}

let throughputChart, statusChart, latencyChart;

export function renderCharts(data, i18n) {
  const t = getThemeColors();

  if (throughputChart) throughputChart.destroy();
  if (statusChart) statusChart.destroy();
  if (latencyChart) latencyChart.destroy();

  const throughputData = data.throughput.map(p => ({
    x: new Date(p.time).getTime(),
    y: p.count,
  }));

  throughputChart = new ApexCharts(document.querySelector("#throughput-chart"), {
    series: [{ name: i18n.seriesName || "Jobs", data: throughputData }],
    chart: { type: "area", height: 280, toolbar: { show: false }, background: "transparent", foreColor: t.text },
    stroke: { curve: "smooth", width: 2 },
    fill: { type: "gradient", gradient: { shadeIntensity: 1, opacityFrom: 0.4, opacityTo: 0.05, stops: [0, 100] } },
    colors: ["#6366f1"],
    xaxis: { type: "datetime", labels: { style: { colors: t.text } } },
    yaxis: { labels: { style: { colors: t.text } } },
    grid: { borderColor: t.grid },
    tooltip: { theme: t.tooltip },
    dataLabels: { enabled: false },
  });
  throughputChart.render();

  const statusLabels = Object.keys(data.status_counts);
  const statusValues = Object.values(data.status_counts);
  const statusColors = statusLabels.map(s => {
    if (s === "success") return "#10b981";
    if (s === "failed") return "#ef4444";
    if (s === "dead_lettered") return "#f97316";
    return "#6b7280";
  });

  if (statusLabels.length > 0) {
    statusChart = new ApexCharts(document.querySelector("#status-chart"), {
      series: statusValues,
      labels: statusLabels,
      chart: { type: "donut", height: 280, background: "transparent", foreColor: t.text },
      colors: statusColors,
      legend: { position: "bottom", labels: { colors: t.text } },
      plotOptions: { pie: { donut: { size: "60%" } } },
      dataLabels: { style: { colors: [t.dataLabel] } },
      tooltip: { theme: t.tooltip },
    });
    statusChart.render();
  } else {
    const el = document.querySelector("#status-chart");
    if (el) el.innerHTML = `<p class="text-center text-sm text-gray-400 dark:text-gray-500 pt-24">${i18n.noData || "No data"}</p>`;
  }

  // Latency chart (only if data is available)
  const latencyEl = document.querySelector("#latency-chart");
  if (latencyEl && data.latency_trend && data.latency_trend.length > 0) {
    const avgData = data.latency_trend.map(p => ({ x: new Date(p.time).getTime(), y: p.avg_ms }));
    const p95Data = data.latency_trend.map(p => ({ x: new Date(p.time).getTime(), y: p.p95_ms }));

    latencyChart = new ApexCharts(latencyEl, {
      series: [
        { name: i18n.latencyAvg || "Avg", data: avgData },
        { name: i18n.latencyP95 || "P95", data: p95Data },
      ],
      chart: { type: "line", height: 280, toolbar: { show: false }, background: "transparent", foreColor: t.text },
      stroke: { curve: "smooth", width: [2, 2], dashArray: [0, 5] },
      colors: ["#6366f1", "#f59e0b"],
      xaxis: { type: "datetime", labels: { style: { colors: t.text } } },
      yaxis: { labels: { style: { colors: t.text }, formatter: v => Math.round(v) + "ms" } },
      grid: { borderColor: t.grid },
      tooltip: { theme: t.tooltip },
      dataLabels: { enabled: false },
      legend: { position: "top", labels: { colors: t.text } },
    });
    latencyChart.render();
  } else if (latencyEl) {
    latencyEl.innerHTML = `<p class="text-center text-sm text-gray-400 dark:text-gray-500 pt-24">${i18n.noData || "No data"}</p>`;
  }
}

let themeObserver = null;

export function observeThemeChanges(getDataFn, i18n) {
  if (themeObserver) themeObserver.disconnect();

  themeObserver = new MutationObserver(() => {
    const data = typeof getDataFn === "function" ? getDataFn() : getDataFn;
    if (data) renderCharts(data, i18n);
  });
  themeObserver.observe(document.documentElement, { attributes: true, attributeFilter: ["class"] });
}
