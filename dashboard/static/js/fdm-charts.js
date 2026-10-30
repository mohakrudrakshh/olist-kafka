
(() => {
  "use strict";

  const INTERVAL = 5000;
  const MAX_SAMPLES = 36;

  const history = {
  lag: [],
  replication: [],
  throughput: []
};

let previousOffsets = null;
let previousSampleTime = null;

  let brokers = [];
  let refreshing = false;

  const $ = id => document.getElementById(id);

  function numeric(value) {
    const n = Number(value);
    return Number.isFinite(n) ? n : 0;
  }

  function addSample(series, value) {
    series.push(numeric(value));

    if (series.length > MAX_SAMPLES) {
      series.shift();
    }
  }

  async function fetchJSON(url) {
    const response = await fetch(url, {
      cache: "no-store"
    });

    if (!response.ok) {
      throw new Error(`${url}: HTTP ${response.status}`);
    }

    return response.json();
  }

  function setupCanvas(id) {
    const canvas = $(id);
    if (!canvas) return null;

    const width = Math.max(canvas.clientWidth, 260);
    const height = 180;
    const dpr = window.devicePixelRatio || 1;

    canvas.width = Math.round(width * dpr);
    canvas.height = Math.round(height * dpr);

    const ctx = canvas.getContext("2d");
    ctx.setTransform(dpr, 0, 0, dpr, 0, 0);

    return { ctx, width, height };
  }

  function drawLine(id, values, color) {
    const canvas = setupCanvas(id);
    if (!canvas) return;

    const { ctx, width, height } = canvas;

    const left = 38;
    const right = 14;
    const top = 18;
    const bottom = 30;

    const chartWidth = width - left - right;
    const chartHeight = height - top - bottom;

    const max = Math.max(1, ...values);

    ctx.font = "11px Segoe UI, Arial";

    for (let i = 0; i <= 4; i++) {
      const y = top + (chartHeight * i / 4);
      const value = max * (1 - i / 4);

      ctx.beginPath();
      ctx.strokeStyle = "#E5EAF0";
      ctx.lineWidth = 1;
      ctx.moveTo(left, y);
      ctx.lineTo(width - right, y);
      ctx.stroke();

      ctx.fillStyle = "#64748B";
      ctx.textAlign = "right";
      ctx.fillText(
        Number.isInteger(value) ? String(value) : value.toFixed(1),
        left - 8,
        y + 4
      );
    }

    if (!values.length) return;

    ctx.beginPath();

    values.forEach((value, index) => {
      const x = left + (
        values.length === 1
          ? chartWidth / 2
          : index * chartWidth / (values.length - 1)
      );

      const y = top + chartHeight * (1 - value / max);

      if (index === 0) ctx.moveTo(x, y);
      else ctx.lineTo(x, y);
    });

    ctx.strokeStyle = color;
    ctx.lineWidth = 2.5;
    ctx.lineJoin = "round";
    ctx.stroke();

    const latest = values[values.length - 1];

    const x = left + (
      values.length === 1 ? chartWidth / 2 : chartWidth
    );

    const y = top + chartHeight * (1 - latest / max);

    ctx.beginPath();
    ctx.arc(x, y, 4, 0, Math.PI * 2);
    ctx.fillStyle = color;
    ctx.fill();

    ctx.fillStyle = "#64748B";
    ctx.font = "10px Segoe UI, Arial";
    ctx.textAlign = "left";
    ctx.fillText(`${values.length} samples`, left, height - 7);

    ctx.textAlign = "right";
    ctx.fillText("5s interval", width - right, height - 7);
  }

  function drawLeadership() {
    const canvas = setupCanvas("fdm-leader-chart");
    if (!canvas) return;

    const { ctx, width, height } = canvas;

    const left = 38;
    const right = 14;
    const top = 20;
    const bottom = 34;

    const chartWidth = width - left - right;
    const chartHeight = height - top - bottom;

    const max = Math.max(
      1,
      ...brokers.map(b => b.leaders)
    );

    ctx.font = "11px Segoe UI, Arial";

    for (let i = 0; i <= 4; i++) {
      const y = top + chartHeight * i / 4;

      ctx.beginPath();
      ctx.strokeStyle = "#E5EAF0";
      ctx.moveTo(left, y);
      ctx.lineTo(width - right, y);
      ctx.stroke();

      ctx.fillStyle = "#64748B";
      ctx.textAlign = "right";
      ctx.fillText(
        (max * (1 - i / 4)).toFixed(0),
        left - 8,
        y + 4
      );
    }

    if (!brokers.length) return;

    const colors = ["#95BF00", "#3979B5", "#7887A5"];
    const slot = chartWidth / brokers.length;

    brokers.forEach((broker, index) => {
      const barWidth = Math.min(64, slot * 0.55);

      const x = left + index * slot +
        (slot - barWidth) / 2;

      const barHeight = chartHeight * broker.leaders / max;
      const y = top + chartHeight - barHeight;

      ctx.fillStyle = colors[index % colors.length];
      ctx.fillRect(x, y, barWidth, barHeight);

      ctx.fillStyle = "#17212B";
      ctx.font = "bold 11px Segoe UI, Arial";
      ctx.textAlign = "center";
      ctx.fillText(
        String(broker.leaders),
        x + barWidth / 2,
        Math.max(12, y - 7)
      );

      ctx.font = "10px Segoe UI, Arial";
      ctx.fillStyle = "#64748B";
      ctx.fillText(
        `Broker ${broker.id}`,
        x + barWidth / 2,
        height - 8
      );
    });
  }

  function drawAll() {
    drawLine("fdm-lag-chart", history.lag, "#3979B5");
    drawLine(
      "fdm-replication-chart",
      history.replication,
      "#D48735"
    );
    drawLeadership();
    drawLine(
  "fdm-throughput-chart",
  history.throughput,
  "#8DB600"
);
  }

  async function refreshCharts() {
    if (refreshing) return;
    refreshing = true;

    try {
      const results = await Promise.allSettled([
        fetchJSON("/api/consumers"),
        fetchJSON("/api/brokers/health")
      ]);

      let successful = 0;

      if (
        results[0].status === "fulfilled" &&
        Array.isArray(results[0].value)
      ) {
        const groups = results[0].value;
        // Collect unique Kafka partition end offsets.
const currentOffsets = new Map();

for (const group of groups) {
  if (group.error || !Array.isArray(group.details)) continue;

  for (const partition of group.details) {
    const key = `${partition.topic}:${partition.partition}`;
    const offset = Number(partition.end_offset);

    if (Number.isFinite(offset)) {
      currentOffsets.set(key, offset);
    }
  }
}

const now = performance.now();

if (previousOffsets !== null && previousSampleTime !== null) {
  const elapsedSeconds = (now - previousSampleTime) / 1000;
  let newMessages = 0;

  for (const [key, offset] of currentOffsets) {
    if (!previousOffsets.has(key)) continue;

    const difference = offset - previousOffsets.get(key);

    if (difference > 0) {
      newMessages += difference;
    }
  }

  const rate = elapsedSeconds > 0
    ? newMessages / elapsedSeconds
    : 0;

  addSample(history.throughput, rate);

  $("fdm-throughput-note").textContent =
    `${rate.toFixed(2)} messages/sec · ${newMessages} new messages`;
} else {
  $("fdm-throughput-note").textContent =
    "Collecting initial offset baseline...";
}

previousOffsets = currentOffsets;
previousSampleTime = now;

        const totalLag = groups.reduce(
          (sum, group) =>
            sum + (group.error ? 0 : numeric(group.total_lag)),
          0
        );

        addSample(history.lag, totalLag);

        $("fdm-lag-note").textContent =
          `Current consumer lag: ${totalLag.toLocaleString()} messages`;

        successful++;
      } else {
        $("fdm-lag-note").textContent =
          "Consumer metrics unavailable";
      }

      if (results[1].status === "fulfilled") {
        const data = results[1].value;

        if (Array.isArray(data.brokers)) {
          brokers = data.brokers
            .filter(b => b.registered)
            .map(b => ({
              id: b.id,
              leaders: numeric(b.leader_partitions)
            }))
            .sort((a, b) => a.id - b.id);

          const underReplicated =
            numeric(data.under_replicated_partitions);

          addSample(history.replication, underReplicated);

          const totalLeaders = brokers.reduce(
            (sum, b) => sum + b.leaders,
            0
          );

          $("fdm-leader-note").textContent =
            `${totalLeaders} partition leaders across ${brokers.length} registered brokers`;

          $("fdm-replication-note").textContent =
            `${underReplicated} under-replicated partitions`;

          successful++;
        }
      }

      $("fdm-chart-status").textContent =
        successful === 2
          ? "● Live · 5s updates"
          : successful === 1
          ? "● Partial telemetry"
          : "● Telemetry unavailable";

      drawAll();

    } catch (error) {
      console.error("Kafka telemetry error:", error);
      $("fdm-chart-status").textContent =
        "● Telemetry unavailable";
    } finally {
      refreshing = false;
    }
  }

  if (!$("fdm-lag-chart")) return;

  window.addEventListener("resize", drawAll);

  refreshCharts();
  setInterval(refreshCharts, INTERVAL);
})();
