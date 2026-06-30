const form = document.getElementById("form-consulta");
const inputPregunta = document.getElementById("pregunta");
const btnEnviar = document.getElementById("btnEnviar");
const estadoBox = document.getElementById("estado");
const resultadoBox = document.getElementById("resultado");
const errorBox = document.getElementById("errorBox");
const errorMsg = document.getElementById("errorMsg");

let chartInstance = null;

form.addEventListener("submit", async (e) => {
  e.preventDefault();
  const pregunta = inputPregunta.value.trim();
  if (!pregunta) return;

  mostrarEstadoCarga();

  try {
    const resp = await fetch("/api/consultar", {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({ pregunta }),
    });

    const data = await resp.json();

    if (!resp.ok) {
      mostrarError(data.error || "Error desconocido.");
      return;
    }

    renderResultado(data);
  } catch (err) {
    mostrarError("No se pudo conectar con el servidor: " + err.message);
  } finally {
    btnEnviar.disabled = false;
    estadoBox.classList.add("is-hidden");
  }
});

function mostrarEstadoCarga() {
  btnEnviar.disabled = true;
  estadoBox.classList.remove("is-hidden");
  resultadoBox.classList.add("is-hidden");
  errorBox.classList.add("is-hidden");
}

function mostrarError(msg) {
  errorMsg.textContent = msg;
  errorBox.classList.remove("is-hidden");
  resultadoBox.classList.add("is-hidden");
}

function renderResultado(data) {
  errorBox.classList.add("is-hidden");
  resultadoBox.classList.remove("is-hidden");

  const motorBadge = document.getElementById("motorBadge");
  motorBadge.textContent = data.motor.toUpperCase();
  motorBadge.className = "badge " + (data.motor === "spark" ? "badge--spark" : "badge--hive");

  document.getElementById("sqlGenerado").textContent = data.sql + ";";

  document.getElementById("metricTiempo").textContent = data.metricas.tiempo_ms.toFixed(2) + " ms";
  document.getElementById("metricCpu").textContent = data.metricas.cpu_pct.toFixed(1) + " %";
  document.getElementById("metricMem").textContent = data.metricas.memoria_mb.toFixed(1) + " MB";

  document.getElementById("respuestaNl").textContent = data.respuesta_nl || "—";

  renderGrafico(data);
  renderTabla(data.columnas, data.resultados);
}

function renderGrafico(data) {
  const viz = data.visualizacion || { chart_type: "table" };
  const columnas = data.columnas;
  const filas = data.resultados;
  const titulo = document.getElementById("graficoTitulo");
  const canvas = document.getElementById("grafico");
  const kpiGrande = document.getElementById("kpiGrande");
  const wrap = canvas.parentElement;

  titulo.textContent = viz.title || "Visualización";

  if (chartInstance) {
    chartInstance.destroy();
    chartInstance = null;
  }

  if (viz.chart_type === "kpi" || (filas.length === 1 && filas[0].length === 1)) {
    canvas.classList.add("is-hidden");
    kpiGrande.classList.remove("is-hidden");
    kpiGrande.textContent = filas.length ? filas[0][0] : "—";
    wrap.style.height = "180px";
    return;
  }

  if (viz.chart_type === "table" || !columnas.length || !filas.length) {
    canvas.classList.add("is-hidden");
    kpiGrande.classList.remove("is-hidden");
    kpiGrande.textContent = "Ver tabla";
    kpiGrande.style.fontSize = "16px";
    wrap.style.height = "100px";
    return;
  }

  canvas.classList.remove("is-hidden");
  kpiGrande.classList.add("is-hidden");
  wrap.style.height = "320px";

  const idxX = viz.x ? columnas.indexOf(viz.x) : 0;
  const idxY = viz.y ? columnas.indexOf(viz.y) : columnas.length - 1;
  const xIdx = idxX >= 0 ? idxX : 0;
  const yIdx = idxY >= 0 ? idxY : columnas.length - 1;

  const labels = filas.map((f) => String(f[xIdx]));
  const valores = filas.map((f) => Number(f[yIdx]) || 0);

  const tipoChart = viz.chart_type === "line" ? "line" : viz.chart_type === "pie" ? "pie" : "bar";

  const colorPrincipal = data.motor === "spark" ? "#B8540A" : "#2B6E4F";
  const paletaPie = ["#2B6E4F", "#B8540A", "#1A1A1A", "#6B6862", "#7A9B8E", "#D8D6D0"];

  chartInstance = new Chart(canvas, {
    type: tipoChart,
    data: {
      labels: labels,
      datasets: [
        {
          label: viz.y || columnas[yIdx],
          data: valores,
          backgroundColor: tipoChart === "pie" ? paletaPie : colorPrincipal,
          borderColor: colorPrincipal,
          borderWidth: tipoChart === "line" ? 2 : 1,
          fill: false,
        },
      ],
    },
    options: {
      responsive: true,
      maintainAspectRatio: false,
      plugins: { legend: { display: tipoChart === "pie" } },
      scales: tipoChart === "pie" ? {} : {
        x: { grid: { display: false } },
        y: { grid: { color: "#EFEEEA" } },
      },
    },
  });
}

function renderTabla(columnas, filas) {
  const head = document.getElementById("tablaHead");
  const body = document.getElementById("tablaBody");
  head.innerHTML = "";
  body.innerHTML = "";

  columnas.forEach((col) => {
    const th = document.createElement("th");
    th.textContent = col;
    head.appendChild(th);
  });

  filas.forEach((fila) => {
    const tr = document.createElement("tr");
    fila.forEach((valor) => {
      const td = document.createElement("td");
      td.textContent = valor;
      tr.appendChild(td);
    });
    body.appendChild(tr);
  });
}