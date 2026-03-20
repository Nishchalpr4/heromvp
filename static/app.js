/**
 * Zone 1 Entity Graph Explorer — Application Logic
 * ===================================================
 * Handles API calls, UI state, extraction workflow, and legend rendering.
 */

// ── Entity type colors (must match backend models.py/ontology) ──────────────
const ENTITY_TYPE_COLORS = {
    "LegalEntity":          "#4A90D9",
    "ExternalOrganization": "#E67E22",
    "BusinessUnit":         "#27AE60",
    "Sector":               "#8E44AD",
    "Industry":             "#2C3E50",
    "SubIndustry":          "#16A085",
    "EndMarket":            "#D35400",
    "Channel":              "#C0392B",
    "ProductDomain":        "#2980B9",
    "ProductFamily":        "#3498DB",
    "ProductLine":          "#1ABC9C",
    "Site":                 "#E74C3C",
    "Geography":            "#F39C12",
    "Person":               "#9B59B6",
    "Role":                 "#7F8C8D",
    "Technology":           "#00BCD4",
    "Capability":           "#FF5722",
    "Brand":                "#FF9800",
    "Initiative":           "#795548",
    "Financial":            "#4CAF50",
    "Program":              "#607D8B",
    "Management":           "#FFD700",
    "Competitors":          "#C0392B",
    "ProductPortfolio":     "#3b82f6",
};

// ── State ──────────────────────────────────────────────────────────
let graph;
let chunkCount = 0;

// ── Initialize ─────────────────────────────────────────────────────
document.addEventListener("DOMContentLoaded", () => {
    graph = new GraphVisualization("#graph-svg", "#node-tooltip");

    // Render legend
    renderLegend();

    // Check health
    checkHealth();

    // Button handlers
    document.getElementById("btn-extract").addEventListener("click", handleExtract);
    document.getElementById("btn-reset").addEventListener("click", handleReset);

    // Ctrl+Enter shortcut
    document.getElementById("text-input").addEventListener("keydown", (e) => {
        if ((e.ctrlKey || e.metaKey) && e.key === "Enter") {
            handleExtract();
        }
    });

    // Sharing logic
    initSharing();

    // Initial data fetch
    fetchGraph();
});

// ── Sharing Logic ───────────────────────────────────────────────
function initSharing() {
    const btnDownloadState = document.getElementById("btn-download-state");
    if (!btnDownloadState) return;

    btnDownloadState.addEventListener("click", () => {
        if (!graph || !graph.nodes || graph.nodes.length === 0) {
            alert("The graph is currently empty.");
            return;
        }

        const state = {
            nodes: graph.nodes,
            links: graph.links,
            timestamp: new Date().toISOString()
        };

        const blob = new Blob([JSON.stringify(state, null, 2)], { type: "application/json" });
        const url = URL.createObjectURL(blob);
        const a = document.createElement("a");
        a.href = url;
        a.download = `zone1_graph_state_${new Date().getTime()}.json`;
        document.body.appendChild(a);
        a.click();
        document.body.removeChild(a);
        URL.revokeObjectURL(url);
    });
}

// ── Legend ──────────────────────────────────────────────────────────
function renderLegend() {
    const legendEl = document.getElementById("graph-legend");
    let html = "";
    for (const [type, color] of Object.entries(ENTITY_TYPE_COLORS)) {
        const label = type.replace(/([A-Z])/g, " $1").trim();
        html += `<div class="legend-item">
            <div class="legend-dot" style="background:${color}"></div>
            <span>${label}</span>
        </div>`;
    }
    legendEl.innerHTML = html;
}

// ── Fetch Initial Data ────────────────────────────────────────────────
async function fetchGraph() {
    try {
        const res = await fetch("/api/graph");
        const data = await res.json();
        if (data && data.nodes) {
            graph.update(data);
            document.getElementById("entity-count").textContent = data.stats.total_entities;
            document.getElementById("relation-count").textContent = data.stats.total_relations;
        }
    } catch (e) {
        console.error("Initial fetch failed:", e);
    }
}

// ── Health Check ───────────────────────────────────────────────────
async function checkHealth() {
    try {
        const res = await fetch("/api/health");
        const data = await res.json();

        const llmInfo = document.getElementById("llm-info");
        if (data.llm_configured) {
            llmInfo.textContent = `LLM: ${data.llm_model}`;
            setStatus("Ready — LLM configured");
        } else {
            llmInfo.textContent = "⚠ LLM_API_KEY not set";
            setStatus("Warning: Set LLM_API_KEY in .env file", true);
        }
    } catch (e) {
        setStatus("Error: Cannot connect to server", true);
    }
}

// ── Extract Handler ────────────────────────────────────────────────
async function handleExtract() {
    const textInput = document.getElementById("text-input");
    const docName = document.getElementById("doc-name").value.trim() || "User Input";
    const sectionRef = document.getElementById("section-ref").value.trim() || "chunk";
    const text = textInput.value.trim();

    const metadata = {
        company_name: document.getElementById("doc-company").value,
        company_ticker: document.getElementById("doc-ticker").value,
        fiscal_year: parseInt(document.getElementById("doc-year").value),
        fiscal_period: document.getElementById("doc-period").value
    };

    if (!text) {
        setStatus("Please paste some text to extract from", true);
        return;
    }

    const btn = document.getElementById("btn-extract");
    const btnText = btn.querySelector(".btn-text");
    const btnLoading = btn.querySelector(".btn-loading");

    btn.disabled = true;
    btnText.style.display = "none";
    btnLoading.style.display = "inline-flex";
    setStatus("Extracting entities...");

    try {
        const res = await fetch("/api/extract", {
            method: "POST",
            headers: { "Content-Type": "application/json" },
            body: JSON.stringify({
                text: text,
                document_name: docName,
                section_ref: sectionRef,
                metadata: metadata
            }),
        });

        if (!res.ok) {
            const err = await res.json();
            throw new Error(err.detail || "Extraction failed");
        }

        const data = await res.json();
        graph.update(data.graph);

        const detailPanel = document.getElementById("detail-panel");
        if (detailPanel) detailPanel.style.display = "none";

        document.getElementById("entity-count").textContent = data.graph.stats.total_entities;
        document.getElementById("relation-count").textContent = data.graph.stats.total_relations;

        chunkCount++;
        document.getElementById("chunk-count").textContent = `${chunkCount} chunk${chunkCount !== 1 ? "s" : ""} processed`;

        showExtractionResult(data);
        addLogEntry(docName, data.diff);

        textInput.value = "";
        setStatus(`Extracted ${data.extraction.entities_extracted} entities`);

    } catch (e) {
        setStatus(`Error: ${e.message}`, true);
        showError(e.message);
    } finally {
        btn.disabled = false;
        btnText.style.display = "inline";
        btnLoading.style.display = "none";
    }
}

// ── Reset Handler ──────────────────────────────────────────────────
async function handleReset() {
    if (!confirm("Reset the entire graph database?")) return;
    
    const btn = document.getElementById("btn-reset");
    const originalText = btn.innerHTML;
    
    try {
        btn.disabled = true;
        btn.innerHTML = `
            <svg class="spinner" width="16" height="16" viewBox="0 0 18 18" style="margin-right: 6px;">
                <circle cx="9" cy="9" r="7" stroke="currentColor" stroke-width="2" fill="none" stroke-dasharray="30 14" stroke-linecap="round"></circle>
            </svg>
            Resetting...
        `;
        setStatus("Resetting graph...");

        const res = await fetch("/api/graph", { method: "DELETE" });
        if (!res.ok) throw new Error("Server reset failed");
        
        graph.reset();
        chunkCount = 0;

        document.getElementById("entity-count").textContent = "0";
        document.getElementById("relation-count").textContent = "0";
        document.getElementById("chunk-count").textContent = "0 chunks processed";
        document.getElementById("extraction-result").style.display = "none";
        document.getElementById("log-entries").innerHTML = "";

        setStatus("Graph reset successfully");
    } catch (e) {
        setStatus(`Error during reset: ${e.message}`, true);
    } finally {
        btn.disabled = false;
        btn.innerHTML = originalText;
    }
}

// ── Show Extraction Result ─────────────────────────────────────────
function showExtractionResult(data) {
    const resultEl = document.getElementById("extraction-result");
    const contentEl = document.getElementById("result-content");

    const diff = data.diff;
    const ext = data.extraction;

    let html = `
        <div class="result-stat">
            <span class="label">Entities extracted</span>
            <span class="value">${ext.entities_extracted}</span>
        </div>
        <div class="result-stat">
            <span class="label">Relations extracted</span>
            <span class="value">${ext.relations_extracted}</span>
        </div>
        <div class="result-stat">
            <span class="label">New entities</span>
            <span class="value">${(diff.new_entities || []).length}</span>
        </div>

        ${ext.thought_process ? `
        <div class="result-warnings" style="border-color:var(--accent-blue); background:rgba(59, 130, 246, 0.05); margin-top:20px;">
            <div style="font-size:10px; color:var(--accent-blue); margin-bottom:4px; text-transform:uppercase; font-weight:600;">System Logic</div>
            <div style="font-size:11px; color:var(--text-secondary);">${ext.thought_process}</div>
        </div>
        ` : ''}

        ${ext.llm_analysis_summary ? `
        <div class="chunk-card" style="margin-top: 20px; border-color: var(--accent-teal);">
             <div class="chunk-summary">${ext.llm_analysis_summary}</div>
        </div>
        ` : ''}

        ${ext.discoveries && ext.discoveries.length > 0 ? `
        <div class="result-warnings" style="border-color:#f59e0b; background:rgba(245, 158, 11, 0.05); margin-top:20px;">
            <div style="font-size:10px; color:#d97706; margin-bottom:6px; text-transform:uppercase; font-weight:700;">🆕 New Types Added To NeonDB</div>
            ${ext.discoveries.map(d => `
                <div style="font-size:11px; margin-bottom:4px; padding: 4px; background: rgba(245,158,11,0.1); border-radius: 4px;">
                    <strong style="color: #b45309;">${d.type}:</strong> ${d.suggested_label}
                </div>
            `).join('')}
        </div>
        ` : ''}
    `;

    contentEl.innerHTML = html;
    resultEl.style.display = "block";

    const jsonEl = document.getElementById("json-output");
    if (jsonEl) jsonEl.textContent = JSON.stringify(data, null, 2);
}

// ── Show Error ─────────────────────────────────────────────────────
function showError(message) {
    const contentEl = document.getElementById("result-content");
    contentEl.innerHTML = `<div style="color:#ef4444;font-size:12px;">${message}</div>`;
    document.getElementById("extraction-result").style.display = "block";
}

// ── Log Entry ──────────────────────────────────────────────────────
function addLogEntry(docName, diff) {
    const logEl = document.getElementById("log-entries");
    const entry = document.createElement("div");
    entry.className = "log-entry";
    entry.innerHTML = `
        <span class="log-doc">${docName}</span>
        <span class="log-stats">+${(diff.new_entities || []).length}E</span>
    `;
    logEl.insertBefore(entry, logEl.firstChild);
}

// ── Status Bar ─────────────────────────────────────────────────────
function setStatus(text, isError = false) {
    const statusEl = document.getElementById("status-text");
    statusEl.textContent = text;
    statusEl.style.color = isError ? "#ef4444" : "#4a5568";
}
