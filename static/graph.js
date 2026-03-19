/**
 * Zone 1 Entity Graph Explorer — D3 Graph Visualization (v2)
 * ===========================================================
 * Clean, readable graph with:
 *   - Hierarchical radial layout (root entity in center)
 *   - Large, clearly labeled nodes with entity type badges
 *   - Curved, labeled edges with arrows
 *   - Smooth zoom & pan
 *   - Click for details, hover for highlights
 *   - Proper spacing so nothing overlaps
 */

class GraphVisualization {
    constructor(svgSelector, tooltipSelector) {
        this.svgEl = document.querySelector(svgSelector);
        this.tooltipEl = document.querySelector(tooltipSelector);

        this.svg = d3.select(this.svgEl);
        this.width = this.svgEl.clientWidth;
        this.height = this.svgEl.clientHeight;

        // Data
        this.rawNodes = [];
        this.rawLinks = [];
        this.nodes = [];
        this.links = [];
        this.collapsedNodes = new Set();

        // Main container
        this.container = this.svg.append("g").attr("class", "graph-world");

        // Layers (order matters: edges behind nodes)
        this.linkGroup = this.container.append("g").attr("class", "links-layer");
        this.nodeGroup = this.container.append("g").attr("class", "nodes-layer");

        // Arrow marker
        const defs = this.svg.append("defs");
        defs.append("marker")
            .attr("id", "arrow")
            .attr("viewBox", "0 -5 10 10")
            .attr("refX", 10)
            .attr("refY", 0)
            .attr("markerWidth", 8)
            .attr("markerHeight", 8)
            .attr("orient", "auto")
            .append("path")
            .attr("d", "M0,-4L8,0L0,4")
            .attr("fill", "#475569");

        // Glow filter for new nodes
        const glowFilter = defs.append("filter")
            .attr("id", "glow")
            .attr("x", "-50%").attr("y", "-50%")
            .attr("width", "200%").attr("height", "200%");
        glowFilter.append("feGaussianBlur")
            .attr("stdDeviation", "4")
            .attr("result", "coloredBlur");
        const feMerge = glowFilter.append("feMerge");
        feMerge.append("feMergeNode").attr("in", "coloredBlur");
        feMerge.append("feMergeNode").attr("in", "SourceGraphic");

        // Drop shadow for nodes
        const shadow = defs.append("filter")
            .attr("id", "shadow")
            .attr("x", "-20%").attr("y", "-20%")
            .attr("width", "140%").attr("height", "140%");
        shadow.append("feDropShadow")
            .attr("dx", "0").attr("dy", "4")
            .attr("stdDeviation", "8")
            .attr("flood-color", "rgba(0,0,0,0.6)");

        // Zoom
        this.zoom = d3.zoom()
            .scaleExtent([0.1, 4])
            .on("zoom", (event) => {
                this.container.attr("transform", event.transform);
            });
        this.svg.call(this.zoom);

        // Force simulation - STRICT ARCHITECTURAL TOP-TO-BOTTOM
        this.simulation = d3.forceSimulation()
            .force("link", d3.forceLink().id(d => d.id).distance(150).strength(0.5))
            .force("charge", d3.forceManyBody().strength(-800).distanceMax(1000)) 
            .force("collision", d3.forceCollide().radius(100).iterations(2))
            
            // Strict Tiering: Force Y pulls nodes to their specific architectural level
            .force("y", d3.forceY(d => this._getNodeLevel(d.type) * 200 + 100).strength(0.8))
            
            // Clustering Force: Keep children near parents
            .force("cluster", (alpha) => this._applyClustering(alpha))

            // Horizontal Separation: Organize categories from Left to Right
            .force("x", d3.forceX(d => {
                const type = d.type;
                const level = this._getNodeLevel(type);
                if (level === 0) return this.width / 2; // Apex centered
                
                // Left Wing
                if (["Management", "Person", "Role"].includes(type)) return this.width * 0.3;
                // Right Wing
                if (["Competitors", "ExternalOrganization"].includes(type)) return this.width * 0.7;
                // Center-Left
                if (["Geography", "Site"].includes(type)) return this.width * 0.4;
                // Center-Right
                if (["BusinessUnit", "ProductDomain", "ProductFamily", "ProductLine"].includes(type)) return this.width * 0.6;
                
                return this.width / 2;
            }).strength(0.3)) // Stronger X force for dedicated columns
            
            .alphaDecay(0.04)
            .on("tick", () => this._tick());

        this.simulation.stop();

        this._emptyStateShown = false;
        this._showEmptyState();

        window.addEventListener("resize", () => this._onResize());

        // Close details handler
        const closeBtn = document.getElementById("btn-close-details");
        if (closeBtn) {
            closeBtn.addEventListener("click", () => {
                document.getElementById("detail-panel").style.display = "none";
            });
        }
    }

    // ── Public API ──────────────────────────────────────────────

    update(graphData) {
        this._hideEmptyState();

        this.rawNodes = graphData.nodes;
        this.rawLinks = graphData.links;

        this._applyFilterAndRender();

        // Auto-fit after settling
        setTimeout(() => this._fitToView(), 2500);
    }

    _applyFilterAndRender() {
        const newNodeIds = new Set(this.rawNodes.filter(n => n.is_new).map(n => n.id));
        const newLinkIds = new Set(this.rawLinks.filter(l => l.is_new).map(l => l.id));

        // Filter out nodes that belong to collapsed parents
        // Rule: If a node S is collapsed, hide all nodes T where S -> HAS_X -> T
        const hiddenNodeIds = new Set();
        this.collapsedNodes.forEach(parentId => {
            this.rawLinks.forEach(l => {
                if (l.source === parentId || (l.source && l.source.id === parentId)) {
                    hiddenNodeIds.add(typeof l.target === 'object' ? l.target.id : l.target);
                }
            });
        });

        const visibleNodes = this.rawNodes.filter(n => !hiddenNodeIds.has(n.id));
        const visibleNodeIds = new Set(visibleNodes.map(n => n.id));
        const visibleLinks = this.rawLinks.filter(l => {
            const sid = typeof l.source === 'object' ? l.source.id : l.source;
            const tid = typeof l.target === 'object' ? l.target.id : l.target;
            return visibleNodeIds.has(sid) && visibleNodeIds.has(tid);
        });

        // Update active simulation data
        const existingNodeMap = new Map(this.nodes.map(n => [n.id, n]));
        const cx = this.width / 2;

        this.nodes = visibleNodes.map(nd => {
            const existing = existingNodeMap.get(nd.id);
            if (existing) {
                return { ...nd, x: existing.x, y: existing.y, fx: existing.fx, fy: existing.fy };
            } else {
                const tierY = this._getNodeLevel(nd.type) * 220 + 80;
                return {
                    ...nd,
                    x: cx + (Math.random() - 0.5) * 400,
                    y: tierY + (Math.random() - 0.5) * 50,
                };
            }
        });

        this.links = visibleLinks.map(l => ({
            ...l,
            source: typeof l.source === 'object' ? l.source.id : l.source,
            target: typeof l.target === 'object' ? l.target.id : l.target,
        }));

        // Render
        this._renderLinks(newLinkIds);
        this._renderNodes(newNodeIds);

        // Restart simulation
        this.simulation.nodes(this.nodes);
        this.simulation.force("link").links(this.links);
        this.simulation.alpha(1).restart();
    }

    _toggleCollapse(nodeId) {
        if (this.collapsedNodes.has(nodeId)) {
            this.collapsedNodes.delete(nodeId);
        } else {
            this.collapsedNodes.add(nodeId);
        }
        this._applyFilterAndRender();
    }

    reset() {
        this.nodes = [];
        this.links = [];
        this.linkGroup.selectAll("*").remove();
        this.nodeGroup.selectAll("*").remove();
        this.simulation.nodes([]);
        this.simulation.force("link").links([]);
        this._showEmptyState();
        // Reset zoom
        this.svg.transition().duration(500).call(
            this.zoom.transform, d3.zoomIdentity
        );
    }

    // ── Rendering ───────────────────────────────────────────────

    _renderNodes(newNodeIds) {
        const self = this;
        const nodeSelection = this.nodeGroup.selectAll(".node-group")
            .data(this.nodes, d => d.id);

        nodeSelection.exit()
            .transition().duration(300)
            .attr("opacity", 0)
            .remove();

        const enter = nodeSelection.enter()
            .append("g")
            .attr("class", d => `node-group ${newNodeIds.has(d.id) ? "node-new" : ""}`)
            .attr("opacity", 0)
            .style("cursor", "pointer")
            .call(this._drag());

        // Background card - ENLARGED
        enter.append("rect")
            .attr("class", "node-bg")
            .attr("width", 220)
            .attr("height", 70)
            .attr("x", -110)
            .attr("y", -35)
            .attr("rx", 8)
            .attr("ry", 8)
            .attr("fill", "#1a2845")
            .attr("stroke", d => {
                if (this.collapsedNodes.has(d.id)) return "#3b82f6"; // Primary blue for collapsed
                return newNodeIds.has(d.id) ? "#fbbf24" : "#334155";
            })
            .attr("stroke-width", d => (newNodeIds.has(d.id) || this.collapsedNodes.has(d.id)) ? 3 : 2)
            .style("filter", "url(#shadow)");
        
        // Collapse Indicator (Tiny (+) or (-) icon)
        enter.append("text")
            .attr("class", "collapse-icon")
            .attr("x", 95)
            .attr("y", -20)
            .attr("fill", "#94a3b8")
            .attr("font-size", "14px")
            .attr("font-weight", "bold")
            .text(d => this.collapsedNodes.has(d.id) ? "+" : "−");

        // Color accent line - larger
        enter.append("rect")
            .attr("width", 5)
            .attr("height", 70)
            .attr("x", -110)
            .attr("y", -35)
            .attr("rx", 2)
            .attr("fill", d => d.color || "#3b82f6");

        // Icon bg - larger
        enter.append("circle")
            .attr("cx", -75)
            .attr("cy", 0)
            .attr("r", 16)
            .attr("fill", d => d.color || "#3b82f6")
            .attr("opacity", 0.2);

        // Icon text - larger
        enter.append("text")
            .attr("x", -75)
            .attr("y", 6)
            .attr("text-anchor", "middle")
            .attr("fill", d => d.color || "#3b82f6")
            .attr("font-size", "14px")
            .attr("font-weight", "700")
            .attr("pointer-events", "none")
            .text(d => this._getNodeIcon(d.type));

        // Type label - larger
        enter.append("text")
            .attr("x", -35)
            .attr("y", -8)
            .attr("fill", d => d.color || "#3b82f6")
            .attr("font-size", "11px")
            .attr("font-weight", "700")
            .attr("letter-spacing", "0.08em")
            .attr("pointer-events", "none")
            .text(d => this._formatType(d.type));

        // Entity name main label - MUCH LARGER
        enter.append("text")
            .attr("x", -35)
            .attr("y", 16)
            .attr("fill", "#ffffff")
            .attr("font-size", "14px")
            .attr("font-weight", "700")
            .attr("pointer-events", "none")
            .text(d => this._truncateLabel(d.label, 24));

        // Interaction
        enter.on("mouseenter", function(event, d) {
                self._highlightConnections(d, true);
                self._showTooltip(event, d);
            })
            .on("mouseleave", function(event, d) {
                self._highlightConnections(d, false);
                self._hideTooltip();
            })
            .on("click", (event, d) => this._showDetails(d, "node"))
            .on("dblclick", (event, d) => {
                event.stopPropagation();
                this._toggleCollapse(d.id);
            });

        // Animate in
        enter.transition().duration(600).ease(d3.easeCubicOut)
            .attr("opacity", 1);

        // Merge
        const merged = enter.merge(nodeSelection);
    }

    _renderLinks(newLinkIds) {
        const self = this;
        const linkSelection = this.linkGroup.selectAll(".edge-group")
            .data(this.links, d => d.id);

        linkSelection.exit()
            .transition().duration(300)
            .attr("opacity", 0)
            .remove();

        const enter = linkSelection.enter()
            .append("g")
            .attr("class", "edge-group")
            .attr("opacity", 0);

        // Curved path - THICKER AND MORE VISIBLE
        enter.append("path")
            .attr("class", d => `edge-path ${newLinkIds.has(d.id) ? "edge-new" : ""}`)
            .attr("fill", "none")
            .attr("stroke", d => {
                if (newLinkIds.has(d.id)) return "#fbbf24";
                if (d.relation === "COMPETES_WITH") return "#ef4444";
                return "#475569";
            })
            .attr("stroke-width", d => newLinkIds.has(d.id) ? 3 : 2)
            .attr("stroke-dasharray", d => d.relation === "COMPETES_WITH" ? "8,5" : "none")
            .attr("stroke-linecap", "round")
            .attr("stroke-linejoin", "round")
            .attr("marker-end", "url(#arrow)");

        // Edge label background - larger
        enter.append("rect")
            .attr("class", "edge-label-bg")
            .attr("rx", 4).attr("ry", 4)
            .attr("fill", "#0f172a")
            .attr("stroke", "#475569")
            .attr("stroke-width", 1);

        // Relation label on edge - LARGER
        enter.append("text")
            .attr("class", "edge-label")
            .attr("text-anchor", "middle")
            .attr("dy", "0.35em")
            .attr("fill", d => newLinkIds.has(d.id) ? "#fbbf24" : "#94a3b8")
            .attr("font-size", "11px")
            .attr("font-weight", "600")
            .attr("letter-spacing", "0.05em")
            .text(d => d.relation.replace(/_/g, " "));

        enter.on("click", (event, d) => this._showDetails(d, "link"));

        // Animate in
        enter.transition().duration(600).delay(200)
            .attr("opacity", 1);

        enter.merge(linkSelection);
    }

    // ── Layout Tick ────────────────────────────────────────────

    _tick() {
        // Update paths with curves
        this.linkGroup.selectAll(".edge-path")
            .attr("d", d => {
                const dx = d.target.x - d.source.x;
                const dy = d.target.y - d.source.y;
                const dist = Math.sqrt(dx * dx + dy * dy) || 1;

                // Box intersection for target to keep arrow visible
                // Card dimensions: 220x70 => half: 110x35
                const W = 110 + 4; // padding
                const H = 35 + 4; // padding
                const scaleTarget = Math.min(
                    Math.abs(dx) > 0.1 ? Math.abs(W / dx) : W,
                    Math.abs(dy) > 0.1 ? Math.abs(H / dy) : H,
                    1.0
                ) * 0.98;

                const sx = d.source.x;
                const sy = d.source.y;
                const tx = d.target.x - dx * scaleTarget;
                const ty = d.target.y - dy * scaleTarget;

                // S-Curve (Cubic Bezier) for Architectural flow
                // We use dynamic control points to ensure the flow is clearly top-down
                const midY = sy + (ty - sy) / 2;
                return `M${sx},${sy} C${sx},${midY} ${tx},${midY} ${tx},${ty}`;
            });

        // Position edge labels at midpoint of S-Curve
        this.linkGroup.selectAll(".edge-label")
            .attr("x", d => (d.source.x + d.target.x) / 2)
            .attr("y", d => (d.source.y + d.target.y) / 2);

        // Position edge label backgrounds (Mathematically, VERY FAST)
        this.linkGroup.selectAll(".edge-label-bg")
            .attr("width", d => d.relation.length * 6 + 10)
            .attr("height", 16)
            .attr("x", d => ((d.source.x + d.target.x) / 2) - (d.relation.length * 6 + 10) / 2)
            .attr("y", d => ((d.source.y + d.target.y) / 2) - 8);

        // Update node positions
        this.nodeGroup.selectAll(".node-group")
            .attr("transform", d => `translate(${d.x}, ${d.y})`);
    }

    // ── Highlight Connections ───────────────────────────────────

    _highlightConnections(hoveredNode, highlight) {
        const connectedNodeIds = new Set();
        connectedNodeIds.add(hoveredNode.id);

        this.links.forEach(l => {
            const sid = typeof l.source === "object" ? l.source.id : l.source;
            const tid = typeof l.target === "object" ? l.target.id : l.target;
            if (sid === hoveredNode.id) connectedNodeIds.add(tid);
            if (tid === hoveredNode.id) connectedNodeIds.add(sid);
        });

        if (highlight) {
            // Dim non-connected
            this.nodeGroup.selectAll(".node-group")
                .transition().duration(200)
                .attr("opacity", d => connectedNodeIds.has(d.id) ? 1 : 0.15);

            this.linkGroup.selectAll(".edge-group")
                .transition().duration(200)
                .attr("opacity", d => {
                    const sid = typeof d.source === "object" ? d.source.id : d.source;
                    const tid = typeof d.target === "object" ? d.target.id : d.target;
                    return (sid === hoveredNode.id || tid === hoveredNode.id) ? 1 : 0.08;
                });
        } else {
            // Restore
            this.nodeGroup.selectAll(".node-group")
                .transition().duration(300)
                .attr("opacity", 1);
            this.linkGroup.selectAll(".edge-group")
                .transition().duration(300)
                .attr("opacity", 1);
        }
    }

    // ── Fit to View ─────────────────────────────────────────────

    _fitToView() {
        if (this.nodes.length === 0) return;

        const padding = 80;
        let minX = Infinity, minY = Infinity, maxX = -Infinity, maxY = -Infinity;
        this.nodes.forEach(n => {
            if (n.x < minX) minX = n.x;
            if (n.y < minY) minY = n.y;
            if (n.x > maxX) maxX = n.x;
            if (n.y > maxY) maxY = n.y;
        });

        const graphWidth = maxX - minX + padding * 2;
        const graphHeight = maxY - minY + padding * 2;
        const scale = Math.min(
            this.width / graphWidth,
            this.height / graphHeight,
            1.5  // Don't zoom in too much
        );
        const translateX = (this.width - (minX + maxX) * scale) / 2;
        const translateY = (this.height - (minY + maxY) * scale) / 2;

        this.svg.transition().duration(800).ease(d3.easeCubicOut)
            .call(this.zoom.transform, d3.zoomIdentity
                .translate(translateX, translateY)
                .scale(scale));
    }

    // ── Drag Behavior ───────────────────────────────────────────

    _drag() {
        return d3.drag()
            .on("start", (event, d) => {
                if (!event.active) this.simulation.alphaTarget(0.1).restart();
                d.fx = d.x;
                d.fy = d.y;
            })
            .on("drag", (event, d) => {
                d.fx = event.x;
                d.fy = event.y;
            })
            .on("end", (event, d) => {
                if (!event.active) this.simulation.alphaTarget(0);
                d.fx = event.x;
                d.fy = event.y;
            });
    }

    // ── Tooltip ─────────────────────────────────────────────────

    _showTooltip(event, d) {
        const color = d.color || "#3b82f6";
        let html = `<div class="tt-type" style="color:${color}">${this._formatType(d.type)}</div>`;
        html += `<div class="tt-name">${d.label}</div>`;

        if (d.aliases && d.aliases.length > 0) {
            html += `<div class="tt-aliases">Also: ${d.aliases.join(", ")}</div>`;
        }

        // Show connections count
        let connections = 0;
        this.links.forEach(l => {
            const sid = typeof l.source === "object" ? l.source.id : l.source;
            const tid = typeof l.target === "object" ? l.target.id : l.target;
            if (sid === d.id || tid === d.id) connections++;
        });
        html += `<div class="tt-connections">${connections} connection${connections !== 1 ? "s" : ""}</div>`;

        if (d.attributes && Object.keys(d.attributes).length > 0) {
            html += `<div class="tt-attrs">`;
            for (const [key, val] of Object.entries(d.attributes)) {
                html += `<div><strong>${key}:</strong> ${val}</div>`;
            }
            html += `</div>`;
        }

        this.tooltipEl.innerHTML = html;
        this.tooltipEl.style.display = "block";

        const rect = this.svgEl.getBoundingClientRect();
        let left = event.clientX - rect.left + 16;
        let top = event.clientY - rect.top - 10;

        const ttRect = this.tooltipEl.getBoundingClientRect();
        if (left + ttRect.width > rect.width) left = left - ttRect.width - 32;
        if (top + ttRect.height > rect.height) top = rect.height - ttRect.height - 8;
        if (top < 0) top = 8;

        this.tooltipEl.style.left = left + "px";
        this.tooltipEl.style.top = top + "px";
    }

    _hideTooltip() {
        this.tooltipEl.style.display = "none";
    }

    // ── Detail Panel ──────────────────────────────────────────────

    _showDetails(d, itemType) {
        const panel = document.getElementById("detail-panel");
        const content = document.getElementById("detail-content");
        if (!panel || !content) return;

        let html = ``;
        if (itemType === "node") {
            const color = d.color || "#3b82f6";
            // Get latest status/confidence from evidence
            const latestEv = d.evidence && d.evidence.length > 0 ? d.evidence[0] : null;
            const status = latestEv ? latestEv.status : 'PENDING';

            html += `
                <div style="display:flex; align-items:center; justify-content:space-between; margin-bottom:12px;">
                    <div class="detail-type-badge" style="color:${color}; border-color:${color}">${this._formatType(d.type)}</div>
                    <div class="status-indicator status-${status}">
                        <div class="status-dot"></div>
                        <span>${status}</span>
                    </div>
                </div>
            `;
            html += `<div class="detail-name">${d.label}</div>`;
            
            if (d.aliases && d.aliases.length > 0) {
                html += `<div class="detail-aliases">Also known as: ${d.aliases.join(", ")}</div>`;
            }

            // --- QUANT SECTION ---
            if (d.quant_metrics && d.quant_metrics.length > 0) {
                html += `
                    <div class="detail-section">
                        <h4>Quantitative Analysis</h4>
                        <div class="quant-section">
                            ${d.quant_metrics.map(q => `
                                <div class="quant-item">
                                    <span class="quant-metric">${q.metric}</span>
                                    <div>
                                        <span class="quant-value">${q.value.toLocaleString()}</span>
                                        <span class="quant-unit">${q.unit || ''}</span>
                                        <span class="quant-period">${q.period || ''}</span>
                                    </div>
                                </div>
                            `).join('')}
                        </div>
                    </div>
                `;
            }

            // Attributes
            if (d.attributes && Object.keys(d.attributes).length > 0) {
                html += `<div class="detail-section"><h4>Attributes</h4>`;
                for (const [key, val] of Object.entries(d.attributes)) {
                    html += `<div class="detail-attr"><span class="attr-key">${key}</span><span class="attr-val">${val}</span></div>`;
                }
                html += `</div>`;
            }

            // Relations
            const rels = [];
            this.links.forEach(l => {
                const sid = typeof l.source === "object" ? l.source.id : l.source;
                const tid = typeof l.target === "object" ? l.target.id : l.target;
                if (sid === d.id) {
                    const target = this.nodes.find(n => n.id === tid);
                    rels.push(`→ <strong>${l.relation.replace(/_/g, " ")}</strong> → ${target ? target.label : tid}`);
                }
                if (tid === d.id) {
                    const source = this.nodes.find(n => n.id === sid);
                    rels.push(`${source ? source.label : sid} → <strong>${l.relation.replace(/_/g, " ")}</strong> →`);
                }
            });
            if (rels.length > 0) {
                html += `<div class="detail-section"><h4>Relations</h4>`;
                rels.forEach(r => html += `<div class="detail-relation">${r}</div>`);
                html += `</div>`;
            }

        } else if (itemType === "link") {
            const latestEv = d.evidence && d.evidence.length > 0 ? d.evidence[0] : null;
            const status = latestEv ? latestEv.status : 'PENDING';

            html += `
                <div style="display:flex; align-items:center; justify-content:space-between; margin-bottom:12px;">
                    <div class="detail-type-badge" style="color:#3b82f6; border-color:#3b82f6">RELATION</div>
                    <div class="status-indicator status-${status}">
                        <div class="status-dot"></div>
                        <span>${status}</span>
                    </div>
                </div>
            `;
            html += `<div class="detail-name">${d.relation.replace(/_/g, " ")}</div>`;
            const src = this.nodes.find(n => n.id === (typeof d.source === "object" ? d.source.id : d.source));
            const tgt = this.nodes.find(n => n.id === (typeof d.target === "object" ? d.target.id : d.target));
            html += `<div class="detail-flow">
                <span class="flow-entity" style="color:${src ? (src.color || '#3b82f6') : '#fff'}">${src ? src.label : "?"}</span>
                <span class="flow-arrow">→</span>
                <span class="flow-entity" style="color:${tgt ? (tgt.color || '#3b82f6') : '#fff'}">${tgt ? tgt.label : "?"}</span>
            </div>`;
        }

        // --- EVIDENCE & TRUST SECTION ---
        if (d.evidence && d.evidence.length > 0) {
            html += `<div class="detail-section"><h4>Evidence Trail</h4>`;
            d.evidence.forEach(ev => {
                const confClass = ev.confidence < 0.8 ? 'low' : '';
                html += `
                    <div class="evidence-box">
                        <p>"${ev.source_text || 'No verbatim quote'}"</p>
                        <div class="evidence-meta">
                            <div>📄 ${ev.document_name} · ${ev.section_ref}</div>
                            <div class="confidence-badge ${confClass}">Trust: ${(ev.confidence * 100).toFixed(0)}%</div>
                        </div>
                    </div>
                `;
            });
            html += `</div>`;
        }

        content.innerHTML = html;
        panel.style.display = "flex";
    }

    // ── Empty State ─────────────────────────────────────────────

    _showEmptyState() {
        if (this._emptyStateShown) return;
        this._emptyStateShown = true;

        const emptyDiv = document.createElement("div");
        emptyDiv.className = "empty-state";
        emptyDiv.id = "graph-empty";
        emptyDiv.innerHTML = `
            <div class="empty-icon">
                <svg width="48" height="48" viewBox="0 0 24 24" fill="none" stroke="#475569" stroke-width="1.5">
                    <circle cx="12" cy="5" r="2"/><circle cx="5" cy="19" r="2"/><circle cx="19" cy="19" r="2"/>
                    <line x1="12" y1="7" x2="5" y2="17"/><line x1="12" y1="7" x2="19" y2="17"/>
                    <line x1="7" y1="19" x2="17" y2="19"/>
                </svg>
            </div>
            <h3>No entities yet</h3>
            <p>Paste a text chunk in the left panel and click "Extract Entities" to build the knowledge graph.</p>
        `;
        document.getElementById("graph-panel").appendChild(emptyDiv);
    }

    _hideEmptyState() {
        const el = document.getElementById("graph-empty");
        if (el) {
            el.remove();
            this._emptyStateShown = false;
        }
    }

    _applyClustering(alpha) {
        // Clustering force: pull specific types towards their parent clusters
        this.nodes.forEach(node => {
            // Persons and Roles cluster around Management
            if (node.type === 'Person' || node.type === 'Role') {
                const parentRel = this.links.find(l => 
                    (l.target.id === node.id || l.target === node.id) && 
                    ['HELD_BY', 'HAS_ROLE'].includes(l.relation)
                );
                if (parentRel) {
                    const parent = typeof parentRel.source === 'object' ? parentRel.source : this.nodes.find(n => n.id === parentRel.source);
                    if (parent) {
                        node.vx += (parent.x - node.x) * alpha * 0.1;
                        node.vy += (parent.y - node.y) * alpha * 0.1;
                    }
                }
            }
        });
    }

    // ── Utilities ───────────────────────────────────────────────

    // ── Architectural Tiers ───────────────────────────────────
    _getNodeLevel(type) {
        const levels = {
            "LegalEntity": 0,           // Apex
            "Management": 1, 
            "Competitors": 1,
            "ProductPortfolio": 1,
            "BusinessUnit": 1,
            "Role": 2,                  // Under Management
            "Person": 3,                // Under Role
            "ExternalOrganization": 3, 
            "Site": 3,
            "ProductDomain": 4, 
            "Technology": 4, 
            "Geography": 4,
            "ProductFamily": 5, 
            "Capability": 5,
            "ProductLine": 6, 
            "Brand": 6,
            "EndMarket": 7, 
            "Channel": 7, 
            "Program": 7
        };
        return levels[type] !== undefined ? levels[type] : 3;
    }

    _truncateLabel(text, maxLen) {
        return text.length > maxLen ? text.substring(0, maxLen - 1) + "…" : text;
    }

    _formatType(type) {
        return type.replace(/([A-Z])/g, " $1").trim().toUpperCase();
    }

    _getNodeRadius(type) {
        if (type === "LegalEntity") return 36;
        if (["Management", "Competitors", "ExternalOrganization"].includes(type)) return 28;
        if (["Person", "Role", "Brand"].includes(type)) return 18;
        return 22;
    }

    _getNodeIcon(type) {
        const abbr = {
            "LegalEntity": "ORG",
            "ExternalOrganization": "EXT",
            "BusinessUnit": "BU",
            "Person": "P",
            "Role": "R",
            "Geography": "GEO",
            "Site": "LOC",
            "ProductDomain": "PD",
            "ProductFamily": "PF",
            "ProductLine": "PRD",
            "Technology": "TEC",
            "Capability": "CAP",
            "Financial": "FIN",
            "Brand": "BR",
            "Initiative": "INI",
            "Sector": "SEC",
            "Industry": "IND",
            "SubIndustry": "SUB",
            "EndMarket": "MKT",
            "Channel": "CHN",
            "Program": "PRO",
            "Management": "MGT",
            "Competitors": "CMP",
            "ProductPortfolio": "PF",
        };
        return abbr[type] || "•";
    }

    _lighten(color, percent) {
        // Simple lighten by mixing with white
        const num = parseInt(color.replace("#", ""), 16);
        const r = Math.min(255, (num >> 16) + percent);
        const g = Math.min(255, ((num >> 8) & 0x00FF) + percent);
        const b = Math.min(255, (num & 0x0000FF) + percent);
        return `rgb(${r},${g},${b})`;
    }

    _onResize() {
        this.width = this.svgEl.clientWidth;
        this.height = this.svgEl.clientHeight;
        this.simulation.force("center", d3.forceCenter(this.width / 2, this.height / 2).strength(0.05));
        this.simulation.force("x", d3.forceX(this.width / 2).strength(0.03));
        this.simulation.force("y", d3.forceY(this.height / 2).strength(0.03));
    }
}
