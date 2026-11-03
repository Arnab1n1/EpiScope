// View 4: allocation. Forecast demand vs current stock vs recommended allocation (+ efficiency-equity curve). What-if: budget, alpha.
(function () {
  var W = 600, H = 330, M = { l: 92, r: 14, t: 8, b: 26 }, svg, eq;
  function init() {
    svg = d3.select("#alloc").append("svg").attr("viewBox", "0 0 " + W + " " + H).attr("role", "img").attr("aria-label", "Allocation by district");
    eq = d3.select("#equity").append("svg").attr("viewBox", "0 0 " + W + " 190").attr("role", "img").attr("aria-label", "Efficiency equity curve");
    var sel = d3.select("#sel-res"); Object.keys(EP.RLABEL).forEach(function (r) { sel.append("option").attr("value", r).text(EP.RLABEL[r]); });
    sel.on("change", function () { EP.set({ resource: this.value }); });
    d3.select("#sl-budget").on("input", function () { EP.set({ budgetIdx: +this.value }); });
    d3.select("#sl-alpha").on("input", function () { EP.set({ alphaIdx: +this.value }); });
    d3.select("#sl-tau").on("input", function () { EP.set({ tau: +this.value }); });
  }
  function render() {
    var s = EP.state, G = EP.D.allocation_grid, D = EP.D, r = s.resource;
    d3.select("#o-budget").text(Math.round(G.levels[s.budgetIdx] * 100) + "% of national gap");
    d3.select("#o-alpha").text(G.alphas[s.alphaIdx]); d3.select("#o-tau").text(s.tau);
    var key = s.budgetIdx + "_" + s.alphaIdx, A = G.alloc[key];
    var sm = G.summary.filter(function (e) { return e.budget_level === G.levels[s.budgetIdx] && e.alpha === G.alphas[s.alphaIdx]; })[0];
    d3.select("#alloc-sub").text(EP.RLABEL[r]);
    var rows = G.districts.map(function (c, i) { return { code: c, name: D.dist[c].name, demand: G.demand[r][i], stock: G.stock[r][i], alloc: A ? A[r][i] : 0, w: G.weights.w[i], wa: G.weights.alert[i], wc: G.weights.capacity[i] }; })
      .map(function (d) { d.gap = Math.max(0, d.demand - d.stock); return d; }).sort(function (a, b) { return b.gap - a.gap; }).slice(0, 14);
    var y = d3.scaleBand().domain(rows.map(function (d) { return d.code; })).range([M.t, H - M.b]).padding(0.18), y2 = d3.scaleBand().domain(["demand", "stock", "alloc"]).range([0, y.bandwidth()]).padding(0.05);
    var xmax = d3.max(rows, function (d) { return Math.max(d.demand, d.stock, d.alloc); }) || 1, x = d3.scaleLinear().domain([0, xmax]).nice().range([M.l, W - M.r]);
    svg.selectAll("*").remove();
    svg.append("g").attr("class", "axis").attr("transform", "translate(0," + (H - M.b) + ")").call(d3.axisBottom(x).ticks(6, "~s"));
    svg.append("g").attr("class", "axis").attr("transform", "translate(" + M.l + ",0)").call(d3.axisLeft(y).tickFormat(function (c) { return D.dist[c].name; }));
    var col = { demand: "#252525", stock: "#969696", alloc: "#1f77b4" };
    var g = svg.selectAll("g.row").data(rows).enter().append("g").attr("transform", function (d) { return "translate(0," + y(d.code) + ")"; })
      .style("cursor", "pointer").attr("opacity", function (d) { return !s.district || s.district === d.code ? 1 : 0.4; })
      .on("click", function (ev, d) { EP.set({ district: s.district === d.code ? null : d.code }); })
      .on("mousemove", function (ev, d) {
        EP.tip.show("<b>" + d.name + "</b><br>forecast demand (p80, 4 wk): " + EP.fmt(d.demand) + "<br>current stock: " + EP.fmt(d.stock) + "<br>recommended allocation: " + EP.fmt(d.alloc) +
          "<br>severity weight " + d.w.toFixed(2) + " (alert " + d.wa.toFixed(1) + ", bed-capacity deficit " + d.wc.toFixed(2) + ")", ev);
      }).on("mouseleave", EP.tip.hide);
    ["demand", "stock", "alloc"].forEach(function (k) {
      g.append("rect").attr("x", M.l).attr("y", y2(k)).attr("height", y2.bandwidth()).attr("width", function (d) { return x(d[k]) - M.l; }).attr("fill", col[k]);
    });
    [["demand", "forecast demand"], ["stock", "current stock"], ["alloc", "recommended allocation"]].forEach(function (k, i) {
      svg.append("rect").attr("x", M.l + 170 * i).attr("y", H - 12).attr("width", 10).attr("height", 8).attr("fill", col[k[0]]);
      svg.append("text").attr("x", M.l + 170 * i + 14).attr("y", H - 4).attr("class", "axis-label").text(k[1]);
    });
    var imp = sm && sm.weighted_unmet_baseline > 0 ? 100 * (1 - sm.weighted_unmet_lp / sm.weighted_unmet_baseline) : null;
    d3.select("#alloc-summary").html(sm && sm.status === "Optimal" ?
      "At <b>" + Math.round(G.levels[s.budgetIdx] * 100) + "%</b> budget and equity floor &alpha;=<b>" + G.alphas[s.alphaIdx] + "</b>: weighted unmet demand <b>" + EP.fmt(sm.weighted_unmet_lp) + "</b> vs population-proportional baseline <b>" + EP.fmt(sm.weighted_unmet_baseline) + "</b>" + (imp !== null ? " (" + EP.fmt1(imp) + "% lower)." : ".") :
      "No feasible plan for this budget / equity floor combination: lower the equity floor or raise the budget.");
    // efficiency-equity curve: all (budget, alpha) cells
    var pts = G.summary.filter(function (e) { return e.status === "Optimal"; });
    var ex = d3.scaleLinear().domain([0, d3.max(pts, function (e) { return e.min_alloc_per_pop_share; }) || 1]).nice().range([M.l, W - M.r]);
    var ey = d3.scaleLinear().domain([0, d3.max(pts, function (e) { return e.weighted_unmet_lp; }) || 1]).nice().range([160, 12]);
    eq.selectAll("*").remove();
    eq.append("g").attr("class", "axis").attr("transform", "translate(0,160)").call(d3.axisBottom(ex).ticks(5, "~s"));
    eq.append("g").attr("class", "axis").attr("transform", "translate(" + M.l + ",0)").call(d3.axisLeft(ey).ticks(5, "~s"));
    eq.append("text").attr("class", "axis-label").attr("x", (W + M.l) / 2).attr("y", 186).attr("text-anchor", "middle").text("equity: minimum allocation per unit population share (higher = more equal)");
    eq.append("text").attr("class", "axis-label").attr("transform", "rotate(-90)").attr("x", -86).attr("y", 14).attr("text-anchor", "middle").text("weighted unmet demand");
    var colr = d3.scaleOrdinal(d3.schemeTableau10);
    G.levels.forEach(function (lv, li) {
      var line = pts.filter(function (e) { return e.budget_level === lv; }).sort(function (a, b) { return a.alpha - b.alpha; });
      eq.append("path").attr("d", d3.line().x(function (e) { return ex(e.min_alloc_per_pop_share); }).y(function (e) { return ey(e.weighted_unmet_lp); })(line)).attr("fill", "none").attr("stroke", colr(li)).attr("stroke-width", 1.3);
      line.forEach(function (e) { eq.append("circle").attr("cx", ex(e.min_alloc_per_pop_share)).attr("cy", ey(e.weighted_unmet_lp)).attr("r", e === sm ? 6 : 3).attr("fill", colr(li)).attr("stroke", e === sm ? "#000" : "none")
        .on("mousemove", function (ev) { EP.tip.show("budget " + Math.round(lv * 100) + "%, &alpha;=" + e.alpha + "<br>weighted unmet " + EP.fmt(e.weighted_unmet_lp), ev); }).on("mouseleave", EP.tip.hide); });
      eq.append("text").attr("x", W - M.r - 4).attr("y", 14 + 12 * li).attr("text-anchor", "end").attr("class", "axis-label").attr("fill", colr(li)).text("budget " + Math.round(lv * 100) + "%");
    });
  }
  EP.alloc = { init: init, render: render };
})();
