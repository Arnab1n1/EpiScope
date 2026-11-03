// Boot: load data, build views, wire shared state.
EP.load().then(function (D) {
  EP.prepare(D);
  var sd = d3.select("#sel-disease");
  EP.DISEASES.forEach(function (d) { sd.append("option").attr("value", d).text(EP.DLABEL[d]); });
  sd.on("change", function () { EP.set({ disease: this.value }); });
  var sr = d3.select("#sel-role");
  ["D01", "D14", "D15"].forEach(function (c) { if (D.dist[c]) sr.append("option").attr("value", c).text("District officer: " + D.dist[c].name + " (detail), others by division"); });
  sr.property("value", D.role === "district" ? D.roleDistrict : "national");
  sr.on("change", function () { location.search = this.value === "national" ? "" : "?role=district&d=" + this.value; });
  d3.select("#btn-clear").on("click", function () { if (EP.state.role !== "district") EP.set({ district: null }); });
  d3.select("#fresh").text("Data through week of " + D.meta.data_through_week + " | built " + D.meta.generated_at);
  var q = D.dq_report, pct = q.rows_in ? (100 * q.rows_quarantined / q.rows_in) : 0;
  d3.select("#dq-tile").text("DQ: " + EP.fmt(q.rows_in) + " rows, " + pct.toFixed(1) + "% quarantined, " + EP.fmt(q.patient_dups_merged) + " duplicate patients merged")
    .attr("title", q.rules.map(function (r) { return r.rule + " " + r.description + ": " + (100 * r.pass_rate).toFixed(2) + "% pass"; }).join("\n"));
  [EP.map, EP.series, EP.heat, EP.alloc, EP.table].forEach(function (v) { v.init(); });
  EP.subscribe(function () { EP.map.render(); EP.series.render(); EP.heat.render(); EP.alloc.render(); EP.table.render(); });
  EP.set({});
}).catch(function (e) { document.body.insertAdjacentHTML("afterbegin", "<p style='color:#b00;padding:12px'>Could not load dashboard data (" + e + "). Run <code>make all</code> then <code>make serve</code>.</p>"); console.error(e); });
