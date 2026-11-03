// View 5: ranked early-warning table (district x disease), sortable; click selects district + disease.
(function () {
  var sortKey = "c2", dir = -1;
  var COLS = [["district", "District"], ["disease", "Disease"], ["c2", "C2 score"], ["status", "Status"], ["fc", "Next-week forecast (80% PI)"], ["trend", "Trend"], ["cover", "Stock cover (wks)"]];
  function init() {
    var th = d3.select("#ewtable thead").append("tr");
    COLS.forEach(function (c) { th.append("th").text(c[1]).attr("tabindex", 0).on("click", function () { if (sortKey === c[0]) dir = -dir; else { sortKey = c[0]; dir = -1; } render(); }); });
  }
  function render() {
    var s = EP.state, D = EP.D, al = D.alerts, rows = [], ri = al.c2_recent_weeks.length - 1;
    var wkEnd = D.weeks[s.range[1]], k = al.c2_recent_weeks.indexOf(wkEnd); if (k < 0) k = ri;     // alert data covers the latest 12 weeks
    D.codes.forEach(function (c) {
      if (!al.c2_recent[c]) return;
      EP.DISEASES.forEach(function (dis) {
        var sc = al.c2_recent[c][dis]; if (!sc) return;
        var c2 = sc[k]; if (c2 === null || c2 === undefined) return;
        var f = D.forecasts.forecasts[c] && D.forecasts.forecasts[c][dis];
        var ser = EP.seriesFor(c, dis), last = ser ? ser[D.weeks.length - 1] : null;
        var cover = null, G = D.allocation_grid, i = G.districts.indexOf(c);
        var wk = (dis === "DENGUE" ? "NS1_KITS" : dis === "DIARRHOEAL" ? "ORS" : dis === "MALARIA" ? "ANTIMALARIALS" : "IV_SALINE");
        var dem = G.demand[wk][i] / 4, st = G.stock[wk][i];
        cover = dem > 0 ? st / dem : null;
        rows.push({ code: c, district: D.dist[c].name, disease: dis, c2: c2, status: c2 > s.tau ? "ALERT" : c2 > s.tau * 0.6 ? "watch" : "normal",
          fc: f ? f.median[0] : null, fclo: f ? f.q10[0] : null, fchi: f ? f.q90[0] : null,
          trend: f && last !== null && last !== undefined ? f.median[0] - last : null, cover: cover });
      });
    });
    var key = sortKey; rows.sort(function (a, b) { var x = a[key], y = b[key]; if (typeof x === "string") return dir * x.localeCompare(y); return dir * ((x === null ? -1e9 : x) - (y === null ? -1e9 : y)); });
    rows = rows.slice(0, 40);
    d3.select("#table-sub").text("week of " + al.c2_recent_weeks[k] + " | tau = " + s.tau);
    var tb = d3.select("#ewtable tbody"); tb.selectAll("*").remove();
    var tr = tb.selectAll("tr").data(rows).enter().append("tr").attr("tabindex", 0).classed("sel", function (d) { return d.code === s.district && d.disease === s.disease; })
      .on("click", function (ev, d) { EP.set({ district: d.code, disease: d.disease }); document.getElementById("sel-disease").value = d.disease; })
      .on("keydown", function (ev, d) { if (ev.key === "Enter") EP.set({ district: d.code, disease: d.disease }); });
    tr.append("td").text(function (d) { return d.district; });
    tr.append("td").text(function (d) { return EP.DLABEL[d.disease]; });
    tr.append("td").text(function (d) { return d.c2.toFixed(2); });
    tr.append("td").attr("class", function (d) { return d.status === "ALERT" ? "alertcell" : ""; }).text(function (d) { return d.status === "ALERT" ? "\u25B2 ALERT" : d.status; });
    tr.append("td").text(function (d) { return d.fc === null ? "n/a" : EP.fmt1(d.fc) + " (" + EP.fmt1(d.fclo) + " to " + EP.fmt1(d.fchi) + ")"; });
    tr.append("td").text(function (d) { return d.trend === null ? "n/a" : (d.trend > 0.5 ? "\u2197 rising" : d.trend < -0.5 ? "\u2198 falling" : "\u2192 flat"); });
    tr.append("td").text(function (d) { return d.cover === null ? "n/a" : EP.fmt1(d.cover); });
  }
  EP.table = { init: init, render: render };
})();
