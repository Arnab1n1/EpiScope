// View 3: seasonal heatmap, ISO week-of-year (x) by year (y) for the selected district/disease. Click a cell to jump to that week.
(function () {
  var W = 600, H = 230, M = { l: 44, r: 10, t: 14, b: 26 }, svg;
  function init() { svg = d3.select("#heat").append("svg").attr("viewBox", "0 0 " + W + " " + H).attr("role", "img").attr("aria-label", "Seasonal heatmap"); }
  function render() {
    var s = EP.state, D = EP.D, code = s.district, dis = s.disease, ser = EP.seriesFor(code, dis);
    d3.select("#heat-sub").text((code ? D.dist[code].name : "All districts") + " | " + EP.DLABEL[dis]);
    svg.selectAll("*").remove();
    if (!ser) return;
    var cells = D.weekDates.map(function (d, i) { var w = EP.isoWeek(d); return { i: i, year: w.year, week: w.week, v: ser[i] }; });
    var years = Array.from(new Set(cells.map(function (c) { return c.year; }))).sort(), x = d3.scaleBand().domain(d3.range(1, 54)).range([M.l, W - M.r]).padding(0.04),
      y = d3.scaleBand().domain(years).range([M.t, H - M.b]).padding(0.06);
    var vals = cells.filter(function (c) { return c.v !== null; }).map(function (c) { return c.v; }).sort(d3.ascending);
    var col = d3.scaleSequentialSqrt(d3.interpolateViridis).domain([0, d3.quantile(vals, 0.98) || 1]);
    if (!document.getElementById("hatch3")) svg.append("defs").append("pattern").attr("id", "hatch3").attr("width", 4).attr("height", 4).attr("patternUnits", "userSpaceOnUse").attr("patternTransform", "rotate(45)").append("rect").attr("width", 1.4).attr("height", 4).attr("fill", "#000");
    svg.append("g").attr("class", "axis").attr("transform", "translate(0," + (H - M.b) + ")").call(d3.axisBottom(x).tickValues(d3.range(1, 54, 4)));
    svg.append("g").attr("class", "axis").attr("transform", "translate(" + M.l + ",0)").call(d3.axisLeft(y));
    svg.append("text").attr("class", "axis-label").attr("x", (W + M.l) / 2).attr("y", H - 2).attr("text-anchor", "middle").text("ISO week of year");
    var a = s.range[0], b = s.range[1];
    var gc = svg.append("g");
    cells.forEach(function (c) {
      if (!x(c.week) || !y(c.year)) return;
      var inR = c.i >= a && c.i <= b;
      gc.append("rect").attr("x", x(c.week)).attr("y", y(c.year)).attr("width", x.bandwidth()).attr("height", y.bandwidth())
        .attr("fill", c.v === null ? "#fff" : col(c.v)).attr("stroke", inR ? "#d62728" : "none").attr("stroke-width", inR ? 1.3 : 0).style("cursor", "pointer")
        .on("click", function () { EP.set({ range: [Math.max(0, c.i - 4), Math.min(D.weeks.length - 1, c.i + 4)] }); })
        .on("mousemove", function (ev) { EP.tip.show("<b>" + c.year + " week " + c.week + "</b><br>" + (c.v === null ? "suppressed (1&ndash;4 cases)" : EP.fmt(c.v) + " cases"), ev); }).on("mouseleave", EP.tip.hide);
      if (c.v === null) gc.append("rect").attr("x", x(c.week)).attr("y", y(c.year)).attr("width", x.bandwidth()).attr("height", y.bandwidth()).attr("fill", "url(#hatch3)").attr("pointer-events", "none");
    });
    // preparedness calendar marks for the selected district's division
    var dv = code ? D.dist[code].division : null;
    var row = D.calendar.rows.filter(function (r) { return r.disease_group === dis && (dv ? r.division === dv : false); })[0];
    if (row) [["preposition_from_week", "pre-position", "#ff7f0e"], ["campaign_from_week", "campaign", "#e377c2"]].forEach(function (m) {
      var wk = row[m[0]], xx = x(wk); if (xx === undefined) return;
      svg.append("line").attr("x1", xx + x.bandwidth() / 2).attr("x2", xx + x.bandwidth() / 2).attr("y1", M.t).attr("y2", H - M.b).attr("stroke", m[2]).attr("stroke-width", 2);
      svg.append("text").attr("x", xx + 3).attr("y", M.t + 9).attr("font-size", 9).attr("fill", m[2]).text(m[1] + " wk " + wk);
    });
  }
  EP.heat = { init: init, render: render };
})();
