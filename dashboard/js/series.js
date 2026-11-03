// View 2: time series with brush (context strip), observed + forecast + prediction bands + alert threshold (all uncertainty shown).
(function () {
  var W = 640, H = 300, M = { l: 52, r: 14, t: 10, b: 26 }, CH = 56, svg, ctx, x, y, xc, brush, gMain;
  function init() {
    svg = d3.select("#series").append("svg").attr("viewBox", "0 0 " + W + " " + H).attr("role", "img").attr("aria-label", "Weekly cases time series");
    gMain = svg.append("g");
    ctx = d3.select("#context").append("svg").attr("viewBox", "0 0 " + W + " " + (CH + 22));
    xc = d3.scaleTime().range([M.l, W - M.r]);
    brush = d3.brushX().extent([[M.l, 2], [W - M.r, CH]]).on("brush end", function (ev) {
      if (!ev.sourceEvent) return;
      var sel = ev.selection; if (!sel) return;
      var dom = EP.D.weekDates, a = d3.bisectCenter(dom, xc.invert(sel[0])), b = d3.bisectCenter(dom, xc.invert(sel[1]));
      EP.set({ range: [Math.min(a, b), Math.max(b, a, a + 1)] });
    });
  }
  function nationalForecast(dis) {
    var F = EP.D.forecasts.forecasts, acc = null;
    Object.keys(F).forEach(function (c) {
      var f = F[c][dis]; if (!f) return;
      if (!acc) acc = { target_weeks: f.target_weeks, median: f.median.map(function () { return 0; }), q10: f.q10.map(function () { return 0; }), q90: f.q90.map(function () { return 0; }), q025: f.q025.map(function () { return 0; }), q975: f.q975.map(function () { return 0; }) };
      ["median", "q10", "q90", "q025", "q975"].forEach(function (k) { f[k].forEach(function (v, i) { acc[k][i] += v; }); });
    });
    return acc;       // sums of district quantiles = conservative (too wide) band; labelled in the chart
  }
  function render() {
    var s = EP.state, D = EP.D, dis = s.disease, code = s.district, ser = EP.seriesFor(code, dis);
    d3.select("#series-sub").text((code ? D.dist[code].name : "All districts (sum of published cells)") + " | " + EP.DLABEL[dis]);
    gMain.selectAll("*").remove(); ctx.selectAll("*").remove();
    if (!ser) return;
    var a = s.range[0], b = s.range[1], idx = d3.range(a, b + 1);
    var fc = code ? (D.forecasts.forecasts[code] && D.forecasts.forecasts[code][dis]) : nationalForecast(dis);
    var fdates = fc ? fc.target_weeks.map(function (t) { return new Date(t + "T00:00:00Z"); }) : [];
    x = d3.scaleTime().domain([D.weekDates[a], fdates.length ? fdates[fdates.length - 1] : D.weekDates[b]]).range([M.l, W - M.r]);
    var al = D.alerts, off = D.weeks.length - al.weeks_threshold.length, th = code && al.threshold[code] && al.threshold[code][dis];
    var thr = idx.map(function (i) { var k = i - off; if (!th || k < 0 || th.mean[k] === null) return null; return th.mean[k] + s.tau * Math.max(th.sd[k] || 0, 1); });
    var ymax = d3.max(idx.map(function (i) { return ser[i]; }).concat(fc ? fc.q975 : [], thr.filter(function (v) { return v !== null; }))) || 1;
    y = d3.scaleLinear().domain([0, ymax * 1.08]).nice().range([H - M.b, M.t]);
    gMain.append("g").attr("class", "axis").attr("transform", "translate(0," + (H - M.b) + ")").call(d3.axisBottom(x).ticks(7).tickFormat(d3.utcFormat("%b %Y")));
    gMain.append("g").attr("class", "axis").attr("transform", "translate(" + M.l + ",0)").call(d3.axisLeft(y).ticks(6));
    gMain.append("text").attr("class", "axis-label").attr("transform", "rotate(-90)").attr("x", -(H / 2)).attr("y", 12).attr("text-anchor", "middle").text("cases reported per week");
    // forecast bands first (never hidden behind the line)
    if (fc) {
      var last = idx[idx.length - 1], lx = D.weekDates[b];
      var pts = function (lo, hi) { return d3.area().x(function (d) { return x(d.t); }).y0(function (d) { return y(d.lo); }).y1(function (d) { return y(d.hi); })(fdates.map(function (t, i) { return { t: t, lo: fc[lo][i], hi: fc[hi][i] }; })); };
      if (b >= D.weeks.length - 1) {
        gMain.append("path").attr("d", pts("q025", "q975")).attr("fill", "#9ecae1").attr("opacity", .45);
        gMain.append("path").attr("d", pts("q10", "q90")).attr("fill", "#4292c6").attr("opacity", .5);
        gMain.append("path").attr("d", d3.line().x(function (d, i) { return x(fdates[i]); }).y(function (d) { return y(d); })(fc.median)).attr("fill", "none").attr("stroke", "#08519c").attr("stroke-width", 2).attr("stroke-dasharray", "5,3");
        gMain.append("text").attr("x", x(fdates[0])).attr("y", M.t + 10).attr("class", "axis-label").attr("fill", "#08519c").text("forecast" + (code ? "" : " (conservative band)"));
      }
    }
    var line = d3.line().defined(function (i) { return ser[i] !== null; }).x(function (i) { return x(D.weekDates[i]); }).y(function (i) { return y(ser[i]); });
    gMain.append("path").attr("d", line(idx)).attr("fill", "none").attr("stroke", "#222").attr("stroke-width", 1.6);
    // suppressed weeks: hatched ticks on the baseline (not zero)
    idx.filter(function (i) { return ser[i] === null; }).forEach(function (i) {
      gMain.append("rect").attr("x", x(D.weekDates[i]) - 2).attr("y", y(0) - 8).attr("width", 4).attr("height", 8).attr("fill", "url(#hatch2)").attr("stroke", "#777").attr("stroke-width", .5);
    });
    if (!document.getElementById("hatch2")) svg.append("defs").append("pattern").attr("id", "hatch2").attr("width", 4).attr("height", 4).attr("patternUnits", "userSpaceOnUse").attr("patternTransform", "rotate(45)").append("rect").attr("width", 1.5).attr("height", 4).attr("fill", "#000");
    // threshold (dashed) + alert triangles; artefact weeks marked
    if (th) {
      gMain.append("path").attr("d", d3.line().defined(function (i, k) { return thr[k] !== null; }).x(function (i) { return x(D.weekDates[i]); }).y(function (i, k) { return y(thr[k]); })(idx))
        .attr("fill", "none").attr("stroke", "#d62728").attr("stroke-dasharray", "4,3").attr("stroke-width", 1.3);
      idx.forEach(function (i, k) { if (thr[k] !== null && ser[i] !== null && ser[i] > thr[k] && ser[i] >= al.min_cases)
        gMain.append("path").attr("d", d3.symbol().type(d3.symbolTriangle).size(60)()).attr("transform", "translate(" + x(D.weekDates[i]) + "," + (y(ser[i]) - 8) + ")").attr("fill", "#d62728"); });
    }
    var aw = (D.seriesObj.artefact_weeks && D.seriesObj.artefact_weeks[code]) || [];
    aw.forEach(function (w) { var i = D.weeks.indexOf(w); if (i >= a && i <= b) gMain.append("rect").attr("x", x(D.weekDates[i]) - 2).attr("y", M.t).attr("width", 4).attr("height", H - M.b - M.t).attr("fill", "#ffb703").attr("opacity", .35); });
    // hover
    var hov = gMain.append("rect").attr("x", M.l).attr("y", M.t).attr("width", W - M.l - M.r).attr("height", H - M.t - M.b).attr("fill", "transparent")
      .on("mousemove", function (ev) {
        var px = d3.pointer(ev, this)[0], dt = x.invert(px), i = d3.bisectCenter(D.weekDates, dt);
        if (i < a || i > b) { EP.tip.hide(); return; }
        var v = ser[i], txt = "<b>Week of " + D.weeks[i] + "</b><br>" + (v === null ? "suppressed (1&ndash;4 cases)" : EP.fmt(v) + " cases");
        if (thr[i - a] !== null && thr[i - a] !== undefined) txt += "<br>threshold: " + EP.fmt1(thr[i - a]);
        if (aw.indexOf(D.weeks[i]) >= 0) txt += "<br><i>reporting artefact suspected (excluded from alerts)</i>";
        EP.tip.show(txt, ev);
      }).on("mouseleave", EP.tip.hide);
    // context strip + brush
    var all = d3.range(D.weeks.length), yc = d3.scaleLinear().domain([0, d3.max(all.map(function (i) { return ser[i]; })) || 1]).range([CH, 2]);
    xc.domain([D.weekDates[0], D.weekDates[D.weeks.length - 1]]);
    ctx.append("path").attr("d", d3.area().defined(function (i) { return ser[i] !== null; }).x(function (i) { return xc(D.weekDates[i]); }).y0(CH).y1(function (i) { return yc(ser[i]); })(all)).attr("fill", "#b8c4de");
    ctx.append("g").attr("class", "axis").attr("transform", "translate(0," + CH + ")").call(d3.axisBottom(xc).ticks(8).tickFormat(d3.utcFormat("%Y")));
    var gb = ctx.append("g").call(brush);
    gb.call(brush.move, [xc(D.weekDates[a]), xc(D.weekDates[b])]);
    d3.select("#footer").html("Data through week of <b>" + D.meta.data_through_week + "</b> | generated " + D.meta.generated_at + " | SYNTHETIC data | suppression threshold: counts 1&ndash;" + (D.meta.suppression.threshold - 1) + " hidden");
  }
  EP.series = { init: init, render: render };
})();
