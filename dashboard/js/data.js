// Data loading + helpers. The browser only receives aggregated, already-suppressed JSON (VIS-4).
EP.DISEASES = ["DENGUE", "DIARRHOEAL", "ILI", "MALARIA"];
EP.DLABEL = { DENGUE: "Dengue", DIARRHOEAL: "Diarrhoeal disease", ILI: "Influenza-like illness", MALARIA: "Malaria" };
EP.RLABEL = { IV_SALINE: "IV saline (units)", NS1_KITS: "NS1 test kits", ORS: "ORS sachets", ANTIMALARIALS: "Antimalarial courses" };

EP.load = function () {
  var p = new URLSearchParams(location.search);
  var role = p.get("role") === "district" ? "district" : "national";
  var dcode = p.get("d");
  var seriesUrl = role === "district" && dcode ? "data/district/" + dcode + ".json" : "data/series.json";
  var names = ["districts", "forecasts", "alerts", "allocation_grid", "calendar", "dq_report", "meta", "evaluation", "division_week"];
  var jobs = names.map(function (n) { return d3.json("data/" + n + ".json"); });
  jobs.push(d3.json(seriesUrl));
  jobs.push(d3.json("data/districts.topo.json").catch(function () { return null; }));   // optional real boundaries
  return Promise.all(jobs).then(function (r) {
    var D = {};
    names.forEach(function (n, i) { D[n] = r[i]; });
    D.seriesObj = r[names.length]; D.topo = r[names.length + 1];
    D.role = role; D.roleDistrict = dcode;
    if (role === "district" && D.seriesObj.alerts) { D.alerts = D.seriesObj.alerts; D.forecasts = Object.assign({}, D.forecasts, { forecasts: D.seriesObj.forecasts }); }   // PRV-3: own district only
    return D;
  });
};

EP.prepare = function (D) {
  EP.D = D;
  D.weeks = D.seriesObj.weeks;
  D.weekDates = D.weeks.map(function (s) { return new Date(s + "T00:00:00Z"); });
  D.dist = {}; D.districts.forEach(function (d) { D.dist[d.code] = d; });
  D.codes = D.districts.map(function (d) { return d.code; });
  D.divs = D.meta.divisions;
  D.divPop = {}; D.districts.forEach(function (d) { D.divPop[d.division] = (D.divPop[d.division] || 0) + d.population; });
  D.visible = function (code) { return !!D.seriesObj.series[code]; };
  // fixed colour classes per disease: quantile breaks of district weekly rates over all weeks (same meaning in every week)
  D.breaks = {};
  EP.DISEASES.forEach(function (dis) {
    var vals = [];
    D.codes.forEach(function (c) {
      var s = D.seriesObj.series[c] && D.seriesObj.series[c][dis]; if (!s) return;
      s.forEach(function (v) { if (v !== null) vals.push(v / D.dist[c].population * 1e5); });
    });
    vals.sort(d3.ascending);
    var q = [0.2, 0.4, 0.6, 0.8, 0.95].map(function (p) { return d3.quantile(vals, p) || 0; });
    D.breaks[dis] = q;
  });
  EP.state.range = [Math.max(0, D.weeks.length - 26), D.weeks.length - 1];
  if (D.role === "district" && D.roleDistrict) { EP.state.district = D.roleDistrict; EP.state.role = "district"; EP.state.roleDistrict = D.roleDistrict; }
};

// Sum of VISIBLE cells for a district/disease over weeks [a,b]; n suppressed cells reported separately.
EP.sumRange = function (code, dis, a, b) {
  var s = EP.D.seriesObj.series[code] && EP.D.seriesObj.series[code][dis];
  if (!s) return null;
  var sum = 0, sup = 0;
  for (var i = a; i <= b; i++) { if (s[i] === null) sup++; else sum += s[i]; }
  return { sum: sum, suppressed: sup, weeks: b - a + 1 };
};
// mean weekly incidence per 100k (visible cells only)
EP.rate = function (code, dis, a, b) {
  var r = EP.sumRange(code, dis, a, b);
  if (r) return { rate: r.sum / Math.max(1, r.weeks - r.suppressed) / EP.D.dist[code].population * 1e5, partial: r.suppressed > 0, r: r };
  // district role: other districts shown at division level only
  var dv = EP.D.dist[code].division, di = EP.D.division_week.divisions.indexOf(dv);
  var row = EP.D.division_week.diseases[dis][di], sum = 0, sup = 0;
  for (var i = a; i <= b; i++) { if (row[i] === null) sup++; else sum += row[i]; }
  return { rate: sum / Math.max(1, b - a + 1 - sup) / EP.D.divPop[dv] * 1e5, partial: sup > 0, divisionLevel: true, division: dv };
};
EP.seriesFor = function (code, dis) {          // visible weekly counts; null = suppressed
  if (code) { var s = EP.D.seriesObj.series[code]; return s ? s[dis] : null; }
  // national total = sum of published (post-suppression) cells, so suppressed cells cannot be recovered from it
  var out = new Array(EP.D.weeks.length).fill(0), any = false;
  EP.D.codes.forEach(function (c) { var s = EP.D.seriesObj.series[c] && EP.D.seriesObj.series[c][dis]; if (!s) return; any = true; s.forEach(function (v, i) { if (v !== null) out[i] += v; }); });
  if (any) return out;
  var tot = new Array(EP.D.weeks.length).fill(0);   // district role: use division matrix
  EP.D.division_week.diseases[dis].forEach(function (row) { row.forEach(function (v, i) { if (v !== null) tot[i] += v; }); });
  return tot;
};
EP.alertAt = function (code, dis, wkIdx) {       // threshold exceeded using client tau
  var al = EP.D.alerts, off = EP.D.weeks.length - al.weeks_threshold.length, k = wkIdx - off;
  if (k < 0 || !al.threshold[code] || !al.threshold[code][dis]) return null;
  var m = al.threshold[code][dis].mean[k], sd = al.threshold[code][dis].sd[k];
  var s = EP.seriesFor(code, dis); if (!s || m === null) return null;
  var v = s[wkIdx]; if (v === null) return { alert: false, thr: m + EP.state.tau * Math.max(sd || 0, 1) };
  var thr = m + EP.state.tau * Math.max(sd || 0, 1);
  return { alert: v > thr && v >= al.min_cases, thr: thr, value: v };
};
EP.isoWeek = function (d) {                     // ISO-8601 week + year (UTC)
  var t = new Date(Date.UTC(d.getUTCFullYear(), d.getUTCMonth(), d.getUTCDate()));
  var day = t.getUTCDay() || 7; t.setUTCDate(t.getUTCDate() + 4 - day);
  var y0 = new Date(Date.UTC(t.getUTCFullYear(), 0, 1));
  return { year: t.getUTCFullYear(), week: Math.ceil(((t - y0) / 864e5 + 1) / 7) };
};
EP.fmt = d3.format(",.0f"); EP.fmt1 = d3.format(",.1f");
EP.colorFor = function (dis, rate) {
  var b = EP.D.breaks[dis], cols = d3.quantize(d3.interpolateViridis, 6), i = 0;
  while (i < b.length && rate > b[i]) i++;
  return cols[i];
};
