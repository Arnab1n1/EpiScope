
// View 1: District choropleth (incidence per 100k) + alert markers.
// Uses real Bangladesh district boundaries from data/districts.topo.json.
// Falls back to a schematic Voronoi map only if the TopoJSON file is missing.

(function () {
  var W = 460, H = 560, svg, g, projection, cells = {}, outline;

  var OUTLINE = [
    [88.55,26.45],[89.3,26.25],[89.75,26.2],[89.85,25.3],
    [90.4,25.15],[91.0,25.2],[91.9,25.15],[92.3,24.95],
    [92.2,24.3],[91.6,24.1],[91.5,23.5],[91.8,23.0],
    [92.1,22.9],[92.6,22.5],[92.55,21.9],[92.35,21.4],
    [92.32,20.75],[92.05,21.3],[91.85,22.1],[91.4,22.6],
    [91.0,22.6],[90.6,22.0],[90.3,21.8],[89.6,21.8],
    [89.1,22.0],[89.0,22.35],[88.7,22.6],[88.9,23.0],
    [88.6,23.7],[88.0,24.4],[88.15,24.9],[88.3,25.4],
    [88.1,25.85],[88.1,26.3]
  ];

  // Official boundary names to project district names.
  var DISTRICT_ALIASES = {
    "Barisal": "Barishal",
    "Bogra": "Bogura",
    "Brahamanbaria": "Brahmanbaria",
    "Chittagong": "Chattogram",
    "Comilla": "Cumilla",
    "Jessore": "Jashore",
    "Maulvibazar": "Moulvibazar",
    "Nawabganj": "Chapainawabganj",
    "Netrakona": "Netrokona"
  };

  function normalizeName(name) {
    return String(name || "")
      .replace(/ District$/i, "")
      .trim()
      .toLowerCase();
  }

  function init() {
    var D = EP.D;

    svg = d3.select("#map")
      .append("svg")
      .attr("viewBox", "0 0 " + W + " " + H)
      .attr("role", "img")
      .attr("aria-label", "District map of incidence");

    var defs = svg.append("defs");

    defs.append("pattern")
      .attr("id", "hatch")
      .attr("width", 6)
      .attr("height", 6)
      .attr("patternUnits", "userSpaceOnUse")
      .attr("patternTransform", "rotate(45)")
      .append("rect")
      .attr("width", 2.2)
      .attr("height", 6)
      .attr("fill", "#000")
      .attr("opacity", .55);

    g = svg.append("g");

    if (D.topo) {
      var obj = D.topo.objects[Object.keys(D.topo.objects)[0]];
      var feats = topojson.feature(D.topo, obj).features;

      projection = d3.geoMercator().fitSize(
        [W, H],
        {
          type: "FeatureCollection",
          features: feats
        }
      );

      var path = d3.geoPath(projection);

      var matchedCount = 0;
      var unmatchedNames = [];

      feats.forEach(function (f) {
        var properties = f.properties || {};

        var originalName =
          properties.shapeName ||
          properties.NAME_2 ||
          properties.name ||
          "";

        var nm = String(originalName)
          .replace(/ District$/i, "")
          .trim();

        var matchedName = DISTRICT_ALIASES[nm] || nm;
        var normalizedMatchedName = normalizeName(matchedName);

        var district = D.districts.find(function (d) {
          return normalizeName(d.name) === normalizedMatchedName;
        });

        if (district) {
          cells[district.code] = {
            d: path(f),
            cx: path.centroid(f)
          };

          matchedCount++;
        } else {
          unmatchedNames.push(originalName);
        }
      });

      D.mapMode = "topojson";

      console.log("Map mode: Real TopoJSON boundaries");
      console.log("Total boundary features:", feats.length);
      console.log("Matched districts:", matchedCount);
      console.log("Unmatched boundaries:", unmatchedNames);

      if (matchedCount !== D.districts.length) {
        console.warn(
          "Some districts could not be matched. Check district names:",
          unmatchedNames
        );
      }

    } else {
      // Fallback: schematic Voronoi map.
      var ring = {
        type: "Polygon",
        coordinates: [OUTLINE.concat([OUTLINE[0]])]
      };

      projection = d3.geoMercator().fitExtent(
        [[8, 8], [W - 8, H - 8]],
        ring
      );

      var pts = D.districts.map(function (d) {
        return projection([d.lon, d.lat]);
      });

      var vor = d3.Delaunay.from(pts).voronoi([0, 0, W, H]);

      defs.append("clipPath")
        .attr("id", "clip")
        .append("path")
        .attr("d", d3.geoPath(projection)(ring));

      g.attr("clip-path", "url(#clip)");

      D.districts.forEach(function (d, i) {
        cells[d.code] = {
          d: vor.renderCell(i),
          cx: pts[i]
        };
      });

      svg.append("path")
        .attr("d", d3.geoPath(projection)(ring))
        .attr("fill", "none")
        .attr("stroke", "#444")
        .attr("stroke-width", 1.2);

      svg.append("text")
        .attr("x", 8)
        .attr("y", H - 6)
        .attr("class", "axis-label")
        .text("Schematic map (Voronoi of district centroids). Add data/districts.topo.json for real boundaries.");

      D.mapMode = "voronoi";

      console.warn("TopoJSON boundary file not found. Using schematic map.");
    }

    // Draw all matched districts.
    D.districts.forEach(function (d) {
      var c = cells[d.code];

      if (!c) return;

      g.append("path")
        .attr("class", "district-shape")
        .attr("id", "ds-" + d.code)
        .attr("d", c.d)
        .attr("fill", "#ddd")
        .attr("tabindex", 0)
        .attr("aria-label", d.name)
        .on("click", function () {
          EP.set({
            district: EP.state.district === d.code ? null : d.code
          });
        })
        .on("keydown", function (ev) {
          if (ev.key === "Enter") {
            EP.set({ district: d.code });
          }
        })
        .on("mousemove", function (ev) {
          tooltip(ev, d);
        })
        .on("mouseleave", EP.tip.hide);

      g.append("path")
        .attr("class", "hatch-ov")
        .attr("id", "hv-" + d.code)
        .attr("d", c.d)
        .attr("fill", "url(#hatch)")
        .attr("pointer-events", "none")
        .attr("opacity", 0);
    });

    g.append("g").attr("id", "markers");

    legend();
  }

  function tooltip(ev, d) {
    var s = EP.state;

    var r = EP.rate(
      d.code,
      s.disease,
      s.range[0],
      s.range[1]
    );

    var txt =
      "<b>" + d.name + "</b> (" + d.division + ")<br>" +
      EP.DLABEL[s.disease] + ": " +
      EP.fmt1(r.rate) + " per 100k / week";

    if (r.divisionLevel) {
      txt += "<br><i>shown at division level for this role</i>";
    } else if (r.partial) {
      txt += "<br><i>" + r.r.suppressed +
        " week(s) suppressed (n&lt;5), not included</i>";
    }

    var a = EP.alertAt(d.code, s.disease, s.range[1]);

    if (a && a.alert) {
      txt += "<br><b style='color:#ff9b8f'>ALERT</b> at threshold " + s.tau;
    }

    EP.tip.show(txt, ev);
  }

  function legend() {
    var s = EP.state;
    var b = EP.D.breaks[s.disease];
    var cols = d3.quantize(d3.interpolateViridis, 6);
    var el = d3.select("#map-legend");

    el.html("<span>cases per 100,000 per week:&nbsp;</span>");

    var lo = 0;

    cols.forEach(function (c, i) {
      var hi = i < b.length ? b[i] : null;

      el.append("span")
        .html(
          "<i style='background:" + c + "'></i>" +
          (
            hi === null
              ? "&gt;" + EP.fmt1(lo)
              : EP.fmt1(lo) + "&ndash;" + EP.fmt1(hi)
          ) +
          "&nbsp;&nbsp;"
        );

      lo = hi;
    });
  }

  function render() {
    var s = EP.state;
    var D = EP.D;

    d3.select("#map-sub")
      .text(
        EP.DLABEL[s.disease] +
        " | weeks " +
        D.weeks[s.range[0]] +
        " to " +
        D.weeks[s.range[1]]
      );

    legend();

    var mk = d3.select("#markers");
    mk.selectAll("*").remove();

    D.districts.forEach(function (d) {
      var c = cells[d.code];

      if (!c) return;

      var r = EP.rate(
        d.code,
        s.disease,
        s.range[0],
        s.range[1]
      );

      d3.select("#ds-" + d.code)
        .attr("fill", EP.colorFor(s.disease, r.rate))
        .classed("sel", s.district === d.code);

      d3.select("#hv-" + d.code)
        .attr("opacity", r.partial ? 1 : 0);

      var a = EP.alertAt(d.code, s.disease, s.range[1]);

      if (a && a.alert) {
        mk.append("path")
          .attr(
            "d",
            d3.symbol()
              .type(d3.symbolTriangle)
              .size(130)()
          )
          .attr("transform", "translate(" + c.cx + ")")
          .attr("fill", "#d62728")
          .attr("stroke", "#fff")
          .attr("stroke-width", 1.2)
          .attr("pointer-events", "none");
      }
    });
  }

  EP.map = {
    init: init,
    render: render
  };

})();
