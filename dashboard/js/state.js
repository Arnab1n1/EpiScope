// Single shared state; every view subscribes and re-renders on change (VIS-3 linked interaction).
window.EP = window.EP || {};
EP.state = {
  district: null, disease: "DENGUE", range: [0, 0], budgetIdx: 2, alphaIdx: 1, tau: 3, resource: "IV_SALINE", role: "national", roleDistrict: null
};
EP.subs = [];
EP.subscribe = function (fn) { EP.subs.push(fn); };
EP.set = function (patch) {
  Object.assign(EP.state, patch);
  EP.subs.forEach(function (fn) { try { fn(EP.state); } catch (e) { console.error(e); } });
};
EP.tip = {
  show: function (html, ev) {
    var t = document.getElementById("tooltip");
    t.innerHTML = html; t.style.opacity = 1;
    var x = ev.clientX + 12, y = ev.clientY + 12;
    t.style.left = Math.min(x, window.innerWidth - 270) + "px"; t.style.top = y + "px";
  },
  hide: function () { document.getElementById("tooltip").style.opacity = 0; }
};
