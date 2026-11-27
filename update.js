// Always the newest app: when it opens, and when it comes back on Start, it asks for its own page past every cache
// (the phone's, the CDN's), and if that page names a newer app.js it goes to it at an address no cache has. Tried
// once per version, so a server still handing out the old page can never make it loop.
(function () {
  var s = document.querySelector('script[src*="app.js"]'), mine = s && (s.getAttribute('src').match(/v=(\d+)/) || [])[1];
  if (!mine) return;
  var tv = location.port === '8796';
  function check(opening) {
    if (!opening && window.metroOnPage && window.metroOnPage()) return;
    var x = new XMLHttpRequest();
    x.open('GET', (tv ? '/' : 'index.html') + '?fresh=' + Date.now());
    x.onload = function () {
      var v = (x.responseText.match(/app\.js\?v=(\d+)/) || [])[1];
      if (!v || v === mine) return;
      var tried = (location.search.match(/[?&]v=(\d+)/) || [])[1];
      if (tried === v) return;
      location.replace(location.pathname + '?v=' + v + location.hash);
    };
    x.send();
  }
  check(true);
  document.addEventListener('visibilitychange', function () { if (!document.hidden) check(false); });
  setInterval(function () { if (!document.hidden) check(false); }, 600000);
})();
