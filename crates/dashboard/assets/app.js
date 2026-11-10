// Pumasi dashboard: the little the server-rendered pages need in the
// browser. Local times, copy buttons and live task output.
(function () {
  "use strict";

  // <time datetime="...Z"> in the browser's time zone.
  function localTimes(root) {
    root.querySelectorAll("time[datetime]").forEach(function (t) {
      if (t.dataset.local) return;
      t.dataset.local = "1";
      var d = new Date(t.getAttribute("datetime"));
      if (!isNaN(d)) {
        t.title = t.textContent;
        var opts = { month: "short", day: "numeric", hour: "2-digit", minute: "2-digit", second: "2-digit" };
        if (d.getFullYear() !== new Date().getFullYear()) opts.year = "numeric";
        t.textContent = d.toLocaleString(document.documentElement.lang || undefined, opts);
      }
    });
    // Chart axes: hours and minutes only.
    root.querySelectorAll("[data-hm]").forEach(function (t) {
      if (t.dataset.local) return;
      t.dataset.local = "1";
      var d = new Date(t.getAttribute("data-hm"));
      if (!isNaN(d)) {
        t.textContent = d.toLocaleTimeString(document.documentElement.lang || undefined, { hour: "2-digit", minute: "2-digit" });
      }
    });
  }

  function copyButtons(root) {
    root.querySelectorAll("[data-copy]").forEach(function (b) {
      if (b.dataset.bound) return;
      b.dataset.bound = "1";
      b.addEventListener("click", function () {
        var box = b.parentElement.querySelector("[data-ticket]");
        if (!box) return;
        box.select();
        var done = function () { b.textContent = "✓"; };
        if (navigator.clipboard) navigator.clipboard.writeText(box.value).then(done, function () {});
        else { document.execCommand("copy"); done(); }
      });
    });
  }

  // A task's stdout and stderr as server-sent events (see jobs.rs).
  function logStreams(root) {
    root.querySelectorAll("[data-log-stream]").forEach(function (sec) {
      if (sec.dataset.bound) return;
      sec.dataset.bound = "1";
      var out = sec.querySelector('[data-log="stdout"]');
      var err = sec.querySelector('[data-log="stderr"]');
      var status = sec.querySelector("[data-log-status]");
      var es = new EventSource(sec.dataset.logStream);
      function append(pre, text) {
        if (!text) return;
        var atEnd = pre.scrollHeight - pre.scrollTop - pre.clientHeight < 40;
        pre.appendChild(document.createTextNode(text));
        if (atEnd) pre.scrollTop = pre.scrollHeight;
      }
      es.addEventListener("chunk", function (e) {
        var c = JSON.parse(e.data);
        if (c.reset) { out.textContent = ""; err.textContent = ""; }
        append(out, c.stdout);
        append(err, c.stderr);
        status.textContent = c.status;
        sec.dataset.attempt = c.attempt;
        if (c.done) {
          es.close();
          sec.dataset.done = "true";
          if (window.htmx) htmx.trigger(document.body, "refresh");
        }
      });
      es.addEventListener("error", function (e) {
        if (e.data) { status.textContent = e.data; es.close(); }
      });
    });
  }

  function init(root) {
    localTimes(root);
    copyButtons(root);
    logStreams(root);
  }

  document.addEventListener("DOMContentLoaded", function () {
    init(document);
    if (window.htmx) htmx.onLoad(function (elt) { init(elt); });
  });
})();
