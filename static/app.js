(function () {
  var list = document.getElementById('events');
  var status = document.getElementById('status');
  var counter = document.getElementById('counter');
  var seen = 0;
  var maxRows = 200;

  function setStatus(text, cls) {
    status.textContent = text;
    status.className = 'badge ' + cls;
  }

  function el(tag, cls, text) {
    var e = document.createElement(tag);
    if (cls) e.className = cls;
    if (text != null) e.textContent = text;
    return e;
  }

  function render(type, data) {
    var li = el('li', 'list-group-item d-flex justify-content-between align-items-start');

    var left = el('div', 'me-auto');
    left.appendChild(el('span', 'event-type text-muted', type || 'event'));

    var name = data.package && data.package.name ? data.package.name : (data.name || '');
    var version = data.version && data.version.number ? data.version.number : (data.version || '');
    var label = el('a', 'event-name link-dark', name + (version ? ' ' + version : ''));
    var href = data.version_url || data.package_url || data.registry_url;
    if (href) {
      label.href = href;
      label.target = '_blank';
    }
    left.appendChild(label);
    li.appendChild(left);

    var registry = data.registry || (data.package && data.package.ecosystem) || '';
    if (registry) li.appendChild(el('span', 'badge bg-light text-dark', registry));

    return li;
  }

  var es = new EventSource('/events');
  es.onopen = function () { setStatus('live', 'bg-success'); };
  es.onerror = function () { setStatus('reconnecting', 'bg-warning text-dark'); };
  es.onmessage = function (e) {
    var data;
    try { data = JSON.parse(e.data); } catch (err) { data = { name: e.data }; }
    list.insertBefore(render(data.event, data), list.firstChild);
    while (list.children.length > maxRows) list.removeChild(list.lastChild);
    seen++;
    counter.textContent = seen;
  };
})();
