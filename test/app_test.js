const assert = require('node:assert/strict');
const fs = require('node:fs');
const test = require('node:test');
const vm = require('node:vm');

class Element {
  constructor(tag) {
    this.tag = tag;
    this.children = [];
  }

  appendChild(child) {
    this.children.push(child);
  }

  insertBefore(child) {
    this.children.unshift(child);
  }

  removeChild(child) {
    this.children.splice(this.children.indexOf(child), 1);
  }

  get firstChild() {
    return this.children[0];
  }

  get lastChild() {
    return this.children[this.children.length - 1];
  }
}

function render(payload) {
  const elements = {
    events: new Element('ul'),
    status: new Element('span'),
    counter: new Element('span'),
  };
  let eventSource;
  const context = {
    document: {
      createElement: (tag) => new Element(tag),
      getElementById: (id) => elements[id],
    },
    EventSource: class {
      constructor(url) {
        this.url = url;
        eventSource = this;
      }
    },
  };

  vm.runInNewContext(fs.readFileSync('static/app.js', 'utf8'), context);
  eventSource.onmessage({data: JSON.stringify(payload)});

  return {
    eventSource,
    label: elements.events.firstChild.firstChild.children[1],
  };
}

test('website renders package API links as HTML links', () => {
  const result = render({
    event: 'version.created',
    registry: 'npmjs.org',
    version_url: 'https://packages.ecosyste.ms/api/v1/registries/npmjs.org/packages/%40scope%2Fexample/versions/1.2.3',
    package: {name: '@scope/example'},
    version: {number: '1.2.3'},
  });

  assert.equal(result.eventSource.url, '/events');
  assert.equal(
    result.label.href,
    'https://packages.ecosyste.ms/registries/npmjs.org/packages/%40scope%2Fexample/versions/1.2.3',
  );
});

test('website renders package links when an event has no version', () => {
  const result = render({
    event: 'package.created',
    registry: 'rubygems.org',
    package_url: 'https://packages.ecosyste.ms/api/v1/registries/rubygems.org/packages/example',
    package: {name: 'example'},
  });

  assert.equal(
    result.label.href,
    'https://packages.ecosyste.ms/registries/rubygems.org/packages/example',
  );
});

test('website leaves other links unchanged', () => {
  const result = render({
    event: 'package.created',
    package_url: 'https://example.com/api/v1/packages/example',
    package: {name: 'example'},
  });

  assert.equal(result.label.href, 'https://example.com/api/v1/packages/example');
});
