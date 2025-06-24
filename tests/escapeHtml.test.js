const assert = require('assert');
const fs = require('fs');
const vm = require('vm');

const code = fs.readFileSync('frontend/static/js/common.js', 'utf8');
const start = code.indexOf('function escapeHtml');
if (start === -1) {
  throw new Error('escapeHtml function not found');
}
let braceCount = 0;
let end = start;
for (let i = start; i < code.length; i++) {
  const ch = code[i];
  if (ch === '{') braceCount++;
  else if (ch === '}') {
    braceCount--;
    if (braceCount === 0) { end = i + 1; break; }
  }
}
const funcSrc = code.slice(start, end);
const context = {};
vm.runInNewContext(funcSrc, context);
const escapeHtml = context.escapeHtml;

assert.strictEqual(
  escapeHtml("<>&\"'`"),
  "&lt;&gt;&amp;&quot;&#039;&#096;"
);
assert.strictEqual(escapeHtml('foo'), 'foo');
assert.strictEqual(escapeHtml('a<b>c'), 'a&lt;b&gt;c');
console.log('escapeHtml tests passed');

