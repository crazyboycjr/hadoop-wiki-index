'use strict';
const views = require('co-views');
const parse = require('co-body');
const net = require('net');
const parser = require('../lib/parser/wikiParser');

const render = views(__dirname + '/../views', {
	map: { html: 'swig' }
});

module.exports.home = function *home(ctx) {
	this.body = yield render('home');
};

function createConnection(options) {
  return new Promise((resolve, reject) => {
    let client = net.createConnection(options, () => {
      resolve(client);
    });
  });
}

function receive(client) {
  return new Promise((resolve, reject) => {
    let data = "";
    client.on('data', chunk => data += chunk);
    client.on('end', () => {
      resolve(data);
    });
    client.on('err', err => reject(err));
  });
}

function xmlSlice(text, starttag, endtag) {
  let b = text.indexOf(starttag), e = text.indexOf(endtag);
  if (b < 0 || e < 0)
    return "";
  return text.substring(b + starttag.length, e);
}

function parseSingle(page) {
  let page_obj = {};
  page_obj.id = xmlSlice(page, '<id>', '</id>');
  page_obj.title = xmlSlice(page, '<title>', '</title>');
  page_obj.text = xmlSlice(page, '<text xml:space="preserve">', '</text>')
  page_obj.text = page_obj.text.replace(/&quot;/g, '"')
                               .replace(/&amp;/g, '&')
                               .replace(/&lt;/g, '<')
                               .replace(/&gt;/g, '>')
                               .replace(/&nbsp;/g, ' ');
  return page_obj;
}

function wikiXMLParse(text) {
  let pages = text.split('</page>');
  pages.length--;
  for (let i in pages)
    pages[i] += '</page>';
  let pages_obj = [];
  for (let page of pages) {
    pages_obj.push(parseSingle(page));
  }
  return pages_obj;
}

/*
 * key: id
 * value: page_content
 */
let pages_hash = new Map();

module.exports.search = function *search() {
  console.log(this.query);
  if (!this.query.words || this.query.words.trim().length === 0) {
    this.redirect('/');
    return;
  }
  let client = yield createConnection({
    host: '127.0.0.1',
    port: 23334
  });
  if (!this.query.strategy)
    this.query.limit = '-' + this.query.limit;
  let request = `${this.query.limit} ${this.query.words}`;
  client.write(request);
  let res = yield receive(client);
  //console.log(res);
  let pages = wikiXMLParse(res);
  //console.log(pages[0]);
  for (let i in pages) {
    pages[i].text = yield parser.parse(pages[i].text, 'html', 'en');
    pages_hash.set(pages[i].id, pages[i]);
  }
  let pages_abstract = [];
  for (let page of pages) {
    let page_abstract = {};
    page_abstract.id = page.id;
    page_abstract.title = page.title;
    page_abstract.text = xmlSlice(page.text, '<p>', '</p>');
    pages_abstract.push(page_abstract);
  }
  this.body = yield render('list', { 'pages': pages_abstract });
}

module.exports.page = function *page(id) {
  console.log('id:', id);
  let page = pages_hash.get(id);
  if (!page) {
    /*
     * Ask for server the requested id
     * For normal case, this will not happen
     */
  }
  this.body = yield render('page', { 'page': page });
}
