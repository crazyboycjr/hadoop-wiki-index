'use strict';
const views = require('co-views');
const parse = require('co-body');
const net = require('net');

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

module.exports.search = function *search() {
  console.log(this.query);
  let client = yield createConnection({
    host: '127.0.0.1',
    port: 23334
  });
  let request = `${this.query.limit} ${this.query.words}`;
  client.write(request);
  let res = yield receive(client);
  console.log(res);
  this.body = yield render("list", { 'list': result });
}
