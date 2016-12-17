'use strict';
const routes = require('./controllers/routes');
const compress = require('koa-compress');
const logger = require('koa-logger');
const serve = require('koa-static');
const route = require('koa-route');
const koa = require('koa');
const path = require('path');
const app = module.exports = koa();

// Logger
app.use(logger());

app.use(route.get('/', routes.home));
app.use(route.get('/search', routes.search));
app.use(route.get('/id/:id', routes.page));

// Serve static files
app.use(serve(path.join(__dirname, 'public')));

// Compress
app.use(compress());

if (!module.parent) {
	app.listen(3000);
	console.log('listening on port 3000');
}
