const fs = require('fs');
const express = require('express');
const createCsv = require('json2csv').parse;
const app = express();

const port = 4000;

const laus = require('./server/lausRoutes');
const unemp = require('./server/unempRoutes');

app.use(function(req, res, next) {
  res.header("Access-Control-Allow-Origin", "*");  res.header("Access-Control-Allow-Headers", "Origin, X-Requested-With, Content-Type, Accept");
  next();
});

app.use('/laus', laus);
app.use('/unemp', unemp);

app.listen(port, () => console.log(`Example app listening on port ${port}!`))
