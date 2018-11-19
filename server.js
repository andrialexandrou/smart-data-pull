const express = require('express');
const app = express();
const port = 4000;

const getSurveyWithValues = require('./scripts/getSurveyWithValues');

app.use(function(req, res, next) {
  res.header("Access-Control-Allow-Origin", "*");  res.header("Access-Control-Allow-Headers", "Origin, X-Requested-With, Content-Type, Accept");
  next();
});


const _ = require('lodash');

const lausFilters = {
  series_id: true,
  period: true,
  label: true,
  seasonality_enum: true,
  area: true,
  area_type: true,
  measure_type: true,
  value: true
};

function collectFilters( queryParams ) {
  const thisRequest = {};
  _.forEach( queryParams, function(value, param) {
    if ( lausFilters[ param ] ) {
      thisRequest[ param ] = value;
    }
  })
  return !_.isEmpty( thisRequest ) && thisRequest;
}

app.get('/laus', (req, res) => {
  const page = req.query.page || 0;
  const filters = collectFilters(req.query);

  getSurveyWithValues( page, filters, (err, dbRes) => {
    if ( err ) {
      console.log('err', err);
      res.status(400).send('Invalid Filter');
    }
    res.send(dbRes && dbRes.rows || dbRes);
  } );
});

app.get('/unemp', (req, res) => {
  res.send( 'howdy yep' );
});

app.listen(port, () => console.log(`Example app listening on port ${port}!`))
