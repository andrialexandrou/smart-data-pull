const fs = require('fs');
const express = require('express');
const createCsv = require('json2csv').parse;
const app = express();
const port = 4000;

const getSurveyWithValues = require('./scripts/getSurveyWithValues');
const getSuggestions = require('./scripts/getSuggestions');

app.use(function(req, res, next) {
  res.header("Access-Control-Allow-Origin", "*");  res.header("Access-Control-Allow-Headers", "Origin, X-Requested-With, Content-Type, Accept");
  next();
});

// SELECT COUNT(*)
// 	FROM public.timewise_measures
// 	WHERE period='M13';

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
    const values = value.split('|');
    if ( lausFilters[ param ] ) {
      thisRequest[ param ] = values;
    }
  })
  return !_.isEmpty( thisRequest ) && thisRequest;
}

function convertToCsv( arrayOfInfo ) {
  const fields = [
    'survey_name',
    'series_title',
    'series_id',
    'period',
    'label',
    'seasonality_enum',
    'area',
    'area_type',
    'measure_type',
    'value'
  ];
  const options = { fields: fields };
  return createCsv( arrayOfInfo, options );
}

const csvFileName = './sheetToDownload.csv';

app.get('/laus/download', (req, res) => {
  const page = req.query.page || 0;
  const filters = collectFilters(req.query);
  if ( !filters ) {
    res.status(404).send('Sorry, need to add filters!');
    return;
  }
  const isDownload = true;
  getSurveyWithValues( isDownload, page, filters, (err, dbRes) => {
    if ( err ) {
      console.log('err', err);
      res.status(400).send('Invalid Filter');
    }
    if ( dbRes && dbRes.rows ) {

      const csv = convertToCsv( dbRes.rows );
      if ( err ) res.status(500).send('Problem creating CSV on server.');

      fs.writeFile( csvFileName, csv, err => {
        if ( err ) res.status(400).send('Problem writing to file!');
        res.download( csvFileName, 'laus.csv');
        setTimeout( () => fs.unlink( csvFileName, () => {}), 10000 );
      })

    } else {
      res.status(200).send('No results! No filter?')
    }
  } );
})

app.get('/laus/suggest', (req, res) => {
  const geography = req.query.geo;

  getSuggestions( geography, (err, dbRes) => {
    if ( err ) console.log('[/laus/suggest]', err);
    const rows = dbRes.rows;
    res.status(200).send( rows );
  } );
})

app.get('/laus', (req, res) => {
  const page = req.query.page || 0;
  const filters = collectFilters(req.query);

  const isDownload = false;
  getSurveyWithValues( isDownload, page, filters, (err, dbRes) => {
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
