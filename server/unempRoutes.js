const express = require('express')
const router = express.Router()
const _ = require('lodash');
const fs = require('fs');
const path = require('path');

const getSurveyWithValues = require('./scripts/getCesSurveyWithValues');
const getSuggestions = require('./scripts/getCesSuggestions');
const createCsv = require('json2csv').parse;

const empFilters = {
  series_id: true,
  area: true,
  seasonality_enum: true,
  industry_type: true,
  state: true,
  supersector_type: true,
  period: true,
  label: true
};

function collectFilters( queryParams ) {
  const thisRequest = {};
  _.forEach( queryParams, function(value, param) {
    const values = value.split('|');
    if ( empFilters[ param ] ) {
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
    'state',
    'area_type',
    'industry',
    'supersector',
    'seasonality_enum',
    'label',
    'period',
    'employment_type',
    'value'
  ];
  const options = { fields: fields };
  return createCsv( arrayOfInfo, options );
}

const csvFileName = './sheetToDownload.csv';

router.get('/', (req, res) => {
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

router.get('/download', (req, res) => {
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
        res.download( csvFileName, 'employment.csv');
        setTimeout( () => fs.unlink( csvFileName, () => {}), 10000 );
      })

    } else {
      res.status(200).send('No results! No filter?')
    }
  } );
})

router.get('/suggest', (req, res) => {
  const geography = req.query.geo;

  getSuggestions( geography, (err, dbRes) => {
    if ( err ) console.log('[/employ/suggest]', err);
    const rows = dbRes.rows;
    res.status(200).send( rows );
  } );
})

module.exports = router
