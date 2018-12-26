const fs = require('fs');
const { Client } = require( 'pg' );
const dbSecrets = require( '../../config/dbConfig' );
const _ = require( 'lodash' );

const client = new Client( dbSecrets );
client.connect();

function createSurveyName() {
  const isNational = true;
  return `Current Employment Statistics - Employment, Hours, and Earnings (${ isNational ? 'National' : 'State and Area' })`;
}

// function createSeriesTitle( employmentName, industryName, seasonalityName) {
//   return `${ employmentName }, ${ industryName }, ${ seasonalityName }`;
// }

function insert( incoming ) {
  const surveyName = createSurveyName();

  var script = `INSERT INTO public.current_employment_descriptive(
    survey_name,
    series_title,
    measure_type_enum,
    series_id,
    supersector_code,
    industry_code,
    employment_code,
    seasonality_enum
  ) VALUES (
    '${surveyName}',
    '${incoming['series_title']}',
    '01',
    '${incoming['series_id']}',
    '${incoming['supersector_code']}',
    '${incoming['industry_code']}',
    '${incoming['data_type_code']}',
    '${incoming['seasonal']}'
  )`;

  client.query( script, (err, res) => {
    if ( err ) {
      if ( err.detail && err.detail.includes('already exists')) {
        // do nothing
      } else {
        console.log(script, '\n', err);
      }
    }
    // res && console.log('RES FROM DB', res.rowCount);
  } );
}

function beginReadAndWrite() {
  fs.readFile("./lists/national_employment_series.tsv", "utf8", (err, data) => {
    if ( err )console.log('err', err);
    var rows = data.split( '\n' );
    var headers = rows.shift().split('\t').map(_.trim);
    var smallBatch = rows.splice(0, 3);

    rows.forEach( row => {
      var items = row.split('\t').map( thing => {
        let modThing = _.trim(thing);
        if ( modThing.includes('\'')) {
          modThing = modThing.replace(/\'/g, '\'\'')
        }
        return modThing;
      } );
      var object = _.zipObject( headers, items );
      insert(object);
    } );
  });
}

module.exports = beginReadAndWrite;

beginReadAndWrite();
const app = require('express')();
app.listen(2000, () => console.log(`Example app listening on port 2000!`))
