const { Client } = require( 'pg' );
const dbSecrets = require( '../config/dbConfig' );

const client = new Client( dbSecrets );

function insert( seriesId, surveyName, seriesTitle, area, areaEnum, measureEnum, seasEnum) {
  var script = `INSERT INTO public.local_unemployment_descriptive(
    series_id,
    survey_name,
    series_title,
    area,
    area_type_enum,
    measure_type_enum,
    seasonality_enum)
    VALUES ($1,
      'Local Area Unemployment Statistics',
      $2,
      $3,
      $4,
      $5,
      $6 )`;
  client.query( script, [
    seriesId,
    seriesTitle,
    area,
    areaEnum,
    measureEnum,
    seasEnum
  ], (err, res) => {
    if ( err ) {
      console.log( script, '\n', err );
    }
  } );
}

function beginReadAndWrite() {
  fs.readFile("local_descriptive.tsv", "utf8", (err, data) => {
    var rows = data.split( '\n' );
    rows.forEach( row => {
      setTimeout( () => {
        var args = row.split( '\t' );
        insert( ...args );
      }, 30);
    } );
  });
}

module.exports = beginReadAndWrite;
