const fs = require( 'fs' );

const { Pool, Client } = require( 'pg' );
const _ = require( 'lodash' );

const dbSecrets = require( '../config/dbConfig' );
const client = new Client( dbSecrets );

client.connect();

function createWhereClause( filters ) {
  const filtersArray = [];
  _.forEach( filters, function(value, param) {
    const phrase = `${ param } LIKE '${ value }%'`;
    filtersArray.push( phrase );
  })
  return filtersArray.length > 1 ?
    ` WHERE ${ filtersArray.join( ' AND ' ) }` :
    ` WHERE ${ filtersArray[ 0 ] }`;
}

function createQuery( filters ) {
  let baseQuery = `SELECT
    table2.survey_name AS survey_name,
    table2.series_title AS series_title,
    table1.series_id AS series_id,
    table1.period AS period,
    table1.label AS label,
    table2.seasonality_enum AS seasonality_enum,
    table2.area AS area,
    table5.area_type AS area_type,
    table4.measure_type AS measure_type,
    table1.value AS value
      FROM public.timewise_measures AS table1
        INNER JOIN public.local_unemployment_descriptive AS table2
        ON table1.series_id = table2.series_id
        INNER JOIN public.measure_type_enums AS table4
        ON table2.measure_type_enum = table4.code
        INNER JOIN public.area_type_enums AS table5
        ON table2.area_type_enum = table5.code`;
  if ( filters ) {
    baseQuery += createWhereClause( filters );
  }
  return baseQuery += ` LIMIT $1 OFFSET $2`;
}

const limit = 100;

module.exports = (page, filters, cb) => {
  const offset = page * limit;
  const query = createQuery( filters );
  return client.query( query,
    [limit, offset],
    cb );
};
