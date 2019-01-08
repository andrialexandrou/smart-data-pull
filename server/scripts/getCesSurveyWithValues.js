const fs = require( 'fs' );

const { Pool, Client } = require( 'pg' );
const _ = require( 'lodash' );

const createCsv = require('json2csv').parse;
const dbSecrets = require( '../../config/dbConfig' );
const client = new Client( dbSecrets );

client.connect();

const tables = {
  survey_name: 'table2',
  series_title: 'table2',
  series_id: 'table1',
  state: 'table8',
  name: 'table9',
  industry_type: 'table6',
  supersector_type: 'table7',
  seasonality_enum: 'table2',
  label: 'table1',
  period: 'table1',
  employment_type: 'table4',
  value: 'table1'
};

function createWhereClause( filters ) {
  const filtersArray = [];
  _.forEach( filters, function(values, param) {
    const table = tables[ param ];
    const phrases = values.map( value => {
      if ( param === 'label' ) {
        return `${ table }.${ param } LIKE '%${ value }%'`;
      } else {
        return `${ table }.${ param } LIKE '${ value }'`;
      }
    })
    filtersArray.push( phrases.join( ' OR ') );
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
    table8.state AS state,
    table9.name AS area_type,
    table6.industry_type AS industry,
    table7.supersector_type AS supersector,
    table2.seasonality_enum AS seasonality_enum,
    table1.label AS label,
    table1.period AS period,
    table4.employment_type AS employment_type,
    table1.value AS value
    FROM public.ces_timewise_measures AS table1
      INNER JOIN public.current_employment_descriptive AS table2
      ON table1.series_id = table2.series_id
      INNER JOIN public.employment_enums AS table4
      ON table2.employment_code = table4.code
      INNER JOIN public.industry_enums AS table6
      ON table2.industry_code = table6.code
      INNER JOIN public.supersector_enums AS table7
      ON table2.supersector_code = table7.code
      LEFT JOIN public.state_enums AS table8
      ON table2.state_code = table8.code
      LEFT JOIN public.area_code_enums AS table9
      ON table2.area_code = table9.code`;
  if ( filters ) {
    baseQuery += createWhereClause( filters );
  }
  return baseQuery;
}

const limit = 100;

module.exports = (isDownload, page, filters, cb) => {
  if ( isDownload ) {
    const query = createQuery( filters );
    return client.query( query, cb );
  } else {
    const offset = page * limit;
    const query = createQuery( filters );
    return client.query( query + ' LIMIT $1 OFFSET $2',
      [limit, offset],
      cb );
  }
};
