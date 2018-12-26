const fs = require( 'fs' );

const { Pool, Client } = require( 'pg' );
const _ = require( 'lodash' );

const dbSecrets = require( '../../config/dbConfig' );
const client = new Client( dbSecrets );

client.connect();

const tables = {
  survey_name: 'table2',
  series_title: 'table1',
  series_id: 'table1',
  period: 'table1',
  label: 'table1',
  seasonality_enum: 'table2',
  area: 'table2',
  area_type: 'table5',
  measure_type: 'table4',
  value: 'table1'
};

function createWhereClause( filters ) {
  const filtersArray = [];
  _.forEach( filters, function(values, param) {
    const table = tables[ param ];
    const phrases = values.map( value => {
      if ( param === 'area' ) {
        return `${ table }.${ param } LIKE '${ value }'`;
      } else {
        return `${ table }.${ param } LIKE '%${ value }%'`;
      }
    })
    filtersArray.push( phrases.join( ' OR ') );
  })
  return filtersArray.length > 1 ?
    ` WHERE ${ filtersArray.join( ' AND ' ) }` :
    ` WHERE ${ filtersArray[ 0 ] }`;
}

function createQuery( filters ) {
  let baseQuery = ``;
  if ( filters ) {
    baseQuery += createWhereClause( filters );
  }
  return baseQuery;
  return baseQuery += ` LIMIT $1 OFFSET $2`;
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
