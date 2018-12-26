const { Pool, Client } = require( 'pg' );
const _ = require( 'lodash' );

const dbSecrets = require( '../../config/dbConfig' );
const client = new Client( dbSecrets );

client.connect();

function createQuery( geoPhrase ) {
  return `SELECT DISTINCT area
  	FROM public.current_employment_descriptive
  	WHERE LOWER(area) LIKE LOWER('%${ geoPhrase }%')
    LIMIT 25`;
}

module.exports = ( phrase, cb ) => {
  const query = createQuery( phrase );
  return client.query( query, cb );
}
