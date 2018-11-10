const fs = require( 'fs' );

const { Pool, Client } = require( 'pg' );
const PgGen   = require( 'pg-gen' );

const dbSecrets = require( '../config/dbConfig' );
console.log('dbSecrets', dbSecrets);
const client = new Client( dbSecrets );
const pgGen = PgGen( { pg: client } );

client.connect();

const query = `SELECT
  table2.survey_name,
  table2.series_title,
  table1.series_id,
  table1.period,
  table1.label,
  table2.seasonality_enum,
  table2.area,
  table5.area_type,
  table4.measure_type,
  table1.value
    FROM public.timewise_measures AS table1
    INNER JOIN public.local_unemployment_descriptive AS table2
    ON table1.series_id = table2.series_id
    INNER JOIN public.measure_type_enums AS table4
    ON table2.measure_type_enum = table4.code
    INNER JOIN public.area_type_enums AS table5
    ON table2.area_type_enum = table5.code`;
const gen = pgGen.atMost( 1000 ).lazyQuery( query );

// gen()// .then( result => {
//   console.log('1', typeof result, result.length);
// })
// .then( result => {
//   console.log('2', typeof result, result.length);
// });

module.exports = () => gen();
// (cb) => {
//   return client.query( selectScript, function( err, res ) {
//     if ( err ) {
//       cb(err);
//     }
//     return cb(null,res);
//   } );
// };
