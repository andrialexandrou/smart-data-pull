const fs = require( 'fs' );
const _ = require('lodash');

const { Pool, Client } = require( 'pg' );
const axios = require( 'axios' );

const dbSecrets = require( '../../config/dbConfig' );
const ApiKey = require( '../../config/apiKey' );
const apiKey = new ApiKey();

const client = new Client( dbSecrets );
client.connect();

let retry = () => {};
function saveForRetry( seriesArray, startYear, endYear ) {
  retry = function () {
    console.log('RETRYING FOR', seriesArray, startYear, endYear);
    request( seriesArray, startYear, endYear );
    retry = () => {};
  }
}

// function getTimePeriodFromResult( result ) {
//   var timelyResults = result.data;
//   timelyResults.forEach( timePeriod => {
//     var isoPeriod = createISOString( timePeriod);
// }

function upsertToDatabase( results ) {
  console.log('results[0]', results[0]);
  results.forEach( result => {
    var id = result.seriesID;
    var timelyResults = result.data;
    timelyResults.forEach( timePeriod => {
      var isoPeriod = createISOString( timePeriod);
      var emPeriod = timePeriod.period;
      var value = timePeriod.value;

      const table = true ?
        'ces_timewise_measures' :
        'laus_timewise_measures';

      const upsert = `INSERT INTO public.${ table }
        (series_id, period, label, value)
        SELECT '${id}', '${emPeriod}', '${isoPeriod}', '${value}'
        WHERE
            NOT EXISTS (
                SELECT series_id, label FROM public.ces_timewise_measures
            WHERE series_id='${id}'
            AND label='${isoPeriod}'
            );`

      client.query(upsert,
        function( err, res) {
          if ( err ) {
            if ( err.error && !err.error.includes('duplicate key value violates')) {
              console.log('client query err', err);
            }
          }
        }
      );
    })
  })
}

function request( seriesArray, startYear, endYear, isLatest ) {
  console.log('args', ...arguments);
  if ( !isLatest ) {
    saveForRetry( seriesArray, startYear, endYear );
  }
  const urlPath = "https://api.bls.gov/publicAPI/v2/timeseries/data/";
  // split into 50 hombre
  if ( !seriesArray ) {
    return;
  }

  const options = {
    "seriesid": seriesArray,
    "registrationkey": apiKey.get(),
  };
  if ( isLatest ) {
    options.latest = true;
  } else {
    options.annualaverage =  true;
    options.startyear =  startYear.toString();
    options.endyear = endYear.toString();
  }

  return new Promise( (resolve, reject) => {
    axios.post( urlPath, options )
    .then( handleResponse )
    .then( resolve )
    .catch( err => {
      console.log('err', err);
      if ( err.response.statusText === "Internal Server Error" ) {
        console.log('[err.response]\n', err.response);
        process.exit();
      }
    });
  });
}

var monthEnums = {
  "January": "01",
  "February": "02",
  "March": "03",
  "April": "04",
  "May": "05",
  "June": "06",
  "July": "07",
  "August": "08",
  "September": "09",
  "October": "10",
  "November": "11",
  "December": "12",
  "Annual": "13"
}

function createISOString( timePeriod ) {
  var year = timePeriod.year;
  var month = monthEnums[ timePeriod.periodName ];

  if ( !month ) {
    console.log( "Couldn't find a month, hombre", timePeriod );
    if ( false /* is annual average */ ) {
      return year;
    }
  }
  if ( month === "13" ) {
    return year;
  } else {
    return year + "-" + month + "-01";
  }
}

function checkMessages( messages ) {
  const hasReachedMax = messages.find( message => message.includes( 'daily threshold' ) );
  if ( hasReachedMax ) {
    apiKey.expire();
    retry();
    return;
  }
  console.log(messages.join('\n'));
}

function handleResponse( res ) {
  var messages = res.data.message; // array
  if ( !res || !res.data ) {
    return;
  }
  if ( messages && messages.length > 0 ) {
    checkMessages( messages );
  }

  if ( !res.data.Results ) {
    console.log('No data\n', res);
    return;
  }
  var results = res.data.Results.series;

  return new Promise( (resolve, reject) => {
    resolve( results );
  } );
}

function requestForAllHistoric( seriesIds ) {
  var setsOfFifty = [];

  while ( seriesIds.length > 0 ) {
    var nextSet = seriesIds.splice( 0, 50 );
    setsOfFifty.push( nextSet );
  }

  (function onRequestComplete() {
    if ( setsOfFifty.length <= 0 ) {
      return;
    }

    var setToRequest = setsOfFifty.shift();
    console.log('\nREQUESTING NEXT SET FOR ALL TIME PERIODs');

    const timeWindow = {};
    timeWindow.start = 1900 + ( new Date() ).getYear() - 1;
    timeWindow.end = 1900 + ( new Date() ).getYear() - 1;

    request( setToRequest, timeWindow.start, timeWindow.end )
      .then( upsertToDatabase )
      .then( () => onRequestComplete() );
  })();
}

function convertRowsToIds( rowsArray ) {
  return rowsArray.map( obj => obj.series_id );
}

function getLabel( rowsArray ) {
  return rowsArray.map( obj => obj.label )[0];
}

function getOneId( type ) {
  const table = type === 'employment' ?
    'current_employment_descriptive' :
    'local_unemployment_descriptive';
  return new Promise( ( resolve, reject ) => {
    client.query(`
      SELECT series_id
        FROM public.${ table }
        LIMIT 1;
      `, function( err, res ) {
        if ( err ) reject(err);
        const rows = convertRowsToIds( res.rows );
        resolve(rows[0]);
      }
    );
  } );
}

function latestForId( type, id ) {
  const table = type === 'employment' ?
    'ces_timewise_measures' :
    'laus_timewise_measures';
  return new Promise( ( resolve, reject ) => {
    client.query(`
      SELECT label
        FROM public.${ table }
        WHERE series_id='${ id }'
        ORDER BY label DESC
        LIMIT 1;
      `, function( err, res ) {
        if ( err ) reject(err);
        const label = getLabel( res.rows );
        resolve({
          id,
          label
        });
      }
    );
  } );
}

function selectIdsFromDatabase( tables ) {
  let queryTemplate = table => `SELECT series_id FROM public.${ table }`;
  const query = tables.map( queryTemplate ).join(' UNION ');
  return new Promise( resolve => {
    client.query( query, function( err, res ) {
        if ( err ) console.log(err);

        const rows = convertRowsToIds( res.rows );
        resolve( rows );
      }
    );
  } );
}

function dailyTest() {
  // get 1 from each
  let isoStringInDb = '';
  const empTable = 'current_employment_descriptive';
  const unempTable = 'local_unemployment_descriptive'
  const tablesToQuery = [];

  function addNecessaryRequests( type ) {
    return new Promise( (resolve, reject) => {
      getOneId( type )
        .then( id => {
          return latestForId( type, id );
        })
        .then( latestInDb => {
          isoStringInDb = latestInDb.label;
          return request( [ latestInDb.id ], null, null, true );
        })
        .then( blsResult => {
          var timePeriod = blsResult[0].data[0];
          var isoPeriod = createISOString( timePeriod);
          const hasNewData = isoPeriod !== isoStringInDb;
          if ( hasNewData ) {
            const table = type === 'employment' ?
              empTable :
              unempTable;
            tablesToQuery.push( table );
          }
          timePeriod = '';
          return;
        })
        .then( resolve );
    } );
  }

  addNecessaryRequests( 'employment' )
  .then( () => {
    return addNecessaryRequests( 'unemployment' );
  } )
  .then( () => {
    console.log('tablesToQuery', tablesToQuery);
    return selectIdsFromDatabase( tablesToQuery );
  })
  .then( rows => {
    console.log('length', rows.length);
    console.log('[0]', rows[0]);
    requestForAllHistoric(rows);
  });
}

dailyTest();
