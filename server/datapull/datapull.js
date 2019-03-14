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

function upsertToDatabase( results ) {
  if (!_.isArray) {
    console.log('Results is not an array', results);
  }
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
        VALUES ('${id}', '${emPeriod}', '${isoPeriod}', '${value}')`;

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

function requestForAllHistoric( seriesIds, currentYear ) {
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
    console.log('\nREQUESTING NEXT SET FOR ALL TIME PERIODS beginning with', setToRequest[0]);

    const timeWindow = {};
    timeWindow.start = currentYear - 1;
    timeWindow.end = currentYear;

    request( setToRequest, timeWindow.start, timeWindow.end )
      .then( upsertToDatabase )
      .then( () => onRequestComplete() );
  })();
}

function convertRowsToIds( rowsArray ) {
  return rowsArray.map( obj => obj.series_id );
}

function getOneId( type, area ) {
  let regionClause;
  const table = type === 'employment' ?
    'current_employment_descriptive' :
    'local_unemployment_descriptive';
  if (type === 'employment') {
    regionClause = area === 'metropolitan' ?
      `WHERE series_id LIKE 'LA%'` :
      `WHERE series_id LIKE 'CE%'`;
  } else {
    regionClause = area === 'metropolitan' ?
      `WHERE series_id LIKE 'LA%'` :
      `WHERE series_id LIKE 'LN%'`;

  }
  return new Promise( ( resolve, reject ) => {
    const query = `
      SELECT series_id
        FROM public.${ table }
        ${ regionClause }
        LIMIT 1;
      `;
    console.log('query', query);
    client.query(query, function( err, res ) {
        if ( err ) reject(err);
        const rows = convertRowsToIds( res.rows );
        resolve(rows[0]);
      }
    );
  } );
}

function getListOfOutdated(type, area, isoPeriod) {
    const empTable = 'ces_timewise_latest';
    const unempTable = 'laus_timewise_latest';
    let regionClause;

    const table = type === 'employment' ?
      empTable :
      unempTable;
    if (type === 'employment') {
      regionClause = area === 'metropolitan' ?
        `AND series_id LIKE 'LA%'` :
        `AND series_id LIKE 'CE%'`;
    } else {
      regionClause = area === 'metropolitan' ?
        `AND series_id LIKE 'LA%'` :
        `AND series_id LIKE 'LN%'`;

    }

    const query = `SELECT series_id FROM public.${ table }
      WHERE label != '${ isoPeriod }'
      ${ regionClause }`;
      console.log('query', query);

    return new Promise( resolve => {
      client.query( query, function( err, res ) {
          if ( err ) console.log(err);

          const rows = convertRowsToIds( res.rows );
          resolve( rows );
        }
      );
    } );
}

function procedureForOutdatedAsync(type, area) {
  var currentYear = 0;
  getOneId(type, area)
  .then( id => {
    console.log('ID', id);
    if (!id) {
      console.log('No ids for', type, area);
      return;
    }
    return request( [ id ], null, null, true )
  } )
  .then( blsResult => {
    var timePeriod = blsResult[0].data[0];
    console.log('timePeriod', timePeriod);
    currentYear = timePeriod.year;
    var isoPeriod = createISOString( timePeriod);
    return getListOfOutdated(type, area, isoPeriod);
  })
  .then(list => {
    if (!list || list && list.length === 0) {
      console.log('No out-of-date records on', new Date());
      return;
    }
    return requestForAllHistoric( list, currentYear );
  } )
  .catch(err => {
    if (err) console.log('err', err);
  })

}

function dailyTest() {
  // procedureForOutdatedAsync('employment', 'federal'); // done
  procedureForOutdatedAsync('employment', 'metropolitan');
  // procedureForOutdatedAsync('unemployment', 'federal');
  // procedureForOutdatedAsync('unemployment', 'metropolitan');
}

dailyTest();
