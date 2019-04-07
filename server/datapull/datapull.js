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

function insert(script) {
  client.query(script, function( err, res) {
    console.log('res from insert', res)
    if ( err ) {
      console.log('err on insert', err)
    } else {
      // do another 
      // needs to only update when all are done. currently doing for every month
      console.log('update script', updateLatestTableWithToday)
    }
  })
}

function updateWithToday() {
  const today = createISOString(new Date());
  const updateLatestTableWithToday = `UPDATE ${ latestTable }
    SET label='${ today }'
    WHERE series_id='${ id }'`;
    

  client.query(updateLatestTableWithToday, function(err,res) {
    // do what
    if ( err ) console.log('err on updateLatestTableWithToday', err)
    if ( res ) {
      console.log(`Success updating ${ id } for period ${ emPeriod } with label ${ today }`, res)
    }
  })
}

function valueTimePeriodArray(id, timePeriod) {
  return [
    id,
    createISOString(timePeriod),
    timePeriod.period,
    timePeriod.value
  ]
}

function whichTable() {
  const whichTable = `SELECT count(*) FROM current_employment_descriptive WHERE series_id = '${ id }'`;
  client.query(whichTable, function(err0, res0) {
    if ( err0 ) console.log('err0 on whichTable script', err0 )
    const isEmployment = res0.rows[ 0 ].count > 0
  
    const measuresTable = isEmployment ?
      'ces_timewise_measures' :
      'laus_timewise_measures';
    const latestTable = isEmployment ?
      'ces_timewise_latest' :
      'laus_timewise_latest';
  })
}

// columns array
// values array of arrays
function makeBatchInsert(table, columns, values) {
  console.log('batchinsert')
  console.log('table', table)
  console.log('columns', columns)
  console.log('values', values)
  const prefix = `INSERT INTO public.${ table } (${ columns.join( ',' )}) VALUES `;
  console.log('prefix', prefix)
  
  const valuesClauses = values.map(valuesArr => {
    const thing = valuesArr.map( val => `'${val}'`);
    const second = thing.join(',')
    return `( ${ second } )`;
  });
  console.log('valuesClauses', valuesClauses)
  const valuesClause = valuesClauses.join(',');
  return prefix + valuesClause;
}

function createBatchUpdateScript( results ) { // formerly upsert
  if (!_.isArray(results)) {
    console.log('Results is not an array', results);
    return;
  }
  
  results.map( result => {
    // every region
    var id = result.seriesID;

    var timelyResults = result.data;
    const arrayOfArrays = timelyResults.map( timePeriod => {
      // every month in this region
      return valueTimePeriodArray( id, timePeriod )
    })
    const script = makeBatchInsert( 
      'ces_timewise_measures',
      ['series_id', 'period', 'label', 'value'],
      arrayOfArrays 
    )
    console.log("script in createBatchUpdate", script)
    return script;
  })
}

function request( seriesArray, startYear, endYear) {
  saveForRetry( seriesArray, startYear, endYear );
  const urlPath = "https://api.bls.gov/publicAPI/v2/timeseries/data/";
  if ( !seriesArray ) {
    return;
  }

  const options = {
    "seriesid": seriesArray,
    "registrationkey": apiKey.get(),
    annualaverage: true,
    startyear: startYear.toString(),
    endyear: endYear.toString()
  };

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
  if ( timePeriod instanceof Date ) {
    return timePeriod.toISOString().substring(0,10)
  }
  var year = timePeriod.year;
  
  if ( month === "13" ) {
    return year;
  } else {
    var month = monthEnums[ timePeriod.periodName ];
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

function requestForTheYear( seriesIds, currentYear, cb ) {
  var setsOfFifty = [];

  while ( seriesIds.length > 0 ) {
    var nextSet = seriesIds.splice( 0, 50 );
    setsOfFifty.push( nextSet );
  }

  (function onRequestComplete() {
    if ( setsOfFifty.length <= 0 ) {
      cb();
      return;
    }

    var setToRequest = setsOfFifty.shift();
    console.log('\nREQUESTING NEXT SET FOR ALL TIME PERIODS beginning with', setToRequest[0]);

    const timeWindow = {};
    timeWindow.start = currentYear - 1;
    timeWindow.end = currentYear;

    request( setToRequest, timeWindow.start, timeWindow.end )
      .then( results => {
        const scripts50x = createBatchUpdateScript( results )
      } )
      .then( () => {
        setTimeout( () => {
          onRequestComplete() 
        }, 500 );
      });
  })();
}

function convertRowsToIds( rowsArray ) {
  return rowsArray.map( obj => obj.series_id );
}

function getListOfOutdatedAsync( type ) {
  const table = type === 'employment' ?
    'ces_timewise_latest' :
    'laus_timewise_latest';

  const today = createISOString( new Date() )
  const findNotToday = `SELECT series_id, label FROM ${ table } WHERE label <> '${ today }'`

  return new Promise( (resolve, reject) => {
    client.query(findNotToday, function(err, res) {
      if ( err ) {
        console.log(err)
      } else {
        const ids = convertRowsToIds( res.rows )
        resolve( ids )
      }
    })
  })
}

function procedureForOutdatedAsync(type) {
  return getListOfOutdatedAsync( type )
    .then( ids => {
      return requestForTheYear( ids, new Date().getUTCFullYear() )
    } )
  
}

function dailyTest() {
  procedureForOutdatedAsync('employment', () => {
    procedureForOutdatedAsync('unemployment');
  })
}

dailyTest();
