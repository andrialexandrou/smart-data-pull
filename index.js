const fs = require( 'fs' );

const { Pool, Client } = require( 'pg' );
const axios = require( 'axios' );

const dbSecrets = require( './config/dbConfig' );
const blsConfig = require( './config/bls.json' );

const client = new Client( dbSecrets );
client.connect();

function request( seriesArray, startYear, endYear ) {
  const urlPath = "https://api.bls.gov/publicAPI/v2/timeseries/data/";
  // split into 50 hombre
  console.log('REQUESTING', startYear, endYear);
  return axios.post( urlPath, {
    "seriesid": seriesArray,
    "registrationkey": blsConfig.apiKey,
    "startyear": startYear.toString(),
    "endyear": endYear.toString()
  } )
  .then( handleResponse )
  .catch( err => {
    console.log('err', err);
    if ( err.response.statusText === "Internal Server Error" ) {
      console.log('[err.response]\n', err.response);
      process.exit();
    }
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
  "December": "12"
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
  return year + "-" + month + "-01";
}

function handleResponse( res ) {
  var message = res.data.message; // array
  // if ( message.includes( "No Data Available for Series" ) ) {
  //   console.log("No data for this year");
  // }
  if ( !res || !res.data ) {
    return;
  }
  if ( res.data.message && res.data.message.length > 0 ) {
    if ( res.data.message[ 0 ].includes('daily threshold') ) {
      console.log('Exceeded max requests for this daily period.');
      process.exit();
    }
    if ( res.data.message[ 0 ].includes('No Data Available') ) {
      return;
    }
  }

  var results = res.data.Results.series;

  results.forEach( result => {
    var id = result.seriesID;
    var timelyResults = result.data;
    timelyResults.forEach( timePeriod => {
      var isoPeriod = createISOString( timePeriod);
      var emPeriod = timePeriod.period;
      var value = timePeriod.value;

      client.query(
        `INSERT INTO public.timewise_measures(series_id, period, label, value)
        VALUES ($1, $2, $3, $4)`,
        [ id, emPeriod, isoPeriod, value ],
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

Array.prototype.delayedForEach = function(callback, timeout, thisArg){
  var i = 0,
    l = this.length,
    self = this,
    caller = function(){
      callback.call(thisArg || self, self[i], i, self);
      (++i < l) && setTimeout(caller, timeout);
    };
  caller();
};

function requestForFourSets( seriesIds ) {
  var setsOfFourty = [];

  while ( seriesIds.length > 0 ) {
    var nextSet = seriesIds.splice( 0, 40 );
    setsOfFourty.push( nextSet );
  }
  (function onRequestComplete() {
    if ( setsOfFourty.length <= 0 ) {
      return;
    }

    var setToRequest = setsOfFourty.shift();
    console.log('REQUESTING NEXT SET FOR ALL FOUR TIME PERIODs');
    request( setToRequest, 2003, 2018 )
      .then( () => request( setToRequest, 1983, 2002 ) )
      // .then( () => request( setToRequest, 1963, 1982 ) )
      // .then( () => request( setToRequest, 1943, 1962 ) )
      .then( () => onRequestComplete() );

  })();
}

fs.readFile( './lists/seriesIds_1.json', (err, res) => {
  if ( err ) console.log('err', err);
  var jsonObject = JSON.parse( res );
  var firstSet = jsonObject.one;
  // var secondSet = jsonObject.two; // only first 2 year chunks
  // var thirdSet = jsonObject.three; // only first 2 year chunks
  // var fourthSet = jsonObject.four; // array partially completed
  // var fifthSet = jsonObject.five; // array partially completed
  // var lastSet = jsonObject.six;
  requestForFourSets( firstSet );
} )
