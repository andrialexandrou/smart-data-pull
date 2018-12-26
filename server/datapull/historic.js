const fs = require( 'fs' );
const _ = require('lodash');

const { Pool, Client } = require( 'pg' );
const axios = require( 'axios' );

const dbSecrets = require( '../../config/dbConfig' );
const blsConfig = require( '../../config/bls.json' );

const client = new Client( dbSecrets );
client.connect();

const apiKey = {
  init() {
    this.keys = _.map( blsConfig, value => value );
  },
  keys: [],
  index: 0,
  expired: false,
  get() {
    if ( this.expired ) {
      this.index++;
      this.expired = false;
    }
    return this.keys[ this.index ];
  },
  reset() {
    this.index = 0;
  },
  expire() {
    this.expired = true;
  }
};

apiKey.init();

let retry = () => {};
function saveForRetry( seriesArray, startYear, endYear ) {
  retry = function () {
    console.log('RETRYING FOR', seriesArray, startYear, endYear);
    request( seriesArray, startYear, endYear );
    retry = () => {};
  }
}

function request( seriesArray, startYear, endYear ) {
  saveForRetry( seriesArray, startYear, endYear );
  const urlPath = "https://api.bls.gov/publicAPI/v2/timeseries/data/";
  // split into 50 hombre
  if ( !seriesArray ) {
    return;
  }
  console.log('REQUESTING SET OF FIFTY BEGINNING WITH', seriesArray[ 0 ], startYear, endYear);

  return axios.post( urlPath, {
    "seriesid": seriesArray,
    "registrationkey": apiKey.get(),
    "annualaverage": true,
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

  results.forEach( result => {
    var id = result.seriesID;
    var timelyResults = result.data;
    timelyResults.forEach( timePeriod => {
      var isoPeriod = createISOString( timePeriod);
      var emPeriod = timePeriod.period;
      var value = timePeriod.value;

      client.query(
        `INSERT INTO public.ces_timewise_measures(series_id, period, label, value)
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

function requestForAllHistoric( seriesIds ) {
  var setsOfFifty = [];

  while ( seriesIds.length > 0 ) {
    var nextSet = seriesIds.splice( 0, 50 );
    setsOfFifty.push( nextSet );
  }
  var indexOf = 0;
  setsOfFifty.forEach( function(set, index) {
    if ( set.includes('SMU27334605552410001')) {
      indexOf = index;
    }
  })
  var taResta = setsOfFifty.splice(indexOf);
  (function onRequestComplete() {
    if ( taResta.length <= 0 ) {
      return;
    }

    var setToRequest = taResta.shift();
    // var setToRequest = setsOfFifty.shift();
    console.log('\nREQUESTING NEXT SET FOR ALL TIME PERIODs');

    request( setToRequest, 1990, 2009 )
      .then( () => request( setToRequest, 2010, 2018 ) )
      .then( () => onRequestComplete() );

  })();
}

function insertIndustryCode( code, name ) {
  client.query(
    `INSERT INTO public.industry_enums(code, industry_type)
    VALUES ($1, $2)`,
    [ code, name ],
    function( err, res) {
      if ( err ) {
        if ( err.error && !err.error.includes('duplicate key value violates')) {
          console.log('client query err', err);
        }
      }
    }
  );
}

function convertRows( rowsArray ) {
  return rowsArray.map( obj => obj.series_id );
}

client.query(`
  SELECT series_id
  	FROM public.current_employment_descriptive;
  `, function( err, res ) {
    if ( err ) console.log(err);

    const rows = convertRows( res.rows );
    requestForAllHistoric(rows);
});
