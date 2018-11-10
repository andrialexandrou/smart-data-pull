const devConfig = require( "./dbDev.json" );
const prodConfig = require( "./dbProd.json" );
const secrets = require( "../secrets.json" );

let config = devConfig;

if ( process.env.NODE_ENV === 'production' ) {
  config = Object.assign(
    {},
    config,
    prodConfig
  );
}

config.password = secrets.password;

module.exports = config;
