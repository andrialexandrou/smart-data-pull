const blsConfig = require( './bls.json' );
const _ = require('lodash');

module.exports = class ApiKey {
  constructor() {
    this.currentKey = ''
    this.keys = []
    this.expired = false
    this.reset();
  }

  cycleKeys() {
    this.currentKey = this.keys.shift();
    this.keys.push( this.currentKey );
  }

  get() {
    if ( this.expired ) {
      this.cycleKeys();
      this.expired = false;
    }
    return this.currentKey;
  }

  reset() {
    this.keys = _.map( blsConfig, value => value );
    this.cycleKeys();
  }

  expire() {
    this.expired = true;
  }
};
