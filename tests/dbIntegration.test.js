const request = require('supertest');

let server;

beforeEach( () => {
  server = require( '../server' );
} );

afterEach( () => {
  server.close();
  server = null;
} );

test('basic request to server', () => {
  request( server ).get( '/laus' )
    .then( response => {
      expect( response.body.length ).toEqual( 100 );
    });
});

test('request with single word query: series_id', () => {
  expect( true ).toEqual( true );
} );

test('request with multi word query: series_id', () => {} );
test('request with single word query: period', () => {} );
test('request with multi word query: period', () => {} );
test('request with single word query: label', () => {} );
test('request with multi word query: label', () => {} );
test('request with single word query: seasonality_enum', () => {} );
test('request with multi word query: seasonality_enum', () => {} );
test('request with single word query: area', () => {} );
test('request with multi word query: area', () => {} );
test('request with single word query: area_type', () => {} );
test('request with multi word query: area_type', () => {} );
test('request with single word query: measure_type', () => {} );
test('request with multi word query: measure_type', () => {} );
test('request with single word query: value', () => {} );
test('request with multi word query: value', () => {} );
