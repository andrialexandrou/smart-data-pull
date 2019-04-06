const ApiKey = require( '../config/apiKey' );
const apiKey = new ApiKey();

test('request with single word query: series_id', () => {
  expect( apiKey.get() ).toEqual( 'abc' );
} );  
