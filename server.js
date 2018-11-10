const express = require('express');
const app = express();
const port = 4000;

const getSurveyWithValues = require('./scripts/getSurveyWithValues');

app.use(function(req, res, next) {
  res.header("Access-Control-Allow-Origin", "*");  res.header("Access-Control-Allow-Headers", "Origin, X-Requested-With, Content-Type, Accept");
  next();
});

app.get('/laus', (req, res) => {
  // getSurveyWithValues( (err, dbRes) => {
  //   res.send(dbRes.rows);
  // } );
  let response = null;
  getSurveyWithValues()
  .then( result => {
    response = result;
    // res.send( result );
    return getSurveyWithValues()
  })
  .then( moreResult => {
    if ( moreResult.length > 0 ) {
      console.log('ADDING MORE');
      response.hasMoreResults = true;
    }
    response.push({hasMoreResults: true});
    res.send( response );
  })
});

app.get('/unemp', (req, res) => {
  res.send( 'howdy yep' );
});

app.listen(port, () => console.log(`Example app listening on port ${port}!`))
