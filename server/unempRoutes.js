var express = require('express')
var router = express.Router()

router.get('/', (req, res) => {
  res.send( 'howdy yep' );
});

module.exports = router
