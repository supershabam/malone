var malone = require('../')
  , karl = malone.createMalone('karl', {redis: {host: 'localhost', port: 6379}})
  , stockton = malone.createMalone('stockton', {redis: {host: 'localhost', port: 6379}})
  ;

karl.ready(function() {
  stockton.ready(function() {
    karl.send('stockton', JSON.stringify({test: 'value1'}));
    karl.send('stockton', JSON.stringify({test: 'value2'}));
    stockton.send('karl', 'I give you da ballI give you da ballI give you da ballI give you da ballI give you da ballI give you da ballI give you da ballI give you da ballI give you da ballI give you da ballI give you da ballI give you da ballI give you da ballI give you da ballI give you da ballI give you da ballI give you da ballI give you da ballI give you da ballI give you da ballI give you da ballI give you da ballI give you da ballI give you da ballI give you da ballI give you da ballI give you da ballI give you da ballI give you da ballI give you da ballI give you da ballI give you da ballI give you da ballI give you da ballI give you da ballI give you da ballI give you da ballI give you da ballI give you da ballI give you da ballI give you da ballI give you da ballI give you da ballI give you da ballI give you da ballI give you da ballI give you da ballI give you da ballI give you da ballI give you da ballI give you da ballI give you da ballI give you da ballI give you da ballI give you da ballI give you da ballI give you da ballI give you da ballI give you da ballI give you da ballI give you da ballI give you da ballI give you da ballI give you da ballI give you da ballI give you da ballI give you da ballI give you da ballI give you da ballI give you da ballI give you da ballI give you da ballI give you da ballI give you da ballI give you da ballI give you da ballI give you da ballI give you da ballI give you da ballI give you da ballI give you da ballI give you da ballI give you da ballI give you da ballI give you da ballI give you da ballI give you da ballI give you da ballI give you da ballI give you da ballI give you da ballI give you da ballI give you da ball');
    stockton.send('karl', 'I give you da ballI give you da ballI give you da ballI give you da ballI give you da ballI give you da ballI give you da ballI give you da ballI give you da ballI give you da ballI give you da ballI give you da ballI give you da ballI give you da ballI give you da ballI give you da ballI give you da ballI give you da ballI give you da ballI give you da ballI give you da ballI give you da ballI give you da ballI give you da ballI give you da ballI give you da ballI give you da ballI give you da ballI give you da ballI give you da ballI give you da ballI give you da ballI give you da ballI give you da ballI give you da ballI give you da ballI give you da ballI give you da ballI give you da ballI give you da ballI give you da ballI give you da ballI give you da ballI give you da ballI give you da ballI give you da ballI give you da ballI give you da ballI give you da ballI give you da ballI give you da ballI give you da ballI give you da ballI give you da ballI give you da ballI give you da ballI give you da ballI give you da ballI give you da ballI give you da ballI give you da ballI give you da ballI give you da ballI give you da ballI give you da ballI give you da ballI give you da ballI give you da ballI give you da ballI give you da ballI give you da ballI give you da ballI give you da ballI give you da ballI give you da ballI give you da ballI give you da ballI give you da ballI give you da ballI give you da ballI give you da ballI give you da ballI give you da ballI give you da ballI give you da ballI give you da ballI give you da ballI give you da ballI give you da ballI give you da ballI give you da ballI give you da ballI give you da ball');
  });
});

karl.on('message', function(message) {
  console.log('karl malone receive', message);
});

stockton.on('message', function(message) {
  console.log('stockton receive', message);
});

