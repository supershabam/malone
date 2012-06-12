"use strict";

var malone = require('../')
  , options
  ;

options = {
  redis: {
    host: 'localhost',
    port: 6379
  }
};

describe('test', function() {
  it('creates a bunch of mailmen at once', function(done) {
    var m1 = malone.createMalone('m1', options);
    var m2 = malone.createMalone('m2', options);
    var m3 = malone.createMalone('m3', options);
    m2.on('message', function(message) {
      m2.send('m3', message);
    });
    m3.on('message', function(message) {
      m3.send('m1', message);
    });
    m1.on('message', function(message) {
      done();
    });    
    m1.ready(function(){
      m2.ready(function(){
        m3.ready(function(){
          m1.send('m2', 'from m1');   
        });
      });
    });
  });
});