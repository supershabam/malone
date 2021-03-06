"use strict";

var Malone = require('../')
  , fs = require('fs')
  , options
  ;

options = {
  redis: {
    host: 'localhost',
    port: 6379
  }
};

describe('multiple mailmen', function() {
  it('creates a bunch of mailmen at once', function(done) {
    var m1 = new Malone(options);
    var m2 = new Malone(options);
    var m3 = new Malone(options);
    m2.on('message', function(message) {
      m2.send(m3.getId(), message);
    });
    m3.on('message', function(message) {
      m3.send(m1.getId(), message);
    });
    m1.on('message', function(message) {
      done();
    });    
    m1.ready(function(){
      m2.ready(function(){
        m3.ready(function(){
          m1.send(m2.getId(), '{"type":"Register","entityId":"device|8746288"}');   
        });
      });
    });
  });
});

describe('parsing', function() {
  it('sends 500 single character messages', function(done) {
    var m = new Malone(options)
      , i
      , count = 0
      ;

    m.ready(function() {
      for (i = 0; i < 500; ++i) {
        m.send(m.getId(), 'a');
      }
    });
    m.on('message', function(message) {
      if (message === 'a') ++count;
      if (count === 500) done();
    });
  });

  it('parses a very long file', function(done) {
    var m = new Malone(options)
      , message = fs.readFileSync(__dirname + '/largemessage.txt')
      ;

    m.ready(function() {
      m.send(m.getId(), message);
    });
    m.on('message', function(msg) {
      if (message.length === msg.length) return done();
      return done('wrong message length');
    });
  });
});