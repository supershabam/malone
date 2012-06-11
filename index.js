var redis  = require('redis')
  , ready  = require('ready')
  , net    = require('net')
  , util   = require('util')
  , events = require('events')
  , os     = require('os')
  ;

function Malone(id, options) {
  events.EventEmitter.call(this);

  this._redisClient = redis.createClient(options.redis.port, options.redis.host, options.redis.options || {});
  this._id = id;
  this._host = options.host || os.hostname();
  this._port = options.port || null;
  this._redisPrefix = options.redisPrefix || 'malone:';
  this._connections = {};
  this._refreshInterval = options.refreshInterval || 60000;
  this._expires = options.expires || 120000;
  this._refresher = null;

  // magical thisness
  this._connectionHandler = this._connectionHandler.bind(this);
  this._listeningHandler = this._listeningHandler.bind(this);
  this._errorHandler = this._errorHandler.bind(this);
  this._clientDataHandler = this._clientDataHandler.bind(this);
  this._refresh = this._refresh.bind(this);
  this._startRefresher = this._startRefresher.bind(this);

  // start server
  this._server = net.createServer();
  this._server.on('connection', this._connectionHandler);
  this._server.on('listening', this._listeningHandler);  
  this._server.on('error', this._errorHandler);
  this._server.listen(this._port, this._host);
}
util.inherits(Malone, events.EventEmitter);
ready.mixin(Malone.prototype);

Malone.prototype.send = function(id, message, cb) {
  var self = this;

  cb = cb || function() {};

  // @TODO LRU cache this lookup
  this._redisClient.get(this._redisPrefix + id, function(err, addr) {
    if (err || !addr) return cb(err || 'unable to find id');
    message = message || '';
    var connection = self._createOrFetchConnection(addr);
    var payload = '' + message.length.toString(16) + '|' + message;
    connection.write(payload);
    cb();
  });
};

Malone.prototype._createOrFetchConnection = function (addr) {
  if (this._connections.hasOwnProperty(addr)) return this._connections[addr];
  
  var port, host, connection;
  port = addr.split(':')[1];
  host = addr.split(':')[0];
  connection = net.connect(port, host);
  this._connections[addr] = connection;
  connection.on('end', function() {

  })  
  return connection;
};

Malone.prototype._removeConnection = function(addr) {
  if (this._connections[addr]) {
    this._connections[addr].close();
    delete this._connections[addr];  
  }
}

// handles clients connecting to us to provide datas
Malone.prototype._connectionHandler = function(client) {
  client.setEncoding('utf8');
  client.on('data', this._clientDataHandler);
};

Malone.prototype._listeningHandler = function() {
  address = this._server.address();
  this._port = address.port;
  this._register();
};

Malone.prototype._errorHandler = function(e) {
  console.error('error in malone', e);
};

Malone.prototype._clientDataHandler = function(payload) {
  var prefix
    , message
    , length
    , pipePos
    , runaway = 200
    ;

  while (payload.length !== 0) {
    pipePos = payload.indexOf('|');
    length = parseInt(payload.slice(0, pipePos), 16);
    message = payload.substr(pipePos + 1, length);
    payload = payload.slice(pipePos + 1 + length, Infinity);

    this.emit('message', message);

    if (--runaway == 0) {
      console.error('malone parsing is not working right...');
      break;
    }
  }
};

Malone.prototype._isReady = function() {
  return this._isRedisReady && this._isListening;
}

Malone.prototype._register = function() {
  var self = this;

  this._redisClient.set(this._redisPrefix + this._id, this._host + ':' + this._port, function registerResult(err) {
    if (err) console.log(err);

    self.ready(true);
    if (self._expires) {
      self._startRefresher();
      self._refresh();
    }
  });
};

Malone.prototype._startRefresher = function() {
  if (this._refresher) clearInterval(this._refresher);
  this._refresher = setInterval(this._refresh, this._refreshInterval);
}

Malone.prototype._refresh = function() {
  this._redisClient.expire(this._redisPrefix + this._id, ~~(this._expires/1000));
}

exports.createMalone = function createMalone(id, options) {
  return new Malone(id, options);
};