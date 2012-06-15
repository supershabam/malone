var redis  = require('redis')
  , ready  = require('ready')
  , net    = require('net')
  , util   = require('util')
  , events = require('events')
  , os     = require('os')
  , goldfish = require('goldfish')
  , cluster = require('cluster')
  ;

function Malone(id, options) {
  events.EventEmitter.call(this);

  // magical thisness
  this._connectionHandler = this._connectionHandler.bind(this);
  this._listeningHandler = this._listeningHandler.bind(this);
  this._errorHandler = this._errorHandler.bind(this);
  this._clientDataHandler = this._clientDataHandler.bind(this);
  this._refresh = this._refresh.bind(this);
  this._startRefresher = this._startRefresher.bind(this);
  this._createOrFetchConnection = this._createOrFetchConnection.bind(this);
  this._fetchAddrFromId = this._fetchAddrFromId.bind(this);

  // mah properties
  this._redisClient = redis.createClient(options.redis.port, options.redis.host, options.redis.options || {});
  this._id = id;
  this._host = options.host || os.hostname();
  this._port = options.port || null;
  this._redisPrefix = options.redisPrefix || 'malone:';
  this._connections = {};
  this._refreshInterval = options.refreshInterval || 60000;
  this._expires = options.expires || 120000;
  this._refresher = null;
  this._addrCache = goldfish.createGoldfish({
    populate: this._fetchAddrFromId,
    expires: options.expires || (5 * 60 * 1000)
  });  

  // start server
  this._server = net.createServer();
  this._server.on('connection', this._connectionHandler);
  this._server.on('listening', this._listeningHandler);  
  this._server.on('error', this._errorHandler);

  /**
   * SUPER-MEGA-STUPID Hack for bypassing Ben Noordhuis's bad decision   
   * https://github.com/joyent/node/issues/3324
   */
  var _isWorker = cluster.isWorker;
  cluster.isWorker = false;
  this._server.listen(this._port || Math.floor(Math.random() * 20000) + 20000, this._host);
  cluster.isWorker = _isWorker;
}
util.inherits(Malone, events.EventEmitter);
ready.mixin(Malone.prototype);

Malone.prototype._fetchAddrFromId = function(id, cb) {
  this._redisClient.get(this._redisPrefix + id, cb);
};

Malone.prototype._send = function(id, message, numRetries, cb) {
  var self = this;
  this._addrCache.get(id, function(err, addr) {
    if (err || !addr) {
      self._addrCache.evict(id);
      if (numRetries > 0) 
        return setTimeout(self._send.bind(self, id, message, --numRetries, cb), 100);      
      return cb(err || 'unable to find id');
    }

    message = message || '';
    self._createOrFetchConnection(addr, function(err, connection) {
      if (err) {
        self._addrCache.evict(id);
        if (numRetries > 0)
          return setTimeout(self._send.bind(self, id, message, --numRetries, cb), 100);
        return cb(err);
      }

      var payload = '' + message.length + '|' + message;
      connection.write(payload);
      return cb();
    });
  });
}

Malone.prototype.send = function(id, message, cb) {
  var self = this;
  cb = cb || function(){};
  this.ready(function() {
    self._send(id, message, 2, cb);
  });
};

Malone.prototype._createOrFetchConnection = function (addr, cb) {
  var port
    , host
    , connection
    , failTimeout
    , failed = false
    , fail
    , self = this
    ;

  if (this._connections.hasOwnProperty(addr)) {
    return cb(null, this._connections[addr]);
  }

  fail = function() {
    if (!failed) {
      failed = true;
      delete self._connections[addr];
      return cb('unable to establish connection');
    }
  };

  port = addr.split(':')[1];
  host = addr.split(':')[0];  

  failTimeout = setTimeout(fail, 500);

  connection = net.connect(port, host, function() {
    self._connections[addr] = connection;
    connection.on('end', function() {
      delete self._connections[addr];
    });

    if (failed) return;
    clearTimeout(failTimeout);
    return cb(null, connection);
  });
  connection.on('error', fail);
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
  var address = this._server.address();
  this._port = address.port;
  this._register();
};

Malone.prototype._errorHandler = function(e) {
  console.error('error in malone', e);
};

Malone.prototype._clientDataHandler = function(payload) {
  var start
    , pipe
    , length
    , message
    ;

  start = 0;
  while((pipe = payload.indexOf('|', start)) !== -1) { 
    length = parseInt(payload.slice(start, pipe), 10);
    if(isNaN(length)) break;
     
    message = payload.slice(pipe + 1, pipe + 1 + length);
    this.emit('message', message);
    start = start + length;
  }
};

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