var redis  = require('redis')
  , net    = require('net')
  , util   = require('util')
  , events = require('events')
  , os     = require('os')
  , Goldfish = require('goldfish')
  , cluster = require('cluster')
  , uuid = require('node-uuid')
  , ready = require('ready')
  , Lazy = require('lazy')
  , nop = function() {}
  ;


/**
 * Create a new malone instance. Accepts an options hash:
 * options{
 *   id: (optional) String - unique id to register as the mailbox address for this instance of malone,
 *   host: (optional) String - hostname that others connecting to this mailbox should use (public hostname/IP),
 *   port: (optional) Integer - port to listen on, random port selected if not specified,
 *   redis: {
 *     host: (default: 'localhost') String - redis host to connect to for registering mailbox address,
 *     port: (default: 6379) Integer - redis port to connect to,
 *     prefix: (default: 'malone:') String - prefix to use on any redis keys that are used by malone
 *   },
 *   expire: (default: (2 * 60 * 1000) //two minutes) Integer - expiration time on redis key for registering mailbox (0 for no expiration, leaves behind redis keys in your database)
 *   refresh: (default: (60 * 1000) //one minute) Integer - frequency to update keys in redis so that they don't expire (as long as the process is still alive) (0 for no refreshing)
 * }
 *
 * Malone functions
 *   .send(id, message, cb) - send to id the message. cb returns error on error otherwise null.
 *   .getId() - returns the ids of this malone instance
 *
 * Malone events
 *   'message' - emitted when this instance of malone is sent a message.
 *   'error' - emitted when an error occurs with malone!
 *   'listening' - emitted when the server starts listening, if you care. passes the server instance so that you can grab the address.
 */
function Malone(id, options) {
  events.EventEmitter.call(this);

  options = options || {};
  options.redis = options.redis || {};
  this._id = id || options.id || uuid.v4();
  this._host = options.host || os.hostname();
  this._port = options.port || 0;
  this._redis = redis.createClient(options.redis.port || 6379, options.redis.host || 'localhost');
  this._expire = options.expire || (2 * 60 * 1000);
  this._refresh = options.refresh || (60 * 1000);
  this._prefix = options.redis.prefix || 'malone:';

  this._connectionCache = new Goldfish({
    populate: this._populateConnectionCache.bind(this)
  });
  this._addrCache = new Goldfish({
    populate: this._populateAddrCache.bind(this)
  });
  this._redis.on('error', this.emit.bind(this, 'error'));

  this._server = net.createServer();
  this._server.on('connection', this._handleClient.bind(this));
  this._server.on('error', this.emit.bind(this, 'error'));
  this._server.on('listening', this.emit.bind(this, 'listening', this._server));
  this._listen();
}
util.inherits(Malone, events.EventEmitter);
ready.mixin(Malone.prototype);

Malone.prototype.send = function(id, message, cb) {
  var self = this;

  cb = cb || nop;

  this._addrCache.get(id, function handleGetAddr(err, addr) {
    if (err) return cb(err);
    self._connectionCache.get(addr, function handleGetConnection(err, connection) {
      if (err) return cb(err);

      try {
        // newline terminated json protocol
        connection.write(JSON.stringify({m:message}) + '\n');
      } catch (err) {
        return cb(err);
      }
      cb();
    });
  });
};

// @TODO try to connect to a local socket if the host is yourself
Malone.prototype._populateConnectionCache = function(addr, cb) {
  var host = addr.split(':')[0]
    , port = addr.split(':')[1]
    , connection
    , hasReturned = false
    , self = this
    ;

  connection = net.connect(port, host, function handleConnected() {
    hasReturned = true;
    cb(null, connection);
  });
  connection.on('error', function handleError(err) {
    if(!hasReturned) {
      hasReturned = true;
      cb(err);
    }
    connection.removeAllListeners();
    connection.end();
    self._connectionCache.evict(addr);
  });
  connection.on('end', function handleEnd() {
    if(!hasReturned) {
      hasReturned = true;
      cb(new Error('connection has been closed'));
    }
    connection.removeAllListeners();
    connection.end();
    self._connectionCache.evict(addr);
  });
};

Malone.prototype._populateAddrCache = function(id, cb) {
  this._redis.get(this._prefix + id, cb);
};

Malone.prototype._handleClient = function(client) {
  Lazy(client)
    .lines
    .map(String)
    .forEach((function handleClientData(line) {
      try {
        this.emit('message', JSON.parse(line).m);
      } catch (err) {
        this.emit('error', err);
      }
    }).bind(this));
};

Malone.prototype._listen = function() {  
  /**
   * SUPER-MEGA-STUPID Hack for bypassing bad decision for port 0 handling
   * https://github.com/joyent/node/issues/3324
   */
  if (cluster.isWorker && this._port === 0) {
    this._port = Math.floor(Math.random() * 40000) + 20000;
  }
  // END SUPER-MEGA-STUPID HACK
  
  this._server.listen(this._port, this._host, (function handleListening() {
    this._port = this._server.address().port;
    this._register();
  }).bind(this));
};

Malone.prototype._register = function() {
  // set redis
  this._redis.set(this._prefix + this._id, this._host + ':' + this._port, (function handleSet(err) {
    if (err) return this.emit('error', err);
    this.ready(true);
    this._setExpire();
  }).bind(this));
};

Malone.prototype._setExpire = function() {
  if (this._expire) {
    this._redis.expire(this._prefix + this._id, ~~(this._expire/1000));
  }
  if (this._refresh) {
    setTimeout((function recurseSetExpire() {
      this._setExpire();
    }).bind(this), this._refresh);
  }
};

module.exports = Malone;