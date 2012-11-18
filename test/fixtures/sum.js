
var levelidb = require('levelidb')
var pad     = require('pad')
var EndStream = require('end-stream')
var del = require("./purge")

function genSum (path, cb) {
  levelidb(path, {createIfMissing: true}, function (err, db) {
    del(db, function () {
      var l = 1e3, i = 0
      var stream = db.writeStream()
      while(l--)
        stream.write({key: pad(6, ''+ ++i, '0'), value: JSON.stringify(i)})
      stream.end()
      if(cb) stream.on('close', cb)
    })
  })
}

if(!module.parent) {

  var dir = '/tmp/map-reduce-sum-test'

  genSum()
}

module.exports = genSum

