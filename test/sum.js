
var MR      = require('..')
var sum     = require('./fixtures/sum')
var levelidb = require('levelidb')
var assert  = require('assert')
var through = require('through')
var mac     = require('macgyver')().autoValidate()
var pad     = require('pad')

sum('/tmp/map-reduce-sum-test', function (err) {
  if (err) {
      throw err
  }

  var mr = MR({
    path: '/tmp/map-reduce-sum-test',
    map: function (key, value) {
      //value = JSON.parse(value)
      this.emit(value % 2 ? 'odd' : 'even', value)
    },
    reduce: function (big, little, key) {
      return JSON.stringify(JSON.parse(big) + JSON.parse(little))
    },
    initial: 0
  }).force()

  mr.on('reduce', mac(function (key, sum) {
    console.log("REDUCE", key, sum)
    if(key.length == 0) {
      assert.equal(JSON.parse(sum), ( 1000 * 1001 ) / 2)
      console.log('passed')

      //mr.readStream({group: ['even']})
        //.pipe(through(console.log))
    }
  }).times(3))
})

sum('/tmp/map-reduce-sum-test-range', function (err) {
  if (err)
    throw err

  var mr = MR({
      path: '/tmp/map-reduce-sum-test-range'
    , start: '000000'
    , end: '000020'
    , map: function (key, value) {
      this.emit("const", value)
    }
    , reduce: function (big, little, key) {
        return JSON.stringify(JSON.parse(big) + JSON.parse(little))
      }
    , initial: 0
  }).force()

  mr.on('reduce', mac(function (key, sum) {
    console.log('range', sum, key)
    assert.equal(JSON.parse(sum), (20 * 21) / 2)
  }).twice())
})
