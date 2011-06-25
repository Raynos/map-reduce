var mapR = require('map-reduce')
  , it = require('it-is')

exports ['emit.error'] = function (test){
  var err = new Error('this is the example error')
    mapR({
      on: [1,23,35,56,456,34534534,12,0,12,3,320],
      map: function (emit){
        emit.error(err)
      },
      done: check
    })
  
  function check(_err,results){
    it(_err).equal(err)

    test.done()
  }
}

exports ['throw Error'] = function (test){
  var err = new Error('this is the example error')
    mapR({
      on: [1,23,35,56,456,34534534,12,0,12,3,320],
      map: function (emit){
        throw err
      },
      done: check
    })
  
  function check(_err,results){
    it(_err).equal(err)

    test.done()
  }
}

exports ['throw Error async'] = function (test){
  var err = new Error('this is the example error')
    mapR({
      on: [1,23,35,56,456,34534534,12,0,12,3,320],
      map: function (emit){
        process.nextTick(emit.safe(function (){ throw err }))
      },
      done: check
    })
  
  function check(_err,results){
    it(_err).equal(err)
    test.done()
  }
}