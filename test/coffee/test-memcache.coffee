#
# tests for nodeunit
#

util     = require('util')
memcache = require('../lib/memcache.js')
port     = 11211;
host     = "127.0.0.1"


num_keys = (obj) ->
  count = 0
  for k of obj
    count++
  return count



mc = null

exports.t = {

  setUp: (callback) ->
    if mc?.conn?
      callback()
      return

    mc = new memcache.Client(port, host);
    mc.on 'error', (e) ->

      if e.errno == 111
        throw "You need to have a memcache server running on localhost:11211 for these tests to run"
        return

      throw "Unexpected error during connection: " + util.inspect(e)
      return

    mc.connect()
    mc.addHandler ->
      # console.log("\nconnected!")
      callback()
      return
    return

  tearDown: (callback) ->
    mc.close()
    # console.log("\nclosed!")
    callback()
    return

  tests: {

    # test nonexistent key is null
    'test null value': (test) ->
      test.expect(2)
      mc.get 'no_such_key', (err, r) ->
        test.equal(null, err);
        test.equal(null, r);
        test.done()
      return

    'test multi null value': (test) ->
      test.expect(2)
      mc.get 'no such key', (err, r) ->
        test.equal(null, err);
        test.equal(num_keys(r), 0);
        test.done()
      return


    # test set, get and expires
    'test set, get, and expires': (test) ->
      test.expect(4)

      # set key
      cb = ->
        mc.get 'set1', (err, r) ->
          # test key is found
          test.equal(null, err);
          test.equal('asdf1', r)

          # test key expires after 1 sec
          test_after_time = ->
            mc.get 'set1', (err, r) ->
              test.equal(null, err);
              test.equal(null, r);
              test.done()
              return
            return

          setTimeout(test_after_time, 4000)
          return
        return

      # set for 1 second
      mc.set('set1', 'asdf1', cb, 1);
      return

    'test set get with integer value': (test) ->
      test.expect(2)

      mc.set 'testKey', 123, ->
        mc.get 'testKey', (err, r) ->
          test.equal(null,err)
          test.equal(123,r)
          test.done()
          return
        return
      return


    # test set and delete
    'test set del': (test) ->
      test.expect(4)

      # set key
      cb = ->
        mc.get 'set2', (err, r) ->
          # test key is found
          test.equal(null, err)
          test.equal('asdf2', r)

          # delete key
          mc.delete 'set2', ->
            mc.get 'set2', (err, r) ->
              # test key is null
              test.equal(null, err)
              test.equal(null, r)
              test.done()
              return
        return

      mc.set('set2', 'asdf2', cb, 0);
      return

    # test utf8 handling
    'utf8': (test) ->
      test.expect(1)

      mc.set 'key1', 'привет', ->
        mc.get 'key1', (err, r) ->
          test.equal('привет', r)
          mc.delete 'key1', ->
            test.done()
          return
        return
      return


    # test connecting and disconnecting
    'con disco': (test) ->
      test.expect(2)

      mc2 = new memcache.Client(port, host)

      mc2.on 'connect', ->
        test.ok(true)
        mc2.close();
        return

      mc2.on 'close', ->
        test.ok(true)
        test.done()
        return

      mc2.connect();
      return


    # increment / decrement
    'inc dec': (test) ->
      test.expect(14)

      mc.set 'inc_bad', 'HELLO', (err, response) ->
        test.equal(response, 'STORED');

        mc.increment 'inc_bad', 2, (err, ok) ->
          test.ok(/^CLIENT_ERROR/.test(err))
          test.equal(ok, null);

          mc.decrement 'inc_bad', 3, (err, ok) ->
            test.ok(/^CLIENT_ERROR/.test(err))
            test.equal(ok, null);

            mc.increment 'inc_bad', null, (err, ok) ->
              test.ok(/^CLIENT_ERROR/.test(err))
              test.equal(ok, null);

              mc.decrement 'inc_bad', null, (err, ok) ->
                test.ok(/^CLIENT_ERROR/.test(err))
                test.equal(ok, null);

                mc.set 'inc_good', '5', (err, response) ->
                  test.equal(response, 'STORED');

                  mc.increment 'inc_good', 2, (err, response) ->
                    test.equal(response, 7);

                    mc.increment 'inc_good', (err, response) ->
                      test.equal(response, 8);

                      mc.decrement 'inc_good', (err, response) ->
                        test.equal(response, 7);

                        mc.decrement 'inc_good', 4, (err, response) ->
                          test.equal(response, 3);
                          test.done()
                          return
      return


    'version': (test) ->
      test.expect(2)

      mc.version (error, success) ->
        test.equal(error, null);
        test.equal(success.length, 5);
        test.done()
        return
      return

    'stats': (test) ->
      test.expect(6)

      mc.stats (error, success) ->
        test.ok(success.pid, "server has a pid");

        mc.stats 'settings', (error, success) ->
          test.ok(success.maxconns)

          mc.stats 'items', (error, success) ->
            test.ok(num_keys(success) > 0)

            mc.stats 'sizes', (error, success) ->
              test.ok(num_keys(success) > 0)

              mc.stats 'slabs', (error, success) ->
                test.ok(num_keys(success) > 0)

                mc.stats 'notreal', (error, success) ->
                  test.equal(error, 'ERROR')
                  test.done()
                  return
      return

  }
}