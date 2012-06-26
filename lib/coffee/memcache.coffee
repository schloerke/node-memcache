#
#  Copyright (c) 2011 Tim Eggert
#
#  Permission is hereby granted, free of charge, to any person obtaining a copy
#  of this software and associated documentation files (the "Software"), to deal
#  in the Software without restriction, including without limitation the rights
#  to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
#  copies of the Software, and to permit persons to whom the Software is
#  furnished to do so, subject to the following conditions:
#
#  The above copyright notice and this permission notice shall be included in
#  all copies or substantial portions of the Software.
#
#  THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
#  IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
#  FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
#  AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
#  LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
#  OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN
#  THE SOFTWARE.
#
#  @author Tim Eggert <tim@elbart.com>
#  @license http://www.opensource.org/licenses/mit-license.html MIT License
#

tcp = require('net')
util = require('util')

crlf     = "\r\n"
crlfLen = crlf.length

endTag     = 'END\r\n'
endTagLen = endTag.length

fullEndTag     = '\r\nEND\r\n'
fullEndTagLen = fullEndTag.length

valueTag = 'VALUE '

error_replies = ['ERROR', 'NOT_FOUND', 'CLIENT_ERROR', 'SERVER_ERROR']
errorRepliesMap = {
  ERROR       : true
  NOT_FOUND   : true
  CLIENT_ERROR: true
  SERVER_ERROR: true
}

isArray = Array.isArray or (obj) ->
  return toString.call(obj) is '[object Array]'


trim = ( ->
  trimLeft  = /^\s+/
  trimRight = /\s+$/
  return String.prototype.trim or (text) ->
    if text?
      return text.toString().replace( trimLeft, "" ).replace( trimRight, "" )
    else
      return ""
)()



Client = exports.Client = (port, host) ->
  this.port      = port or 11211
  this.host      = host or 'localhost'
  this.strBuffer = ''
  this.conn      = null
  this.sends     = 0
  this.replies   = 0
  this.callbacks = []
  this.handles   = []

  this.tmpMultiGetValue = {}

  return this


util.inherits(Client, process.EventEmitter)

Client.prototype.connect = ->

  if this.conn
    return

  self = this

  self.conn = new tcp.createConnection(self.port, self.host)
  self.conn.addListener "connect", ->
    self.conn.setTimeout(0)          # try to stay connected.
    self.conn.setNoDelay()
    self.emit("connect")
    self.dispatchHandles()
    return

  self.conn.addListener "data", (data) ->
    self.strBuffer += data
    # util.debug(data)
    self.recieves += 1
    self.handle_received_data()
    return

  self.conn.addListener "end", ->
    if self.conn?.readyState
      self.conn.end()
      self.conn = null
    return

  self.conn.addListener "close", ->
    self.conn = null
    self.emit("close")
    return

  self.conn.addListener "timeout", ->
    self.conn = null
    self.emit("timeout")
    return

  self.conn.addListener "error", (ex) ->
    self.conn = null
    self.emit("error", ex)
    return


Client.prototype.addHandler = (callback) ->
  this.handles.push(callback)

  if this.conn.readyState == 'open'
    this.dispatchHandles()
  return


Client.prototype.dispatchHandles = ->
  for handle in this.handles
    # util.debug('dispatching handle ' + handle)
    handle?()

  this.handles = []
  return


Client.prototype.query = (query, type, callback) ->
  this.callbacks.push({ type, fn: callback })
  this.sends++
  this.conn.write(query + crlf)


Client.prototype.close = ->
  if this.conn?.readyState is "open"
    this.conn.end()
    this.conn = null
  return


Client.prototype.get = (key, callback) ->
  # allow for multi get calls
  keyIsArray = false
  if isArray(key)
    keyIsArray = true

    # replace whitespace with length > 2 with single space
    key = key.join(' ').replace(/\s{2,}/, " ")
    key = trim.call(key)

  key = trim.call(key)

  if keyIsArray or (key.indexOf(" ") isnt -1)
    return this.query('get ' + key, 'get_multi', callback)
  else
    return this.query('get ' + key, 'get', callback)



# all of these store ops (everything bu "cas") have the same format
do_store_action = (action) ->
  return (key, value, callback, lifetime, flags) ->
    return this.store(action, key, value, callback, lifetime, flags)

Client.prototype.set     = do_store_action("set")
Client.prototype.add     = do_store_action("add")
Client.prototype.replace = do_store_action("replace")
Client.prototype.append  = do_store_action("append")
Client.prototype.prepend = do_store_action("prepend")


Client.prototype.store   = (cmd, key, value, callback, lifetime, flags) ->

  if (typeof callback) isnt 'function'
    lifetime = callback
    callback = null

  flags    or= 0
  lifetime or= 0

  valueStr = value.toString()
  valueLen = Buffer.byteLength(valueStr)
  query    = [cmd, key, flags, lifetime, valueLen]

  queryCmd = query.join(' ') + crlf + valueStr
  return this.query(queryCmd, 'simple', callback)


# "cas" is a store op that takes an extra "unique" argument
Client.prototype.cas = (key, value, unique, callback, lifetime, flags) ->

  if (typeof callback) isnt 'function'
    lifetime = callback
    callback = null

  flags    or= 0
  lifetime or= 0
  valueLen = value.length or 0
  query    = ['cas', key, flags, lifetime, valueLen, unique]

  queryCmd = query.join(' ') + crlf + value
  return this.query(queryCmd, 'simple', callback)


Client.prototype.del = (key, callback) ->
  util.error("mc.del() is deprecated - use mc.delete() instead")
  return this.delete(key, callback)


Client.prototype.delete = (key, callback) ->
  return this.query('delete ' + key, 'simple', callback)


Client.prototype.version = (callback) ->
  return this.query('version', 'version', callback)


incr_decr = (type) ->
  cmd = type + ' '
  return (key, value, callback) ->

    if (typeof value) is 'function'
      callback = value
      value    = 1

    value    ?= 1
    callback ?= null

    queryCmd = cmd + key + ' ' + value
    return this.query(queryCmd, 'simple', callback)


Client.prototype.increment = incr_decr("incr")
Client.prototype.decrement = incr_decr("decr")


Client.prototype.stats = (type, callback) ->

  if (typeof type) is 'function'
    callback = type
    type     = null

  queryCmd = "stats"
  if type
    queryCmd += " " + type

  return this.query(queryCmd, 'stats', callback)


Client.prototype.handle_received_data = ->

  while this.strBuffer.length > 0

    # console.error("while strBuffer: '" + this.strBuffer + "'")

    result = this.determine_reply_handler(this.strBuffer)
    # console.error("while result: ", result)

    # if it returned nothing... don't do anything
    unless result
      return

    [resultValue, nextResultAt, resultError, waitOnCallback] = result

    # chop the strBuffer
    this.strBuffer = this.strBuffer.substring(nextResultAt)

    if waitOnCallback is true
      return

    # pop off the first callback and call it
    callback = this.callbacks.shift()
    if callback?.fn
      this.replies++
      callback.fn(resultError, resultValue)

  return


Client.prototype.determine_reply_handler = (strBuffer) ->

  # all responses at least have one full line (including strBuffer)
  crlfPos = strBuffer.indexOf(crlf)
  if crlfPos is -1
    return null

  # determine errors
  cutPos = strBuffer.indexOf(' ')
  if cutPos < 0
    cutPos = crlfPos

  firstLine = strBuffer.substr(0, cutPos)
  if errorRepliesMap[firstLine]
    return this.handle_error(strBuffer)

  # call the handler for the current message type
  type = this.callbacks[0]?.type or null
  if type
    # console.error("handler type: ", type)
    return this['handle_' + type](strBuffer)

  return null




Client.prototype.handle_get = (strBuffer) ->

  # console.error("handle_get strBuffer: '" +  strBuffer + "'")
  # empty result... return early
  if strBuffer.indexOf(endTag) is 0
    return [null, endTagLen]

  # get the first line
  firstLineLen = strBuffer.indexOf(crlf) + crlfLen

  # find out how many bytes it is (4th item in array)
  expectedByteLen = strBuffer.substr(0, firstLineLen).split(' ')[3]
  expectedByteLen = parseInt(expectedByteLen, 10)

  # get a string that does not contain the first line
  noFirstLineDataStr = strBuffer.substr(firstLineLen)
  # console.error("handle_get noFirstLineDataStr: ", noFirstLineDataStr)

  # make a new buffer that allocates the expected byte length
  resultBuffer = new Buffer(expectedByteLen)

  # using the data string, write the expected amount of bytes to the buffer. should work out perfectly
  bytesWritten = resultBuffer.write(noFirstLineDataStr, 0, expectedByteLen)

  # console.error("handle bytes: ", bytesWritten, expectedByteLen)
  # if there isn't enough stuff, return early
  if bytesWritten < expectedByteLen
    return null

  # get it back to a string, so it can be used
  resultValueStr       = resultBuffer.toString()
  resultValueStrLength = resultValueStr.length

  # console.error("handle_get resultValueStr: '" + resultValueStr + "'")

  # does it finish with a full end tag?
  endPos = firstLineLen + resultValueStrLength
  # console.error(endPos, fullEndTagLen)
  # console.error("strBuffer.substr(endPos, fullEndTagLen): ", strBuffer.substr(endPos, fullEndTagLen))
  unless strBuffer.substr(endPos, fullEndTagLen) is fullEndTag
    # still need more information
    #   like the end tag... wait until next time
    return null

  return [resultValueStr, endPos + fullEndTagLen, null]




Client.prototype.handle_get_multi = (strBuffer) ->

  ret = this.tmpMultiGetValue

  collectedLength = 0

  while strBuffer.length > 0

    # found the end tag
    if strBuffer.indexOf(endTag) is 0
      collectedLength += endTagLen

      # found an END\r\f. Reset the multiGetValue to a blank {}
      this.tmpMultiGetValue = {}

      return [ret, collectedLength, null, false]

    # get the first line
    firstLineLen = strBuffer.indexOf(crlf) + crlfLen
    firstLine    = strBuffer.substr(0, firstLineLen)

    firstLineParts = firstLine.split(' ')

    # "VALUE"       = firstLineParts[0]
    key             = firstLineParts[1]
    # flags         = firstLineParts[2]
    expectedByteLen = firstLineParts[3]
    expectedByteLen = parseInt(expectedByteLen, 10)

    # get a string that does not contain the first line
    noFirstLineDataStr = strBuffer.substr(firstLineLen)

    # make a new buffer that allocates the expected byte length
    resultBuffer = new Buffer(expectedByteLen)

    # using the data string, write the expected amount of bytes to the buffer. should work out perfectly
    bytesWritten = resultBuffer.write(noFirstLineDataStr, 0, expectedByteLen)

    # if there isn't enough stuff, return early
    if bytesWritten < expectedByteLen
      # return early... only chop off the collected length and don't callback
      # no result, only collected length, no error, no callback
      return [null, collectedLength, null, true]

    # get it back to a string, so it can be used
    resultValueStr       = resultBuffer.toString()
    resultValueStrLength = resultValueStr.length

    # does it finish with a full end tag?
    endPos = firstLineLen + resultValueStrLength

    unless strBuffer.substr(endPos, crlfLen) is crlf
      # still need more information
      # no result, only collected length, no error, no callback
      return [null, collectedLength, null, true]

    ret[key] = resultValueStr
    collectedLength += endPos + crlfLen

  # end while loop


  # stuff didn't match nicely... return null
  # no result, only collected length, no error, no callback
  return [null, collectedLength, null, true]



boolKeys = {
  "hash_is_expanding"    : true
  "slab_reassign_running": true
}
stringKeys = {
  "version": true
}
Client.prototype.handle_stats = (strBuffer) ->

  # special case - no stats at all
  if strBuffer.indexOf(endTag) is 0
    return [{}, 5]


  # find the terminator
  idx = strBuffer.indexOf(fullEndTag)
  if idx is -1
    # wait for more data if we don't have an end yet
    return null


  # read the lines
  statsData = strBuffer.substr(0, idx + crlfLen) # read stats, including last crlf
  ret       = {}
  line      = null

  lines = statsData.split(crlf)
  for line in lines
    [header, key, value] = line.split(' ')
    continue unless header is "STAT"

    isBoolKey = boolKeys[key]
    isNumberKey = not stringKeys[key]
    if isBoolKey or isNumberKey
      value = parseFloat(value)
      if isBoolKey
        value = not (value is 0)

    ret[key] = value

  return [ret, idx + fullEndTagLen, null]



Client.prototype.handle_simple = (strBuffer) ->
  crlfPos = strBuffer.indexOf(crlf)
  if crlfPos is -1
    return null

  line = strBuffer.substr(0, crlfPos)
  return [line, (crlfPos + crlfLen), null]


Client.prototype.handle_version = (strBuffer) ->
  lineLen      = strBuffer.indexOf(crlf)
  if lineLen is -1
    return null

  value = strBuffer.substring('VERSION '.length, lineLen)
  return [value, lineLen + crlfLen, null]


Client.prototype.handle_error = (strBuffer) ->
  crlfPos = strBuffer.indexOf(crlf)
  if crlfPos is -1
    return null

  line = strBuffer.substr(0, crlfPos)
  return [null, (crlfPos + crlfLen), line]




