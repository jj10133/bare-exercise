const test = require('brittle')
const tcp = require('bare-tcp')
const tcpCat = require('.')

// Starts a bare-tcp server bound to an ephemeral localhost port and
// unref'd so it never keeps the test process alive - we don't rely on
// server.close() completing.
function fixture(onConnection) {
  return new Promise((resolve) => {
    const server = tcp.createServer(onConnection)

    server.listen(0, '127.0.0.1', () => {
      server.unref()
      resolve(server.address().port)
    })
  })
}

test('connects, writes, and resolves with everything received until close', async (t) => {
  const port = await fixture((socket) => {
    socket.once('data', (chunk) => {
      socket.end('echo: ' + chunk.toString())
    })
  })

  const response = await tcpCat('127.0.0.1', port, 'ping')

  t.ok(Buffer.isBuffer(response), 'resolves with a Buffer')
  t.is(response.toString(), 'echo: ping')
})

test('accepts a Buffer as well as a string for the request payload', async (t) => {
  const port = await fixture((socket) => {
    socket.on('data', (chunk) => socket.end(chunk))
  })

  const response = await tcpCat('127.0.0.1', port, Buffer.from([1, 2, 3]))

  t.alike(response, Buffer.from([1, 2, 3]))
})

test('resolves with an empty Buffer when the peer closes without sending anything', async (t) => {
  const port = await fixture((socket) => socket.end())

  const response = await tcpCat('127.0.0.1', port, 'ping')

  t.ok(Buffer.isBuffer(response))
  t.is(response.length, 0)
})

test('rejects when the connection is refused', async (t) => {
  // Nothing is listening on this port.
  await t.exception(tcpCat('127.0.0.1', 1, 'ping'))
})

test('rejects on an invalid host', async (t) => {
  await t.exception(tcpCat('not-an-ip-address', 80, 'ping'))
})

test('throws synchronously on bad arguments', async (t) => {
  t.exception.all(() => tcpCat(123, 80, 'ping'), /host must be a non-empty string/)
  t.exception.all(() => tcpCat('127.0.0.1', -1, 'ping'), /port must be an integer/)
  t.exception.all(() => tcpCat('127.0.0.1', 80, 42), /data must be a string/)
})

test('rejects with a timeout error if nothing was ever received', async (t) => {
  const port = await fixture(() => {}) // accepts but never writes or closes

  await t.exception(tcpCat('127.0.0.1', port, 'ping', { timeout: 200 }), /Timed out/)
})

test('resolves with partial data if the timeout elapses after some bytes arrived', async (t) => {
  const port = await fixture((socket) => {
    socket.write('partial') // never closes
  })

  const response = await tcpCat('127.0.0.1', port, 'ping', { timeout: 200 })

  t.is(response.toString(), 'partial')
})

test('handles a response larger than the internal read chunk size', async (t) => {
  const large = Buffer.alloc(256 * 1024, 'x') // larger than the 64KB read buffer

  const port = await fixture((socket) => socket.end(large))

  const response = await tcpCat('127.0.0.1', port, 'ping')

  t.alike(response, large)
})
