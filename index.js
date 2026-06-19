const binding = require('./binding')

/**
 * Connects to `host:port` over TCP, writes `data`, and resolves with
 * everything the remote sends back before it closes the connection.
 *
 * Encoding/validation lives here in JS; the native side only ever deals
 * with raw bytes and numbers.
 *
 * @param {string} host - IPv4 or IPv6 address to connect to
 * @param {number} port
 * @param {string|Buffer|Uint8Array} data - bytes to write once connected
 * @param {object} [opts]
 * @param {number} [opts.timeout=10000] - ms to wait for the remote to
 *   close the connection before giving up. If any data has already been
 *   received, it's returned as-is; otherwise the call rejects.
 * @returns {Promise<Buffer>}
 */

const DEFAULT_TIMEOUT = 10000 //ms

module.exports = function tcpCat(host, port, data, opts = {}) {
  if (typeof host !== 'string' || host.length === 0) {
    throw new TypeError('host must be a non-empty string')
  }

  if (!Number.isInteger(port) || port < 0 || port > 65535) {
    throw new RangeError('port must be an integer between 0 and 65535')
  }

  if (typeof data === 'string') data = Buffer.from(data)
  else if (!ArrayBuffer.isView(data)) {
    throw new TypeError('data must be a string, Buffer, or TypedArray')
  }

  const { timeout = DEFAULT_TIMEOUT } = opts

  if (!Number.isInteger(timeout) || timeout < 0) {
    throw new RangeError('timeout must be a non-negative integer')
  }

  return binding.tcpCat(host, port, data, timeout).then((arraybuffer) => Buffer.from(arraybuffer))
}
