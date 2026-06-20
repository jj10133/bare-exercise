import tcpCat from '../index.js'

const response = await tcpCat('1.1.1.1', 80, 'GET / HTTP/1.1\r\n\r\nHost: cloudflare.com\r\n')
console.log(response.toString())
