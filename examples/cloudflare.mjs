import tcpCat from '../index.js'

const response = await tcpCat('1.1.1.1', 80, 'GET / HTTP/1.1\r\nHost: cloudflare.com\r\nConnection: close\r\n\r\n')
console.log(response.toString())
