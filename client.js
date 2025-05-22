'use strict'

const RPC = require('@hyperswarm/rpc')
const DHT = require('hyperdht')
const Hypercore = require('hypercore')
const Hyperbee = require('hyperbee')
const crypto = require('crypto')




const main = async () => {
  // hyperbee db
  const hcore = new Hypercore('./db/rpc-client')
  const hbee = new Hyperbee(hcore, { keyEncoding: 'utf-8', valueEncoding: 'binary' })
  await hbee.ready()

  // resolved distributed hash table seed for key pair
  let dhtSeed = (await hbee.get('dht-seed'))?.value
  if (!dhtSeed) {
    // not found, generate and store in db
    dhtSeed = crypto.randomBytes(32)
    await hbee.put('dht-seed', dhtSeed)
  }

  // start distributed hash table, it is used for rpc service discovery
  const dht = new DHT({
    port: 50001,
    keyPair: DHT.keyPair(dhtSeed),
    bootstrap: [{ host: '127.0.0.1', port: 30001 }] // note boostrap points to dht that is started via cli
  })
  await dht.ready()

  // public key of rpc server, used instead of address, the address is discovered via dht
  const serverPubKey = Buffer.from('6fd1075d1ee417f01fcfcafc601ec43bbcf486bf87757b02c0ec8078d806f5b6', 'hex')

  // rpc lib
  const rpc = new RPC({ dht })

  // 1. Trigger fetchPricesPipeline
  console.log('Calling fetchPricesPipeline on server...');
  let raw = await rpc.request(serverPubKey, 'fetchPricesPipeline', Buffer.from('{}','utf-8'));
  let response = JSON.parse(raw.toString('utf-8'));
  console.log('fetchPricesPipeline result:', response);

  // 2. Get latest for btc and eth
  console.log('Calling getLatestPriceData for [btc, eth]...');
  raw = await rpc.request(serverPubKey, 'getLatestPriceData', Buffer.from(JSON.stringify({ pairs: ['btc','eth'] }), 'utf-8'));
  response = JSON.parse(raw.toString('utf-8'));
  console.log('getLatestPriceData:', response);

  // 3. Get price history for btc in the last 24h
  const now = Date.now();
  const oneDayAgo = now - 24*60*60*1000;
  console.log('Calling getPriceDataInRange for [btc] in last 24h...');
  raw = await rpc.request(
    serverPubKey,
    'getPriceDataInRange',
    Buffer.from(JSON.stringify({start: oneDayAgo, end: now, pairs:['btc']}),'utf-8')
  );
  response = JSON.parse(raw.toString('utf-8'));
  console.log('getPriceDataInRange:', response);


  // closing connection
  await rpc.destroy()
  await dht.destroy()
}

main().catch(console.error)