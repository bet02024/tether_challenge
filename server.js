'use strict'

const RPC = require('@hyperswarm/rpc')
const DHT = require('hyperdht')
const Hypercore = require('hypercore')
const Hyperbee = require('hyperbee')
const crypto = require('crypto')
const cron = require('node-cron');
const priceStorage = require('./priceStorage');
const priceService = require('./priceService');



const main = async () => {
  // hyperbee db
  const hcore = new Hypercore('./db/rpc-server')
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
    port: 40001,
    keyPair: DHT.keyPair(dhtSeed),
    bootstrap: [{ host: '127.0.0.1', port: 30001 }] // note boostrap points to dht that is started via cli
  })
  await dht.ready()

  // resolve rpc server seed for key pair
  let rpcSeed = (await hbee.get('rpc-seed'))?.value
  if (!rpcSeed) {
    rpcSeed = crypto.randomBytes(32)
    await hbee.put('rpc-seed', rpcSeed)
  }

  // setup rpc server
  const rpc = new RPC({ seed: rpcSeed, dht })
  const rpcServer = rpc.createServer()
  await rpcServer.listen()
  console.log('rpc server started listening on public key:', rpcServer.publicKey.toString('hex'))

    // Expose latest price data for given pairs
    rpcServer.respond('getLatestPriceData', async (reqRaw) => {
      const req = JSON.parse(reqRaw.toString('utf-8'));
      const pairs = req.pairs || [];
      const result = await priceStorage.getLatestPriceData(pairs);
      return Buffer.from(JSON.stringify(result), 'utf-8');
    });
  
    // Expose fetchPricesPipeline
    rpcServer.respond('fetchPricesPipeline', async (_reqRaw) => {
      const result = await priceService.fetchPricesPipeline();
      return Buffer.from(JSON.stringify(result), 'utf-8');
    });
  
    // Expose historical price data for given range and pairs
    rpcServer.respond('getPriceDataInRange', async (reqRaw) => {
      const req = JSON.parse(reqRaw.toString('utf-8'));
      const { start, end, pairs = [] } = req;
      const s = Number(start);
      const e = Number(end);
      const rangeResults = await priceStorage.getPriceDataInRange(s, e, pairs);
      return Buffer.from(JSON.stringify(rangeResults), 'utf-8');
    });

}

// Schedule fetchPricesPipeline every 30 seconds
cron.schedule('*/30 * * * * *', async () => {
    try {
      console.log('Scheduled fetchPricesPipeline (server)...');
      await priceService.fetchPricesPipeline();
    } catch (err) {
      console.error('Error running fetchPricesPipeline in cron:', err);
    }
  });

main().catch(console.error)