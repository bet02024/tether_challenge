const Hypercore = require('hypercore');
const Hyperbee = require('hyperbee');
const path = require('path');

// Use a dedicated folder for the db
const dbPath = path.join(__dirname, 'datastorage', 'prices');
const core = new Hypercore(dbPath);
const bee = new Hyperbee(core, { keyEncoding: 'utf-8', valueEncoding: 'json' });

// Ensure ready before operations
async function ready() {
  if (core.ready && bee.ready) {
    await core.ready();
    await bee.ready();
  }
}

// Filter results array to only those matching pairs by id or symbol
function filterResultsToPairs(results, pairs = []) {
  if (!Array.isArray(pairs) || pairs.length === 0) return results;
  if (!Array.isArray(results)) return [];
  const set = new Set(pairs.map(s => s.toLowerCase()));
  return results.filter(
    (entry) =>
      (entry?.symbol && set.has(entry.symbol.toLowerCase())) ||
      (entry?.id && set.has(entry.id.toLowerCase()))
  );
}

// Save prices using timestamp as key
async function savePriceData({ results, lastUpdated }) {
  await ready();
  const key = lastUpdated || new Date().toISOString();
  await bee.put(key, results);
  return true;
}

async function dthSeed() {
    await ready();
  // resolved distributed hash table seed for key pair
  let dhtSeed = (await bee.get('dht-seed'))?.value;
  if (!dhtSeed) {
    // not found, generate and store in db
    dhtSeed = crypto.randomBytes(32);
    await bee.put('dht-seed', dhtSeed);
  }
  return dhtSeed;
}


async function rpcSeed() {
 await ready();
  // resolve rpc server seed for key pair
  let rpcSeed = (await bee.get('rpc-seed'))?.value
  if (!rpcSeed) {
    rpcSeed = crypto.randomBytes(32);
    await bee.put('rpc-seed', rpcSeed);
  }
  return rpcSeed;
}



// Get the most recent price record that matches pairs, showing only requested pairs in 'value'
async function getLatestPriceData(pairs = []) {
  await ready();
  const rs = bee.createReadStream({ reverse: true });
  for await (const { key, value } of rs) {
    const filtered = filterResultsToPairs(value, pairs);
    if (filtered.length > 0) {
      return { key, value: filtered };
    }
    if (pairs.length === 0 && value) {
      return { key, value };
    }
  }
  return null;
}

// Get all records within a time range (start, end in ms, inclusive) and filter to only show requested pairs
async function getPriceDataInRange(start, end, pairs = []) {
  await ready();
  const startIso = new Date(Number(start)).toISOString();
  const endIso = new Date(Number(end)).toISOString();
  const rs = bee.createReadStream({ gte: startIso, lte: endIso });
  const results = [];
  for await (const { key, value } of rs) {
    const filtered = filterResultsToPairs(value, pairs);
    if (filtered.length > 0) {
      results.push({ key, value: filtered });
    }
    if (pairs.length === 0 && value) {
      results.push({ key, value });
    }
  }
  return results;
}

module.exports = {
  savePriceData,
  getLatestPriceData,
  getPriceDataInRange,
  rpcSeed,
  dthSeed,
};
