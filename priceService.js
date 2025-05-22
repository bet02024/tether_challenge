require('dotenv').config();
const axios = require('axios');
const priceStorage = require('./priceStorage');
let latestResults = [];
let lastUpdated = null;

const axiosConfig = {
  headers: {
    'x-cg-demo-api-key': process.env['CG-API-KEY']
  }
};

  // Get top 5 coins by market cap
async function fetchTop5Coins() {
  const url = 'https://api.coingecko.com/api/v3/coins/markets?vs_currency=usd&order=market_cap_desc&per_page=5&page=1';
  const { data } = await axios.get(url, axiosConfig);
  return data.map(c => ({ id: c.id, symbol: c.symbol, name: c.name }));
}

// Get tickers for a specific coin
async function fetchCoinTickers(coinId) {
  const url = `https://api.coingecko.com/api/v3/coins/${coinId}/tickers`;
  const { data } = await axios.get(url, axiosConfig);
  return data.tickers;
}

async function getAveragePriceForCoin(coinId) {
  const tickers = await fetchCoinTickers(coinId);
  //Filter by USDt, and exclude the trust_score != green entries
  const usdtTickers = tickers.filter(t => t.target === 'USDT' && t.trust_score === 'green');
  // Sort by volume
  usdtTickers.sort((a, b) => {
    return (b.volume || 0) - (a.volume || 0);
  });
  // Pick the top 3 exchanges
  const top3 = usdtTickers.slice(0, 3);
  const prices = top3.map(t => t.last).filter(Number.isFinite);
  const exchanges = top3.map(t => t.market.name);
  // Compute average price
  const average = prices.length ? (prices.reduce((a, b) => a + b, 0) / prices.length) : null;
  return { average, exchanges, prices };
}



async function fetchPricesPipeline() {
  try {
    const coins = await fetchTop5Coins();
    const results = [];
    for (const coin of coins) {
      if (coin.id === "tether") {
        results.push({
            id: coin.id,
            symbol: coin.symbol,
            name: coin.name,
            average: 1.0,
            exchanges: [],
            prices: [1.0],
          });
      } else {
        const { average, exchanges, prices } = await getAveragePriceForCoin(coin.id);
        results.push({
            id: coin.id,
            symbol: coin.symbol,
            name: coin.name,
            average,
            exchanges,
            prices,
        });
      }
    }
    latestResults = results;
    lastUpdated = new Date().toISOString();
    // Save results to Hyperbee/Hypercore DB
    try {
      await priceStorage.savePriceData({ results, lastUpdated });
    } catch (storageErr) {
      console.error('Error saving to DB:', storageErr);
    }

    console.log(JSON.stringify(results))
    return { success: true, results, lastUpdated };
  } catch (e) {
    console.log(e);
    return { success: false, error: e.message };
  }
}

// Expose API
module.exports = {
  fetchPricesPipeline,
  getLatestResults: () => ({ results: latestResults, lastUpdated }),
};
