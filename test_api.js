const express = require('express');
const priceService = require('./priceService');
const priceStorage = require('./priceStorage');

const app = express();
const PORT = process.env.PORT || 4000;


// TEST API For priceService & priceStorage methods
// Start scheduler as soon as server starts
priceService.startScheduler();

app.get('/fetch-now', async (req, res) => {
  const result = await priceService.fetchPricesPipeline();
  if (result.success) {
    res.json({ status: 'fetched', time: result.lastUpdated, data: result.results });
  } else {
    res.status(500).json({ error: result.error });
  }
});

// Parse pairs param "btc,eth,ltc" to ["btc","eth","ltc"]
function parsePairs(queryValue) {
  if (!queryValue) return [];
  return queryValue.split(',').map(s => s.trim()).filter(Boolean);
}


//GET /latest?pairs=btc,eth
app.get('/latest', async (req, res) => {
  try {
    const pairs = parsePairs(req.query.pairs);
    const dbResult = await priceStorage.getLatestPriceData( pairs);
    if (dbResult && dbResult.value) {
      res.json({ status: 'latest', time: dbResult.key, data: dbResult.value });
    } else {
      res.status(503).json({ error: 'No matching data yet in database.' });
    }
  } catch (e) {
    res.status(500).json({ error: 'Database error', detail: e.message });
  }
});

//GET /history?start=1716403200000&end=1719008000000&pairs=btc,eth
app.get('/history', async (req, res) => {
  const { start, end, pairs: pairsParam } = req.query;
  if (!start || !end) {
    return res.status(400).json({ error: 'Provide both start and end (milliseconds) as query params.' });
  }
  // Parse start/end as numbers (milliseconds)
  const startNum = Number(start);
  const endNum = Number(end);
  if (isNaN(startNum) || isNaN(endNum)) {
    return res.status(400).json({ error: 'Start and end must be numbers (milliseconds since epoch).' });
  }
  const pairs = parsePairs(pairsParam);
  try {
    const history = await priceStorage.getPriceDataInRange(startNum, endNum, pairs);
    res.json({ status: 'history', count: history.length, data: history });
  } catch (e) {
    res.status(500).json({ error: 'Database error', detail: e.message });
  }
});

app.get('/', (req, res) => {
  res.send('Use /fetch-now, /latest[?pairs=btc,eth], or /history?start=...&end=...[&pairs=btc,eth], dates in milliseconds');
});

app.listen(PORT, () => {
  console.log(`API server running on http://localhost:${PORT}`);
});
