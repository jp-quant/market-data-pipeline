# Level2 Channel Batching

## Problem
Coinbase's `level2` channel has stricter subscription limits on the number of `product_ids` per subscription compared to other channels like `ticker` and `market_trades`.

With 100+ products, subscribing all at once causes errors:
```
Subscription error for level2: too many product_ids
```

Meanwhile, `ticker` and `market_trades` work fine with the same product list.

## Solution
**Channel-specific product batching**: Split `product_ids` into smaller batches only for `level2` subscriptions.

### Implementation

#### 1. Configuration (`config/config.yaml`)
```yaml
coinbase:
  product_ids:
    - "BTC-USD"
    - "ETH-USD"
    # ... 100+ products
  channels:
    - "ticker"
    - "level2"  # Now works with batching!
    - "market_trades"
  level2_batch_size: 10  # Max products per level2 subscription
```

#### 2. Collector Logic (`ingestion/collectors/coinbase_ws.py`)

**Constructor**: Added `level2_batch_size` parameter (default: 10)

**Subscription Flow**:
```python
for channel in self.channels:
    if channel == "level2":
        # Batch level2 subscriptions
        await self._subscribe_level2_batched(websocket, jwt_token)
    else:
        # Single subscription for other channels
        await self._subscribe_channel(websocket, channel, self.product_ids, jwt_token)
```

**Batching Helper**:
```python
async def _subscribe_level2_batched(self, websocket, jwt_token):
    """
    Split product_ids into batches for level2 subscriptions.
    
    Example with 100 products and batch_size=10:
    - Sends 10 separate subscribe messages
    - Each with 10 products
    - 0.5s delay between batches to avoid rate limiting
    """
    for i in range(0, len(self.product_ids), self.level2_batch_size):
        batch = self.product_ids[i:i + self.level2_batch_size]
        await self._subscribe_channel(websocket, "level2", batch, jwt_token)
        
        # Rate limiting delay
        if i + self.level2_batch_size < len(self.product_ids):
            await asyncio.sleep(0.5)
```

### Benefits
1. **Scalable**: Works with 100+ products without subscription errors
2. **Channel-specific**: Only batches where needed (level2), keeps other channels simple
3. **Configurable**: Adjust `level2_batch_size` if Coinbase changes limits
4. **Production-ready**: Includes rate limiting delays, error logging, batch tracking

### Testing

Enable level2 in config:
```yaml
coinbase:
  channels:
    - "ticker"
    - "level2"  # Uncomment to enable
    - "market_trades"
```

Run ingestion:
```bash
python scripts/run_ingestion.py
```

Expected logs:
```
[CoinbaseCollector] Subscribing to level2 batch 1/10: 10 products: ['BTC-USD', 'ETH-USD', ...]
[CoinbaseCollector] Subscribe response for level2: type=subscriptions, channel=l2_data
[CoinbaseCollector] Subscribing to level2 batch 2/10: 10 products: ['SOL-USD', 'XRP-USD', ...]
...
```

### Tuning

If you still get errors with `level2_batch_size: 10`, reduce it:
```yaml
level2_batch_size: 5  # More conservative
```

If Coinbase increases limits, you can raise it:
```yaml
level2_batch_size: 20  # Fewer subscription messages
```

### Notes
- Message size limit (10MB) remains separate from subscription batching
- Each batch gets its own subscription confirmation
- Level2 data from all batches flows through same message loop
- Parser handles all batches seamlessly (no changes needed)
