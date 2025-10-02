"""
Enhanced Cryptocurrency Market Data API
Provides real-time market data from multiple exchanges
"""

import asyncio
import uvicorn
import datetime
from typing import Optional, List, Dict
from fastapi import FastAPI, HTTPException, Query
from fastapi.responses import JSONResponse
from pydantic import BaseModel, Field

from krakenWebSocket import KrakenWebSocket
from binanceWebSocket import BinanceWebsocket
from kuCoinWebSocket import KuCoinWebsocket
from huobiWebSocket import HuobiWebSocket

# API Metadata
app = FastAPI(
    title="Crypto Market Data API",
    description="""
    ## Real-time Cryptocurrency Market Data API
    
    This API aggregates real-time market data from multiple cryptocurrency exchanges:
    - **Binance** - World's largest crypto exchange
    - **Kraken** - Trusted and secure platform
    - **KuCoin** - Wide variety of trading pairs
    - **Huobi** - Global digital asset exchange
    
    ### Features
    - Real-time price feeds via WebSocket connections
    - Aggregated market data across exchanges
    - Price comparison and arbitrage opportunities
    - Historical snapshot data
    - Market statistics and analytics
    
    ### Rate Limits
    No rate limits currently enforced for testing phase.
    """,
    version="2.0.0",
    contact={
        "name": "API Support",
        "email": "support@example.com",
    },
    license_info={
        "name": "MIT",
    }
)

# Data Models
class PriceData(BaseModel):
    ask: float = Field(..., description="Best ask price")
    bid: float = Field(..., description="Best bid price")
    average: float = Field(..., description="Average of ask and bid")
    spread: float = Field(..., description="Spread between ask and bid")
    spread_percentage: float = Field(..., description="Spread as percentage")
    timestamp: int = Field(..., description="Unix timestamp in milliseconds")

class MarketDepth(BaseModel):
    pair: str
    exchange: str
    ask: float
    bid: float
    spread: float
    volume_24h: Optional[float] = None

class ArbitrageOpportunity(BaseModel):
    pair: str
    buy_exchange: str
    sell_exchange: str
    buy_price: float
    sell_price: float
    profit_percentage: float
    timestamp: int

# Exchange Configuration
exchanges = {
    'kraken': KrakenWebSocket,
    'binance': BinanceWebsocket,
    'kucoin': KuCoinWebsocket,
    'huobi': HuobiWebSocket
}

exchanges_data = {
    'kraken': {'assets': {}, 'price': {}, 'last_update': 0}, 
    'binance': {'assets': {}, 'price': {}, 'last_update': 0},
    'kucoin': {'assets': {}, 'price': {}, 'last_update': 0},
    'huobi': {'assets': {}, 'price': {}, 'last_update': 0}
}

# Initialize WebSocket connections
ws_connections = {
    'kraken': KrakenWebSocket(exchanges_data['kraken']),
    'binance': BinanceWebsocket(exchanges_data['binance']),
    'kucoin': KuCoinWebsocket(exchanges_data['kucoin']),
    'huobi': HuobiWebSocket(exchanges_data['huobi'])
}

@app.on_event('startup')
async def startup_event():
    """Initialize WebSocket connections to all exchanges"""
    for name, ws in ws_connections.items():
        asyncio.create_task(ws.connect())

# ==================== CORE ENDPOINTS ====================

@app.get('/', tags=["General"])
async def root():
    """
    API Health Check and Information
    
    Returns the current status of the API and available endpoints.
    """
    return {
        'status': 'online',
        'version': '2.0.0',
        'timestamp': get_timestamp(),
        'exchanges': list(exchanges_data.keys()),
        'total_pairs': sum(len(e['assets']) for e in exchanges_data.values()),
        'documentation': '/docs',
        'openapi': '/openapi.json'
    }

@app.get('/health', tags=["General"])
async def health_check():
    """
    Detailed Health Check
    
    Returns connection status for each exchange and data freshness.
    """
    health = {}
    current_time = get_timestamp()
    
    for exchange, data in exchanges_data.items():
        last_update = data.get('last_update', 0)
        age_seconds = (current_time - last_update) / 1000 if last_update else None
        
        health[exchange] = {
            'connected': last_update > 0,
            'pairs_count': len(data.get('assets', {})),
            'prices_count': len(data.get('price', {})),
            'last_update': last_update,
            'age_seconds': round(age_seconds, 2) if age_seconds else None,
            'status': 'healthy' if age_seconds and age_seconds < 60 else 'stale' if age_seconds else 'disconnected'
        }
    
    return {
        'timestamp': current_time,
        'overall_status': 'healthy' if all(h['connected'] for h in health.values()) else 'degraded',
        'exchanges': health
    }

# ==================== PRICE ENDPOINTS ====================

@app.get('/price/{pair}', tags=["Prices"])
async def get_pair_price(
    pair: str = Query(..., description="Trading pair symbol (e.g., BTCUSDT, ETHUSDT)"),
    exchange: Optional[str] = Query(None, description="Specific exchange (kraken, binance, kucoin, huobi)"),
    include_spread: bool = Query(True, description="Include spread calculations")
):
    """
    Get Current Price for a Trading Pair
    
    Returns real-time price data for the specified trading pair across one or all exchanges.
    """
    pair = pair.upper()
    result = {}
    
    exchanges_to_check = [exchange.lower()] if exchange else exchanges_data.keys()
    
    for exch in exchanges_to_check:
        if exch not in exchanges_data:
            continue
            
        price_data = exchanges_data[exch].get('price', {})
        if pair in price_data:
            data = price_data[pair]
            ask = float(data.get('ask', 0))
            bid = float(data.get('bid', 0))
            spread = ask - bid
            
            result[exch] = {
                'ask': ask,
                'bid': bid,
                'average': float(data.get('average', (ask + bid) / 2)),
                'spread': round(spread, 8) if include_spread else None,
                'spread_percentage': round((spread / ask * 100), 4) if include_spread and ask > 0 else None,
                'timestamp': data.get('tss', data.get('ts', get_timestamp()))
            }
    
    if not result:
        raise HTTPException(status_code=404, detail=f"Pair {pair} not found on specified exchange(s)")
    
    return {
        'pair': pair,
        'timestamp': get_timestamp(),
        'data': result
    }

@app.get('/prices/all', tags=["Prices"])
async def get_all_prices(
    exchange: Optional[str] = Query(None, description="Filter by exchange"),
    limit: int = Query(100, ge=1, le=1000, description="Maximum number of pairs to return")
):
    """
    Get All Current Prices
    
    Returns current prices for all trading pairs, optionally filtered by exchange.
    """
    result = {}
    
    exchanges_to_check = [exchange.lower()] if exchange else exchanges_data.keys()
    
    for exch in exchanges_to_check:
        if exch not in exchanges_data:
            continue
        
        prices = exchanges_data[exch].get('price', {})
        result[exch] = dict(list(prices.items())[:limit])
    
    return {
        'timestamp': get_timestamp(),
        'total_pairs': sum(len(v) for v in result.values()),
        'data': result
    }

@app.get('/prices/compare/{pair}', tags=["Prices"])
async def compare_prices(
    pair: str = Query(..., description="Trading pair to compare"),
    min_exchanges: int = Query(2, ge=2, le=4, description="Minimum exchanges required")
):
    """
    Compare Prices Across Exchanges
    
    Returns price comparison data showing best bid/ask across all exchanges.
    """
    pair = pair.upper()
    prices = {}
    
    for exch, data in exchanges_data.items():
        if pair in data.get('price', {}):
            price_data = data['price'][pair]
            prices[exch] = {
                'ask': float(price_data.get('ask', 0)),
                'bid': float(price_data.get('bid', 0)),
                'timestamp': price_data.get('tss', price_data.get('ts', 0))
            }
    
    if len(prices) < min_exchanges:
        raise HTTPException(
            status_code=404, 
            detail=f"Pair {pair} found on only {len(prices)} exchange(s), minimum {min_exchanges} required"
        )
    
    # Find best prices
    best_ask = min(prices.items(), key=lambda x: x[1]['ask'])
    best_bid = max(prices.items(), key=lambda x: x[1]['bid'])
    
    return {
        'pair': pair,
        'timestamp': get_timestamp(),
        'exchanges_count': len(prices),
        'best_ask': {
            'exchange': best_ask[0],
            'price': best_ask[1]['ask']
        },
        'best_bid': {
            'exchange': best_bid[0],
            'price': best_bid[1]['bid']
        },
        'all_prices': prices
    }

# ==================== ARBITRAGE ENDPOINTS ====================

@app.get('/arbitrage/opportunities', tags=["Arbitrage"])
async def find_arbitrage_opportunities(
    min_profit: float = Query(0.5, ge=0, description="Minimum profit percentage"),
    limit: int = Query(50, ge=1, le=200, description="Maximum opportunities to return")
):
    """
    Find Arbitrage Opportunities
    
    Identifies price differences across exchanges that could be exploited for profit.
    """
    opportunities = []
    checked_pairs = set()
    
    # Collect all unique pairs
    for exch, data in exchanges_data.items():
        checked_pairs.update(data.get('price', {}).keys())
    
    for pair in checked_pairs:
        exchange_prices = {}
        
        # Collect prices for this pair across exchanges
        for exch, data in exchanges_data.items():
            if pair in data.get('price', {}):
                price_data = data['price'][pair]
                exchange_prices[exch] = {
                    'ask': float(price_data.get('ask', 0)),
                    'bid': float(price_data.get('bid', 0))
                }
        
        if len(exchange_prices) < 2:
            continue
        
        # Find arbitrage opportunities
        for buy_exch, buy_data in exchange_prices.items():
            for sell_exch, sell_data in exchange_prices.items():
                if buy_exch == sell_exch:
                    continue
                
                buy_price = buy_data['ask']
                sell_price = sell_data['bid']
                
                if buy_price > 0:
                    profit_pct = ((sell_price - buy_price) / buy_price) * 100
                    
                    if profit_pct >= min_profit:
                        opportunities.append({
                            'pair': pair,
                            'buy_exchange': buy_exch,
                            'sell_exchange': sell_exch,
                            'buy_price': buy_price,
                            'sell_price': sell_price,
                            'profit_percentage': round(profit_pct, 4),
                            'timestamp': get_timestamp()
                        })
    
    # Sort by profit percentage
    opportunities.sort(key=lambda x: x['profit_percentage'], reverse=True)
    
    return {
        'timestamp': get_timestamp(),
        'opportunities_found': len(opportunities[:limit]),
        'total_checked': len(checked_pairs),
        'data': opportunities[:limit]
    }

@app.get('/arbitrage/{pair}', tags=["Arbitrage"])
async def get_pair_arbitrage(pair: str):
    """
    Get Arbitrage Data for Specific Pair
    
    Returns detailed arbitrage analysis for a single trading pair.
    """
    pair = pair.upper()
    exchange_prices = {}
    
    for exch, data in exchanges_data.items():
        if pair in data.get('price', {}):
            price_data = data['price'][pair]
            exchange_prices[exch] = {
                'ask': float(price_data.get('ask', 0)),
                'bid': float(price_data.get('bid', 0))
            }
    
    if len(exchange_prices) < 2:
        raise HTTPException(status_code=404, detail=f"Pair {pair} must be on at least 2 exchanges")
    
    opportunities = []
    for buy_exch, buy_data in exchange_prices.items():
        for sell_exch, sell_data in exchange_prices.items():
            if buy_exch != sell_exch and buy_data['ask'] > 0:
                profit_pct = ((sell_data['bid'] - buy_data['ask']) / buy_data['ask']) * 100
                opportunities.append({
                    'buy_exchange': buy_exch,
                    'sell_exchange': sell_exch,
                    'buy_price': buy_data['ask'],
                    'sell_price': sell_data['bid'],
                    'profit_percentage': round(profit_pct, 4)
                })
    
    opportunities.sort(key=lambda x: x['profit_percentage'], reverse=True)
    
    return {
        'pair': pair,
        'timestamp': get_timestamp(),
        'exchanges': list(exchange_prices.keys()),
        'best_opportunity': opportunities[0] if opportunities else None,
        'all_opportunities': opportunities
    }

# ==================== ASSETS ENDPOINTS ====================

@app.get('/assets', tags=["Assets"])
async def get_assets(
    exchange: Optional[str] = Query(None, description="Filter by exchange"),
    search: Optional[str] = Query(None, description="Search for specific asset")
):
    """
    Get Available Trading Assets
    
    Returns list of all available trading pairs across exchanges.
    """
    result = {}
    
    exchanges_to_check = [exchange.lower()] if exchange else exchanges_data.keys()
    
    for exch in exchanges_to_check:
        if exch not in exchanges_data:
            continue
        
        assets = exchanges_data[exch].get('assets', {})
        
        if search:
            search_upper = search.upper()
            filtered = {k: v for k, v in assets.items() if search_upper in k.upper()}
            result[exch] = filtered
        else:
            result[exch] = assets
    
    return {
        'timestamp': get_timestamp(),
        'total_assets': sum(len(v) for v in result.values()),
        'data': result
    }

@app.get('/assets/common', tags=["Assets"])
async def get_common_assets():
    """
    Get Common Assets Across Exchanges
    
    Returns trading pairs that are available on multiple exchanges.
    """
    exchange_pairs = {}
    
    for exch, data in exchanges_data.items():
        pairs = set(data.get('assets', {}).keys())
        exchange_pairs[exch] = pairs
    
    if len(exchange_pairs) < 2:
        return {'timestamp': get_timestamp(), 'common_pairs': [], 'count': 0}
    
    # Find intersection
    common = set.intersection(*exchange_pairs.values())
    
    return {
        'timestamp': get_timestamp(),
        'total_exchanges': len(exchange_pairs),
        'common_pairs_count': len(common),
        'common_pairs': sorted(list(common))[:100],  # Limit for readability
        'available_on': {pair: [exch for exch, pairs in exchange_pairs.items() if pair in pairs] 
                        for pair in sorted(list(common))[:100]}
    }

# ==================== STATISTICS ENDPOINTS ====================

@app.get('/statistics/exchange/{exchange}', tags=["Statistics"])
async def get_exchange_statistics(exchange: str):
    """
    Get Exchange Statistics
    
    Returns statistical information about a specific exchange.
    """
    exchange = exchange.lower()
    
    if exchange not in exchanges_data:
        raise HTTPException(status_code=404, detail=f"Exchange {exchange} not found")
    
    data = exchanges_data[exchange]
    prices = data.get('price', {})
    
    if not prices:
        return {
            'exchange': exchange,
            'status': 'no_data',
            'timestamp': get_timestamp()
        }
    
    # Calculate statistics
    spreads = []
    for pair, price_data in prices.items():
        ask = float(price_data.get('ask', 0))
        bid = float(price_data.get('bid', 0))
        if ask > 0:
            spread_pct = ((ask - bid) / ask) * 100
            spreads.append(spread_pct)
    
    return {
        'exchange': exchange,
        'timestamp': get_timestamp(),
        'total_pairs': len(data.get('assets', {})),
        'active_prices': len(prices),
        'last_update': data.get('last_update', 0),
        'average_spread_percentage': round(sum(spreads) / len(spreads), 4) if spreads else 0,
        'min_spread_percentage': round(min(spreads), 4) if spreads else 0,
        'max_spread_percentage': round(max(spreads), 4) if spreads else 0
    }

@app.get('/statistics/overview', tags=["Statistics"])
async def get_overview_statistics():
    """
    Get Overall Market Statistics
    
    Returns aggregated statistics across all exchanges.
    """
    stats = {
        'timestamp': get_timestamp(),
        'exchanges': {},
        'totals': {
            'unique_pairs': set(),
            'total_prices': 0,
            'connected_exchanges': 0
        }
    }
    
    for exch, data in exchanges_data.items():
        assets = data.get('assets', {})
        prices = data.get('price', {})
        last_update = data.get('last_update', 0)
        
        stats['exchanges'][exch] = {
            'pairs': len(assets),
            'prices': len(prices),
            'last_update': last_update,
            'connected': last_update > 0
        }
        
        stats['totals']['unique_pairs'].update(assets.keys())
        stats['totals']['total_prices'] += len(prices)
        if last_update > 0:
            stats['totals']['connected_exchanges'] += 1
    
    stats['totals']['unique_pairs'] = len(stats['totals']['unique_pairs'])
    
    return stats

# ==================== MARKET DEPTH ====================

@app.get('/depth/{pair}', tags=["Market Depth"])
async def get_market_depth(pair: str):
    """
    Get Market Depth for Trading Pair
    
    Returns bid/ask spread and depth information across exchanges.
    """
    pair = pair.upper()
    depth_data = []
    
    for exch, data in exchanges_data.items():
        if pair in data.get('price', {}):
            price_data = data['price'][pair]
            ask = float(price_data.get('ask', 0))
            bid = float(price_data.get('bid', 0))
            
            depth_data.append({
                'exchange': exch,
                'ask': ask,
                'bid': bid,
                'spread': ask - bid,
                'spread_percentage': round(((ask - bid) / ask * 100), 4) if ask > 0 else 0,
                'timestamp': price_data.get('tss', price_data.get('ts', get_timestamp()))
            })
    
    if not depth_data:
        raise HTTPException(status_code=404, detail=f"Pair {pair} not found")
    
    return {
        'pair': pair,
        'timestamp': get_timestamp(),
        'exchanges_count': len(depth_data),
        'depth': depth_data
    }

# ==================== HELPER FUNCTIONS ====================

def get_timestamp() -> int:
    """Get current Unix timestamp in milliseconds"""
    return int(datetime.datetime.now().timestamp() * 1000)

# ==================== RUN SERVER ====================

if __name__ == '__main__':
    uvicorn.run(app, host="0.0.0.0", port=8000)
