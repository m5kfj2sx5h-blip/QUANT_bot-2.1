import asyncio
import ccxt.pro as ccxtpro
import ccxt
import logging
import time
import json
import statistics  # ADDED: Missing import for measure_network_latency
from typing import Dict, List, Optional, Any, Callable
from exchanges_websocket import BinanceUSWebSocket, KrakenWebSocket, CoinbaseWebSocket
from market_context import MarketContext, AuctionState, MarketPhase, MacroSignal
from auction_context_module import AuctionContextModule

logger = logging.getLogger(__name__)

class DataFeed:
    """
    UNIFIED DATA ACQUISITION LAYER
    - WebSocket (LOW_LATENCY mode) and REST polling (HIGH_LATENCY mode)
    - Market Context & Auction Theory integration
    - Centralized balance tracking for dynamic capital mode
    """
    def __init__(self, config: Dict, main_logger: logging.Logger):
        self.config = config
        self.logger = main_logger
        self.exchanges = {}
        self.price_data = {}
        self.market_contexts = {}
        self.auction_analyzer = AuctionContextModule()
        self.data_callbacks = []
        
        # Capital Mode Support
        self.exchange_balances = {}  # exchange_name -> total_balance_usd
        self.latency_mode = 'HIGH_LATENCY'  # Default, set by orchestrator
        
        # Connection State
        self.running = False
        self.ws_connections = {}
        self.pro_exchanges = {}
        
        self.logger.info("✅ Unified DataFeed initialized")
    
    # ==================== CAPITAL MODE INTEGRATION ====================
    def set_latency_mode(self, mode: str):
        """Set the data acquisition mode based on network latency."""
        self.latency_mode = mode
        self.logger.info(f"📡 DataFeed mode set to: {self.latency_mode}")
    
    def get_total_balance_usd(self, exchange_name: str) -> float:
        """
        CRITICAL METHOD for Dynamic Capital Mode.
        Returns total balance in USD for a given exchange.
        Called by SystemOrchestrator._check_sufficient_balances().
        """
        try:
            # Return cached balance if available and recent
            if exchange_name in self.exchange_balances:
                cached_balance, timestamp = self.exchange_balances[exchange_name]
                if time.time() - timestamp < 30:  # Cache valid for 30 seconds
                    return cached_balance
            
            # Fetch fresh balance
            exchange = self.exchanges.get(exchange_name)
            if not exchange:
                self.logger.error(f"Exchange {exchange_name} not found in DataFeed")
                return 0.0
            
            # Fetch all balances
            balance = exchange.fetch_balance()
            
            # Calculate total in USD
            total_usd = 0.0
            btc_price = self._get_btc_price_for_exchange(exchange_name)
            
            for currency, amount in balance['total'].items():
                if amount <= 0:
                    continue
                    
                if currency in ['USDT', 'USDC', 'USD']:
                    total_usd += amount
                elif currency == 'BTC':
                    total_usd += amount * btc_price
                # Add other currencies as needed
            
            # Update cache
            self.exchange_balances[exchange_name] = (total_usd, time.time())
            
            self.logger.debug(f"💰 {exchange_name} total balance: ${total_usd:.2f}")
            return total_usd
            
        except Exception as e:
            self.logger.error(f"Error getting balance for {exchange_name}: {e}")
            return 0.0
    
    def _get_btc_price_for_exchange(self, exchange_name: str) -> float:
        """Get current BTC price in USD for an exchange."""
        try:
            # Try to get from price_data first
            for symbol_data in self.price_data.values():
                if exchange_name in symbol_data:
                    data = symbol_data[exchange_name]
                    return (data.get('bid', 0) + data.get('ask', 0)) / 2
            
            # Fallback: fetch fresh ticker
            exchange = self.exchanges.get(exchange_name)
            if exchange:
                ticker = exchange.fetch_ticker('BTC/USDT')
                return ticker['last'] if ticker['last'] else ticker['bid']
                
        except Exception as e:
            self.logger.debug(f"Could not get BTC price for {exchange_name}: {e}")
        
        return 40000.0  # Conservative fallback
    
    def measure_network_latency(self) -> float:
        """Measure average network latency to exchanges."""
        latencies = []
        for name, exchange in self.exchanges.items():
            try:
                start = time.time()
                # Simple API call to measure latency
                exchange.fetch_time()
                latency = (time.time() - start) * 1000  # Convert to ms
                latencies.append(latency)
                self.logger.debug(f"   {name} latency: {latency:.1f}ms")
            except Exception as e:
                self.logger.debug(f"Latency check failed for {name}: {e}")
        
        return statistics.mean(latencies) if latencies else 500.0
    # ==================== END CAPITAL MODE INTEGRATION ====================
    
    def subscribe(self, callback: Callable):
        """Subscribe to real-time data updates."""
        self.data_callbacks.append(callback)
    
    async def _process_incoming_data(self, data: Dict):
        """Process data from any source and notify subscribers."""
        for callback in self.data_callbacks:
            try:
                await callback(data)
            except Exception as e:
                self.logger.error(f"Data callback error: {e}")
    
    def update_market_context(self, symbol: str, exchange: str, bids: List, asks: List, last_price: float):
        """Update market context with new order book data"""
        try:
            if symbol not in self.market_contexts:
                self.market_contexts[symbol] = MarketContext(primary_symbol=symbol)
            
            context = self.market_contexts[symbol]
            context.timestamp = time.time()
            
            # Update auction context
            context = self.auction_analyzer.analyze_order_book(bids, asks, last_price, context)
            
            # Update market phase based on auction state
            self._update_market_phase(context)
            
            # Update execution confidence
            self._update_execution_confidence(context)
            
            # Log significant context changes
            if context.auction_state != AuctionState.BALANCED:
                self.logger.debug(f"Market Context [{symbol}]: {context.to_dict()}")
                
        except Exception as e:
            self.logger.error(f"Error updating market context: {e}")
    
    def _update_market_phase(self, context: MarketContext):
        """Update market phase based on auction analysis"""
        if context.auction_state == AuctionState.IMBALANCED_BUYING:
            context.market_phase = MarketPhase.ACCUMULATION
            context.market_sentiment = 0.8
        elif context.auction_state == AuctionState.IMBALANCED_SELLING:
            context.market_phase = MarketPhase.DISTRIBUTION
            context.market_sentiment = -0.8
        elif context.auction_state == AuctionState.ACCEPTING:
            context.market_phase = MarketPhase.MARKUP
            context.market_sentiment = 0.5
        elif context.auction_state == AuctionState.REJECTING:
            context.market_phase = MarketPhase.MARKDOWN
            context.market_sentiment = -0.5
        else:
            context.market_phase = MarketPhase.UNKNOWN
            context.market_sentiment = 0.0
    
    def _update_execution_confidence(self, context: MarketContext):
        """Update execution confidence based on market conditions"""
        # Higher confidence when there's clear auction direction
        if context.auction_state in [AuctionState.IMBALANCED_BUYING, AuctionState.IMBALANCED_SELLING]:
            context.execution_confidence = 0.9
        elif context.auction_state in [AuctionState.ACCEPTING, AuctionState.REJECTING]:
            context.execution_confidence = 0.7
        elif context.auction_state == AuctionState.BALANCED:
            context.execution_confidence = 0.5
        else:
            context.execution_confidence = 0.3
            
        # Adjust based on sentiment strength
        context.execution_confidence *= (1.0 + abs(context.market_sentiment))
    
    # ==================== WEB SOCKET MODE (LOW_LATENCY) ====================
    async def start_websocket_feed(self):
        """Start WebSocket connections for LOW_LATENCY mode."""
        self.logger.info("🔌 Starting LOW-LATENCY WebSocket data feed")
        
        try:
            # Initialize authenticated exchanges
            await self._init_pro_exchanges()
            
            # Initialize custom WebSocket connections for redundancy
            await self._init_custom_websockets()
            
            self.running = True
            
            # Start watching order books
            asyncio.create_task(self._watch_pro_orderbooks())
            
            self.logger.info("✅ WebSocket feed active (LOW_LATENCY mode)")
            
        except Exception as e:
            self.logger.error(f"Failed to start WebSocket feed: {e}")
            # Fall back to custom WebSockets
            await self._fallback_to_custom_websockets()
    
    async def _init_pro_exchanges(self):
        """Initialize authenticated ccxt.pro exchanges."""
        exchange_configs = self.config.get('exchanges', {})
        
        for name, config in exchange_configs.items():
            if not config.get('enabled', False):
                continue
                
            try:
                # Common config
                pro_config = {
                    'apiKey': config.get('api_key', ''),
                    'secret': config.get('api_secret', ''),
                    'enableRateLimit': True,
                    'timeout': 30000,
                }
                
                # Exchange-specific initialization
                if name == 'binance':
                    self.pro_exchanges[name] = ccxtpro.binanceus(pro_config)
                elif name == 'kraken':
                    self.pro_exchanges[name] = ccxtpro.kraken(pro_config)
                elif name == 'coinbase':
                    self.pro_exchanges[name] = ccxtpro.coinbase(pro_config)
                else:
                    continue
                
                # Also store in regular exchanges dict for balance checks
                self.exchanges[name] = ccxt.__dict__[name]({
                    'apiKey': config.get('api_key', ''),
                    'secret': config.get('api_secret', ''),
                    'enableRateLimit': True
                })
                
                # Load markets
                await self.pro_exchanges[name].load_markets()
                self.exchanges[name].load_markets()
                
                self.logger.info(f"✅ {name.upper()} initialized (WebSocket + REST)")
                
            except Exception as e:
                self.logger.error(f"❌ Failed to init {name}: {e}")
    
    async def _init_custom_websockets(self):
        """Initialize custom WebSocket connections."""
        try:
            # Binance
            binance_ws = BinanceUSWebSocket("btcusdt")
            await binance_ws.connect()
            binance_ws.subscribe(self._handle_websocket_data)
            self.ws_connections['binance'] = binance_ws
            
            # Kraken
            kraken_ws = KrakenWebSocket("XBT/USD")
            await kraken_ws.connect()
            kraken_ws.subscribe(self._handle_websocket_data)
            self.ws_connections['kraken'] = kraken_ws
            
            # Coinbase
            coinbase_ws = CoinbaseWebSocket("BTC-USD")
            await coinbase_ws.connect()
            coinbase_ws.subscribe(self._handle_websocket_data)
            self.ws_connections['coinbase'] = coinbase_ws
            
            self.logger.info("✅ Custom WebSocket connections established")
            
        except Exception as e:
            self.logger.error(f"❌ Custom WebSocket init failed: {e}")
    
    async def _handle_websocket_data(self, data: Dict):
        """Handle incoming WebSocket data from custom connections."""
        try:
            exchange = data.get('exchange', '')
            data_type = data.get('type', '')
            
            if data_type == 'orderbook':
                # Map exchange names
                if exchange == 'binance_us':
                    exchange = 'binance'
                    symbol = 'BTC/USDT'
                elif exchange == 'kraken':
                    symbol = 'BTC/USD'
                elif exchange == 'coinbase':
                    symbol = 'BTC/USD'
                else:
                    return
                
                # Extract best bid/ask
                bids = data.get('bids', [])
                asks = data.get('asks', [])
                
                if bids and asks:
                    best_bid = float(bids[0][0]) if bids[0] else None
                    best_ask = float(asks[0][0]) if asks[0] else None
                    
                    if best_bid and best_ask:
                        # Update price data
                        if symbol not in self.price_data:
                            self.price_data[symbol] = {}
                        
                        self.price_data[symbol][exchange] = {
                            'bid': best_bid,
                            'ask': best_ask,
                            'bids': bids[:5],
                            'asks': asks[:5],
                            'timestamp': data.get('timestamp', time.time())
                        }
                        
                        # Update market context
                        last_price = (best_bid + best_ask) / 2
                        self.update_market_context(symbol, exchange, bids, asks, last_price)
                        
                        # Notify subscribers
                        await self._process_incoming_data({
                            'type': 'price_update',
                            'symbol': symbol,
                            'exchange': exchange,
                            'bid': best_bid,
                            'ask': best_ask,
                            'timestamp': time.time()
                        })
                        
        except Exception as e:
            self.logger.error(f"WebSocket data handling error: {e}")
    
    async def _watch_pro_orderbooks(self):
        """Watch order books using ccxt.pro."""
        tasks = []
        
        for name, pro_exch in self.pro_exchanges.items():
            # Determine symbols to watch
            symbols = ['BTC/USDT', 'BTC/USD']  # Monitor both
            
            for symbol in symbols:
                if hasattr(pro_exch, 'markets') and symbol in pro_exch.markets:
                    tasks.append(self._watch_single_book(name, pro_exch, symbol))
        
        if tasks:
            await asyncio.gather(*tasks, return_exceptions=True)
    
    async def _watch_single_book(self, exch_name: str, exchange, symbol: str):
        """Watch a single order book."""
        while self.running:
            try:
                orderbook = await exchange.watch_order_book(symbol)
                
                if symbol not in self.price_data:
                    self.price_data[symbol] = {}
                
                # Extract best bid/ask
                best_bid = orderbook['bids'][0][0] if orderbook['bids'] else None
                best_ask = orderbook['asks'][0][0] if orderbook['asks'] else None
                
                if best_bid and best_ask:
                    self.price_data[symbol][exch_name] = {
                        'bid': best_bid,
                        'ask': best_ask,
                        'bids': orderbook['bids'][:5],
                        'asks': orderbook['asks'][:5],
                        'timestamp': orderbook['timestamp']
                    }
                    
                    # Update market context
                    last_price = (best_bid + best_ask) / 2
                    self.update_market_context(
                        symbol, 
                        exch_name, 
                        orderbook['bids'][:10], 
                        orderbook['asks'][:10], 
                        last_price
                    )
                    
                    # Notify subscribers
                    await self._process_incoming_data({
                        'type': 'price_update',
                        'symbol': symbol,
                        'exchange': exch_name,
                        'bid': best_bid,
                        'ask': best_ask,
                        'timestamp': time.time()
                    })
                
                # Small sleep to prevent overwhelming
                await exchange.sleep(0.01)
                
            except Exception as e:
                self.logger.error(f"ccxt.pro WebSocket error on {exch_name} {symbol}: {e}")
                await asyncio.sleep(5)
    
    async def _fallback_to_custom_websockets(self):
        """Fall back to using only custom WebSockets."""
        self.logger.warning("🔄 Falling back to custom WebSocket connections")
        self.running = True
        
        # Keep the connections alive
        while self.running:
            await asyncio.sleep(1)
    # ==================== END WEB SOCKET MODE ====================
    
    # ==================== REST POLLING MODE (HIGH_LATENCY) ====================
    async def start_rest_polling(self):
        """Start REST polling for HIGH_LATENCY mode."""
        self.logger.info("🔌 Starting HIGH-LATENCY REST polling")
        self.running = True
        
        # Initialize REST-only exchanges
        exchange_configs = self.config.get('exchanges', {})
        
        for name, config in exchange_configs.items():
            if not config.get('enabled', False):
                continue
                
            try:
                rest_config = {
                    'apiKey': config.get('api_key', ''),
                    'secret': config.get('api_secret', ''),
                    'enableRateLimit': True,
                    'timeout': 10000,
                }
                
                # Create REST exchange
                self.exchanges[name] = ccxt.__dict__[name](rest_config)
                self.exchanges[name].load_markets()
                
                self.logger.info(f"✅ {name.upper()} initialized (REST only)")
                
            except Exception as e:
                self.logger.error(f"❌ Failed to init REST {name}: {e}")
    
    async def poll_prices_rest(self, symbols: List[str]) -> Dict[str, Dict]:
        """Poll exchanges for ticker data (REST)."""
        import concurrent.futures
        
        def fetch_ticker(args):
            name, exchange, symbol = args
            try:
                ticker = exchange.fetch_ticker(symbol)
                return (name, symbol, ticker['bid'], ticker['ask'], ticker['last'])
            except Exception as e:
                self.logger.debug(f"Failed to fetch {symbol} from {name}: {e}")
                return (name, symbol, None, None, None)
        
        # Prepare tasks
        tasks = []
        for name, exchange in self.exchanges.items():
            for symbol in symbols:
                if hasattr(exchange, 'markets') and symbol in exchange.markets:
                    tasks.append((name, exchange, symbol))
        
        # Use ThreadPoolExecutor for concurrent polling
        with concurrent.futures.ThreadPoolExecutor(max_workers=len(tasks)) as executor:
            results = list(executor.map(fetch_ticker, tasks))
        
        # Process results
        timestamp = time.time()
        for name, symbol, bid, ask, last in results:
            if symbol not in self.price_data:
                self.price_data[symbol] = {}
            
            if bid and ask:
                self.price_data[symbol][name] = {
                    'bid': bid,
                    'ask': ask,
                    'timestamp': timestamp
                }
                
                # Create simulated order book for market context
                simulated_bids = [[bid * 0.999, 1.0], [bid * 0.998, 2.0], [bid * 0.997, 0.5]]
                simulated_asks = [[ask * 1.001, 1.0], [ask * 1.002, 2.0], [ask * 1.003, 0.5]]
                
                # Update market context
                self.update_market_context(symbol, name, simulated_bids, simulated_asks, last or bid)
        
        return self.price_data.copy()
    # ==================== END REST POLLING MODE ====================
    
    # ==================== PUBLIC INTERFACE ====================
    async def start(self):
        """Start data feed based on latency mode."""
        if self.latency_mode == 'LOW_LATENCY':
            await self.start_websocket_feed()
        else:
            await self.start_rest_polling()
    
    async def get_prices(self, symbols: List[str]) -> Dict[str, Dict]:
        """Get current prices for requested symbols."""
        if self.latency_mode == 'LOW_LATENCY' and self.running:
            # Return cached WebSocket data
            result = {}
            for symbol in symbols:
                if symbol in self.price_data:
                    result[symbol] = self.price_data[symbol].copy()
                else:
                    result[symbol] = {}
            return result
        else:
            # Actively poll via REST
            return await self.poll_prices_rest(symbols)
    
    async def stop(self):
        """Stop all connections."""
        self.logger.info("🛑 Stopping DataFeed")
        self.running = False
        
        # Close ccxt.pro exchanges
        for name, exch in self.pro_exchanges.items():
            try:
                await exch.close()
            except:
                pass
                
        # Close custom WebSocket connections
        for name, ws in self.ws_connections.items():
            try:
                await ws.ws.close()
            except:
                pass
        
        # Clear caches
        self.price_data.clear()
        self.exchange_balances.clear()
        
        self.logger.info("✅ DataFeed stopped")
    
    def get_last_price(self, symbol: str) -> float:
        """Get last price for a symbol (used by fallback balance calculation)."""
        try:
            if symbol in self.price_data:
                for exchange_data in self.price_data[symbol].values():
                    if 'bid' in exchange_data and 'ask' in exchange_data:
                        return (exchange_data['bid'] + exchange_data['ask']) / 2
        except Exception as e:
            self.logger.debug(f"Could not get last price for {symbol}: {e}")
        
        return 40000.0  # Conservative fallback