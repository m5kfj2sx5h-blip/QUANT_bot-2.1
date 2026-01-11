#!/usr/bin/env python3
"""
PROFESSIONAL ARBITRAGE TRADING SYSTEM - MAIN ORCHESTRATOR
Version: 2.0.0
Description: Core orchestration engine for multi-exchange arbitrage trading system
Author: Quantum Trading Systems
"""

import asyncio
import json
import logging
import os
import signal
import sys
import time
import traceback
from datetime import datetime
from typing import Dict, List, Optional, Any

# Import your custom modules
from data_feed import DataFeed
from market_context import MarketContext
from order_executor import OrderExecutor
from monitoring import SystemMonitor
from market_context import ArbitrageAnalyzer
from exchange_wrappers import ExchangeWrapperFactory

class SystemOrchestrator:
    """
    Main orchestrator for the multi-exchange arbitrage bot.
    Manages all system components and coordinates the trading cycle.
    """
    
    def __init__(self, config_path: str = "config/bot_config.json"):
        """Initialize the orchestrator with configuration."""
        self.config_path = config_path
        self.config = self._load_config()
        self._setup_logging()
        self.logger = logging.getLogger("ArbitrageBot.Orchestrator")
        
        # Core components
        self.data_feed = None
        self.exchange_wrappers: Dict[str, Any] = {}
        self.market_context = None
        self.market_context = None
        self.order_executor = None
        self.monitoring = None
        
        # State management
        self.running = False
        self.start_time = None
        self.trade_cycles = 0
        self.successful_trades = 0
        self.failed_trades = 0
        self.current_profit = 0.0
        self.estimated_balance = 0.0
        self.capital_mode = "BALANCED"
        self.available_capital_usd = 0.0
        self.settings = {}
        
        # Performance tracking
        self.cycle_times = []
        self.latency_mode = "unknown"
        self.consecutive_failures = 0
        
    def _load_config(self) -> Dict[str, Any]:
        """Load configuration from JSON file."""
        try:
            with open(self.config_path, 'r') as f:
                config = json.load(f)
            self._log(f"✅ Configuration loaded from {self.config_path}")
            return config
        except Exception as e:
            self._log(f"❌ Failed to load config: {e}", level=logging.ERROR)
            sys.exit(1)
    
    def _setup_logging(self):
        """Configure logging for the system."""
        logging.basicConfig(
            level=logging.INFO,
            format='%(asctime)s | %(levelname)-8s | %(name)-25s | %(message)s',
            datefmt='%Y-%m-%d %H:%M:%S'
        )
    
    def _log(self, message: str, level: int = logging.INFO):
        """Convenience method for logging."""
        logger = logging.getLogger("ArbitrageBot.None")
        logger.log(level, message)
    
    def _initialize_components(self):
        """Initialize all system components."""
        self.logger.info("🔄 Initializing system components...")
        
        # Initialize settings cache
        self.settings = {
            'position_size': self.config.get('position_size', 500.0),
            'max_position_size': self.config.get('max_position_size', 5000.0),
            'min_position_size_usd': self.config.get('min_position_size_usd', 10.0),
            'min_profit_threshold': self.config.get('min_profit_threshold', 0.5),
            'slippage_tolerance_percent': self.config.get('slippage_tolerance_percent', 0.1),
            'min_stable_per_exchange': self.config.get('min_stable_per_exchange', 1500.0),
            'cycle_delay': self.config.get('cycle_delay', 2.0),
            'max_consecutive_failures': self.config.get('max_consecutive_failures', 5)
        }
        
        # Initialize DataFeed
        self.data_feed = DataFeed(self.config, self.logger)
        self.logger.info("✅ Unified DataFeed initialized")
        
        # Initialize exchange wrappers - FIXED SECTION
        exchange_configs = self.config.get('exchanges', {})
        global_exchange_settings = self.config.get('exchange_settings', {})
        
        for exchange_id, exchange_config in exchange_configs.items():
            if exchange_config.get('enabled', False):
                try:
                    # Get environment variable names from config
                    api_key_env_var = exchange_config.get('api_key')
                    api_secret_env_var = exchange_config.get('api_secret')
                    
                    if not api_key_env_var or not api_secret_env_var:
                        self.logger.error(f"❌ API key/secret env var names not configured for {exchange_id}")
                        continue
                    
                    # Load actual values from environment
                    api_key = os.getenv(api_key_env_var)
                    api_secret = os.getenv(api_secret_env_var)
                    
                    if not api_key or not api_secret:
                        self.logger.error(f"❌ Could not load API credentials from environment for {exchange_id}. Check your .env file.")
                        continue
                    
                    # Build the config dictionary for the wrapper
                    wrapper_config = {
                        'api_key': api_key,
                        'api_secret': api_secret,
                        # Merge global settings
                        **global_exchange_settings
                    }
                    
                    # Call factory with correct two arguments
                    wrapper = ExchangeWrapperFactory.create_wrapper(exchange_id, wrapper_config)
                    
                    if wrapper:
                        self.exchange_wrappers[exchange_id] = wrapper
                        self.logger.info(f"✅ {exchange_id} initialized successfully")
                    else:
                        self.logger.error(f"❌ Factory failed to create wrapper for {exchange_id}")
                        
                except Exception as e:
                    self.logger.error(f"❌ Failed to initialize {exchange_id}: {e}")
        
        if not self.exchange_wrappers:
            self.logger.error("❌ No exchanges initialized. Check configuration.")
            sys.exit(1)
        
        # Initialize Market Context
        self.market_context = MarketContext(self.config, self.logger)
        
        # Initialize Arbitrage Analyzer
        self.market_context = ArbitrageAnalyzer(
            self.market_context.get_context(), self.config, self.logger
        )
        
        # Initialize Order Executor
        self.order_executor = OrderExecutor(self.config, self.logger)
        
        # Initialize System Monitor
        self.monitoring = SystemMonitor(self.config, self.logger)
        
        # Initialize latency mode
        self._initialize_latency_mode()
        
        self.logger.info("✅ All system components initialized successfully")
    
    def _initialize_latency_mode(self):
        """Determine initial latency mode based on configuration."""
        # Simplified logic - you can expand this
        self.latency_mode = "high_latency"  # Default
        self.logger.info(f"📡 Latency mode: {self.latency_mode}")
    
    def _update_capital_mode(self):
        """Update capital allocation mode based on exchange balances."""
        try:
            # Get balances from all exchanges
            balances = {}
            total_balance = 0.0
            
            for exchange_id, wrapper in self.exchange_wrappers.items():
                balance_data = wrapper.get_balance()
                if balance_data and 'total' in balance_data:
                    # Extract USD equivalent - you may need to adjust this based on your wrapper
                    usd_balance = balance_data['total'].get('USD', 0.0)
                    if usd_balance == 0:
                        # Try USDT if USD is 0
                        usd_balance = balance_data['total'].get('USDT', 0.0)
                    
                    balances[exchange_id] = usd_balance
                    total_balance += usd_balance
            
            if not balances:
                self.logger.warning("⚠️  No balance data available")
                return
            
            # Calculate balance ratios
            min_balance = min(balances.values())
            max_balance = max(balances.values())
            
            if min_balance == 0:
                self.logger.error("⚠️  Zero balance detected on at least one exchange")
                self.capital_mode = "BOTTLENECKED"
                self.available_capital_usd = 0
                return
            
            balance_ratio = max_balance / min_balance
            
            # Determine mode
            if balance_ratio > 1.5:  # More than 50% imbalance
                self.capital_mode = "BOTTLENECKED"
                # Use 95% of smallest balance
                self.available_capital_usd = min_balance * 0.95
                self.logger.info(f"🔧 Capital Mode: {self.capital_mode} (Ratio: {balance_ratio:.2f})")
                self.logger.info(f"💰 Available: ${self.available_capital_usd:.2f} (95% of smallest balance)")
            else:
                self.capital_mode = "BALANCED"
                # Use 40% of average balance
                avg_balance = sum(balances.values()) / len(balances)
                self.available_capital_usd = avg_balance * 0.40
                self.logger.info(f"🔧 Capital Mode: {self.capital_mode} (Ratio: {balance_ratio:.2f})")
                self.logger.info(f"💰 Available: ${self.available_capital_usd:.2f} (40% of average balance)")
            
            self.estimated_balance = total_balance
            
        except Exception as e:
            self.logger.error(f"❌ Error updating capital mode: {e}")
            self.capital_mode = "BALANCED"
            self.available_capital_usd = 1000.0  # Fallback
    
    def run(self):
        """Main execution loop."""
        self.running = True
        self.start_time = time.time()
        
        try:
            self._initialize_components()
            self._setup_signal_handlers()
            
            self.logger.info("🚀 Arbitrage Bot Started Successfully!")
            self.logger.info("=" * 60)
            
            while self.running:
                try:
                    cycle_start = time.time()
                    self._execute_trading_cycle()
                    cycle_time = time.time() - cycle_start
                    
                    # Record cycle time
                    self.cycle_times.append(cycle_time)
                    if len(self.cycle_times) > 100:
                        self.cycle_times.pop(0)
                    
                    # Dynamic delay based on performance
                    avg_cycle_time = sum(self.cycle_times) / len(self.cycle_times) if self.cycle_times else 0.1
                    delay = max(0.1, self.settings.get('cycle_delay', 2.0) - avg_cycle_time)
                    
                    time.sleep(delay)
                    
                except KeyboardInterrupt:
                    self.logger.info("🛑 Keyboard interrupt received")
                    break
                except Exception as e:
                    self.logger.error(f"❌ Error in main loop: {e}")
                    self.consecutive_failures += 1
                    
                    if self.consecutive_failures > self.settings.get('max_consecutive_failures', 5):
                        self.logger.error("🚨 Too many consecutive failures. Shutting down.")
                        break
                    
                    # Exponential backoff
                    backoff = min(60, 2 ** self.consecutive_failures)
                    self.logger.info(f"⏸️  Backing off for {backoff} seconds")
                    time.sleep(backoff)
            
        except Exception as e:
            self.logger.error(f"❌ Fatal error in main loop: {e}")
        
        finally:
            self.shutdown()
    
    def _execute_trading_cycle(self):
        """Execute a single trading cycle."""
        self.trade_cycles += 1
        
        try:
            # Update market data
            market_data = self.data_feed.get_market_data()
            if not market_data:
                self.logger.warning("⚠️  No market data available")
                return
            
            # Update market context
            self.market_context.update(market_data)
            
            # Update capital mode based on current balances
            self._update_capital_mode()
            
            # Skip if no capital available
            if self.available_capital_usd < self.settings['min_position_size_usd']:
                self.logger.warning(f"⚠️  Insufficient capital: ${self.available_capital_usd:.2f}")
                return
            
            # Find arbitrage opportunities
            opportunities = self.market_context.find_opportunities(
                market_data, 
                self.available_capital_usd
            )
            
            if not opportunities:
                # Log every 10 cycles to avoid spam
                if self.trade_cycles % 10 == 0:
                    self.logger.info("🔍 No arbitrage opportunities found")
                return
            
            # Sort by profit potential
            opportunities.sort(key=lambda x: x.get('expected_profit_usd', 0), reverse=True)
            best_opportunity = opportunities[0]
            
            # Check if profit meets threshold
            min_profit = self.settings['min_profit_threshold']
            if best_opportunity.get('expected_profit_usd', 0) < min_profit:
                self.logger.info(f"📊 Best opportunity (${best_opportunity['expected_profit_usd']:.2f}) below threshold (${min_profit:.2f})")
                return
            
            # Execute the trade
            self.logger.info(f"🎯 Executing trade: {best_opportunity.get('description', 'N/A')}")
            
            result = self.order_executor.execute_arbitrage(
                best_opportunity,
                self.available_capital_usd,
                self.exchange_wrappers
            )
            
            if result.get('success', False):
                self.successful_trades += 1
                profit = result.get('realized_profit_usd', 0)
                self.current_profit += profit
                self.consecutive_failures = 0
                
                self.logger.info(f"✅ Trade successful! Profit: ${profit:.2f}")
                self.logger.info(f"📈 Total Profit: ${self.current_profit:.2f}")
            else:
                self.failed_trades += 1
                self.logger.error(f"❌ Trade failed: {result.get('error', 'Unknown error')}")
            
            # Update system monitor
            self.monitoring.update_metrics({
                'trade_cycles': self.trade_cycles,
                'successful_trades': self.successful_trades,
                'failed_trades': self.failed_trades,
                'current_profit': self.current_profit,
                'capital_mode': self.capital_mode,
                'available_capital_usd': self.available_capital_usd
            })
            
            # Log every 10 cycles
            if self.trade_cycles % 10 == 0:
                self._log_cycle_summary()
            
        except Exception as e:
            self.logger.error(f"❌ Error in trading cycle: {e}")
            self.failed_trades += 1
    
    def _log_cycle_summary(self):
        """Log a summary of recent performance."""
        avg_cycle_time = sum(self.cycle_times) / len(self.cycle_times) if self.cycle_times else 0
        
        self.logger.info("=" * 50)
        self.logger.info(f"📊 Cycle {self.trade_cycles} Summary:")
        self.logger.info(f"   Successful Trades: {self.successful_trades}")
        self.logger.info(f"   Failed Trades: {self.failed_trades}")
        self.logger.info(f"   Total Profit: ${self.current_profit:.2f}")
        self.logger.info(f"   Capital Mode: {self.capital_mode}")
        self.logger.info(f"   Available Capital: ${self.available_capital_usd:.2f}")
        self.logger.info(f"   Avg Cycle Time: {avg_cycle_time:.2f}s")
        self.logger.info("=" * 50)
    
    def _setup_signal_handlers(self):
        """Setup signal handlers for graceful shutdown."""
        signal.signal(signal.SIGINT, self._signal_handler)
        signal.signal(signal.SIGTERM, self._signal_handler)
    
    def _signal_handler(self, signum, frame):
        """Handle shutdown signals."""
        self.logger.info(f"📡 Received signal {signum}. Initiating shutdown...")
        self.running = False
    
    def shutdown(self):
        """Gracefully shutdown the system."""
        self.logger.info("🛑 Initiating graceful system shutdown...")
        self.running = False
        
        # Stop data feed
        if self.data_feed:
            self.logger.info("🛑 Stopping DataFeed")
            self.data_feed.stop()
            self.logger.info("✅ Data feed stopped")
        
        # Close exchange connections
        for exchange_id, wrapper in self.exchange_wrappers.items():
            try:
                # If your wrapper has a close/disconnect method
                if hasattr(wrapper, 'disconnect'):
                    wrapper.disconnect()
                    self.logger.info(f"✅ {exchange_id} disconnected")
            except:
                pass
        
        # Log session summary
        self._log_session_summary()
        self.logger.info("👋 System shutdown complete. Goodbye!")
    
    def _log_session_summary(self):
        """Log session summary statistics."""
        try:
            if not self.start_time:
                self.logger.warning("No start time recorded for session summary")
                return
            
            session_duration = datetime.now() - datetime.fromtimestamp(self.start_time)
            hours, remainder = divmod(session_duration.total_seconds(), 3600)
            minutes, seconds = divmod(remainder, 60)
            
            self.logger.info("=" * 70)
            self.logger.info("📊 ARBITRAGE BOT SESSION SUMMARY")
            self.logger.info("=" * 70)
            self.logger.info(f"⏱️  Session Duration: {int(hours)}h {int(minutes)}m {int(seconds)}s")
            self.logger.info(f"🔁 Total Trade Cycles: {self.trade_cycles}")
            self.logger.info(f"✅ Successful Trades: {self.successful_trades}")
            self.logger.info(f"❌ Failed Trades: {self.failed_trades}")
            self.logger.info(f"📈 Current Profit: ${self.current_profit:.2f}")
            self.logger.info(f"💰 Estimated Balance: ${self.estimated_balance:.2f}")
            self.logger.info(f"🔧 Capital Mode: {self.capital_mode}")
            self.logger.info(f"💵 Available Capital: ${self.available_capital_usd:.2f}")
            self.logger.info("=" * 70)
        except Exception as e:
            self.logger.error(f"Failed to log session summary: {e}")


if __name__ == "__main__":
    orchestrator = SystemOrchestrator()
    orchestrator.run()