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

# Add these two lines at the TOP of system_orchestrator.py
from dotenv import load_dotenv
load_dotenv()  # Loads variables from .env into os.environ

# FIX 1: Import stable modules (CORRECTED)
from data_feed import DataFeed
from market_context import MarketContext, ArbitrageAnalyzer  # Both from same module
from order_executor import OrderExecutor
from health_monitor import HealthMonitor  # CHANGED: Use health_monitor instead of monitoring
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
        
        # FIX 2: Correct component initialization
        self.data_feed = None
        self.exchange_wrappers: Dict[str, Any] = {}
        self.market_context = None  # Single declaration
        self.arbitrage_analyzer = None  # Separate analyzer
        self.order_executor = None
        self.health_monitor = None  # CHANGED: Use health_monitor
        
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
            
            # VERIFY config has correct structure
            exchanges = config.get('exchanges', {})
            if isinstance(exchanges, list):
                raise ValueError("Configuration error: 'exchanges' must be a dictionary, not a list")
            
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
        
        # Initialize exchange wrappers - CORRECTED SECTION
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
                        **exchange_config,  # Include exchange-specific settings
                        **global_exchange_settings
                    }
                    
                    # FIX 3: Correct factory call - TWO arguments only
                    wrapper = ExchangeWrapperFactory.create_wrapper(exchange_id, wrapper_config)
                    
                    if wrapper:
                        self.exchange_wrappers[exchange_id] = wrapper
                        self.logger.info(f"✅ {exchange_id} initialized successfully")
                    else:
                        self.logger.error(f"❌ Factory failed to create wrapper for {exchange_id}")
                        
                except Exception as e:
                    self.logger.error(f"❌ Failed to initialize {exchange_id}: {e}")
                    traceback.print_exc()
        
        if not self.exchange_wrappers:
            self.logger.error("❌ No exchanges initialized. Check configuration.")
            sys.exit(1)
        
        # FIX 4: Initialize Market Context and Arbitrage Analyzer separately
        self.market_context = MarketContext(self.config, self.logger)
        self.logger.info("✅ Market Context initialized")
        
        # Get context for analyzer
        context_data = self.market_context.get_context()
        self.arbitrage_analyzer = ArbitrageAnalyzer(context_data, self.config, self.logger)
        self.logger.info("✅ Arbitrage Analyzer initialized")
        
        # Initialize Order Executor
        self.order_executor = OrderExecutor(self.config, self.logger)
        self.logger.info("✅ Order Executor initialized")
        
        # FIX 5: Initialize Health Monitor (not SystemMonitor)
        self.health_monitor = HealthMonitor(self.config, self.logger)
        self.logger.info("✅ Health Monitor initialized")
        
        # Initialize latency mode
        self._initialize_latency_mode()
        
        self.logger.info("✅ All system components initialized successfully")
    
    def _initialize_latency_mode(self):
        """Determine initial latency mode based on configuration."""
        # Check if we have WebSocket support
        has_websocket = False
        for exchange_id, wrapper in self.exchange_wrappers.items():
            if hasattr(wrapper, 'use_websocket') and wrapper.use_websocket:
                has_websocket = True
                break
        
        if has_websocket and len(self.exchange_wrappers) <= 3:
            self.latency_mode = "low_latency"
        else:
            self.latency_mode = "high_latency"
        
        self.logger.info(f"📡 Latency mode: {self.latency_mode}")
    
    def _update_capital_mode(self):
        """Update capital allocation mode based on exchange balances."""
        try:
            # Get balances from all exchanges
            balances = {}
            total_balance = 0.0
            
            for exchange_id, wrapper in self.exchange_wrappers.items():
                try:
                    # Use the correct method based on wrapper interface
                    if hasattr(wrapper, 'get_balance'):
                        balance_data = wrapper.get_balance()
                    elif hasattr(wrapper, 'fetch_balance'):
                        balance_data = wrapper.fetch_balance()
                    else:
                        self.logger.warning(f"⚠️  No balance method found for {exchange_id}")
                        continue
                    
                    # Extract USD equivalent
                    if balance_data and 'total' in balance_data:
                        usd_balance = balance_data['total'].get('USD', 0.0)
                        if usd_balance == 0:
                            usd_balance = balance_data['total'].get('USDT', 0.0)
                        
                        balances[exchange_id] = usd_balance
                        total_balance += usd_balance
                        self.logger.debug(f"  {exchange_id} balance: ${usd_balance:.2f}")
                    
                except Exception as e:
                    self.logger.error(f"❌ Error getting balance from {exchange_id}: {e}")
            
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
            
            # FIX 6: Integrate dynamic position sizing
            self._integrate_dynamic_position_sizing()
            
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
                    traceback.print_exc()
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
            traceback.print_exc()
        
        finally:
            self.shutdown()
    
    def _integrate_dynamic_position_sizing(self):
        """Integrate the dynamic position sizing from market_context."""
        try:
            # Check if dynamic position sizing is available
            if hasattr(self.market_context, 'calculate_dynamic_position_size'):
                self.logger.info("✅ Dynamic position sizing available")
                self.use_dynamic_sizing = True
                
                # Update settings with dynamic parameters
                if hasattr(self.market_context, 'get_dynamic_parameters'):
                    dynamic_params = self.market_context.get_dynamic_parameters()
                    self.settings.update(dynamic_params)
                    self.logger.info(f"📊 Updated with dynamic parameters: {dynamic_params}")
            else:
                self.logger.warning("⚠️  Dynamic position sizing not available, using static")
                self.use_dynamic_sizing = False
                
        except Exception as e:
            self.logger.error(f"❌ Failed to integrate dynamic position sizing: {e}")
            self.use_dynamic_sizing = False
    
    def _execute_trading_cycle(self):
        """Execute a single trading cycle."""
        self.trade_cycles += 1
        
        try:
            # Step 1: Check system health
            if not self.health_monitor.check_system_health():
                self.logger.error("❌ System health check failed")
                self.consecutive_failures += 1
                return
            
            # Step 2: Update market data
            market_data = self.data_feed.get_market_data()
            if not market_data:
                self.logger.warning("⚠️  No market data available")
                return
            
            # Step 3: Update market context with new data
            self.market_context.update(market_data)
            
            # Step 4: Update capital mode based on current balances
            self._update_capital_mode()
            
            # Step 5: Skip if no capital available
            if self.available_capital_usd < self.settings['min_position_size_usd']:
                self.logger.warning(f"⚠️  Insufficient capital: ${self.available_capital_usd:.2f}")
                return
            
            # Step 6: Find arbitrage opportunities
            opportunities = self.arbitrage_analyzer.find_opportunities(
                market_data, 
                self.available_capital_usd
            )
            
            if not opportunities:
                if self.trade_cycles % 10 == 0:
                    self.logger.info("🔍 No arbitrage opportunities found")
                return
            
            # Step 7: Sort by profit potential
            opportunities.sort(key=lambda x: x.get('expected_profit_usd', 0), reverse=True)
            best_opportunity = opportunities[0]
            
            # Step 8: Apply dynamic position sizing if available
            if self.use_dynamic_sizing and 'exchange_id' in best_opportunity:
                try:
                    exchange_id = best_opportunity['exchange_id']
                    # Get current inventory
                    wrapper = self.exchange_wrappers.get(exchange_id)
                    if wrapper and hasattr(wrapper, 'get_inventory'):
                        current_inventory = wrapper.get_inventory()
                        
                        # Calculate dynamic position size
                        dynamic_size = self.market_context.calculate_dynamic_position_size(
                            exchange_id=exchange_id,
                            current_inventory=current_inventory,
                            market_data=market_data,
                            config=self.config
                        )
                        
                        # Update the opportunity with dynamic size
                        if isinstance(dynamic_size, (int, float)) and dynamic_size > 0:
                            best_opportunity['position_size'] = min(
                                dynamic_size,
                                self.available_capital_usd
                            )
                            self.logger.info(f"📈 Dynamic position size: ${dynamic_size:.2f}")
                except Exception as e:
                    self.logger.warning(f"⚠️  Dynamic sizing failed, using static: {e}")
            
            # Step 9: Check if profit meets threshold
            min_profit = self.settings['min_profit_threshold']
            expected_profit = best_opportunity.get('expected_profit_usd', 0)
            
            if expected_profit < min_profit:
                self.logger.info(f"📊 Best opportunity (${expected_profit:.2f}) below threshold (${min_profit:.2f})")
                return
            
            # Step 10: Execute the trade
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
                
                # Update health monitor with success
                self.health_monitor.update_trade_success(profit)
            else:
                self.failed_trades += 1
                error_msg = result.get('error', 'Unknown error')
                self.logger.error(f"❌ Trade failed: {error_msg}")
                
                # Update health monitor with failure
                self.health_monitor.update_trade_failure(error_msg)
            
            # Log every 10 cycles
            if self.trade_cycles % 10 == 0:
                self._log_cycle_summary()
            
        except Exception as e:
            self.logger.error(f"❌ Error in trading cycle: {e}")
            traceback.print_exc()
            self.failed_trades += 1
    
    def _log_cycle_summary(self):
        """Log a summary of recent performance."""
        if not self.cycle_times:
            return
            
        avg_cycle_time = sum(self.cycle_times) / len(self.cycle_times)
        
        self.logger.info("=" * 50)
        self.logger.info(f"📊 Cycle {self.trade_cycles} Summary:")
        self.logger.info(f"   Successful Trades: {self.successful_trades}")
        self.logger.info(f"   Failed Trades: {self.failed_trades}")
        self.logger.info(f"   Total Profit: ${self.current_profit:.2f}")
        self.logger.info(f"   Capital Mode: {self.capital_mode}")
        self.logger.info(f"   Available Capital: ${self.available_capital_usd:.2f}")
        self.logger.info(f"   Avg Cycle Time: {avg_cycle_time:.2f}s")
        self.logger.info(f"   Latency Mode: {self.latency_mode}")
        self.logger.info(f"   Dynamic Sizing: {'Enabled' if self.use_dynamic_sizing else 'Disabled'}")
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
            try:
                self.data_feed.stop()
                self.logger.info("✅ Data feed stopped")
            except Exception as e:
                self.logger.error(f"❌ Error stopping data feed: {e}")
        
        # Close exchange connections
        for exchange_id, wrapper in self.exchange_wrappers.items():
            try:
                if hasattr(wrapper, 'disconnect'):
                    wrapper.disconnect()
                    self.logger.info(f"✅ {exchange_id} disconnected")
                elif hasattr(wrapper, 'close'):
                    wrapper.close()
                    self.logger.info(f"✅ {exchange_id} closed")
            except Exception as e:
                self.logger.error(f"❌ Error disconnecting {exchange_id}: {e}")
        
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
            
            success_rate = (self.successful_trades / self.trade_cycles * 100) if self.trade_cycles > 0 else 0
            
            self.logger.info("=" * 70)
            self.logger.info("📊 ARBITRAGE BOT SESSION SUMMARY")
            self.logger.info("=" * 70)
            self.logger.info(f"⏱️  Session Duration: {int(hours)}h {int(minutes)}m {int(seconds)}s")
            self.logger.info(f"🔁 Total Trade Cycles: {self.trade_cycles}")
            self.logger.info(f"✅ Successful Trades: {self.successful_trades}")
            self.logger.info(f"❌ Failed Trades: {self.failed_trades}")
            self.logger.info(f"📈 Success Rate: {success_rate:.1f}%")
            self.logger.info(f"💰 Total Profit: ${self.current_profit:.2f}")
            self.logger.info(f"🏦 Estimated Balance: ${self.estimated_balance:.2f}")
            self.logger.info(f"🔧 Capital Mode: {self.capital_mode}")
            self.logger.info(f"💵 Available Capital: ${self.available_capital_usd:.2f}")
            self.logger.info(f"⚡ Latency Mode: {self.latency_mode}")
            self.logger.info(f"📊 Dynamic Sizing: {'Enabled' if getattr(self, 'use_dynamic_sizing', False) else 'Disabled'}")
            self.logger.info("=" * 70)
        except Exception as e:
            self.logger.error(f"Failed to log session summary: {e}")


if __name__ == "__main__":
    orchestrator = SystemOrchestrator()
    orchestrator.run()
