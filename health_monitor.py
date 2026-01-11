#!/usr/bin/env python3
"""
HEALTH MONITOR & PERFORMANCE TELEMETRY SYSTEM
Version: 2.0.0 | Component: System Health & Optimization
Author: Quant Trading System | Last Updated: 2026-01-10

Features:
- Real-time performance metrics collection
- Adaptive cycle time optimization
- API error tracking and rate limiting detection
- Memory leak detection and prevention
- Trade execution quality monitoring
- Network latency tracking
- Self-healing recommendations
"""

import os
import time
import logging
import psutil
import asyncio
from collections import deque, defaultdict
from dataclasses import dataclass, field
from typing import Dict, List, Tuple, Optional, Deque
from enum import Enum
from datetime import datetime, timedelta
import statistics
import json

logger = logging.getLogger(__name__)

class HealthStatus(Enum):
    """System health status levels"""
    OPTIMAL = "OPTIMAL"          # All systems functioning perfectly
    HEALTHY = "HEALTHY"          # Minor issues, system functional
    DEGRADED = "DEGRADED"        # Performance issues detected
    CRITICAL = "CRITICAL"        # Critical issues, intervention needed
    FAILED = "FAILED"            # System failure

@dataclass
class SystemMetrics:
    """Comprehensive system performance metrics"""
    # Timing metrics
    cycle_times: Deque[float] = field(default_factory=lambda: deque(maxlen=1000))
    trade_execution_times: Deque[float] = field(default_factory=lambda: deque(maxlen=500))
    api_response_times: Dict[str, Deque[float]] = field(default_factory=lambda: defaultdict(lambda: deque(maxlen=100)))
    
    # Error metrics
    api_errors: Deque[Tuple[float, str, str]] = field(default_factory=lambda: deque(maxlen=1000))
    trade_errors: Deque[Tuple[float, str, float]] = field(default_factory=lambda: deque(maxlen=500))
    system_errors: Deque[Tuple[float, str]] = field(default_factory=lambda: deque(maxlen=200))
    
    # Success metrics
    successful_trades: Deque[Tuple[float, float, float]] = field(default_factory=lambda: deque(maxlen=500))
    api_successes: Deque[Tuple[float, str, float]] = field(default_factory=lambda: deque(maxlen=1000))
    
    # Network metrics
    network_latencies: Deque[float] = field(default_factory=lambda: deque(maxlen=200))
    exchange_latencies: Dict[str, Deque[float]] = field(default_factory=lambda: defaultdict(lambda: deque(maxlen=100)))
    
    # Resource metrics
    memory_usage_mb: Deque[float] = field(default_factory=lambda: deque(maxlen=200))
    cpu_percentages: Deque[float] = field(default_factory=lambda: deque(maxlen=200))
    
    # Quality metrics
    fill_rates: Deque[float] = field(default_factory=lambda: deque(maxlen=200))
    slippage_percentages: Deque[float] = field(default_factory=lambda: deque(maxlen=200))
    profit_loss_ratios: Deque[float] = field(default_factory=lambda: deque(maxlen=200))
    
    def to_dict(self) -> Dict:
        """Convert metrics to dictionary for reporting"""
        current_time = time.time()
        
        # Calculate recent error rates (last 5 minutes)
        recent_errors = [
            e for e in self.api_errors 
            if current_time - e[0] < 300
        ]
        
        recent_trade_errors = [
            e for e in self.trade_errors 
            if current_time - e[0] < 300
        ]
        
        # Calculate API success rate (last 15 minutes)
        recent_api_calls = [
            s for s in self.api_successes 
            if current_time - s[0] < 900
        ]
        
        api_success_rate = 0.0
        if recent_api_calls:
            success_count = len([s for s in recent_api_calls if s[2] < 1000])  # Response time < 1s
            api_success_rate = success_count / len(recent_api_calls)
        
        # Calculate cycle time statistics
        avg_cycle_time = 0.0
        cycle_time_std = 0.0
        if self.cycle_times:
            avg_cycle_time = statistics.mean(self.cycle_times)
            if len(self.cycle_times) > 1:
                cycle_time_std = statistics.stdev(self.cycle_times)
        
        return {
            'timestamp': current_time,
            'cycle_time_ms': round(avg_cycle_time * 1000, 1),
            'cycle_time_std_ms': round(cycle_time_std * 1000, 1),
            'api_error_rate_5min': len(recent_errors) / 5.0 if recent_errors else 0.0,
            'trade_error_rate_5min': len(recent_trade_errors) / 5.0,
            'api_success_rate_15min': round(api_success_rate, 3),
            'avg_trade_time_ms': round(statistics.mean(self.trade_execution_times) * 1000, 1) if self.trade_execution_times else 0.0,
            'avg_network_latency_ms': round(statistics.mean(self.network_latencies) * 1000, 1) if self.network_latencies else 0.0,
            'avg_fill_rate': round(statistics.mean(self.fill_rates), 3) if self.fill_rates else 1.0,
            'avg_slippage_pct': round(statistics.mean(self.slippage_percentages), 3) if self.slippage_percentages else 0.0,
            'memory_usage_mb': round(statistics.mean(self.memory_usage_mb), 1) if self.memory_usage_mb else 0.0,
            'cpu_percentage': round(statistics.mean(self.cpu_percentages), 1) if self.cpu_percentages else 0.0,
            'total_metrics_collected': {
                'cycles': len(self.cycle_times),
                'trades': len(self.trade_execution_times),
                'api_calls': len(self.api_successes),
                'errors': len(self.api_errors)
            }
        }

class HealthMonitor:
    """
    COMPREHENSIVE HEALTH MONITORING SYSTEM
    
    Responsibilities:
    1. Monitor system performance and detect anomalies
    2. Optimize cycle times based on current conditions
    3. Track API health and detect rate limiting
    4. Monitor resource usage and detect leaks
    5. Provide self-healing recommendations
    6. Generate performance reports and alerts
    """
    
    def __init__(self, window_size: int = 100, config_path: str = 'config/health_config.json'):
        """Initialize health monitoring system"""
        self.metrics = SystemMetrics()
        self.window_size = window_size
        self.config_path = config_path
        
        # Load configuration
        self.config = self._load_config()
        
        # State tracking
        self.current_mode = "HIGH_LATENCY"
        self.health_status = HealthStatus.OPTIMAL
        self.last_health_check = time.time()
        self.last_metrics_report = time.time()
        
        # Alert thresholds
        self.alert_thresholds = self.config.get('alert_thresholds', {})
        
        # Adaptive parameters
        self.cycle_time_history = deque(maxlen=50)
        self.mode_switch_history = deque(maxlen=20)
        
        # Resource monitoring
        self.process = psutil.Process()
        self.start_time = time.time()
        
        # Performance optimization
        self.optimal_cycle_times = {
            'HIGH_LATENCY': 5.0,
            'LOW_LATENCY': 1.0
        }
        
        # Initialize logging
        self._setup_monitoring_logger()
        
        logger.info(f"✅ Health Monitor initialized (window_size={window_size})")
    
    def _load_config(self) -> Dict:
        """Load health monitoring configuration"""
        default_config = {
            "alert_thresholds": {
                "api_error_rate": 0.5,      # errors per minute
                "memory_growth_mb": 10.0,    # MB per hour
                "cpu_usage_percent": 80.0,   # sustained CPU usage
                "cycle_time_increase": 2.0,  # 2x normal cycle time
                "network_latency_ms": 500.0  # average network latency
            },
            "optimization": {
                "min_cycle_time": 0.5,
                "max_cycle_time": 30.0,
                "adaptation_rate": 0.1,
                "stability_samples": 10
            },
            "reporting": {
                "metrics_interval": 60.0,
                "health_check_interval": 300.0,
                "detailed_report_interval": 3600.0
            },
            "recovery": {
                "error_cooldown_seconds": 60.0,
                "mode_switch_cooldown": 300.0,
                "max_consecutive_errors": 5
            }
        }
        
        try:
            if os.path.exists(self.config_path):
                with open(self.config_path, 'r') as f:
                    user_config = json.load(f)
                    # Deep merge
                    self._merge_configs(default_config, user_config)
            
            logger.debug(f"Health config loaded: {json.dumps(default_config, indent=2)}")
            return default_config
            
        except Exception as e:
            logger.warning(f"Health config load failed, using defaults: {e}")
            return default_config
    
    def _merge_configs(self, base: Dict, update: Dict):
        """Recursively merge configuration dictionaries"""
        for key, value in update.items():
            if key in base and isinstance(base[key], dict) and isinstance(value, dict):
                self._merge_configs(base[key], value)
            else:
                base[key] = value
    
    def _setup_monitoring_logger(self):
        """Setup dedicated logger for monitoring events"""
        # Create monitoring logs directory
        os.makedirs('logs/monitoring', exist_ok=True)
        
        # Monitoring file handler
        monitor_handler = logging.FileHandler(
            'logs/monitoring/health_events.log',
            encoding='utf-8'
        )
        monitor_handler.setLevel(logging.INFO)
        
        monitor_formatter = logging.Formatter(
            '%(asctime)s.%(msecs)03d | %(levelname)s | %(message)s',
            datefmt='%Y-%m-d %H:%M:%S'
        )
        monitor_handler.setFormatter(monitor_formatter)
        
        # Add handler to health monitor logger
        self.monitor_logger = logging.getLogger('HealthMonitor')
        self.monitor_logger.addHandler(monitor_handler)
        self.monitor_logger.setLevel(logging.INFO)
    
    def adjust_cycle_time(self, last_cycle_time: float, current_mode: str, capital_mode: str = "BALANCED") -> float:
        """
        Dynamically adjust cycle time based on system performance
        
        Args:
            last_cycle_time: Time taken for the last cycle (seconds)
            current_mode: Current bot mode (HIGH_LATENCY or LOW_LATENCY)
            capital_mode: Capital allocation mode (BOTTLENECKED or BALANCED) from SystemOrchestrator
        
        Returns:
            Adjusted sleep time for next cycle
        """
        self.current_mode = current_mode
        
        # Record cycle time
        self.metrics.cycle_times.append(last_cycle_time)
        self.cycle_time_history.append(last_cycle_time)
        
        # Calculate base sleep time based on mode
        if current_mode == "LOW_LATENCY":
            base_target = 1.0
            max_cycle = 2.0
        else:
            base_target = 5.0
            max_cycle = 10.0
        
        # Check for performance issues
        if len(self.cycle_time_history) >= 10:
            avg_recent = statistics.mean(list(self.cycle_time_history)[-10:])
            
            # If cycles are taking too long, increase sleep time
            if avg_recent > max_cycle:
                adjustment = min(5.0, avg_recent - max_cycle)
                sleep_time = base_target + adjustment
                self.monitor_logger.warning(
                    f"⚠️ Cycle time {avg_recent:.1f}s > max {max_cycle}s, "
                    f"increasing sleep to {sleep_time:.1f}s (Capital Mode: {capital_mode})"
                )
            else:
                # Normal operation - use adaptive adjustment
                sleep_time = self._calculate_adaptive_sleep(
                    last_cycle_time, 
                    base_target, 
                    current_mode
                )
        else:
            # Not enough data yet, use conservative values
            sleep_time = base_target * 1.5
        
        # Apply limits
        min_sleep = self.config['optimization']['min_cycle_time']
        max_sleep = self.config['optimization']['max_cycle_time']
        sleep_time = max(min_sleep, min(max_sleep, sleep_time))
        
        # Update optimal cycle times
        self.optimal_cycle_times[current_mode] = sleep_time
        
        return sleep_time
    
    def _calculate_adaptive_sleep(self, last_cycle_time: float, base_target: float, mode: str) -> float:
        """
        Calculate adaptive sleep time using PID-like control
        
        Args:
            last_cycle_time: Last measured cycle time
            base_target: Target cycle time for this mode
            mode: Current operating mode
        
        Returns:
            Adaptive sleep time
        """
        # Get adaptation parameters
        adaptation_rate = self.config['optimization']['adaptation_rate']
        
        # Calculate error (difference from target)
        error = last_cycle_time - base_target
        
        # Simple proportional control
        adjustment = error * adaptation_rate
        
        # Apply adjustment to base target
        sleep_time = base_target + adjustment
        
        # Log significant adjustments
        if abs(adjustment) > base_target * 0.2:  # >20% adjustment
            self.monitor_logger.info(
                f"🔄 Cycle time adjustment: {last_cycle_time:.2f}s -> "
                f"{sleep_time:.2f}s (error: {error:.2f}s)"
            )
        
        return sleep_time
    
    def log_api_error(self, exchange_name: str, error_type: str = "unknown", response_time: float = None):
        """
        Log API error with comprehensive tracking
        
        Args:
            exchange_name: Name of the exchange
            error_type: Type of error (rate_limit, network, auth, etc.)
            response_time: Response time if available
        """
        timestamp = time.time()
        error_entry = (timestamp, exchange_name, error_type)
        
        self.metrics.api_errors.append(error_entry)
        
        # Calculate error rate for recent window
        error_rate = self._calculate_error_rate(300)  # 5-minute window
        
        # Check if error rate exceeds threshold
        threshold = self.alert_thresholds.get('api_error_rate', 0.5)
        if error_rate > threshold:
            self.monitor_logger.warning(
                f"⚠️ High API error rate: {error_rate:.1f}/min "
                f"(threshold: {threshold}/min)"
            )
            
            # Update health status
            if error_rate > threshold * 2:
                self.health_status = HealthStatus.CRITICAL
            else:
                self.health_status = HealthStatus.DEGRADED
        
        # Log the error
        logger.debug(f"API Error logged: {exchange_name} - {error_type}")
    
    def log_api_success(self, exchange_name: str, response_time: float):
        """
        Log successful API call for performance tracking
        
        Args:
            exchange_name: Name of the exchange
            response_time: Response time in seconds
        """
        timestamp = time.time()
        success_entry = (timestamp, exchange_name, response_time)
        
        self.metrics.api_successes.append(success_entry)
        self.metrics.api_response_times[exchange_name].append(response_time)
        
        # Check for slow responses
        slow_threshold = 1.0  # 1 second
        if response_time > slow_threshold:
            self.monitor_logger.info(
                f"🐌 Slow API response: {exchange_name} - {response_time:.2f}s"
            )
    
    def log_trade_execution(self, execution_time: float, success: bool, 
                           profit: float = 0.0, slippage: float = 0.0):
        """
        Log trade execution for performance analysis
        
        Args:
            execution_time: Time taken to execute trade
            success: Whether trade was successful
            profit: Profit from trade (if successful)
            slippage: Slippage percentage
        """
        timestamp = time.time()
        
        # Record execution time
        self.metrics.trade_execution_times.append(execution_time)
        
        if success:
            # Successful trade
            trade_entry = (timestamp, profit, slippage)
            self.metrics.successful_trades.append(trade_entry)
            self.metrics.slippage_percentages.append(slippage)
            
            # Calculate fill rate (assuming filled if successful)
            self.metrics.fill_rates.append(1.0)
            
            # Log high slippage
            if slippage > 0.001:  # >0.1% slippage
                self.monitor_logger.info(
                    f"📉 High slippage: {slippage:.3%} on profitable trade"
                )
                
        else:
            # Failed trade
            error_entry = (timestamp, 'trade_failed', execution_time)
            self.metrics.trade_errors.append(error_entry)
            self.metrics.fill_rates.append(0.0)
            
            # Check for consecutive failures
            recent_failures = [
                e for e in self.metrics.trade_errors
                if timestamp - e[0] < 300  # Last 5 minutes
            ]
            
            max_consecutive = self.config['recovery']['max_consecutive_errors']
            if len(recent_failures) >= max_consecutive:
                self.monitor_logger.warning(
                    f"🚨 {max_consecutive} consecutive trade failures"
                )
                self.health_status = HealthStatus.DEGRADED
    
    def log_network_latency(self, latency_ms: float):
        """
        Log network latency measurement
        
        Args:
            latency_ms: Network latency in milliseconds
        """
        latency_seconds = latency_ms / 1000.0
        self.metrics.network_latencies.append(latency_seconds)
        
        # Check against threshold
        threshold_ms = self.alert_thresholds.get('network_latency_ms', 500.0)
        if latency_ms > threshold_ms:
            self.monitor_logger.warning(
                f"🌐 High network latency: {latency_ms:.0f}ms "
                f"(threshold: {threshold_ms}ms)"
            )
    
    def log_exchange_latency(self, exchange_name: str, latency_ms: float):
        """
        Log exchange-specific latency
        
        Args:
            exchange_name: Name of the exchange
            latency_ms: Latency to this exchange in milliseconds
        """
        latency_seconds = latency_ms / 1000.0
        self.metrics.exchange_latencies[exchange_name].append(latency_seconds)
        
        # Log consistently high latency
        if len(self.metrics.exchange_latencies[exchange_name]) >= 10:
            recent_latencies = list(self.metrics.exchange_latencies[exchange_name])[-10:]
            avg_latency = statistics.mean(recent_latencies) * 1000
            
            if avg_latency > 1000:  # >1 second average
                self.monitor_logger.info(
                    f"🐌 High average latency to {exchange_name}: {avg_latency:.0f}ms"
                )
    
    def log_rebalance_suggestion(self, suggestion_details: Dict):
        """
        CENTRAL HOOK for rebalance alerts.
        Called by the future RebalanceAlerter to log structured suggestions.
        
        Args:
            suggestion_details: Dict with keys:
                - message: Primary alert text
                - action: e.g., 'TRANSFER_USDT'
                - amount: Amount to transfer
                - from_exchange: Source exchange
                - to_exchange: Target exchange
                - network: Suggested network
                - fee_estimate: Estimated fee
        """
        self.monitor_logger.warning(
            f"🔄 REBALANCE SUGGESTION: {suggestion_details.get('message', 'N/A')}"
        )
        # Future: Add structured logging to metrics
    
    def update_resource_usage(self):
        """Update system resource usage metrics"""
        try:
            # Memory usage
            memory_mb = self.process.memory_info().rss / 1024 / 1024
            self.metrics.memory_usage_mb.append(memory_mb)
            
            # CPU usage
            cpu_percent = self.process.cpu_percent(interval=0.1)
            self.metrics.cpu_percentages.append(cpu_percent)
            
            # Check for memory leaks
            if len(self.metrics.memory_usage_mb) >= 50:
                recent_memory = list(self.metrics.memory_usage_mb)[-50:]
                
                # Calculate memory growth rate (MB per hour)
                time_window = 300  # 5 minutes in data points
                if len(recent_memory) >= time_window:
                    memory_growth = recent_memory[-1] - recent_memory[-time_window]
                    growth_per_hour = memory_growth * (3600 / (300 * 5))  # Convert to per hour
                    
                    threshold = self.alert_thresholds.get('memory_growth_mb', 10.0)
                    if growth_per_hour > threshold:
                        self.monitor_logger.warning(
                            f"💾 Potential memory leak: {growth_per_hour:.1f}MB/hour "
                            f"(threshold: {threshold}MB/hour)"
                        )
                        self.health_status = HealthStatus.DEGRADED
            
            # Check CPU usage
            if len(self.metrics.cpu_percentages) >= 10:
                recent_cpu = list(self.metrics.cpu_percentages)[-10:]
                avg_cpu = statistics.mean(recent_cpu)
                
                threshold = self.alert_thresholds.get('cpu_usage_percent', 80.0)
                if avg_cpu > threshold:
                    self.monitor_logger.warning(
                        f"🔥 High CPU usage: {avg_cpu:.1f}% "
                        f"(threshold: {threshold}%)"
                    )
                    
        except Exception as e:
            logger.debug(f"Resource monitoring error: {e}")
    
    def _calculate_error_rate(self, window_seconds: float = 300) -> float:
        """
        Calculate error rate for a given time window
        
        Args:
            window_seconds: Time window in seconds
        
        Returns:
            Error rate (errors per minute)
        """
        current_time = time.time()
        cutoff = current_time - window_seconds
        
        # Count errors in window
        error_count = sum(
            1 for error in self.metrics.api_errors
            if error[0] > cutoff
        )
        
        # Convert to errors per minute
        errors_per_minute = error_count / (window_seconds / 60)
        
        return errors_per_minute
    
    def get_health_status(self) -> Dict:
        """
        Get comprehensive health status report
        
        Returns:
            Dictionary with health status and metrics
        """
        current_time = time.time()
        
        # Update resource usage
        self.update_resource_usage()
        
        # Check if health check is due
        health_check_interval = self.config['reporting']['health_check_interval']
        if current_time - self.last_health_check > health_check_interval:
            self._perform_health_check()
            self.last_health_check = current_time
        
        # Prepare status report
        metrics_dict = self.metrics.to_dict()
        
        status_report = {
            'timestamp': current_time,
            'system_id': f"HM_{int(self.start_time)}",
            'health_status': self.health_status.value,
            'current_mode': self.current_mode,
            'uptime_hours': (current_time - self.start_time) / 3600,
            'optimal_cycle_times': self.optimal_cycle_times,
            'metrics': metrics_dict,
            'recommendations': self._generate_recommendations(),
            'alerts': self._get_active_alerts()
        }
        
        return status_report
    
    def _perform_health_check(self):
        """Perform comprehensive health check"""
        self.monitor_logger.info("🏥 Performing comprehensive health check...")
        
        # Check various system aspects
        checks = []
        
        # 1. Check error rates
        error_rate = self._calculate_error_rate(300)
        error_threshold = self.alert_thresholds.get('api_error_rate', 0.5)
        checks.append(('API Error Rate', error_rate <= error_threshold, f"{error_rate:.2f}/min"))
        
        # 2. Check cycle time stability
        if len(self.metrics.cycle_times) >= 20:
            recent_cycles = list(self.metrics.cycle_times)[-20:]
            cycle_std = statistics.stdev(recent_cycles) if len(recent_cycles) > 1 else 0
            stable = cycle_std < (statistics.mean(recent_cycles) * 0.5)
            checks.append(('Cycle Stability', stable, f"std: {cycle_std:.2f}s"))
        
        # 3. Check resource usage
        if self.metrics.memory_usage_mb:
            memory = list(self.metrics.memory_usage_mb)[-1]
            memory_ok = memory < 500  # <500MB
            checks.append(('Memory Usage', memory_ok, f"{memory:.1f}MB"))
        
        # 4. Check trade success rate
        if self.metrics.successful_trades and self.metrics.trade_errors:
            total_trades = len(self.metrics.successful_trades) + len(self.metrics.trade_errors)
            success_rate = len(self.metrics.successful_trades) / total_trades
            success_ok = success_rate > 0.7  # >70% success rate
            checks.append(('Trade Success', success_ok, f"{success_rate:.1%}"))
        
        # Update health status based on checks
        failed_checks = [c for c in checks if not c[1]]
        
        if not failed_checks:
            self.health_status = HealthStatus.OPTIMAL
        elif len(failed_checks) <= 2:
            self.health_status = HealthStatus.HEALTHY
        elif len(failed_checks) <= 4:
            self.health_status = HealthStatus.DEGRADED
        else:
            self.health_status = HealthStatus.CRITICAL
        
        # Log check results
        self.monitor_logger.info(
            f"Health Check: {self.health_status.value} | "
            f"Failed: {len(failed_checks)}/{len(checks)}"
        )
        
        for check in checks:
            status = "✅" if check[1] else "❌"
            self.monitor_logger.debug(f"  {status} {check[0]}: {check[2]}")
    
    def _generate_recommendations(self) -> List[str]:
        """Generate system optimization recommendations"""
        recommendations = []
        current_time = time.time()
        
        # Check for mode switch recommendation
        if self.current_mode == "HIGH_LATENCY":
            # Check if we should switch to low latency
            if len(self.metrics.network_latencies) >= 10:
                avg_latency = statistics.mean(list(self.metrics.network_latencies)[-10:]) * 1000
                if avg_latency < 50:  # <50ms average latency
                    # Check when we last switched modes
                    if self.mode_switch_history:
                        last_switch = self.mode_switch_history[-1]
                        cooldown = self.config['recovery']['mode_switch_cooldown']
                        if current_time - last_switch > cooldown:
                            recommendations.append(
                                f"Switch to LOW_LATENCY mode (avg latency: {avg_latency:.0f}ms)"
                            )
        
        # Check for cycle time adjustment
        if len(self.cycle_time_history) >= 20:
            avg_cycle = statistics.mean(list(self.cycle_time_history)[-20:])
            current_sleep = self.optimal_cycle_times.get(self.current_mode, 5.0)
            
            if avg_cycle < current_sleep * 0.7:  # Cycles much faster than sleep time
                recommendations.append(
                    f"Decrease cycle sleep time (cycles: {avg_cycle:.1f}s, sleep: {current_sleep:.1f}s)"
                )
            elif avg_cycle > current_sleep * 1.3:  # Cycles slower than sleep time
                recommendations.append(
                    f"Increase cycle sleep time (cycles: {avg_cycle:.1f}s, sleep: {current_sleep:.1f}s)"
                )
        
        # Check for exchange issues
        for exchange, latencies in self.metrics.exchange_latencies.items():
            if len(latencies) >= 10:
                avg_latency = statistics.mean(list(latencies)[-10:]) * 1000
                if avg_latency > 2000:  # >2 seconds
                    recommendations.append(
                        f"Investigate {exchange} connectivity (avg latency: {avg_latency:.0f}ms)"
                    )
        
        return recommendations
    
    def _get_active_alerts(self) -> List[Dict]:
        """Get list of active alerts"""
        alerts = []
        current_time = time.time()
        
        # Check for high error rates (last 15 minutes)
        error_rate = self._calculate_error_rate(900)  # 15 minutes
        if error_rate > self.alert_thresholds.get('api_error_rate', 0.5):
            alerts.append({
                'type': 'HIGH_ERROR_RATE',
                'severity': 'WARNING' if error_rate < 2.0 else 'CRITICAL',
                'message': f'API error rate: {error_rate:.1f}/min',
                'timestamp': current_time
            })
        
        # Check for memory growth
        if len(self.metrics.memory_usage_mb) >= 100:
            recent_memory = list(self.metrics.memory_usage_mb)[-100:]
            memory_growth = recent_memory[-1] - recent_memory[0]
            
            growth_per_hour = memory_growth * (3600 / (100 * 5))  # Assuming 5s cycles
            
            threshold = self.alert_thresholds.get('memory_growth_mb', 10.0)
            if growth_per_hour > threshold:
                alerts.append({
                    'type': 'MEMORY_LEAK_SUSPECTED',
                    'severity': 'WARNING',
                    'message': f'Memory growth: {growth_per_hour:.1f}MB/hour',
                    'timestamp': current_time
                })
        
        # Check for consecutive trade failures
        if self.metrics.trade_errors:
            recent_failures = [
                e for e in self.metrics.trade_errors
                if current_time - e[0] < 300  # Last 5 minutes
            ]
            
            max_consecutive = self.config['recovery']['max_consecutive_errors']
            if len(recent_failures) >= max_consecutive:
                alerts.append({
                    'type': 'CONSECUTIVE_TRADE_FAILURES',
                    'severity': 'CRITICAL',
                    'message': f'{len(recent_failures)} consecutive trade failures',
                    'timestamp': current_time
                })
        
        return alerts
    
    def generate_report(self, detailed: bool = False) -> Dict:
        """
        Generate comprehensive health report
        
        Args:
            detailed: Whether to include detailed metrics
        
        Returns:
            Health report dictionary
        """
        report = self.get_health_status()
        
        if detailed:
            # Add detailed metrics
            report['detailed_metrics'] = {
                'cycle_time_distribution': self._get_distribution(self.metrics.cycle_times, 'seconds'),
                'trade_time_distribution': self._get_distribution(self.metrics.trade_execution_times, 'seconds'),
                'api_response_distribution': self._get_aggregated_distribution(self.metrics.api_response_times, 'seconds'),
                'memory_usage_distribution': self._get_distribution(self.metrics.memory_usage_mb, 'MB'),
                'exchange_latencies': {
                    exchange: self._get_distribution(latencies, 'seconds')
                    for exchange, latencies in self.metrics.exchange_latencies.items()
                }
            }
        
        return report
    
    def _get_distribution(self, data: Deque, unit: str) -> Dict:
        """Calculate distribution statistics for a dataset"""
        if not data:
            return {}
        
        values = list(data)
        
        return {
            'count': len(values),
            'mean': round(statistics.mean(values), 3),
            'median': round(statistics.median(values), 3),
            'std': round(statistics.stdev(values) if len(values) > 1 else 0, 3),
            'min': round(min(values), 3),
            'max': round(max(values), 3),
            'unit': unit
        }
    
    def _get_aggregated_distribution(self, data_dict: Dict[str, Deque], unit: str) -> Dict:
        """Calculate aggregated distribution across multiple datasets"""
        all_values = []
        for values in data_dict.values():
            all_values.extend(values)
        
        return self._get_distribution(deque(all_values), unit)
    
    def save_report(self, filepath: str = None):
        """
        Save health report to file
        
        Args:
            filepath: Path to save report (defaults to timestamped file)
        """
        if filepath is None:
            timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
            filepath = f'logs/monitoring/health_report_{timestamp}.json'
        
        report = self.generate_report(detailed=True)
        
        try:
            with open(filepath, 'w') as f:
                json.dump(report, f, indent=2, default=str)
            
            self.monitor_logger.info(f"📊 Health report saved to {filepath}")
            
        except Exception as e:
            logger.error(f"Failed to save health report: {e}")

# Singleton instance for easy access
_health_monitor_instance = None

def get_health_monitor(window_size: int = 100) -> HealthMonitor:
    """Get or create singleton health monitor instance"""
    global _health_monitor_instance
    if _health_monitor_instance is None:
        _health_monitor_instance = HealthMonitor(window_size)
    return _health_monitor_instance