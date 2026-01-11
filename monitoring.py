# File: monitoring.py
"""
Monitoring module that provides SystemMonitor and PerformanceTracker
This is a compatibility module for system_orchestrator.py
"""

import logging
import time
from typing import Dict, List, Any

logger = logging.getLogger(__name__)

# Try to import from health_monitor if it exists
try:
    from health_monitor import HealthMonitor
    
    class SystemMonitor(HealthMonitor):
        """System monitor based on HealthMonitor"""
        def __init__(self, exchange_wrappers: Dict, config: Dict):
            super().__init__(exchange_wrappers, config)
            logger.info("✅ SystemMonitor initialized")
        
        def update_exchange_health(self, exchange_name: str, is_healthy: bool, latency_ms: float):
            pass
        
        def record_trade(self, trade_data: Dict):
            pass
        
        def get_system_health(self) -> Dict:
            return {"status": "operational"}
            
except ImportError:
    # Fallback if health_monitor doesn't exist
    class SystemMonitor:
        """Fallback SystemMonitor"""
        def __init__(self, exchange_wrappers: Dict, config: Dict):
            self.exchange_wrappers = exchange_wrappers
            self.config = config
            logger.info("✅ SystemMonitor (fallback) initialized")
        
        def update_exchange_health(self, exchange_name: str, is_healthy: bool, latency_ms: float):
            pass
        
        def record_trade(self, trade_data: Dict):
            pass
        
        def get_system_health(self) -> Dict:
            return {"status": "operational"}


class PerformanceTracker:
    """Performance tracker for trades"""
    def __init__(self):
        self.trades: List[Dict] = []
        logger.info("✅ PerformanceTracker initialized")
    
    def add_trade(self, trade: Dict):
        self.trades.append(trade)
    
    def get_metrics(self) -> Dict:
        return {"total_trades": len(self.trades)}