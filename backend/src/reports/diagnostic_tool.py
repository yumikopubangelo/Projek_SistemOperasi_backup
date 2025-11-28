"""
IoT System Diagnostic Tool
Checks all components and identifies issues
"""

import requests
import json
from datetime import datetime
from colorama import init, Fore, Style

init(autoreset=True)

class IoTDiagnostic:
    def __init__(self, base_url="https://api.aquaguard.sbs"):
        self.base_url = base_url
        self.results = []
        
    def print_header(self, text):
        print(f"\n{Fore.CYAN}{'='*70}")
        print(f"{Fore.CYAN}{text}")
        print(f"{Fore.CYAN}{'='*70}")
    
    def print_success(self, text):
        print(f"{Fore.GREEN}✓ {text}")
        
    def print_error(self, text):
        print(f"{Fore.RED}✗ {text}")
        
    def print_warning(self, text):
        print(f"{Fore.YELLOW}⚠ {text}")
    
    def test_endpoint(self, endpoint, name, timeout=5):
        """Test individual endpoint"""
        url = f"{self.base_url}{endpoint}"
        try:
            response = requests.get(url, timeout=timeout)
            
            if response.status_code == 200:
                data = response.json()
                self.print_success(f"{name}: OK ({response.elapsed.total_seconds():.2f}s)")
                return True, data
            else:
                self.print_error(f"{name}: HTTP {response.status_code}")
                return False, None
                
        except requests.Timeout:
            self.print_error(f"{name}: TIMEOUT (>{timeout}s)")
            return False, None
        except requests.ConnectionError:
            self.print_error(f"{name}: CONNECTION REFUSED")
            return False, None
        except Exception as e:
            self.print_error(f"{name}: {str(e)}")
            return False, None
    
    def check_health(self):
        """Check system health"""
        self.print_header("1. HEALTH CHECK")
        success, data = self.test_endpoint("/health", "System Health")
        
        if success and data:
            print(f"   Services:")
            services = data.get('services', {})
            for service, status in services.items():
                if status:
                    self.print_success(f"   {service}: Active")
                else:
                    self.print_error(f"   {service}: Inactive")
    
    def check_stats(self):
        """Check stats endpoint"""
        self.print_header("2. STATS CHECK")
        success, data = self.test_endpoint("/stats", "Stats Endpoint")
        
        if success and data:
            print(f"\n   Buffer Stats:")
            buffer = data.get('buffer', {})
            print(f"   - Current Size: {buffer.get('current_size', 'N/A')}")
            print(f"   - Total Sent: {buffer.get('total_sent', 'N/A')}")
            print(f"   - Pending: {buffer.get('pending', 'N/A')}")
            
            print(f"\n   Telegram Stats:")
            telegram = data.get('telegram', {})
            print(f"   - Total Sent: {telegram.get('total_sent', 'N/A')}")
            print(f"   - Status: {telegram.get('status', 'Active')}")
            
            # Check timestamp format
            timestamp = data.get('timestamp')
            if timestamp:
                try:
                    dt = datetime.fromisoformat(timestamp.replace('Z', '+00:00'))
                    self.print_success(f"   Timestamp format: OK ({dt})")
                except:
                    self.print_error(f"   Timestamp format: INVALID ({timestamp})")
    
    def check_latest_data(self):
        """Check latest sensor data"""
        self.print_header("3. LATEST SENSOR DATA")
        success, data = self.test_endpoint("/data/latest", "Latest Data")
        
        if success and data:
            sensor_data = data.get('data', {})
            
            if sensor_data:
                print(f"\n   Sensor Values:")
                print(f"   - TDS: {sensor_data.get('tds_ppm', 'N/A')} ppm")
                print(f"   - Turbidity: {sensor_data.get('kekeruhan_ntu', 'N/A')} NTU")
                print(f"   - Temperature: {sensor_data.get('suhu_celsius', 'N/A')} °C")
                
                # Check timestamp
                timestamp = sensor_data.get('@timestamp')
                if timestamp:
                    try:
                        dt = datetime.fromisoformat(timestamp.replace('Z', '+00:00'))
                        self.print_success(f"   Data timestamp: {dt}")
                    except:
                        self.print_error(f"   Data timestamp: INVALID ({timestamp})")
                else:
                    self.print_error("   Data timestamp: MISSING")
            else:
                self.print_warning("   No sensor data available")
    
    def check_historical(self):
        """Check historical data"""
        self.print_header("4. HISTORICAL DATA")
        success, data = self.test_endpoint("/data/historical?size=10", "Historical Data")
        
        if success and data:
            count = data.get('count', 0)
            if count > 0:
                self.print_success(f"   Found {count} historical records")
                
                # Check first record
                records = data.get('data', [])
                if records:
                    first = records[0]
                    print(f"\n   First Record:")
                    print(f"   - TDS: {first.get('tds_ppm', 'N/A')}")
                    print(f"   - Turbidity: {first.get('kekeruhan_ntu', 'N/A')}")
                    print(f"   - Temperature: {first.get('suhu_celsius', 'N/A')}")
                    print(f"   - Timestamp: {first.get('@timestamp', 'N/A')}")
            else:
                self.print_warning("   No historical data found")
    
    def check_ml_status(self):
        """Check ML service status"""
        self.print_header("5. ML SERVICE")
        success, data = self.test_endpoint("/ml/status", "ML Status", timeout=10)
        
        if success and data:
            status = data.get('status', 'UNKNOWN')
            message = data.get('message', '')
            
            if status == "NORMAL" or status == "RUNNING":
                self.print_success(f"   Status: {status}")
            elif status == "WARNING":
                self.print_warning(f"   Status: {status}")
            else:
                self.print_error(f"   Status: {status}")
            
            if message:
                print(f"   Message: {message}")
            
            # Check jobs
            jobs = data.get('jobs', [])
            if jobs:
                print(f"\n   ML Jobs:")
                for job in jobs:
                    job_id = job.get('job_id', 'unknown')
                    job_status = job.get('status', 'unknown')
                    print(f"   - {job_id}: {job_status}")
    
    def check_ml_anomalies(self):
        """Check ML anomalies"""
        self.print_header("6. ML ANOMALIES")
        success, data = self.test_endpoint(
            "/ml/anomalies?size=10&hours_back=24", 
            "ML Anomalies",
            timeout=10
        )
        
        if success and data:
            total = data.get('total', 0)
            returned = data.get('returned', 0)
            
            if total > 0:
                self.print_success(f"   Found {total} anomalies (returned {returned})")
                
                anomalies = data.get('anomalies', [])
                if anomalies:
                    print(f"\n   Recent Anomalies:")
                    for i, anom in enumerate(anomalies[:3], 1):
                        score = anom.get('record_score', 0)
                        timestamp = anom.get('timestamp', 'N/A')
                        print(f"   {i}. Score: {score:.1f}, Time: {timestamp}")
            else:
                self.print_warning("   No anomalies detected (this is good!)")
    
    def check_predictions(self):
        """Check prediction service"""
        self.print_header("7. PREDICTIONS")
        success, data = self.test_endpoint(
            "/prediction/filter-rul?hours_back=168",
            "Filter RUL Prediction",
            timeout=10
        )
        
        if success and data:
            status = data.get('status', 'unknown')
            
            if status == "success":
                self.print_success("   Prediction: OK")
                days = data.get('days_remaining')
                health = data.get('filter_health_percent')
                
                if days is not None:
                    print(f"   - Days Remaining: {days}")
                if health is not None:
                    print(f"   - Filter Health: {health}%")
            elif status == "insufficient_data":
                self.print_warning("   Prediction: Insufficient data")
            else:
                self.print_error(f"   Prediction: {status}")
    
    def check_queue(self):
        """Check queue service"""
        self.print_header("8. QUEUE SERVICE")
        success, data = self.test_endpoint("/queue/stats", "Queue Stats")
        
        if success and data:
            stats = data.get('stats', {})
            print(f"\n   Queue Statistics:")
            print(f"   - Queued Tasks: {stats.get('queued_tasks', 0)}")
            print(f"   - Active Tasks: {stats.get('active', 0)}")
            print(f"   - Completed: {stats.get('total_completed', 0)}")
            print(f"   - Failed: {stats.get('total_failed', 0)}")
    
    def run_all_tests(self):
        """Run all diagnostic tests"""
        print(f"\n{Fore.YELLOW}{Style.BRIGHT}IoT SYSTEM DIAGNOSTIC TOOL")
        print(f"{Fore.YELLOW}Target: {self.base_url}")
        print(f"{Fore.YELLOW}Time: {datetime.now()}\n")
        
        self.check_health()
        self.check_stats()
        self.check_latest_data()
        self.check_historical()
        self.check_ml_status()
        self.check_ml_anomalies()
        self.check_predictions()
        self.check_queue()
        
        self.print_header("DIAGNOSTIC COMPLETE")
        print(f"\n{Fore.GREEN}If all checks passed, your system is healthy!")
        print(f"{Fore.YELLOW}If errors found, check the logs and fix the issues above.\n")


if __name__ == "__main__":
    import sys
    
    # Allow custom URL
    base_url = sys.argv[1] if len(sys.argv) > 1 else "https://api.aquaguard.sbs"
    
    diagnostic = IoTDiagnostic(base_url)
    diagnostic.run_all_tests()