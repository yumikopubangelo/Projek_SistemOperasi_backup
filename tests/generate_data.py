import requests
import time
import random
import argparse
from datetime import datetime
from colorama import init, Fore, Style

init(autoreset=True)

# ==================== CONFIGURATION ====================
BASE_URL = "http://localhost:5000"
SECRET_KEY = "AAEAAWVsYXN0aWMva5liYW5hL2Vucm9sbC1wcm9jZXNzLXRva2VuLTE3NjE4NzM2OTgzNjY6bTJVX0R5eERST3VxUFpPOWotY2lHZQ"

# Statistics
total_sent = 0
total_anomalies = 0
total_normal = 0
failed_requests = 0

# ==================== DATA GENERATION ====================

class SensorDataGenerator:
    """Generate realistic sensor data with anomalies"""
    
    def __init__(self, anomaly_rate=0.7):
        self.anomaly_rate = anomaly_rate
        
        # Normal ranges
        self.normal_tds_range = (50, 300)
        self.normal_turbidity_range = (0.5, 4.5)
        self.normal_temp_range = (25, 32)
        
        # Anomaly thresholds from config
        self.tds_high = 500
        self.tds_critical = 700
        self.turbidity_high = 5.0
        self.turbidity_critical = 10.0
        
        # Depot IDs
        self.depot_ids = [f"DEPOT_{i:03d}" for i in range(1, 11)]
    
    def generate_normal(self):
        """Generate normal data"""
        return {
            "depot_id": random.choice(self.depot_ids),
            "tds_ppm": round(random.uniform(*self.normal_tds_range), 2),
            "kekeruhan_ntu": round(random.uniform(*self.normal_turbidity_range), 2),
            "suhu_celsius": round(random.uniform(*self.normal_temp_range), 2)
        }
    
    def generate_tds_anomaly(self, severity='random'):
        """Generate TDS anomaly"""
        if severity == 'random':
            severity = random.choice(['medium', 'high', 'critical', 'critical'])  # More critical
        
        if severity == 'medium':
            tds = round(random.uniform(self.tds_high, self.tds_critical), 2)
        elif severity == 'high':
            tds = round(random.uniform(self.tds_critical, 900), 2)
        else:  # critical
            tds = round(random.uniform(900, 1500), 2)
        
        return {
            "depot_id": random.choice(self.depot_ids),
            "tds_ppm": tds,
            "kekeruhan_ntu": round(random.uniform(*self.normal_turbidity_range), 2),
            "suhu_celsius": round(random.uniform(*self.normal_temp_range), 2)
        }, 'TDS', severity, tds
    
    def generate_turbidity_anomaly(self, severity='random'):
        """Generate Turbidity anomaly"""
        if severity == 'random':
            severity = random.choice(['medium', 'high', 'critical', 'critical'])
        
        if severity == 'medium':
            turb = round(random.uniform(self.turbidity_high, self.turbidity_critical), 2)
        elif severity == 'high':
            turb = round(random.uniform(self.turbidity_critical, 20.0), 2)
        else:  # critical
            turb = round(random.uniform(20.0, 50.0), 2)
        
        return {
            "depot_id": random.choice(self.depot_ids),
            "tds_ppm": round(random.uniform(*self.normal_tds_range), 2),
            "kekeruhan_ntu": turb,
            "suhu_celsius": round(random.uniform(*self.normal_temp_range), 2)
        }, 'Turbidity', severity, turb
    
    def generate_combined_anomaly(self):
        """Generate both TDS and Turbidity anomalies"""
        tds = round(random.uniform(self.tds_high, 1200), 2)
        turb = round(random.uniform(self.turbidity_high, 30.0), 2)
        
        return {
            "depot_id": random.choice(self.depot_ids),
            "tds_ppm": tds,
            "kekeruhan_ntu": turb,
            "suhu_celsius": round(random.uniform(*self.normal_temp_range), 2)
        }, 'Combined', 'critical', f"TDS={tds:.1f}, Turb={turb:.1f}"
    
    def generate(self):
        """Generate data based on anomaly rate"""
        is_anomaly = random.random() < self.anomaly_rate
        
        if not is_anomaly:
            return self.generate_normal(), False, None, None, None
        
        # Choose anomaly type (weighted towards TDS)
        anomaly_type = random.choices(
            ['tds', 'turbidity', 'combined'],
            weights=[0.5, 0.3, 0.2]  # 50% TDS, 30% Turbidity, 20% Combined
        )[0]
        
        if anomaly_type == 'tds':
            return *self.generate_tds_anomaly(), True
        elif anomaly_type == 'turbidity':
            return *self.generate_turbidity_anomaly(), True
        else:
            return *self.generate_combined_anomaly(), True

# ==================== SENDER ====================

def send_data(data, base_url):
    """Send data to server"""
    global total_sent, total_anomalies, total_normal, failed_requests
    
    headers = {
        "Authorization": SECRET_KEY,
        "Content-Type": "application/json"
    }
    
    try:
        response = requests.post(
            f"{base_url}/sensor",
            json=data,
            headers=headers,
            timeout=5
        )
        
        if response.status_code in [200, 201]:
            total_sent += 1
            return True, response.json().get('anomaly_detected', False)
        else:
            failed_requests += 1
            return False, False
    except Exception as e:
        failed_requests += 1
        return False, False

def print_status_line(data, is_anomaly, anomaly_type, severity, value, server_detected):
    """Print colored status line"""
    timestamp = datetime.now().strftime("%H:%M:%S")
    
    if is_anomaly:
        if anomaly_type == 'Combined':
            print(f"{Fore.RED}[{timestamp}] ANOMALY - {anomaly_type} ({value}) - Server: {server_detected}{Style.RESET_ALL}")
        else:
            print(f"{Fore.RED}[{timestamp}] ANOMALY - {anomaly_type}: {value:.1f} ({severity}) - Server: {server_detected}{Style.RESET_ALL}")
    else:
        print(f"{Fore.GREEN}[{timestamp}] NORMAL - TDS: {data['tds_ppm']:.1f}, Turb: {data['kekeruhan_ntu']:.1f}{Style.RESET_ALL}")

def print_statistics():
    """Print current statistics"""
    print(f"\n{Fore.CYAN}{'='*70}")
    print(f"STATISTICS")
    print(f"{'='*70}{Style.RESET_ALL}")
    print(f"Total Sent:        {total_sent}")
    print(f"{Fore.GREEN}Normal Data:       {total_normal} ({total_normal/total_sent*100 if total_sent > 0 else 0:.1f}%){Style.RESET_ALL}")
    print(f"{Fore.RED}Anomalies:         {total_anomalies} ({total_anomalies/total_sent*100 if total_sent > 0 else 0:.1f}%){Style.RESET_ALL}")
    print(f"{Fore.YELLOW}Failed Requests:   {failed_requests}{Style.RESET_ALL}")
    print(f"{Fore.CYAN}{'='*70}{Style.RESET_ALL}\n")

# ==================== MAIN ====================

def main():
    global total_sent, total_anomalies, total_normal
    
    parser = argparse.ArgumentParser(description='Generate sensor data with anomalies')
    parser.add_argument('--anomaly', type=float, default=0.7, help='Anomaly rate (0.0-1.0, default: 0.7)')
    parser.add_argument('--interval', type=float, default=5, help='Seconds between sends (default: 5)')
    parser.add_argument('--duration', type=int, default=None, help='Duration in seconds (default: unlimited)')
    parser.add_argument('--url', type=str, default=BASE_URL, help=f'Server URL (default: {BASE_URL})')
    
    args = parser.parse_args()
    
    # Use args.url instead of modifying global
    server_url = args.url
    
    # Validate anomaly rate
    if not 0 <= args.anomaly <= 1:
        print(f"{Fore.RED}Error: Anomaly rate must be between 0.0 and 1.0{Style.RESET_ALL}")
        return
    
    # Print header
    print(f"{Fore.MAGENTA}{Style.BRIGHT}")
    print("""
    ╔══════════════════════════════════════════════════════════════╗
    ║                                                              ║
    ║           AQUAGUARD CONTINUOUS DATA GENERATOR               ║
    ║                                                              ║
    ╚══════════════════════════════════════════════════════════════╝
    """)
    print(f"{Style.RESET_ALL}")
    
    print(f"{Fore.CYAN}Configuration:{Style.RESET_ALL}")
    print(f"  Server URL:        {server_url}")
    print(f"  Anomaly Rate:      {args.anomaly*100:.0f}%")
    print(f"  Send Interval:     {args.interval}s")
    print(f"  Duration:          {'Unlimited' if args.duration is None else f'{args.duration}s'}")
    print(f"\n{Fore.YELLOW}Press Ctrl+C to stop{Style.RESET_ALL}\n")
    
    # Initialize generator
    generator = SensorDataGenerator(anomaly_rate=args.anomaly)
    
    # Check server connectivity
    try:
        response = requests.get(f"{server_url}/health", timeout=5)
        if response.status_code == 200:
            print(f"{Fore.GREEN}✓ Server is online{Style.RESET_ALL}\n")
        else:
            print(f"{Fore.YELLOW}⚠ Server returned status {response.status_code}{Style.RESET_ALL}\n")
    except Exception as e:
        print(f"{Fore.RED}✗ Cannot connect to server: {e}{Style.RESET_ALL}")
        print(f"{Fore.YELLOW}Continuing anyway...{Style.RESET_ALL}\n")
    
    # Start sending
    start_time = time.time()
    last_stats_time = start_time
    
    try:
        while True:
            # Check duration
            if args.duration and (time.time() - start_time) >= args.duration:
                print(f"\n{Fore.CYAN}Duration reached. Stopping...{Style.RESET_ALL}")
                break
            
            # Generate data
            data, is_anomaly, anomaly_type, severity, value = generator.generate()
            
            # Track statistics
            if is_anomaly:
                total_anomalies += 1
            else:
                total_normal += 1
            
            # Send data (pass server_url as parameter)
            success, server_detected = send_data(data, server_url)
            
            # Print status
            if success:
                print_status_line(data, is_anomaly, anomaly_type, severity, value, server_detected)
            else:
                print(f"{Fore.RED}[ERROR] Failed to send data{Style.RESET_ALL}")
            
            # Print statistics every 30 seconds
            if time.time() - last_stats_time >= 30:
                print_statistics()
                last_stats_time = time.time()
            
            # Wait for next interval
            time.sleep(args.interval)
    
    except KeyboardInterrupt:
        print(f"\n{Fore.YELLOW}Stopped by user{Style.RESET_ALL}")
    
    # Final statistics
    print_statistics()
    
    duration = time.time() - start_time
    rate = total_sent / duration if duration > 0 else 0
    
    print(f"{Fore.CYAN}Session Summary:{Style.RESET_ALL}")
    print(f"  Duration:          {duration:.1f}s")
    print(f"  Send Rate:         {rate:.2f} samples/second")
    print(f"  Success Rate:      {(total_sent/(total_sent+failed_requests)*100) if (total_sent+failed_requests) > 0 else 0:.1f}%")
    print(f"\n{Fore.GREEN}Data generation complete!{Style.RESET_ALL}\n")

if __name__ == "__main__":
    main()