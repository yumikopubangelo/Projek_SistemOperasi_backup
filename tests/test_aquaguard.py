"""
AUTOMATED TESTING SUITE WITH ANOMALY GENERATION - AQUAGUARD
Run: python test_aquaguard_with_anomalies.py
"""

import requests
import time
import json
import random
from datetime import datetime
from colorama import init, Fore, Style

# Initialize colorama for colored output
init(autoreset=True)

# ==================== CONFIGURATION ====================
BASE_URL = "http://localhost:5000"
SECRET_KEY = "AAEAAWVsYXN0aWMva5liYW5hL2Vucm9sbC1wcm9jZXNzLXRva2VuLTE3NjE4NzM2OTgzNjY6bTJVX0R5eERST3VxUFpPOWotY2lHZQ"
TIMEOUT = 10

# Anomaly Configuration (60% chance of anomaly)
ANOMALY_RATE = 0.6

# Test counters
tests_run = 0
tests_passed = 0
tests_failed = 0
anomalies_sent = 0
normal_data_sent = 0

# ==================== UTILITY FUNCTIONS ====================
def print_header(title):
    """Print section header"""
    print(f"\n{'='*70}")
    print(f"{Fore.CYAN}{Style.BRIGHT}{title.center(70)}")
    print(f"{'='*70}\n")

def print_test(test_name):
    """Print test name"""
    print(f"{Fore.YELLOW}[TEST] {test_name}...", end=" ")

def print_pass(message=""):
    """Print pass result"""
    global tests_passed
    tests_passed += 1
    print(f"{Fore.GREEN}✓ PASS{Style.RESET_ALL} {message}")

def print_fail(message=""):
    """Print fail result"""
    global tests_failed
    tests_failed += 1
    print(f"{Fore.RED}✗ FAIL{Style.RESET_ALL} {message}")

def print_info(message):
    """Print info message"""
    print(f"{Fore.BLUE}[INFO] {message}{Style.RESET_ALL}")

def print_warning(message):
    """Print warning message"""
    print(f"{Fore.YELLOW}[WARNING] {message}{Style.RESET_ALL}")

def print_anomaly(message):
    """Print anomaly message"""
    print(f"{Fore.RED}[ANOMALY] {message}{Style.RESET_ALL}")

def run_test(test_func):
    """Decorator to track test execution"""
    global tests_run
    tests_run += 1
    try:
        test_func()
    except Exception as e:
        print_fail(f"Exception: {str(e)}")

# ==================== DATA GENERATION ====================

def generate_normal_data():
    """Generate normal sensor data"""
    return {
        "depot_id": f"DEPOT_{random.randint(1, 10):03d}",
        "tds_ppm": round(random.uniform(50, 300), 2),        # Normal: 50-300 ppm
        "kekeruhan_ntu": round(random.uniform(0.5, 4.5), 2), # Normal: 0.5-4.5 NTU
        "suhu_celsius": round(random.uniform(25, 32), 2)     # Normal: 25-32 °C
    }

def generate_tds_anomaly():
    """Generate TDS anomaly (high TDS)"""
    severity = random.choice(['medium', 'high', 'critical'])
    
    if severity == 'medium':
        tds = round(random.uniform(500, 650), 2)  # Above HIGH threshold
    elif severity == 'high':
        tds = round(random.uniform(650, 800), 2)  # Above CRITICAL threshold
    else:
        tds = round(random.uniform(800, 1200), 2) # Very high
    
    return {
        "depot_id": f"DEPOT_{random.randint(1, 10):03d}",
        "tds_ppm": tds,
        "kekeruhan_ntu": round(random.uniform(0.5, 4.5), 2),
        "suhu_celsius": round(random.uniform(25, 32), 2)
    }, 'TDS', severity, tds

def generate_turbidity_anomaly():
    """Generate Turbidity anomaly (high turbidity)"""
    severity = random.choice(['medium', 'high', 'critical'])
    
    if severity == 'medium':
        turbidity = round(random.uniform(5.0, 8.0), 2)    # Above HIGH threshold
    elif severity == 'high':
        turbidity = round(random.uniform(8.0, 15.0), 2)   # Above CRITICAL threshold
    else:
        turbidity = round(random.uniform(15.0, 30.0), 2)  # Very high
    
    return {
        "depot_id": f"DEPOT_{random.randint(1, 10):03d}",
        "tds_ppm": round(random.uniform(50, 300), 2),
        "kekeruhan_ntu": turbidity,
        "suhu_celsius": round(random.uniform(25, 32), 2)
    }, 'Turbidity', severity, turbidity

def generate_combined_anomaly():
    """Generate both TDS and Turbidity anomaly"""
    tds = round(random.uniform(550, 900), 2)
    turbidity = round(random.uniform(6.0, 20.0), 2)
    
    return {
        "depot_id": f"DEPOT_{random.randint(1, 10):03d}",
        "tds_ppm": tds,
        "kekeruhan_ntu": turbidity,
        "suhu_celsius": round(random.uniform(25, 32), 2)
    }, 'Combined', 'high', f"TDS={tds}, Turb={turbidity}"

def generate_sensor_data(force_anomaly=None):
    """
    Generate sensor data with 60% chance of anomaly
    
    Args:
        force_anomaly: None (random), 'normal', 'tds', 'turbidity', 'combined'
    
    Returns:
        tuple: (data_dict, is_anomaly, anomaly_type, severity, value)
    """
    global anomalies_sent, normal_data_sent
    
    if force_anomaly == 'normal':
        normal_data_sent += 1
        return generate_normal_data(), False, None, None, None
    
    # Determine if this should be an anomaly
    is_anomaly = force_anomaly is not None or random.random() < ANOMALY_RATE
    
    if not is_anomaly:
        normal_data_sent += 1
        return generate_normal_data(), False, None, None, None
    
    # Choose anomaly type
    if force_anomaly:
        anomaly_type = force_anomaly
    else:
        anomaly_type = random.choice(['tds', 'tds', 'turbidity', 'combined'])  # More TDS anomalies
    
    anomalies_sent += 1
    
    if anomaly_type == 'tds':
        data, type_name, severity, value = generate_tds_anomaly()
        return data, True, type_name, severity, value
    elif anomaly_type == 'turbidity':
        data, type_name, severity, value = generate_turbidity_anomaly()
        return data, True, type_name, severity, value
    else:  # combined
        data, type_name, severity, value = generate_combined_anomaly()
        return data, True, type_name, severity, value

def send_sensor_data(data, show_details=True):
    """Send sensor data to server"""
    headers = {
        "Authorization": SECRET_KEY,
        "Content-Type": "application/json"
    }
    
    try:
        response = requests.post(f"{BASE_URL}/sensor", json=data, headers=headers, timeout=TIMEOUT)
        
        if show_details:
            if response.status_code == 200 or response.status_code == 201:
                result = response.json()
                anomaly_detected = result.get('anomaly_detected', False)
                
                if anomaly_detected:
                    print_anomaly(f"Data sent with anomaly: TDS={data['tds_ppm']:.1f}, Turb={data['kekeruhan_ntu']:.1f}")
                else:
                    print_info(f"Normal data sent: TDS={data['tds_ppm']:.1f}, Turb={data['kekeruhan_ntu']:.1f}")
        
        return response.status_code in [200, 201]
    except Exception as e:
        if show_details:
            print_fail(f"Error sending data: {str(e)}")
        return False

# ==================== TEST CASES ====================

def test_server_running():
    """Test 1: Server is running and accessible"""
    print_test("Server Running Check")
    try:
        response = requests.get(f"{BASE_URL}/health", timeout=TIMEOUT)
        if response.status_code == 200:
            print_pass(f"Server accessible at {BASE_URL}")
        else:
            print_fail(f"Server returned status {response.status_code}")
    except requests.exceptions.ConnectionError:
        print_fail("Cannot connect to server. Is Flask running?")
    except Exception as e:
        print_fail(f"Error: {str(e)}")

def test_health_endpoint():
    """Test 2: Health endpoint returns correct structure"""
    print_test("Health Endpoint Structure")
    try:
        response = requests.get(f"{BASE_URL}/health", timeout=TIMEOUT)
        data = response.json()
        
        if data.get('status') == 'healthy':
            print_pass(f"Health check OK. ES: {data.get('elasticsearch', 'unknown')}")
        else:
            print_fail(f"Health check failed")
    except Exception as e:
        print_fail(f"Error: {str(e)}")

def test_send_normal_data():
    """Test 3: Send normal sensor data"""
    print_test("Send Normal Data (5 samples)")
    
    success_count = 0
    for i in range(5):
        data, is_anomaly, _, _, _ = generate_sensor_data(force_anomaly='normal')
        if send_sensor_data(data, show_details=False):
            success_count += 1
        time.sleep(0.1)
    
    if success_count == 5:
        print_pass(f"All {success_count} normal data samples sent successfully")
    else:
        print_fail(f"Only {success_count}/5 samples sent successfully")

def test_send_tds_anomalies():
    """Test 4: Send TDS anomalies"""
    print_test("Send TDS Anomalies (10 samples)")
    
    success_count = 0
    for i in range(10):
        data, is_anomaly, anomaly_type, severity, value = generate_sensor_data(force_anomaly='tds')
        if send_sensor_data(data, show_details=False):
            success_count += 1
            print_anomaly(f"  #{i+1} TDS Anomaly: {value:.1f} ppm ({severity})")
        time.sleep(0.15)
    
    if success_count == 10:
        print_pass(f"All {success_count} TDS anomalies sent successfully")
    else:
        print_fail(f"Only {success_count}/10 anomalies sent successfully")

def test_send_turbidity_anomalies():
    """Test 5: Send Turbidity anomalies"""
    print_test("Send Turbidity Anomalies (10 samples)")
    
    success_count = 0
    for i in range(10):
        data, is_anomaly, anomaly_type, severity, value = generate_sensor_data(force_anomaly='turbidity')
        if send_sensor_data(data, show_details=False):
            success_count += 1
            print_anomaly(f"  #{i+1} Turbidity Anomaly: {value:.1f} NTU ({severity})")
        time.sleep(0.15)
    
    if success_count == 10:
        print_pass(f"All {success_count} turbidity anomalies sent successfully")
    else:
        print_fail(f"Only {success_count}/10 anomalies sent successfully")

def test_send_combined_anomalies():
    """Test 6: Send combined anomalies"""
    print_test("Send Combined Anomalies (5 samples)")
    
    success_count = 0
    for i in range(5):
        data, is_anomaly, anomaly_type, severity, value = generate_sensor_data(force_anomaly='combined')
        if send_sensor_data(data, show_details=False):
            success_count += 1
            print_anomaly(f"  #{i+1} Combined Anomaly: {value}")
        time.sleep(0.15)
    
    if success_count == 5:
        print_pass(f"All {success_count} combined anomalies sent successfully")
    else:
        print_fail(f"Only {success_count}/5 anomalies sent successfully")

def test_send_mixed_data():
    """Test 7: Send mixed data (60% anomaly rate)"""
    print_test("Send Mixed Data (50 samples, ~60% anomalies)")
    
    success_count = 0
    anomaly_count = 0
    
    print()  # New line for better readability
    
    for i in range(50):
        data, is_anomaly, anomaly_type, severity, value = generate_sensor_data()
        
        if send_sensor_data(data, show_details=False):
            success_count += 1
            if is_anomaly:
                anomaly_count += 1
        
        # Progress indicator every 10 samples
        if (i + 1) % 10 == 0:
            print_info(f"  Progress: {i+1}/50 samples sent ({anomaly_count} anomalies so far)")
        
        time.sleep(0.1)
    
    anomaly_percentage = (anomaly_count / 50) * 100
    
    if success_count == 50:
        print_pass(f"All 50 samples sent. Anomalies: {anomaly_count}/50 ({anomaly_percentage:.1f}%)")
    else:
        print_fail(f"Only {success_count}/50 samples sent successfully")

def test_data_retrieval():
    """Test 8: Verify data can be retrieved"""
    print_test("Data Retrieval Verification")
    
    try:
        response = requests.get(f"{BASE_URL}/data/latest", timeout=TIMEOUT)
        
        if response.status_code == 200:
            data = response.json()
            if data['status'] == 'success' and 'data' in data:
                sensor_data = data['data']
                print_pass(f"Latest data retrieved: TDS={sensor_data.get('tds_ppm', 'N/A'):.1f} ppm")
            else:
                print_fail("Invalid response structure")
        else:
            print_fail(f"Status: {response.status_code}")
    except Exception as e:
        print_fail(f"Error: {str(e)}")

def test_ml_anomalies():
    """Test 9: Check ML anomalies endpoint"""
    print_test("ML Anomalies Endpoint")
    
    try:
        # Give ML time to process
        time.sleep(2)
        
        response = requests.get(f"{BASE_URL}/ml/anomalies?hours_back=1&size=10", timeout=TIMEOUT)
        
        if response.status_code == 200:
            data = response.json()
            if data.get('status') == 'success':
                count = len(data.get('anomalies', []))
                total = data.get('total', 0)
                print_pass(f"ML endpoint working. Found {count} anomalies (total: {total})")
            else:
                print_info("ML endpoint accessible but no anomalies detected yet")
        else:
            print_fail(f"Status: {response.status_code}")
    except Exception as e:
        print_fail(f"Error: {str(e)}")

def test_stats_endpoint():
    """Test 10: Check system stats"""
    print_test("System Stats Endpoint")
    
    try:
        response = requests.get(f"{BASE_URL}/stats", timeout=TIMEOUT)
        
        if response.status_code == 200:
            data = response.json()
            
            if 'buffer_manager' in data:
                buffer_stats = data['buffer_manager']
                received = buffer_stats.get('total_received', 0)
                flushed = buffer_stats.get('total_flushed', 0)
                pending = buffer_stats.get('pending', 0)
                
                print_pass(f"Stats OK. Received: {received}, Flushed: {flushed}, Pending: {pending}")
            else:
                print_fail("Invalid stats structure")
        else:
            print_fail(f"Status: {response.status_code}")
    except Exception as e:
        print_fail(f"Error: {str(e)}")

def test_stress_test():
    """Test 11: Stress test with rapid data sending"""
    print_test("Stress Test (100 rapid requests)")
    
    success_count = 0
    start_time = time.time()
    
    for i in range(100):
        data, _, _, _, _ = generate_sensor_data()
        if send_sensor_data(data, show_details=False):
            success_count += 1
        
        # Progress every 25 requests
        if (i + 1) % 25 == 0:
            elapsed = time.time() - start_time
            rate = (i + 1) / elapsed
            print_info(f"  Progress: {i+1}/100 ({rate:.1f} req/s)")
    
    duration = time.time() - start_time
    rate = 100 / duration
    
    if success_count >= 95:
        print_pass(f"{success_count}/100 successful. Rate: {rate:.1f} req/s, Time: {duration:.2f}s")
    else:
        print_fail(f"Only {success_count}/100 successful")

# ==================== CONTINUOUS MONITORING MODE ====================

def continuous_monitoring_mode(duration_seconds=60):
    """
    Continuously send data for monitoring/demo purposes
    
    Args:
        duration_seconds: How long to run (default 60 seconds)
    """
    print_header("CONTINUOUS MONITORING MODE")
    print_info(f"Sending data continuously for {duration_seconds} seconds...")
    print_info(f"Anomaly rate: {ANOMALY_RATE*100:.0f}%")
    print_info("Press Ctrl+C to stop\n")
    
    start_time = time.time()
    count = 0
    anomaly_count = 0
    
    try:
        while (time.time() - start_time) < duration_seconds:
            data, is_anomaly, anomaly_type, severity, value = generate_sensor_data()
            
            if send_sensor_data(data, show_details=True):
                count += 1
                if is_anomaly:
                    anomaly_count += 1
            
            time.sleep(2)  # Send every 2 seconds
            
            # Show progress every 10 samples
            if count % 10 == 0:
                elapsed = time.time() - start_time
                print_info(f"Progress: {count} samples sent, {anomaly_count} anomalies ({elapsed:.0f}s elapsed)")
    
    except KeyboardInterrupt:
        print_warning("\nMonitoring stopped by user")
    
    elapsed = time.time() - start_time
    print_info(f"\nMonitoring complete:")
    print_info(f"  Duration: {elapsed:.1f}s")
    print_info(f"  Total samples: {count}")
    print_info(f"  Anomalies: {anomaly_count} ({anomaly_count/count*100:.1f}%)")
    print_info(f"  Rate: {count/elapsed:.2f} samples/second")

# ==================== TEST EXECUTION ====================

def run_all_tests():
    """Execute all tests"""
    print(f"{Fore.MAGENTA}{Style.BRIGHT}")
    print("""
    ╔══════════════════════════════════════════════════════════════╗
    ║                                                              ║
    ║         AQUAGUARD AUTOMATED TEST SUITE WITH ANOMALIES       ║
    ║                                                              ║
    ║              Testing with 60% Anomaly Rate                  ║
    ║                                                              ║
    ╚══════════════════════════════════════════════════════════════╝
    """)
    print(Style.RESET_ALL)
    
    start_time = time.time()
    
    # Phase 1: Basic Connectivity
    print_header("PHASE 1: BASIC CONNECTIVITY")
    run_test(test_server_running)
    run_test(test_health_endpoint)
    
    # Phase 2: Normal Data
    print_header("PHASE 2: NORMAL DATA TRANSMISSION")
    run_test(test_send_normal_data)
    
    # Phase 3: Anomaly Generation
    print_header("PHASE 3: ANOMALY GENERATION")
    run_test(test_send_tds_anomalies)
    run_test(test_send_turbidity_anomalies)
    run_test(test_send_combined_anomalies)
    
    # Phase 4: Mixed Data (Main Test)
    print_header("PHASE 4: MIXED DATA TRANSMISSION (60% ANOMALIES)")
    run_test(test_send_mixed_data)
    
    # Phase 5: Data Verification
    print_header("PHASE 5: DATA VERIFICATION")
    run_test(test_data_retrieval)
    run_test(test_ml_anomalies)
    run_test(test_stats_endpoint)
    
    # Phase 6: Stress Test
    print_header("PHASE 6: STRESS TEST")
    run_test(test_stress_test)
    
    # Summary
    duration = time.time() - start_time
    
    print(f"\n{'='*70}")
    print(f"{Fore.CYAN}{Style.BRIGHT}TEST SUMMARY".center(70))
    print(f"{'='*70}\n")
    
    print(f"Total Tests Run:      {tests_run}")
    print(f"{Fore.GREEN}Tests Passed:         {tests_passed} ({tests_passed/tests_run*100:.1f}%){Style.RESET_ALL}")
    print(f"{Fore.RED}Tests Failed:         {tests_failed} ({tests_failed/tests_run*100:.1f}%){Style.RESET_ALL}")
    print(f"\n{Fore.YELLOW}Data Sent Summary:{Style.RESET_ALL}")
    print(f"  Normal Data:        {normal_data_sent}")
    print(f"  Anomalies:          {anomalies_sent} ({anomalies_sent/(normal_data_sent+anomalies_sent)*100:.1f}%)")
    print(f"  Total:              {normal_data_sent + anomalies_sent}")
    print(f"\nExecution Time:       {duration:.2f} seconds")
    
    print(f"\n{'='*70}\n")
    
    if tests_failed == 0:
        print(f"{Fore.GREEN}{Style.BRIGHT}🎉 ALL TESTS PASSED! System is ready!{Style.RESET_ALL}")
        print(f"{Fore.CYAN}Tip: Check dashboard at {BASE_URL} to see the anomalies!{Style.RESET_ALL}")
        return 0
    else:
        print(f"{Fore.YELLOW}{Style.BRIGHT}⚠️ Some tests failed. Please check and fix issues.{Style.RESET_ALL}")
        return 1

# ==================== MAIN ====================

if __name__ == "__main__":
    import sys
    
    try:
        # Check for continuous mode flag
        if len(sys.argv) > 1 and sys.argv[1] == '--continuous':
            duration = int(sys.argv[2]) if len(sys.argv) > 2 else 60
            continuous_monitoring_mode(duration)
        else:
            exit_code = run_all_tests()
            
            # Ask if user wants to run continuous monitoring
            print(f"\n{Fore.CYAN}Do you want to run continuous monitoring? (y/n): {Style.RESET_ALL}", end="")
            choice = input().strip().lower()
            
            if choice == 'y':
                print(f"{Fore.CYAN}Duration in seconds (default 60): {Style.RESET_ALL}", end="")
                duration_input = input().strip()
                duration = int(duration_input) if duration_input.isdigit() else 60
                continuous_monitoring_mode(duration)
            
            exit(exit_code)
    except KeyboardInterrupt:
        print(f"\n\n{Fore.YELLOW}[INTERRUPTED] Testing stopped by user{Style.RESET_ALL}")
        exit(1)