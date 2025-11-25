import numpy as np
from datetime import datetime, timedelta
from scipy import stats
from collections import deque

class WaterQualityPredictor:
    """Prediction engine for water quality forecasting"""
    
    def __init__(self):
        self.min_data_points = 10  # Minimum data untuk prediksi
        
    def predict_time_to_threshold(self, historical_data, parameter='tds_ppm', 
                                  threshold=700, current_value=None):
        """
        Prediksi berapa lama parameter mencapai threshold
        
        Returns:
        {
            'will_reach_threshold': bool,
            'estimated_hours': float,
            'estimated_date': str,
            'confidence': float,
            'trend': 'rising' | 'stable' | 'falling',
            'current_value': float,
            'threshold': float
        }
        """
        
        if len(historical_data) < self.min_data_points:
            return {
                'status': 'insufficient_data',
                'message': f'Need at least {self.min_data_points} data points'
            }
        
        # Extract values and timestamps
        values = [d[parameter] for d in historical_data]
        timestamps = [datetime.fromisoformat(d['@timestamp'].replace('Z', '+00:00')) 
                     for d in historical_data]
        
        # Convert timestamps to hours since first reading
        hours = [(t - timestamps[0]).total_seconds() / 3600 for t in timestamps]
        
        # Linear regression
        slope, intercept, r_value, p_value, std_err = stats.linregress(hours, values)
        
        # Current value
        if current_value is None:
            current_value = values[-1]
        
        # Determine trend
        if slope > 1:  # Increasing more than 1 ppm/hour
            trend = 'rising'
        elif slope < -1:
            trend = 'falling'
        else:
            trend = 'stable'
        
        # Calculate time to threshold
        will_reach = False
        estimated_hours = None
        estimated_date = None
        confidence = abs(r_value)  # R-squared as confidence
        
        if trend == 'rising' and current_value < threshold:
            # How many hours until reaching threshold?
            hours_to_threshold = (threshold - current_value) / slope
            
            if hours_to_threshold > 0 and hours_to_threshold < 720:  # Max 30 days
                will_reach = True
                estimated_hours = hours_to_threshold
                estimated_date = (datetime.now() + timedelta(hours=hours_to_threshold)).isoformat()
        
        return {
            'status': 'success',
            'will_reach_threshold': will_reach,
            'estimated_hours': round(estimated_hours, 1) if estimated_hours else None,
            'estimated_days': round(estimated_hours / 24, 1) if estimated_hours else None,
            'estimated_date': estimated_date,
            'confidence': round(confidence * 100, 1),
            'trend': trend,
            'slope': round(slope, 4),
            'current_value': round(current_value, 2),
            'threshold': threshold,
            'r_squared': round(r_value ** 2, 3)
        }
    
    def predict_next_value(self, historical_data, parameter='tds_ppm', 
                          hours_ahead=1, method='linear'):
        """
        Prediksi nilai parameter X jam ke depan
        
        Methods:
        - 'linear': Linear regression
        - 'sma': Simple Moving Average
        - 'ema': Exponential Moving Average
        """
        
        if len(historical_data) < self.min_data_points:
            return {'status': 'insufficient_data'}
        
        values = [d[parameter] for d in historical_data]
        
        if method == 'linear':
            # Linear regression prediction
            x = np.arange(len(values))
            slope, intercept = np.polyfit(x, values, 1)
            
            next_x = len(values) + (hours_ahead / 
                    (5 / 3600))  # Assuming 5-second sampling
            predicted_value = slope * next_x + intercept
            
        elif method == 'sma':
            # Simple Moving Average (last 10 readings)
            window_size = min(10, len(values))
            predicted_value = np.mean(values[-window_size:])
            
        elif method == 'ema':
            # Exponential Moving Average
            alpha = 0.3  # Smoothing factor
            ema = values[0]
            for value in values[1:]:
                ema = alpha * value + (1 - alpha) * ema
            predicted_value = ema
        
        else:
            return {'status': 'error', 'message': 'Unknown method'}
        
        # Calculate prediction interval (±2 standard deviations)
        std_dev = np.std(values[-10:])
        lower_bound = predicted_value - 2 * std_dev
        upper_bound = predicted_value + 2 * std_dev
        
        return {
            'status': 'success',
            'predicted_value': round(predicted_value, 2),
            'lower_bound': round(lower_bound, 2),
            'upper_bound': round(upper_bound, 2),
            'confidence_interval': '95%',
            'method': method,
            'hours_ahead': hours_ahead
        }
    
    def calculate_filter_rul(self, historical_data, parameter='tds_ppm',
                            initial_tds=50, critical_tds=700):
        """
        Calculate Remaining Useful Life of filter
        
        Assumes:
        - Filter starts at initial_tds
        - Filter needs replacement at critical_tds
        - Linear degradation
        """
        
        if len(historical_data) < self.min_data_points:
            return {
                'status': 'insufficient_data',
                'message': f'Need at least {self.min_data_points} data points',
                'days_remaining': None,
                'current_tds': None,
                'tds_increase_rate': None
            }
        
        values = [d[parameter] for d in historical_data]
        timestamps = [datetime.fromisoformat(d['@timestamp'].replace('Z', '+00:00')) 
                     for d in historical_data]
        
        # Calculate degradation rate (ppm per hour)
        time_span_hours = (timestamps[-1] - timestamps[0]).total_seconds() / 3600
        tds_increase = values[-1] - values[0]
        degradation_rate = tds_increase / time_span_hours if time_span_hours > 0 else 0
        
        # Current TDS
        current_tds = values[-1]
        
        # Remaining capacity
        remaining_capacity = critical_tds - current_tds
        
        # Hours until critical
        if degradation_rate > 0:
            hours_remaining = remaining_capacity / degradation_rate
            days_remaining = hours_remaining / 24
        else:
            hours_remaining = float('inf')
            days_remaining = float('inf')
        
        # Health percentage
        filter_health = ((critical_tds - current_tds) / (critical_tds - initial_tds)) * 100
        filter_health = max(0, min(100, filter_health))
        
        # TDS rate per day
        tds_rate_per_day = degradation_rate * 24
        
        return {
            'status': 'success',
            'filter_health_percent': round(filter_health, 1),
            'days_remaining': round(days_remaining, 1) if days_remaining != float('inf') else None,
            'estimated_replacement_date': (datetime.now() + timedelta(days=days_remaining)).strftime('%Y-%m-%d') 
                                         if days_remaining != float('inf') else None,
            'degradation_rate_ppm_per_day': round(tds_rate_per_day, 2),
            'tds_increase_rate': round(tds_rate_per_day, 2),  # For dashboard compatibility
            'current_tds': round(current_tds, 2),
            'critical_tds': critical_tds,
            'recommendation': self._get_recommendation(days_remaining, filter_health)
        }
    
    def _get_recommendation(self, days_remaining, health_percent):
        """Generate actionable recommendation"""
        
        if days_remaining is None or days_remaining == float('inf'):
            return "Filter condition stable, continue monitoring"
        
        if days_remaining < 1:
            return "URGENT: Replace filter immediately!"
        elif days_remaining < 3:
            return "WARNING: Filter replacement needed within 3 days"
        elif days_remaining < 7:
            return "NOTICE: Schedule filter replacement this week"
        elif health_percent < 20:
            return "CAUTION: Filter health below 20%, prepare for replacement"
        else:
            return f"Filter condition good, estimated {int(days_remaining)} days remaining"