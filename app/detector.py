""" 
    pip install pandas numpy lightgbm scikit-learn scipy
"""

import pandas as pd
import lightgbm as lgb
import time
from scipy.stats import entropy, skew, kurtosis
from collections import deque
import json
from tqdm import tqdm
import logging
from configs import EngineConfig
# import pymysql.cursors


class AdvancedDDoSDetector:
    def __init__(self, path = None):
        self.model_name = None
        self.model_path = path
        self.model = None
        self.metadata = None
        self.feature_names = None
        self.real_time_buffer = {}
        self.window_size = EngineConfig.DETECTOR_WINDOW_SIZE  # seconds

        if self.model_path:
            path_parts = self.model_path.split('/')
            self.model_name = path_parts[1]
            self.load_model(self.model_path)


    def load_model (self, path):
        try:
            # Load LightGBM model
            self.model = lgb.Booster(model_file=f"{path}.txt")
            
            # Load metadata
            with open(f"{path}_metadata.json", 'r') as f:
                self.metadata = json.load(f)
            
            self.best_params = self.metadata['best_params']
            self.best_iteration = self.metadata['best_iteration']
            
            logging.info(f"ML Model: {self.model_name} | BEST: {self.best_iteration} | FEATURES: {len(self.metadata['feature_names'])}")
            
            return self
            
        except Exception as e:
            logging.error(f"Error loading model: {str(e)}")
            raise

    
    def extract_advanced_features (self, df, window='1min', utc=True):
        # df['timestamp'] = pd.to_datetime(df['timestamp'], utc=utc)
        # df = df.set_index('timestamp')

        # df['bytes_sent'] = df['bytes_sent'].astype('float32')
        # df['request_time'] = df['request_time'].astype('float32')
        # df['ip'] = df['ip'].astype('category')

        grouped = df.groupby(['ip', pd.Grouper(key='timestamp', freq=window)])
        
        features = []
        for (ip, _), group in grouped:
            feature_dict = self._calculate_features(ip, group, window)
            if feature_dict is not None:
                features.append(feature_dict)
        
        return pd.DataFrame(features)
  

    def _calculate_features(self, ip, group, window='1min'):
        """Calculate features for a single group."""
        try:
            # Basic count features
            request_count = len(group)
            
            # Time series features
            start = time.time()
            timestamps = group.timestamp.sort_values()
            logging.debug(f'timestamps: {time.time() - start}')
            
            start = time.time()
            intervals = timestamps.diff().dt.total_seconds().fillna(0)
            logging.debug(f'intervals: {time.time() - start}')
            
            # Advanced statistical features
            start = time.time()
            bytes_stats = group.bytes_sent.agg(['mean', 'std', 'min', 'max', 'skew', 'kurt'])
            time_stats = group.request_time.agg(['mean', 'std', 'min', 'max', 'skew', 'kurt'])
            logging.debug(f'Advanced statistical features: {time.time() - start}')

            # Extract methods, paths, and versions only once
            start = time.time()
            request_parts = group.request.str.extract(r'(\S+)\s+(\S+)(?:\s+(\S+))?', expand=False)
            methods = request_parts[0]
            paths = request_parts[1]
            versions = request_parts[2]
            logging.debug(f'Extract methods, paths: {time.time() - start}')
            
            # Request pattern analysis
            start = time.time()
            method_counts = methods.value_counts()
            path_counts = paths.value_counts()
            logging.debug(f'Request pattern analysis: {time.time() - start}')
            
            # Calculate unique paths ratio
            start = time.time()
            total_requests = len(paths)
            unique_paths = paths.nunique()
            unique_paths_ratio = unique_paths / total_requests if total_requests > 0 else 0
            logging.debug(f'Calculate unique paths ratio: {time.time() - start}')
            
            # Calculate various entropies
            start = time.time()
            method_entropy = self.calculate_entropy(method_counts)
            path_entropy = self.calculate_entropy(path_counts)
            status_entropy = self.calculate_entropy(group.status.value_counts())
            logging.debug(f'Calculate various entropies: {time.time() - start}')
            
            # Burst analysis
            start = time.time()
            burst_features = self.calculate_burst_features(intervals)
            logging.debug(f'Burst analysis: {time.time() - start}')
            
            # Protocol analysis
            start = time.time()
            protocol_features = self.analyze_protocols(methods, versions)
            logging.debug(f'Protocol analysis: {time.time() - start}')

            # Error rates
            start = time.time()
            error_status = group.status
            error_rate = (error_status >= 400).mean()
            error_4xx_rate = ((error_status >= 400) & (error_status < 500)).mean()
            error_5xx_rate = (error_status >= 500).mean()
            logging.debug(f'Error rates: {time.time() - start}')

            # Session length and requests per second
            start = time.time()
            session_duration = (timestamps.iloc[-1] - timestamps.iloc[0]).total_seconds()
            request_rate = request_count / self.window_size
            
            # Use actual time span between first and last request for more precise RPS calculation
            requests_per_second = request_count / session_duration if session_duration > 0 else request_count

            logging.debug(f'Session length and requests per second: {time.time() - start}')
            
            """ 
            timestamps: 0.0
            intervals: 0.0009944438934326172
            Advanced statistical features: 0.002001523971557617
            Extract methods, paths: 0.0009992122650146484
            Request pattern analysis: 0.0
            Calculate unique paths ratio: 0.0
            Calculate various entropies: 0.001010894775390625
            Burst analysis: 0.0
            Protocol analysis: 0.0
            Error rates: 0.0009951591491699219
            Session length and requests per second: 0.0
            TOTAL: ~0.008
            """
            
            return {
                'ip': ip,
                'timestamp': timestamps.iloc[-1],
                'request_count': request_count,
                'request_rate': request_rate,
                'unique_paths': unique_paths,
                'unique_paths_ratio': unique_paths_ratio,
                'path_entropy': path_entropy,
                'method_entropy': method_entropy,
                'status_entropy': status_entropy,
                'interval_mean': intervals.mean(),
                'interval_std': intervals.std(),
                'interval_skew': skew(intervals),
                'interval_kurt': kurtosis(intervals),
                **burst_features,
                **protocol_features,
                'bytes_mean': bytes_stats['mean'],
                'bytes_std': bytes_stats['std'],
                'bytes_skew': bytes_stats['skew'],
                'bytes_kurt': bytes_stats['kurt'],
                'bytes_min': bytes_stats['min'],
                'bytes_max': bytes_stats['max'],
                'time_mean': time_stats['mean'],
                'time_std': time_stats['std'],
                'time_skew': time_stats['skew'],
                'time_kurt': time_stats['kurt'],
                'time_min': time_stats['min'],
                'time_max': time_stats['max'],
                'error_rate': error_rate,
                'error_4xx_rate': error_4xx_rate,
                'error_5xx_rate': error_5xx_rate,
                'session_length': session_duration,
                'requests_per_second': requests_per_second,
                # 'requests_per_minute': requests_per_minute
            }
        except Exception as e:
            logging.error(f"Error calculating features for IP {ip}: {str(e)}")
            return None

        
    @staticmethod
    def calculate_burst_features (intervals):
        """Calculate burst-related features using vectorized operations."""
        if len(intervals) < 2:
            return {
                'burst_rate': 0,
                'burst_duration_mean': 0,
                'burst_size_mean': 0
            }

        # Define burst threshold
        burst_threshold = 0.1

        # Identify bursts
        in_burst = intervals <= burst_threshold
        burst_change = in_burst.ne(in_burst.shift()).cumsum()
        burst_groups = intervals[in_burst].groupby(burst_change[in_burst])

        burst_sizes = burst_groups.size()
        burst_durations = burst_groups.sum()

        return {
            'burst_rate': len(burst_sizes) / len(intervals),
            'burst_duration_mean': burst_durations.mean() if not burst_durations.empty else 0,
            'burst_size_mean': burst_sizes.mean() if not burst_sizes.empty else 0
        }
    
    @staticmethod
    def analyze_protocols (methods, versions):
        """Analyze protocol-specific patterns without redundant computations."""
        # Calculate method ratios
        method_total = len(methods)
        get_ratio = (methods == 'GET').sum() / method_total if method_total > 0 else 0
        post_ratio = (methods == 'POST').sum() / method_total if method_total > 0 else 0
        other_method_ratio = 1 - get_ratio - post_ratio

        # Calculate version ratios
        version_total = len(versions.dropna())
        http1_ratio = versions.str.contains('HTTP/1', na=False).sum() / version_total if version_total > 0 else 0
        http2_ratio = versions.str.contains('HTTP/2', na=False).sum() / version_total if version_total > 0 else 0

        return {
            'get_ratio': get_ratio,
            'post_ratio': post_ratio,
            'other_method_ratio': other_method_ratio,
            'http1_ratio': http1_ratio,
            'http2_ratio': http2_ratio
        }
    
    @staticmethod
    def calculate_entropy (series):
        """Calculate Shannon entropy of a series."""
        if len(series) <= 1:
            return 0
        probabilities = series / series.sum()
        return entropy(probabilities)


    def predict (self, X):
        try:
            prediction = self.model.predict(
                X,
                num_threads=1,
                # device='gpu',  # Specify GPU usage
                # gpu_platform_id=0,  # GPU platform ID
                # gpu_device_id=0,   # GPU device ID
                # gpu_use_dp=True    # Use double precision for GPU
            )
            
            is_ddos = (prediction[0] >= EngineConfig.DETECTOR_TRESHOLD_VALUE).astype(int)
            probas = (1 - prediction[0], prediction[0])
           
            return (is_ddos, probas)

        except Exception as e:
            print(f"Error during prediction: {str(e)}")
            raise


    def prepare_data(self, features_df):
        """Prepare features for prediction."""
        X = features_df.drop(['ip', 'timestamp'], axis=1)
        # self.feature_names = X.columns
        return X.fillna(0)


    def update_real_time_buffer(self, request_data):
        """Update real-time feature buffer for an IP."""
        ip = request_data['ip']
        timestamp = request_data['timestamp']
        
        # Initialize buffer for new IP
        if ip not in self.real_time_buffer:
            self.real_time_buffer[ip] = deque(maxlen=1000)  # Limit buffer size
            
        # Add new request
        self.real_time_buffer[ip].append(request_data)
        
        # Remove old requests
        cutoff_time = timestamp - pd.Timedelta(seconds=self.window_size)
        while self.real_time_buffer[ip] and self.real_time_buffer[ip][0]['timestamp'] < cutoff_time:
            self.real_time_buffer[ip].popleft()
 

    def predict_ip (self, ip_data):
        # Convert buffer to DataFrame
        # start = time.time()
        ip_data = pd.DataFrame.from_dict(ip_data)
        # print(f'DataFrame: {time.time() - start}')
    
        # print(len(ip_data))
        
        # Extract features
        # start = time.time()
        features = self.extract_advanced_features(ip_data, window='1min', utc=False)
        # print(f'Extract features: {time.time() - start}')
        # print(features.head())

        last_features = []
        
        # Make prediction
        if len(features) > 0:
            last_features = features.iloc[-1:]

            # start = time.time()
            X = self.prepare_data(last_features)
            # print(f'prepare_data: {time.time() - start}')

            # start = time.time()
            prediction, probabilities = self.predict(X)
            # print(f'predict: {time.time() - start}')

            return prediction, probabilities, last_features
        
        return False
    
    
    def predict_real_time (self, request_data):
        """Make real-time prediction for a single request."""

        # Update buffer
        # start = time.time()
        self.update_real_time_buffer(request_data)
        # print(f'Update buffer: {time.time() - start}')
        
        # Convert buffer to DataFrame
        # start = time.time()
        ip_data = pd.DataFrame(list(self.real_time_buffer[request_data['ip']]))
        # print(f'DataFrame: {time.time() - start}')

        # print(len(ip_data))
        
        # Extract features
        # start = time.time()
        features = self.extract_advanced_features(ip_data, window='1min', utc=True)
        # print(f'Extract features: {time.time() - start}')
        # print(features.head())

        last_features = []
        
        # Make prediction
        if len(features) > 0:
            last_features = features.iloc[-1:]

            # start = time.time()
            X = self.prepare_data(last_features)
            # print(f'prepare_data: {time.time() - start}')

            # start = time.time()
            prediction, probabilities = self.predict(X)
            # print(f'predict: {time.time() - start}')

            return prediction, probabilities, last_features

        """
        --------------------------------------------------
        Update buffer: 0.0
        DataFrame: 0.0
        Extract features: 0.00803232192993164
        prepare_data: 0.0
        predict: 0.0
        --------------------------------------------------
        0.022/sec -> 2727/min
        """
        
        return False


    def predict_test (self, logfile):

        """ mysql = pymysql.connect(
            host='127.0.0.1',
            user='root',
            database='ddos_detector',
            password='',
            cursorclass=pymysql.cursors.DictCursor,
            autocommit=True
        ) """

        data = pd.read_csv(logfile, parse_dates=['timestamp'])

        print(f"Records: { len(data) }")
        print("-"*100)

        pbar = tqdm(data.iterrows())
        detects = 0

        for i, x in pbar:
            if i >= 1700:
                break

            ip = x['ip']
            timestamp = x['timestamp']
            request = x['request']
            status = x['status']
            bytes = x['bytes_sent']
            rtime = x['request_time']

            request_data = {
                'ip': ip,
                'timestamp': timestamp,
                'request': request,
                'status': status,
                'bytes_sent': bytes,
                'request_time': rtime
            }
            
            start = time.time()
            is_ddos, (normal_probability, ddos_probability), last_feature = self.predict_real_time(request_data)
            # quit()
            speed = time.time() - start

            pbar.set_postfix({
                'speed': speed,
                'DDoS': detects,
            })

            if is_ddos:
                detects += 1
                line = i+1
                ts_format = timestamp.strftime('%Y-%m-%d %H:%M:%S')

                # mysql.ping(reconnect=True)
                # with mysql.cursor() as cursor:
                #     cursor.execute("INSERT IGNORE INTO detects VALUES (NULL, NOW(), %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)", (
                #         ts_format,
                #         self.model_name,
                #         line,
                #         logfile,
                #         ip,
                #         ddos_probability,
                #         normal_probability,
                #         x.to_json(),
                #         last_feature.to_json(),
                #         speed,
                #         pbar.format_dict['rate'],
                #     ))

                print(  f"#{line} | {ts_format} | "
                        f"Status: {status} | "
                        f"IP: {ip} | "  
                        f"Is DDoS: {is_ddos} | "  
                        f"DDoS: {round(ddos_probability * 100)}% | "
                        f"Normal: {round(normal_probability * 100)}%"
                )
                # print('-'*100)
                # print(x)
                # print('-'*100)
                # print(last_feature)
                # print('-'*100)

    
# Example usage
if __name__ == "__main__":
    pass
    # model = 'lgbm-20m-bytes-100k-ips-hp-tune' # @1700: 22 @ 130 rps
    # model = 'lgbm-20m-bytes-100k-ips-research' # [00:13, 128.74it/s, speed=0.007, DDoS=25]
    # model_path = f'models/{model}/ddos_detector'

    # Initialize detector
    # detector = AdvancedDDoSDetector(model_path)
    # detector.predict_test(logfile='d:/ddos/normal-ddos-200k.csv')