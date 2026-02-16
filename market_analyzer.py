import logging
from typing import Dict, Optional, List
from datetime import datetime, timedelta
import requests

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

class MarketAnalyzer:
    """
    A class to analyze market trends and provide insights for business model evolution.
    
    Attributes:
        api_keys: Dictionary containing API keys for different data sources.
        data_sources: List of tuples representing (source_name, source_type).
        max_retries: Number of retry attempts for failed API calls.
    """
    
    def __init__(self):
        self.api_keys = {
            'alphavantage': 'YOUR_API_KEY',
            'iexcloud': 'YOUR_API_KEY'
        }
        self.data_sources = [('alphavantage', 'finance'), ('newsapi', 'news')]
        self.max_retries = 3
    
    def fetch_data(self, source: str) -> Dict:
        """
        Fetches market data from a specified source.
        
        Args:
            source (str): Name of the data source to fetch from.
            
        Returns:
            Dict: Parsed response data if successful.
            
        Raises:
            requests.exceptions.RequestException: If fetching fails beyond max_retries.
        """
        try:
            if source == 'alphavantage':
                params = {
                    'function': 'TIME_SERIES_DAILY',
                    'symbol': 'AAPL',
                    'apikey': self.api_keys['alphavantage']
                }
                response = requests.get('https://www.alphavantage.co/query', params=params)
            elif source == 'iexcloud':
                params = {
                    'symbols': 'AAPL',
                    'type': 'daily'
                }
                response = requests.get('https://api.iexcloud.io/v1/batch', params=params)
            elif source == 'newsapi':
                params = {
                    'q': 'finance',
                    'apiKey': self.api_keys['.newsapi']
                }
                response = requests.get('https://www.newsapi.org/v2/top-headlines', params=params)
            
            response.raise_for_status()
            return response.json()
        except requests.exceptions.RequestException as e:
            logger.error(f"Failed to fetch data from {source}: {str(e)}")
            if self.max_retries > 0:
                logger.info(f"Retrying in 5 seconds... ({self.max_retries} attempts left)")
                import time
                time.sleep(5)
                return self.fetch_data(source)
            raise
    
    def analyze_trends(self) -> Dict:
        """
        Analyzes market trends across all registered data sources.
        
        Returns:
            Dict: Analysis results including sentiment and trend indicators.
        """
        analysis = {'timestamp': datetime.now().isoformat()}
        for source, source_type in self.data_sources:
            try:
                data = self.fetch_data(source)
                if source_type == 'finance':
                    # Process financial data
                    current_price = data['Time Series (Daily)']['4. close']
                    analysis[source] = {
                        'price': float(current_price),
                        'trend': self._calculate_trend(data)
                    }
                elif source_type == 'news':
                    # Process news sentiment
                    articles = data.get('articles', [])
                    positive_count = sum(1 for article in articles if 'bullish' in article['title'].lower())
                    total_articles = len(articles)
                    analysis[source] = {
                        'sentiment': positive_count / total_articles if total_articles > 0 else 0
                    }
            except Exception as e:
                logger.error(f"Error processing {source}: {str(e)}")
        
        return analysis
    
    def _calculate_trend(self, data: Dict) -> str:
        """
        Internal method to calculate the trend based on historical prices.
        
        Args:
            data (Dict): Parsed response from a financial data source.
            
        Returns:
            str: 'upward', 'downward', or 'neutral' trend.
        """
        try:
            prices = [float(day['4. close']) for day in data['Time Series (Daily)'].values()]
            if len(prices) < 2:
                return 'neutral'
            
            # Simple moving average
            sma = sum(prices[-3:]) / 3
            current_price = prices[-1]
            if current_price > sma:
                return 'upward'
            elif current_price < sma:
                return 'downward'
            else:
                return 'neutral'
        except (KeyError, IndexError) as e:
            logger.error(f"Failed to calculate trend: {str(e)}")
            return 'neutral'

# Example usage
if __name__ == '__main__':
    analyzer = MarketAnalyzer()
    result = analyzer.analyze_trends()
    logger.info(result)