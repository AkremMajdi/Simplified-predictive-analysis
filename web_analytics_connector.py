from typing import Dict, Any, List, Optional
import pandas as pd
from datetime import datetime, timedelta
import json
from .base_connector import BaseDataConnector

class GoogleAnalyticsConnector(BaseDataConnector):
  
    """
    Connector for Google Analytics Data API (GA4)
    """
    
    def __init__(self, access_token: str, property_id: str):
        """
        Initialize Google Analytics connector
        
        Args:
            access_token: Google Analytics API access token
            property_id: GA4 property ID
        """
        super().__init__(
            api_key=access_token,
            base_url="https://analyticsdata.googleapis.com/v1beta",
            rate_limit_requests=100,  # Google Analytics API rate limits
            rate_limit_window=100  # 100 seconds
        )
        self.property_id = property_id
    
    def _get_headers(self) -> Dict[str, str]:
        """Override to use Google's OAuth token format"""
        headers = super()._get_headers()
        headers['Authorization'] = f'Bearer {self.api_key}'
        return headers
    
    def get_available_datasets(self) -> List[Dict[str, Any]]:
        """
        Get available Google Analytics metrics and dimensions
        
        Returns:
            List of available metrics and dimensions
        """
        return [
            {'name': 'sessions', 'type': 'metric', 'description': 'Total number of sessions'},
            {'name': 'users', 'type': 'metric', 'description': 'Total number of users'},
            {'name': 'pageviews', 'type': 'metric', 'description': 'Total number of pageviews'},
            {'name': 'bounceRate', 'type': 'metric', 'description': 'Bounce rate percentage'},
            {'name': 'sessionDuration', 'type': 'metric', 'description': 'Average session duration'},
            {'name': 'date', 'type': 'dimension', 'description': 'Date dimension'},
            {'name': 'country', 'type': 'dimension', 'description': 'Country dimension'},
            {'name': 'deviceCategory', 'type': 'dimension', 'description': 'Device category'},
            {'name': 'source', 'type': 'dimension', 'description': 'Traffic source'},
            {'name': 'medium', 'type': 'dimension', 'description': 'Traffic medium'}
        ]
    
    def get_data(self, metrics: List[str], dimensions: List[str] = None,
                 start_date: str = '30daysAgo', end_date: str = 'today',
                 **kwargs) -> pd.DataFrame:
        """
        Get Google Analytics data
        
        Args:
            metrics: List of metrics to retrieve
            dimensions: List of dimensions to group by
            start_date: Start date (YYYY-MM-DD or relative like '30daysAgo')
            end_date: End date (YYYY-MM-DD or relative like 'today')
            
        Returns:
            DataFrame with analytics data
        """
        if dimensions is None:
            dimensions = ['date']
        
        # Prepare request body
        request_body = {
            'dateRanges': [{'startDate': start_date, 'endDate': end_date}],
            'metrics': [{'name': metric} for metric in metrics],
            'dimensions': [{'name': dimension} for dimension in dimensions]
        }
        
        endpoint = f'properties/{self.property_id}:runReport'
        response = self._make_request(endpoint, method='POST', data=request_body)
        
        if 'rows' not in response:
            return pd.DataFrame()
        
        # Extract data
        rows = response['rows']
        dimension_headers = [dim['name'] for dim in response.get('dimensionHeaders', [])]
        metric_headers = [met['name'] for met in response.get('metricHeaders', [])]
        
        # Flatten data
        flattened_data = []
        for row in rows:
            row_data = {}
            
            # Add dimensions
            for i, dim_value in enumerate(row.get('dimensionValues', [])):
                row_data[dimension_headers[i]] = dim_value['value']
            
            # Add metrics
            for i, met_value in enumerate(row.get('metricValues', [])):
                row_data[metric_headers[i]] = float(met_value['value'])
            
            flattened_data.append(row_data)
        
        df = pd.DataFrame(flattened_data)
        
        # Convert date column if present
        if 'date' in df.columns:
            df['date'] = pd.to_datetime(df['date'])
        
        return self._validate_data(df)
    
    def get_traffic_overview(self, start_date: str = '30daysAgo', 
                           end_date: str = 'today') -> pd.DataFrame:
        """
        Get traffic overview data
        
        Args:
            start_date: Start date
            end_date: End date
            
        Returns:
            DataFrame with traffic overview
        """
        metrics = ['sessions', 'users', 'pageviews', 'bounceRate', 'averageSessionDuration']
        dimensions = ['date']
        
        return self.get_data(metrics, dimensions, start_date, end_date)
    
    def get_traffic_sources(self, start_date: str = '30daysAgo', 
                          end_date: str = 'today') -> pd.DataFrame:
        """
        Get traffic sources data
        
        Args:
            start_date: Start date
            end_date: End date
            
        Returns:
            DataFrame with traffic sources
        """
        metrics = ['sessions', 'users']
        dimensions = ['source', 'medium']
        
        return self.get_data(metrics, dimensions, start_date, end_date)


class SimilarwebConnector(BaseDataConnector):
    """
    Connector for Similarweb API
    """
    
    def __init__(self, api_key: str):
        """
        Initialize Similarweb connector
        
        Args:
            api_key: Similarweb API key
        """
        super().__init__(
            api_key=api_key,
            base_url="https://api.similarweb.com/v1",
            rate_limit_requests=100,  # Similarweb API rate limits
            rate_limit_window=3600  # 1 hour
        )
    
    def _get_headers(self) -> Dict[str, str]:
        """Override to use Similarweb's API key format"""
        headers = super()._get_headers()
        headers['Authorization'] = f'Bearer {self.api_key}'
        return headers
    
    def get_available_datasets(self) -> List[Dict[str, Any]]:
        """
        Get available Similarweb data endpoints
        
        Returns:
            List of available endpoints
        """
        return [
            {'name': 'website_overview', 'description': 'Website traffic overview'},
            {'name': 'traffic_sources', 'description': 'Website traffic sources'},
            {'name': 'audience_interests', 'description': 'Audience interests'},
            {'name': 'similar_sites', 'description': 'Similar websites'},
            {'name': 'top_pages', 'description': 'Top performing pages'}
        ]
    
    def get_website_overview(self, domain: str, start_date: str, end_date: str,
                           country: str = 'world', granularity: str = 'monthly') -> pd.DataFrame:
        """
        Get website traffic overview
        
        Args:
            domain: Website domain
            start_date: Start date (YYYY-MM)
            end_date: End date (YYYY-MM)
            country: Country code or 'world'
            granularity: 'monthly' or 'daily'
            
        Returns:
            DataFrame with website overview data
        """
        params = {
            'start_date': start_date,
            'end_date': end_date,
            'country': country,
            'granularity': granularity,
            'main_domain_only': 'false'
        }
        
        endpoint = f'website/{domain}/total-traffic-and-engagement/visits'
        response = self._make_request(endpoint, params)
        
        if 'visits' not in response:
            return pd.DataFrame()
        
        visits_data = response['visits']
        
        # Convert to DataFrame
        df = pd.DataFrame(visits_data)
        df['domain'] = domain
        df['date'] = pd.to_datetime(df['date'])
        
        return self._validate_data(df)
    
    def get_traffic_sources(self, domain: str, start_date: str, end_date: str,
                          country: str = 'world') -> pd.DataFrame:
        """
        Get website traffic sources
        
        Args:
            domain: Website domain
            start_date: Start date (YYYY-MM)
            end_date: End date (YYYY-MM)
            country: Country code or 'world'
            
        Returns:
            DataFrame with traffic sources data
        """
        params = {
            'start_date': start_date,
            'end_date': end_date,
            'country': country,
            'main_domain_only': 'false'
        }
