from typing import List, Dict, Any
from polygon import RESTClient
import logging


POLYGON_API_KEY = "nxyzUpEyvzZ1aQh7zm4kK4y4z3oARubi"
client = RESTClient(api_key=POLYGON_API_KEY)

def extract(ticker: str) -> List[Dict[str, Any]]:
	logger = logging.getLogger("airflow.task")
	try:
		aggs = []
		for a in client.list_aggs(
				ticker=ticker,
				multiplier=1,
				timespan="day",
				from_="2025-10-01",
				to="2025-10-02",
				limit=2,
		):
			aggs.append({
				"ticker": ticker,
				"open": a.open,
				"high": a.high,
				"low": a.low,
				"close": a.close,
				"volume": a.volume,
				"timestamp": a.timestamp,
			})
		
		if not aggs:
			logger.info(f"No data returned for {ticker}. Possibly market closed.")
			return []
		
		logger.info(f"Extracted {len(aggs)} records for {ticker}.")
		print(aggs)
		print(aggs, file=open("AAPL.json", 'a'))
		return aggs
	except Exception as e:
		logger.error(f"Extract failed: {e}")
		raise
	
extract("AAPL")