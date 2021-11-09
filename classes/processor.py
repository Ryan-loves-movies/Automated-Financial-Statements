import yfinance as yf
from yahoofinancials import YahooFinancials as YF

class processor():
    def __init__(self, ticker_list):
        self.ticker_list = ticker_list
        self.ticker_string = " ".join(ticker_list)
        self.data = []
    def download(self):
        data = yf.download(tickers=self.ticker_string, period='max', interval='1d', prepost=True, threads=True)
        print(data)
    def clean(self):
        pass
    def format(self):
        pass