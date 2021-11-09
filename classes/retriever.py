import xlwings as xw
import pandas as pd

class retriever():
    """
    Retrieve ticker symbols from the first column (i.e. column A) in the sheet and
    return them in a list
    """
    def __init__(self, file_name, sheet_name):
        self.excel_file = file_name
        self.sheet_name = sheet_name
        self.tickers = []
    def retrieve_data(self, ticker_col):
        df = pd.read_excel(self.excel_file, sheet_name=self.sheet_name)
        tickers = df[ticker_col].tolist()
        self.tickers = tickers
        return tickers