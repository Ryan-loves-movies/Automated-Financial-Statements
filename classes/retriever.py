import pandas as pd

class retriever():
    """
    Retrieve ticker symbols from the first column (i.e. column A) in the sheet and
    return them in a list
    """
    def __init__(self, wb, sheet_name):
        """

        Parameters
        ----------
        wb: wb = xlwings.Book.caller()
        sheet_name: Name of the sheet to extract the ticker symbols and forms to get from

        """
        # self.path = str(active_workbook).replace(':', ' ')
        # self.excel_file = self.path + '/' + file_name
        self.wb = wb.sheets[sheet_name]
        self.tickers = []
    def retrieve_data(self, ticker_col, form_col):
        """

        Parameters
        ----------
        ticker_col: Name of the column for tickers in the table extracted from the sheet
        form_col: Name of the column for forms in the table extracted from the sheet

        Returns a list of lists that combine the 2 inputs
        i.e. [['TSLA', '10-K'],
                ['RBLX', '10-Q'],
                ...]
        -------

        """
        df = pd.DataFrame(self.wb.used_range.value)
        df.columns = df.iloc[0]
        df = df.drop(df.index[0])
        print(df)
        # df = pd.read_excel(self.excel_file, sheet_name=self.sheet_name, engine = 'openpyxl')
        tickers = df[ticker_col].tolist()
        forms = df[form_col].tolist()
        full_list = []
        num = 0
        for i in tickers:
            full_list.append([i,forms[num]])
            num+=1
        return full_list