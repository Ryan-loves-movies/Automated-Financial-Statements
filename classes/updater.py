import pandas as pd

class list_updater():
    """
    Basically a hassle free concatenator for lists --> Doesn't combine based on index or whatever
    """
    def __init__(self, num_of_rows, df):
        """

        Parameters
        ----------
        num_of_rows: Number of rows in list -- as in number of lists in the list that you want
        df: Original list of lists that you pass in

        """
        self.num_of_rows = num_of_rows
        new_df = []
        if df:
            len_of_row = len(df[0])
            for i in range(self.num_of_rows):
                try:
                    new_df.append(df[i])
                except:
                    new_df.append([None] * len_of_row)
            self.df = new_df
        else:
            self.df = [[] for _ in range(self.num_of_rows)]

    def join(self, df2):
        """

        Parameters
        ----------
        df2: list of lists to join to the original list

        Returns the new list (and also updates original list to updated list)
        -------

        """
        len_of_row_in_df2 = len(df2[0])

        if len(df2) <= self.num_of_rows:
            for iter, item in enumerate(self.df):
                try:
                    for element in df2[iter]:
                        item.append(element)
                except:
                    for _ in range(len_of_row_in_df2):
                        item.append(None)
        else:
            for iter, item in enumerate(df2):
                try:
                    for element in item:
                        self.df[iter].append(element)
                except:
                    for _ in range(len_of_row_in_df2):
                        self.df[iter].append(None)
        return self.df


class updater():
    def __init__(self, wb, sheet_name):
        """"
        Parameters
        ----------
        sheet_name: Sheet name of data to upload to -- i.e. 'Balance Sheet', 'Income Statement'
        """
        self.wb = wb.sheets[sheet_name]
    def update(self, data, col_for_data):
        """

        Parameters
        ----------
        data: Raw output from processor().download()
              (i.e. {'AAPL': {'10-K': [['2021-09-25',
                                              '10-K',
                                              [[Current assets:,None,None], ...],
                                              ...
                                    }
                           'MSFT': {'forms': [
                                              [[Current assets:,None,None], ...],
                                              ...
                                              ]
                                    }
                          }
                    )
        col_for_data: column in excel sheet to put data in -- In number form

        Returns nothing -- Updates the excel sheet directly instead
        -------

        """
        # Function to convert column number to excel column name
        def excel_column_name(n):
            """
            Number to Excel-style column name, e.g., 1 = A, 26 = Z, 27 = AA, 703 = AAA.
            """
            name = ''
            while n > 0:
                n, r = divmod(n - 1, 26)
                name = chr(r + ord('A')) + name
            return name

        row_num = 1
        for ticker in data:
            ticker_symbol = ticker
            forms = data.get(ticker)
            col_num = col_for_data

            # Only 10-K forms to extract
            all_forms = forms
            for num_of_forms, form in enumerate(all_forms):
                table_to_upload = form
                if num_of_forms == 0:
                    ticker_table = list_updater(100, table_to_upload)
                # If table_to_upload isn't empty
                elif table_to_upload:
                    ticker_table.join(table_to_upload)
                else:
                    continue

            ticker_table = pd.DataFrame(ticker_table.df)
            ticker_table = ticker_table.dropna(axis=0, how='all')

            self.wb.range((excel_column_name(col_num) + str(row_num))).value = ticker_table.values
            self.wb.range((excel_column_name(col_num) + str(row_num))).value = ticker_symbol
            row_num += max(50, ticker_table.shape[0])
