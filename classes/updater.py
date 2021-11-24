import pandas as pd
from xlwings import Range
from datetime import datetime, date
from pprint import pprint


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
        len_of_row = len(df[0])
        iter = 0
        while iter < self.num_of_rows:
            try:
                new_df.append(df[iter])
            except:
                new_df.append([None] * len_of_row)
            iter += 1
        self.df = new_df
    def join(self, df2):
        """

        Parameters
        ----------
        df2: list of lists to join to the original list

        Returns the new list (and also updates original list to updated list)
        -------

        """
        len_of_row_in_df2 = len(df2[0])
        iter = 0
        for item in self.df:
            try:
                for element in df2[iter]:
                    item.append(element)
            except:
                for i in range(len_of_row_in_df2):
                    item.append(None)
            iter += 1
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
                           'MSFT': {'10-K': [['2021-06-30',
                                              '10-K',
                                              [[Current assets:,None,None], ...],
                                              ...
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
            if forms.get('10-Q')==None:
                forms = forms.get('10-K')
                num_of_forms = 0
                for form in forms:
                    table_to_upload = form[2]
                    # table_to_upload = pd.DataFrame(form[2])
                    # table_to_upload.columns = table_to_upload.iloc[0].values
                    # table_to_upload = table_to_upload.drop(table_to_upload.index[0])
                    # table_to_upload = table_to_upload.set_index(table_to_upload.columns[0])
                    if num_of_forms == 0:
                        # print(table_to_upload)
                        # ticker_table = table_to_upload
                        ticker_table = list_updater(100, table_to_upload)
                    else:
                        # if num_of_forms == max_forms:
                        ticker_table.join(table_to_upload)
                        # ticker_table = pd.concat([ticker_table, table_to_upload], ignore_index=True)
                        # else:
                        #     table_to_upload = table_to_upload.iloc[:,:-1]
                        #     ticker_table = ticker_table.join(table_to_upload, lsuffix='__'+ 'left', rsuffix='__'+'right')
                    num_of_forms += 1
                ticker_table = pd.DataFrame(ticker_table.df)
                ticker_table = ticker_table.dropna(axis=0, how='all')
                # ticker_table.columns = ticker_table.iloc[0].values
                # ticker_table = ticker_table.drop(ticker_table.index[0])
                # ticker_table = ticker_table.set_index(ticker_table.columns[0])
                # print(f'Shape of pandas dataframe is {ticker_table.shape}')

                # Range((self.sheet_name, excel_column_name(col_num) + str(row_num))).value = ticker_table.values
                self.wb.range((excel_column_name(col_num) + str(row_num))).value = ticker_table.values
                self.wb.range((excel_column_name(col_num) + str(row_num))).value = ticker_symbol
                row_num += max(50, ticker_table.shape[0])

            # 10-Q and 10-K forms to extract
            else:
                forms10q = forms.get('10-Q')
                forms10k = forms.get('10-K')

                # Arrange and compile the forms in date order
                all_forms = []
                while (forms10q != []) or (forms10k != []):
                    # Ensure that if either of the lists are empty, no comparisons are made and the loop
                    # continues
                    if forms10k == []:
                        all_forms.append(forms10q.pop(0))
                        continue
                    elif forms10q == []:
                        all_forms.append(forms10k.pop(0))
                        continue

                    latest_10q = datetime.strptime(forms10q[0][0], '%Y-%M-%d')
                    latest_10k = datetime.strptime(forms10k[0][0], '%Y-%M-%d')
                    # Get the latest filing date
                    date_now = max(latest_10k, latest_10q)
                    # Compare if equal or not instead of directly older or earlier so that it is easier
                    # to write the exception for if the dates are equal
                    if date_now == latest_10k:
                        all_forms.append(forms10k.pop(0))
                    # 2 if statements so if the dates are the same, it will append the 10k first, then the
                    # 10q in the same loop
                    if date_now == latest_10q:
                        all_forms.append(forms10q.pop(0))

                iter = 0
                for form in all_forms:
                    table_to_upload = form[2]
                    # table_to_upload = pd.DataFrame(form[2])
                    # table_to_upload.columns = table_to_upload.iloc[0].values
                    # table_to_upload = table_to_upload.drop(table_to_upload.index[0])
                    # table_to_upload = table_to_upload.set_index(table_to_upload.columns[0])
                    if iter == 0:
                        ticker_table = list_updater(100, table_to_upload)
                        # ticker_table = table_to_upload
                    else:
                        ticker_table.join(table_to_upload)
                        # ticker_table = pd.concat([ticker_table, table_to_upload])
                    iter += 1
                ticker_table = pd.DataFrame(ticker_table.df)
                ticker_table = ticker_table.dropna(axis=0, how='all')
                # ticker_table.columns = ticker_table.iloc[0].values
                # ticker_table = ticker_table.drop(ticker_table.index[0])
                # ticker_table = ticker_table.set_index(ticker_table.columns[0])
                # print(f'Shape of pandas dataframe is {ticker_table.shape}')
                # Range((self.sheet_name, excel_column_name(col_num) + str(row_num))).value = ticker_table.values
                self.wb.range((excel_column_name(col_num) + str(row_num))).value = ticker_table.values
                self.wb.range((excel_column_name(col_num) + str(row_num))).value = ticker_symbol
                row_num += max(50, ticker_table.shape[0])
            # row_num += 50

                # if latest_10q > latest_10k:
                #     time_for_10q = True
                # else:
                #     time_for_10q = False
                # num_of_forms = 0
                # max_forms = len(forms10q) + len(forms10k)
                # while (forms10q != []) or (forms10k != []):
                #     num_of_forms += 1
                #     if time_for_10q == True:
                #         # pprint(forms10q[0][2])
                #         table_to_upload = forms10q[0][2]
                #         # table_to_upload.columns = table_to_upload.iloc[0].values
                #         # table_to_upload = table_to_upload.drop(table_to_upload.index[0])
                #         # table_to_upload = table_to_upload.set_index(table_to_upload.columns[0])
                #         # table_to_upload = pd.DataFrame(table_to_upload)
                #         if num_of_forms == 1:
                #             ticker_table = list_updater(50, table_to_upload)
                #         else:
                #             print(forms10q)
                #             print(forms10k)
                #             print('table to upload:\n', table_to_upload)
                #             ticker_table.join(table_to_upload)
                #             # if num_of_forms == max_forms:
                #             # ticker_table = ticker_table.join(table_to_upload, lsuffix='__'+ 'left', rsuffix='__'+'right')
                #             # else:
                #             #     table_to_upload = table_to_upload.iloc[:, :-1]
                #             #     pd.set_option('display.max_columns', None)
                #             #     print(ticker_table)
                #             #     ticker_table = ticker_table.join(table_to_upload, lsuffix='__'+ 'left', rsuffix='__'+'right')
                #             #     print('After Joining, ')
                #             #     print(ticker_table)
                #         date_now = datetime.strptime(forms10q[0][0], '%Y-%m-%d').date().replace(year=2021)
                #         nov = datetime.strptime('2021-11-01', '%Y-%m-%d').date().replace(year=2021)
                #         # Earlier than 5th May
                #         if date_now > nov:
                #             time_for_10q = False
                #         else:
                #             time_for_10q = True
                #         forms10q.pop(0)
                #
                #
                #     else:
                #         # pprint(forms10k[0][2])
                #         table_to_upload = forms10k[0][2]
                #         # table_to_upload.columns = table_to_upload.iloc[0].values
                #         # table_to_upload = table_to_upload.drop(table_to_upload.index[0])
                #         # table_to_upload = table_to_upload.set_index(table_to_upload.columns[0])
                #         # table_to_upload = pd.DataFrame(table_to_upload)
                #         if num_of_forms == 1:
                #             ticker_table = list_updater(50, table_to_upload)
                #         else:
                #             ticker_table.join(table_to_upload)
                #             # if num_of_forms == max_forms:
                #             #     ticker_table = ticker_table.join(table_to_upload, lsuffix='__'+ 'left', rsuffix='__'+'right')
                #             # else:
                #             #     table_to_upload = table_to_upload.iloc[:, :-1]
                #             #     ticker_table = ticker_table.join(table_to_upload, lsuffix='__'+ 'left', rsuffix='__'+'right')
                #         forms10k.pop(0)
                #         time_for_10q = True

            # For next ticker tables to fill up
