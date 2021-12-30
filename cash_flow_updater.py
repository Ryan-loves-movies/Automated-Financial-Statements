import pathlib
script_path = str(pathlib.Path(__file__).parent.resolve()) + '/classes'
import sys
sys.path.append(script_path)
from processor import processor
from retriever import retriever
from updater import updater

import time
import xlwings as xw

def excel_column_name(n):
    """
    Number to Excel-style column name, e.g., 1 = A, 26 = Z, 27 = AA, 703 = AAA.
    """
    name = ''
    while n > 0:
        n, r = divmod(n - 1, 26)
        name = chr(r + ord('A')) + name
    return name

def main(sheet_name = 'Cash Flow Statements' , config_name = 'Cash Flow config', ticker_col = 'tickers', forms_col = 'forms', data_col = 2):
    """

    Parameters
    ----------
    sheet_name: Name of sheet in the excel workbook to update
    config_name: Name of sheet to obtain the config parameters from (i.e. Ticker Symbols and Forms)
    ticker_col: Name of column for tickers (Defaults to 'tickers')
    forms_col: Name of column for forms (Defaults to 'forms')
    data_col: Column number for the data to be updated to (Defaults to 2nd column -- where first column
                                                                                     gives progress %)

    Returns nothing -- Updates excel sheet directly from given parameters
    -------

    """
    wb = xw.Book.caller()

    very_start = time.perf_counter()
    # Retrieve tickers from column specified
    puller = retriever(wb, config_name)
    tickers_with_forms = puller.retrieve_data(ticker_col, forms_col)
    print(f'retrieving took {time.perf_counter()-very_start}s')

    # Update list of tickers with balance sheet data
    start = time.perf_counter()
    balance_sheet_data = processor(tickers_with_forms)
    list_of_json_cik = balance_sheet_data.get_json_cik()
    first_request = time.perf_counter()
    print(f'get_json_cik took {first_request - start}s')

    while time.perf_counter() - first_request < 0.15:
        continue
    start = time.perf_counter()
    links = balance_sheet_data.get_form_links(list_of_json_cik, 'statement of cash flows')
    print(f'get_form_links took {time.perf_counter()-start}')

    wb.sheets[sheet_name].range((excel_column_name(data_col-1) + '1')).value = f'30% Done, That took {str(time.perf_counter()-very_start)[:6]}s'

    start = time.perf_counter()
    data = balance_sheet_data.download(links, table_type='cash_flow_statement_tables')
    print(f'processor().download() took {time.perf_counter()-start}')

    wb.sheets[sheet_name].range((excel_column_name(data_col-1) + '2')).value = f'90% Done, That took a total of {str(time.perf_counter()-very_start)[:6]}s! Updating Soon!!'

    # Update full data onto excel sheet specified
    excel_updater = updater(wb, sheet_name)
    excel_updater.update(data=data,col_for_data=data_col)
    wb.sheets[sheet_name].range((excel_column_name(data_col-1) + '3')).value = f'100% Done, That took a total of {str(time.perf_counter()-very_start)[:6]}s!'

if __name__ == '__main__':
    xw.Book('financial_statements.xlsm').set_mock_caller()
    main()