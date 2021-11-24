import pandas as pd
import time
from classes.retriever import retriever
from classes.processor import processor
from classes.updater import updater
import xlwings as xw
from xlwings import Range


def excel_column_name(n):
    """
    Number to Excel-style column name, e.g., 1 = A, 26 = Z, 27 = AA, 703 = AAA.
    """
    name = ''
    while n > 0:
        n, r = divmod(n - 1, 26)
        name = chr(r + ord('A')) + name
    return name

def main(sheet_name = 'Balance Sheet' , config_name = 'Balance Sheet config', ticker_col = 'tickers', forms_col = 'forms', data_col = 2):
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
    # wb = xw.Book.caller().sheets[sheet_name]
    # wb.range('A5').value = str(active_workbook)

    # This makes sure xw.Book command works -- not sure why but I'm guessing
    # it has to do with selecting the specific excel file as the one to
    # tamper with
    # xw.Book("financial_statements.xlsm").set_mock_caller()
    very_start = time.time()
    # Retrieve tickers from column specified
    puller = retriever(wb, config_name)
    tickers_with_forms = puller.retrieve_data(ticker_col, forms_col)
    # print(puller.tickers)

    # Update list of tickers with balance sheet data
    balance_sheet_data = processor(tickers_with_forms)
    list_of_json_cik = balance_sheet_data.get_json_cik()

    # start = time.time()
    links = balance_sheet_data.get_form_links(list_of_json_cik)
    # print(f'get_form_links took {time.time()-start}')

    wb.sheets[sheet_name].range((excel_column_name(data_col-1) + '1')).value = f'10% Done, That took {str(time.time()-very_start)[:6]}s'
    # Range((sheet_name, excel_column_name(data_col - 1) + '1')).wrap_text = True

    # start = time.time()
    data = balance_sheet_data.download(links)
    # print(f'processor().download() took {time.time()-start}')

    wb.sheets[sheet_name].range((excel_column_name(data_col-1) + '2')).value = f'80% Done, That took a total of {str(time.time()-very_start)[:6]}s! Updating Soon!!'
    # Range((sheet_name, excel_column_name(data_col - 1) + '2')).wrap_text = True

    # Update full data onto excel sheet specified
    excel_updater = updater(wb, sheet_name)
    excel_updater.update(data=data,col_for_data=data_col)
    # Range((sheet_name, excel_column_name(data_col - 1) + '3')).value = f'100% Done, That took a total of {time.time() - start}s!'
    wb.sheets[sheet_name].range((excel_column_name(data_col-1) + '3')).value = f'100% Done, That took a total of {str(time.time()-very_start)[:6]}s!'

if __name__ == '__main__':
    xw.Book('financial_statements.xlsm').set_mock_caller()
    main()

