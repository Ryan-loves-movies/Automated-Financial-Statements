from classes.retriever import retriever
from classes.processor import processor
from classes.updater import updater
import xlwings as xw

def main(sheet_name, ticker_col = 'tickers', data_col = 3):
    # This makes sure xw.Book command works -- not sure why but I'm guessing
    # it has to do with selecting the specific excel file as the one to
    # tamper with
    xw.Book("financial_statements.xlsm").set_mock_caller()

    # Retrieve tickers from column specified
    puller = retriever('financial_statements.xlsm', sheet_name)
    puller.retrieve_data(ticker_col)
    # print(puller.tickers)

    # Update list of tickers with balance sheet data
    balance_sheet_data = processor(puller.tickers)
    balance_sheet_data.download()
    balance_sheet_data.clean()
    balance_sheet_data.format()

    # Update full data onto excel sheet specified
    excel_updater = updater(sheet_name)
    excel_updater.update(data_col)

# def throw():
#     xw.Book("financial_statements.xlsm").set_mock_caller()
#     tryout = retriever('financial_statements.xlsm','Balance Sheet')
#     print(tryout.retrieve_data('tickers'))
main('Balance Sheet')