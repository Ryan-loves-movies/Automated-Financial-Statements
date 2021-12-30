import re
from io import StringIO
import pandas as pd
from pprint import pprint
import numpy as np
import requests
from lxml import etree, html

import pathlib
script_path = str(pathlib.Path(__file__).parent.resolve())
import sys
sys.path.append(script_path)
from table_finder import finder




class scraper:
    def __init__(self):
        pass
    def get_balance_sheet_tables(self, resp, html_bool = True, filingsummary = False):
        """

        Parameters
        ----------
        resp: Just the raw response from requests.get('...').content /
        resp: Just the raw response text from aiohttp.ClientSession().get(url).read()
        html_bool: If True, will scrape based on latest formats of filings to get table
              Else, will scrape using old format for older filings (typically before 2000)
              (True --> html files
              False --> txt files)

        Returns a list of lists which, from which multiindexed headers and indexes could be extracted
        --> However, parsing directly to a table and updating in excel is sufficient
        -------

        """
        # Newer forms, based on html
        # if html_bool==True
        if html_bool:
            if filingsummary != 'False':
                if filingsummary == 'xml':
                    xmlparser = etree.XMLParser()

                    # Parse the xml webpage
                    dom = etree.fromstring(resp, parser=xmlparser)
                    # Transform the webpage to a single table from the .xslt file provided by edgar
                    xsl_filename = f'{pathlib.Path(__file__).parent.resolve()}/InstanceReport.xslt'
                    xslt = etree.parse(xsl_filename, parser=xmlparser)
                    transform = etree.XSLT(xslt)
                    newdoc = transform(dom)

                    table = etree.tostring(newdoc.xpath('//table[1]')[0])
                    df = pd.DataFrame(pd.read_html(table)[0])
                else:
                    doc = etree.fromstring(resp, parser = etree.HTMLParser())
                    table_in_doc = etree.tostring(doc.xpath('//table[1]')[0])
                    df = pd.DataFrame(pd.read_html(table_in_doc)[0])

                # Get index of table
                first_column = df[df.columns[0]]
                second_part_index = list(first_column)

                if filingsummary == 'html':
                    # Get multiindex header of table
                    first_column_name = str(first_column.name).lower().replace('consolidated balance sheets', '').replace('-',
                                                                                                                     '').strip(' ')

                    # Up to you -- np.nan for other columns or the same column name
                    new_column = [[first_column_name] + [np.nan for _ in range(len(list(df.columns)[1:])-1)], list(df.columns)[1:]]
                    # new_column = [([first_column_name] * len(list(df.columns)[1:])), list(df.columns)[1:]]
                    # new_column = pd.MultiIndex.from_arrays(new_column)

                    # Add 2 NaN values behind each header since this isn't really multiindex -- the table printed in excel
                    # doesn't show multiindex as well as jupyter notebook anyway
                    new_column = [[np.nan, np.nan] + i for i in new_column]
                else:
                    # Since the columns declared are already multiindexed columns, the table from xml will have to be parsed
                    # slightly differently
                    columns = [list(a) for a in df.columns]
                    columns = list(map(list, zip(*columns)))
                    new_column = [[np.nan] + i for i in columns]

                # Drop index of table
                df = df.drop(df.columns[0], axis=1)

                # Separate rows into first index rows and second index rows -- first index row are rows with empty values
                null_rows = df[df.isnull().all(1)]
                null_rows = list(null_rows.index)
                first_index = []
                for position, i in enumerate(second_part_index):
                    if position in null_rows:
                        first_index.append(i)
                    else:
                        first_index.append('Null')

                # Getting multiindex for index of table -- i.e. second and first index of the multiindex
                second_index = [b if a == 'Null' else np.nan for a, b in zip(first_index, second_part_index)]
                first_index = [np.nan if i == 'Null' else i for i in first_index]
                new_index = [[a, b] for a, b in zip(first_index, second_index)]
                # new_index = [first_index, second_index]
                # new_index = pd.MultiIndex.from_arrays(new_index)


                # # Set the new multiindex headers and indexes
                # df.index = new_index
                # df.columns = new_column

                # Merge everything together into a single table
                table_values = df.values.tolist()
                table_values = [a + b for a, b in zip(new_index, table_values)]
                table_values = new_column + table_values
                return table_values

            # Scraping directly from document itself
            else:
                # Declare parser so that efficiency is preserved
                utf8_parser = html.HTMLParser(encoding='utf-8')
                fulldoc = html.document_fromstring(resp, parser=utf8_parser)

                list_of_hyperlink_texts = ['balance sheet']
                list_of_texts_in_table = ['current assets:', 'current assets', 'total current assets']
                list_of_texts_before_table = ['consolidated balance sheet',
                                              'condensed consolidated balance sheet',
                                              'consolidated condensed balance sheet']

                table_finder = finder(fulldoc)
                # Try finding through the 3 different methods outlined under table_finder
                table = table_finder.find_hyperlink_text_to_table(list_of_hyperlink_texts, form='balance sheet')
                if table == 'Null':
                    table = table_finder.find_text_in_table(list_of_texts_in_table, form='balance sheet')
                if table == 'Null':
                    table = table_finder.find_text_before_table(list_of_texts_before_table, form='balance sheet')
                # if table is empty
                if table == 'Null':
                    print(f'Couldnt find table for some reason -- {etree.tostring(fulldoc.xpath(".//table[1]")[0])}')
                    return []

                # Convert string to pandas dataframe -- it seems unicode encoding provides the best results (for the dataframe)
                # Remove $ from the data and () that indicate negative flows for easier cleaning
                table = str(etree.tostring(table, encoding='unicode', method='html')).replace("$", " ").replace('(', '-').replace(')', ' ')
                panda_table = pd.DataFrame(pd.read_html(table)[0])

                # Set asset and whatever items in the balance sheet as index
                panda_table = panda_table.set_index(panda_table.iloc[:, 0])

                # function to fill list with None values either in front of the list or at the back
                def fill_list(li, num, direction):
                    if direction == 'f':
                        while len(li) < num:
                            li.insert(0, None)
                    elif direction == 'b':
                        while len(li) < num:
                            li.append(None)
                    return li

                # Create a new dataframe with cleaned data (i.e. no NaN Values and no duplicate headers and whatnot)
                new_df = []

                asset_trigger = False
                abnormal = False
                for row in panda_table.itertuples():
                    new_row = []

                    for element in range(len(row)):
                        item = row[element]
                        if pd.isnull(item):
                            pass
                        else:
                            # try and except clauses to make integers integers and not floats
                            try:
                                if str(int(item)) not in new_row:
                                    new_row.append(str(int(item)))
                            except:
                                if str(item) not in new_row:
                                    new_row.append(str(item))
                        if 'assets' in str(row[element]).lower():
                            asset_trigger = True

                    # fill row_data with None values
                    if asset_trigger == False:
                        new_row = fill_list(new_row, 3, 'f')
                    else:
                        new_row = fill_list(new_row, 3, 'b')

                    # All data should only be length 3, show the exceptions
                    if len(new_row) > 3:
                        print(f'this row had more than 3 values for some reason - {new_row}')
                        abnormal = True
                    else:
                        new_df.append(new_row)
                if abnormal:
                    panda_table.to_pickle('./abnormal_table.pkl')

                panda_table = pd.DataFrame(new_df)
                df = panda_table.dropna(axis=0, how='all').reset_index(drop=True)

                # Converting to multiindex
                # Get index of table
                first_column = df[df.columns[0]]
                second_part_index = list(first_column)

                # Get multiindex header of table
                first_column_name = str(first_column.name).lower().replace('consolidated balance sheets', '').replace('-',
                                                                                                                 '').strip(
                    ' ')

                # Up to you -- np.nan for other columns or the same column name
                new_column = [[first_column_name] + [np.nan for _ in range(len(list(df.columns)[1:]) - 1)], list(df.columns)[1:]]
                # new_column = [([first_column_name] * len(list(df.columns)[1:])), list(df.columns)[1:]]
                # new_column = pd.MultiIndex.from_arrays(new_column)
                new_column = [[np.nan, np.nan] + i for i in new_column]

                # Drop index of table
                df = df.drop(df.columns[0], axis=1)

                # Separate rows into first index rows and second index rows -- first index row are rows with empty values
                null_rows = df[df.isnull().all(1)]
                null_rows = list(null_rows.index)
                first_index = []
                for position, i in enumerate(second_part_index):
                    if position in null_rows:
                        first_index.append(i)
                    else:
                        first_index.append('Null')

                # Getting multiindex for index of table -- i.e. second and first index of the multiindex
                second_index = [b if a == 'Null' else np.nan for a, b in zip(first_index, second_part_index)]
                first_index = [np.nan if i == 'Null' else i for i in first_index]
                new_index = [[a, b] for a, b in zip(first_index, second_index)]
                # new_index = [first_index, second_index]
                # new_index = pd.MultiIndex.from_arrays(new_index)

                # # Set the new multiindex headers and indexes
                # df.index = new_index
                # df.columns = new_column
                table_values = df.values.tolist()
                table_values = [a + b for a, b in zip(new_index, table_values)]
                table_values = new_column + table_values
                return table_values

        # Older forms -- Non-html forms
        else:
            doc_str = re.split("\n", str(resp.decode('utf-8')))

            find_next_table = False
            table_printer = []
            in_table = False
            date = '1990'
            for num, i in enumerate(doc_str):
                # Find date of filing
                if 'conformed period of report' in i.lower():
                    date = re.split('\s', i)[-1][:4]
                # Find table for balance sheet
                if 'balance sheet' in i.lower():
                    find_next_table = True
                    continue
                if find_next_table:
                    if '<table>' in i.lower():
                        in_table = True
                        find_next_table = False
                    continue

                if in_table:
                    table_printer.append(i)
                    if '</table>' in i.lower():
                        full_str = '\n'.join(table_printer[:-1])
                        # Table Validation
                        if 'current assets' in full_str:
                            starter = 0
                            for num, el in enumerate(table_printer[:-1]):
                                if date in el.lower():
                                    row = [i for i in re.split('\s', el) if i != '']
                                    print(row)
                                    col = [' '.join(row[:-2]), row[-2], row[-1]]
                                    break
                            for num, el in enumerate(table_printer[:-1]):
                                if ('<s>' in el.lower()) or ('<c>' in el.lower()):
                                    starter = num
                            table_str = '\n'.join(table_printer[starter:-1])
                            table_str = table_str.replace('-', ' ')
                            table_str = table_str.replace(')', ' ')
                            table_str = table_str.replace('(', '-')
                            table_str = table_str.replace('$', ' ')
                            na_values = ['-' * i for i in range(20)] + ['=' * i for i in range(20)]
                            table = pd.DataFrame(pd.read_fwf(StringIO(table_str), na_values=na_values))
                            table.columns = col
                            table = table.dropna(axis=0, how='all')

                            df = table
                            # Converting to multiindex
                            # Get index of table
                            first_column = df[df.columns[0]]
                            second_part_index = list(first_column)

                            # Get multiindex header of table
                            first_column_name = str(first_column.name).lower().replace('consolidated balance sheets',
                                                                                       '').replace('-', ' ').strip(' ')

                            # Up to you -- np.nan for other columns or the same column name
                            new_column = [[first_column_name] + [np.nan for _ in range(len(list(df.columns)[1:]) - 1)],
                                          list(df.columns)[1:]]
                            #                     new_column = [([first_column_name] * len(list(df.columns)[1:])), list(df.columns)[1:]]
                            #                     new_column = pd.MultiIndex.from_arrays(new_column)
                            new_column = [[np.nan, np.nan] + i for i in new_column]

                            # Drop index of table
                            df = df.drop(df.columns[0], axis=1)

                            # Separate rows into first index rows and second index rows -- first index row are rows with empty values
                            null_rows = df[df.isnull().all(1)]
                            null_rows = list(null_rows.index)
                            first_index = []
                            for position, i in enumerate(second_part_index):
                                if position in null_rows:
                                    first_index.append(i)
                                else:
                                    first_index.append('Null')

                            # Getting multiindex for index of table -- i.e. second and first index of the multiindex
                            second_index = [b if a == 'Null' else np.nan for a, b in
                                            zip(first_index, second_part_index)]
                            first_index = [np.nan if i == 'Null' else i for i in first_index]
                            new_index = [[a, b] for a, b in zip(first_index, second_index)]
                            #                     new_index = [first_index, second_index]
                            #                     new_index = pd.MultiIndex.from_arrays(new_index)

                            # # Set the new multiindex headers and indexes
                            #                     df.index = new_index
                            #                     df.columns = new_column
                            table_values = df.values.tolist()
                            table_values = [a + b for a, b in zip(new_index, table_values)]
                            table_values = new_column + table_values

                            return table_values
                        else:
                            table_printer = []
                            in_table = False

            print(f"Couldn't find table for {date}")
            return []

    def get_income_statement_tables(self, resp, html_bool=True, filingsummary = False):
        """"

        Parameters
        ----------
        resp: Just the raw response text from aiohttp.ClientSession().get(url).read()
        html_bool: If True, will scrape based on latest formats of filings to get table
              Else, will scrape using old format for older filings (typically before 2000)
              (True --> html files
              False --> txt files)

        Returns a list of lists which can be directly converted into a pandas dataframe
        --> Containing the raw table data with raw row headers and columns
        -------

        """

        # For newer 10-K -- html forms
        if html_bool:
            if filingsummary != 'False':
                if filingsummary == 'xml':
                    xmlparser = etree.XMLParser()

                    # Parse the xml webpage
                    dom = etree.fromstring(resp, parser=xmlparser)
                    # Transform the webpage to a single table from the .xslt file provided by edgar
                    xsl_filename = f'{pathlib.Path(__file__).parent.resolve()}/InstanceReport.xslt'
                    xslt = etree.parse(xsl_filename, parser=xmlparser)
                    transform = etree.XSLT(xslt)
                    newdoc = transform(dom)

                    table = etree.tostring(newdoc.xpath('//table[1]')[0])
                    df = pd.DataFrame(pd.read_html(table)[0])
                else:
                    doc = etree.fromstring(resp, parser = etree.HTMLParser())
                    table_in_doc = etree.tostring(doc.xpath('//table[1]')[0])
                    df = pd.DataFrame(pd.read_html(table_in_doc)[0])

                # Get index of table
                first_column = df[df.columns[0]]
                second_part_index = list(first_column)

                if filingsummary == 'html':
                    # Get multiindex header of table
                    first_column_name = str(first_column.name).lower().replace('-','').strip(' ')

                    # Up to you -- np.nan for other columns or the same column name
                    new_column = [[first_column_name] + [np.nan for _ in range(len(list(df.columns)[1:])-1)], list(df.columns)[1:]]
                    # new_column = [([first_column_name] * len(list(df.columns)[1:])), list(df.columns)[1:]]
                    # new_column = pd.MultiIndex.from_arrays(new_column)

                    # Add 2 NaN values behind each header since this isn't really multiindex -- the table printed in excel
                    # doesn't show multiindex as well as jupyter notebook anyway
                    new_column = [[np.nan, np.nan] + i for i in new_column]
                else:
                    # Since the columns declared are already multiindexed columns, the table from xml will have to be parsed
                    # slightly differently
                    columns = [list(a) for a in df.columns]
                    columns = list(map(list, zip(*columns)))
                    new_column = [[np.nan] + i for i in columns]

                # Drop index of table
                df = df.drop(df.columns[0], axis=1)

                # Separate rows into first index rows and second index rows -- first index row are rows with empty values
                null_rows = df[df.isnull().all(1)]
                null_rows = list(null_rows.index)
                first_index = []
                for position, i in enumerate(second_part_index):
                    if position in null_rows:
                        first_index.append(i)
                    else:
                        first_index.append('Null')

                # Getting multiindex for index of table -- i.e. second and first index of the multiindex
                second_index = [b if a == 'Null' else np.nan for a, b in zip(first_index, second_part_index)]
                first_index = [np.nan if i == 'Null' else i for i in first_index]
                new_index = [[a, b] for a, b in zip(first_index, second_index)]

                # Merge everything together into a single table
                table_values = df.values.tolist()
                table_values = [a + b for a, b in zip(new_index, table_values)]
                table_values = new_column + table_values
                return table_values
            else:
                # Declare parser so that efficiency is preserved
                utf8_parser = html.HTMLParser(encoding='utf-8')
                fulldoc = html.document_fromstring(resp, parser=utf8_parser)

                list_of_hyperlink_texts = ['statement of income', 'statements of income', 'statement of operation',
                                           'statements of operation']
                list_of_texts_before_table = ['consolidated statement of income',
                                              'consolidated statements of income',
                                              'condensed consolidated statement of income',
                                              'condensed consolidated statements of income',
                                              'consolidated condensed statement of income',
                                              'consolidated condensed statements of income',

                                              'consolidated statement of operation',
                                              'consolidated statements of operation',
                                              'condensed consolidated statement of operation',
                                              'condensed consolidated statements of operation',
                                              'consolidated condensed statement of operation',
                                              'consolidated condensed statements of operation'
                                              ]
                list_of_texts_in_table = ['net income', 'operating income', 'net revenue', 'net sales']

                table_finder = finder(fulldoc)
                # Try finding through the 3 different methods outlined under table_finder
                table = table_finder.find_hyperlink_text_to_table(list_of_hyperlink_texts,
                                                                  form='statement of operations')
                if table == 'Null':
                    table = table_finder.find_text_before_table(list_of_texts_before_table,
                                                                form='statement of operations')
                if table == 'Null':
                    table = table_finder.find_text_in_table(list_of_texts_in_table, form='statement of operations')
                # if table is empty
                if table == 'Null':
                    print(f'Couldnt find table for some reason -- {etree.tostring(fulldoc.xpath(".//table[1]")[0])}')
                    return []

                # Convert string to pandas dataframe -- it seems unicode encoding provides the best results (for the dataframe)
                # Remove $ from the data and () that indicate negative flows for easier cleaning
                table = str(etree.tostring(table, encoding='unicode', method='html')).replace("$", " ").replace('(',
                                                                                                                '-').replace(
                    ')', ' ')
                panda_table = pd.DataFrame(pd.read_html(table)[0])
                df = panda_table.drop_duplicates().dropna(how='all').reset_index(drop=True).T.drop_duplicates().dropna(
                    how='all').reset_index(drop=True).T

                # Get last row num that begins with a NaN value
                row_num = 0
                df = df.fillna('Null')
                for _ in df.itertuples(index=False):
                    row = df.iloc[row_num]
                    if row[0] == 'Null':
                        row_num += 1
                        continue
                    else:
                        row_num -= 1
                        break
                last_header_row = list(set([i for i in df.iloc[row_num, :] if i != 'Null']))
                df = df.replace('Null', np.nan)

                # Get columns where dates are the same
                # Get the one with most data and filter it out
                for column in last_header_row:
                    # Get all columns where dates are the same
                    all_columns = df[df.columns[df.iloc[row_num] == column]]
                    columns = [i for i in all_columns.columns]
                    first_column = df[columns[0]]
                    other_columns = columns[1:]
                    # Fill NaN values in first column with values from the other duplicate columns, then drop the duplicate columns
                    for i in other_columns:
                        first_column = first_column.fillna(all_columns[i])
                        df = df.drop(columns=[i])
                    df[columns[0]] = first_column

                df.columns = df.iloc[row_num - 1, :] + df.iloc[row_num, :]
                df = df.drop(row_num)
                df = df.drop(row_num - 1)
                df = df.dropna(axis=1, how='all')

                # Converting to multiindex
                # Get index of table
                first_column = df[df.columns[0]]
                second_part_index = list(first_column)

                # Get multiindex header of table
                first_column_name = str(first_column.name).lower().replace('-', '').strip(' ')

                # Up to you -- np.nan for other columns or the same column name
                new_column = [[first_column_name] + [np.nan for _ in range(len(list(df.columns)[1:]) - 1)], list(df.columns)[1:]]
                # new_column = [([first_column_name] * len(list(df.columns)[1:])), list(df.columns)[1:]]
                # new_column = pd.MultiIndex.from_arrays(new_column)
                new_column = [[np.nan, np.nan] + i for i in new_column]

                # Drop index of table
                df = df.drop(df.columns[0], axis=1)

                # Separate rows into first index rows and second index rows -- first index row are rows with empty values
                null_rows = df[df.isnull().all(1)]
                null_rows = list(null_rows.index)
                first_index = []
                for position, i in enumerate(second_part_index):
                    if position in null_rows:
                        first_index.append(i)
                    else:
                        first_index.append('Null')

                # Getting multiindex for index of table -- i.e. second and first index of the multiindex
                second_index = [b if a == 'Null' else np.nan for a, b in zip(first_index, second_part_index)]
                first_index = [np.nan if i == 'Null' else i for i in first_index]
                new_index = [[a, b] for a, b in zip(first_index, second_index)]
                # new_index = [first_index, second_index]
                # new_index = pd.MultiIndex.from_arrays(new_index)

                # # Set the new multiindex headers and indexes
                # df.index = new_index
                # df.columns = new_column
                table_values = df.values.tolist()
                table_values = [a + b for a, b in zip(new_index, table_values)]
                table_values = new_column + table_values
                return table_values
        # For older forms -- txt files
        else:
            doc_str = re.split("\n", str(resp.decode('utf-8')))

            find_next_table = False
            table_printer = []
            in_table = False
            date = '1990'
            for num, i in enumerate(doc_str):
                # Find date of filing
                if 'conformed period of report' in i.lower():
                    date = re.split('\s', i)[-1][:4]

                # Find table for balance sheet
                names = ['statement of operation', 'statement of income', 'statements of operation',
                         'statements of income']
                checker = [name for name in names if name in i.lower()]
                if checker:
                    find_next_table = True
                    continue

                if find_next_table:
                    if '<table>' in i.lower():
                        in_table = True
                        find_next_table = False
                    continue

                if in_table:
                    table_printer.append(i)
                    if '</table>' in i.lower():
                        full_str = '\n'.join(table_printer[:-1])
                        if 'net income' in full_str.lower():
                            starter = 0
                            for num, el in enumerate(table_printer[:-1]):
                                if date in el.lower():
                                    row = [i for i in re.split('\s', el) if i != '']
                                    print(row)
                                    col = [' '.join(row[:-3]), row[-3], row[-2], row[-1]]
                                    break
                            for num, el in enumerate(table_printer[:-1]):
                                if ('<s>' in el.lower()) or ('<c>' in el.lower()):
                                    starter = num
                            table_str = '\n'.join(table_printer[starter:-1])
                            table_str = table_str.replace('-', ' ')
                            table_str = table_str.replace(')', ' ')
                            table_str = table_str.replace('(', '-')
                            table_str = table_str.replace('$', ' ')
                            na_values = ['-' * i for i in range(20)] + ['=' * i for i in range(20)]
                            table = pd.DataFrame(pd.read_fwf(StringIO(table_str), na_values=na_values))
                            table.columns = col
                            table = table.dropna(axis=0, how='all')

                            df = table
                            # Converting to multiindex
                            # Get index of table
                            first_column = df[df.columns[0]]
                            second_part_index = list(first_column)

                            # Get multiindex header of table
                            first_column_name = str(first_column.name).lower().replace('-', ' ').strip(' ')

                            # Up to you -- np.nan for other columns or the same column name
                            new_column = [[first_column_name] + [np.nan for _ in range(len(list(df.columns)[1:]) - 1)],
                                          list(df.columns)[1:]]
                            new_column = [[np.nan, np.nan] + i for i in new_column]

                            # Drop index of table
                            df = df.drop(df.columns[0], axis=1)

                            # Separate rows into first index rows and second index rows -- first index row are rows with empty values
                            null_rows = df[df.isnull().all(1)]
                            null_rows = list(null_rows.index)
                            first_index = []
                            for position, i in enumerate(second_part_index):
                                if position in null_rows:
                                    first_index.append(i)
                                else:
                                    first_index.append('Null')

                            # Getting multiindex for index of table -- i.e. second and first index of the multiindex
                            second_index = [b if a == 'Null' else np.nan for a, b in
                                            zip(first_index, second_part_index)]
                            first_index = [np.nan if i == 'Null' else i for i in first_index]
                            new_index = [[a, b] for a, b in zip(first_index, second_index)]


                            table_values = df.values.tolist()
                            table_values = [a + b for a, b in zip(new_index, table_values)]
                            table_values = new_column + table_values

                            return table_values
                        else:
                            table_printer = []
                            in_table = False
            print(f"Couldn't find table for {date}")
            return []


    def get_cash_flow_statement_tables(self, resp, html_bool=True, filingsummary=False):
        # For newer 10-K -- html forms
        if html_bool:
            if filingsummary != 'False':
                if filingsummary == 'xml':
                    xmlparser = etree.XMLParser()

                    # Parse the xml webpage
                    dom = etree.fromstring(resp, parser=xmlparser)
                    # Transform the webpage to a single table from the .xslt file provided by edgar
                    xsl_filename = f'{pathlib.Path(__file__).parent.resolve()}/InstanceReport.xslt'
                    xslt = etree.parse(xsl_filename, parser=xmlparser)
                    transform = etree.XSLT(xslt)
                    newdoc = transform(dom)

                    table = etree.tostring(newdoc.xpath('//table[1]')[0])
                    df = pd.DataFrame(pd.read_html(table)[0])
                else:
                    doc = etree.fromstring(resp, parser=etree.HTMLParser())
                    table_in_doc = etree.tostring(doc.xpath('//table[1]')[0])
                    df = pd.DataFrame(pd.read_html(table_in_doc)[0])

                # Get index of table
                first_column = df[df.columns[0]]
                second_part_index = list(first_column)

                if filingsummary == 'html':
                    # Get multiindex header of table
                    first_column_name = str(first_column.name).lower().replace('-', '').strip(' ')

                    # Up to you -- np.nan for other columns or the same column name
                    new_column = [[first_column_name] + [np.nan for _ in range(len(list(df.columns)[1:]) - 1)],
                                  list(df.columns)[1:]]
                    # new_column = [([first_column_name] * len(list(df.columns)[1:])), list(df.columns)[1:]]
                    # new_column = pd.MultiIndex.from_arrays(new_column)

                    # Add 2 NaN values behind each header since this isn't really multiindex -- the table printed in excel
                    # doesn't show multiindex as well as jupyter notebook anyway
                    new_column = [[np.nan, np.nan] + i for i in new_column]
                else:
                    # Since the columns declared are already multiindexed columns, the table from xml will have to be parsed
                    # slightly differently
                    columns = [list(a) for a in df.columns]
                    columns = list(map(list, zip(*columns)))
                    new_column = [[np.nan] + i for i in columns]

                # Drop index of table
                df = df.drop(df.columns[0], axis=1)

                # Separate rows into first index rows and second index rows -- first index row are rows with empty values
                null_rows = df[df.isnull().all(1)]
                null_rows = list(null_rows.index)
                first_index = []
                for position, i in enumerate(second_part_index):
                    if position in null_rows:
                        first_index.append(i)
                    else:
                        first_index.append('Null')

                # Getting multiindex for index of table -- i.e. second and first index of the multiindex
                second_index = [b if a == 'Null' else np.nan for a, b in zip(first_index, second_part_index)]
                first_index = [np.nan if i == 'Null' else i for i in first_index]
                new_index = [[a, b] for a, b in zip(first_index, second_index)]

                # Merge everything together into a single table
                table_values = df.values.tolist()
                table_values = [a + b for a, b in zip(new_index, table_values)]
                table_values = new_column + table_values
                return table_values
            else:
                # Declare parser so that efficiency is preserved
                utf8_parser = html.HTMLParser(encoding='utf-8')
                fulldoc = html.document_fromstring(resp, parser=utf8_parser)

                list_of_hyperlink_texts = ['statement of cash flow', 'statements of cash flow']
                list_of_texts_before_table = ['consolidated statement of cash flow',
                                             'consolidated statements of cash flow',
                                             'condensed consolidated statement of cash flow',
                                             'condensed consolidated statements of cash flow',
                                             'consolidated condensed statement of cash flow',
                                             'consolidated condensed statements of cash flow']
                list_of_texts_in_table = ['operating activities', 'financing activities', 'investing activities', 'net income']

                table_finder = finder(fulldoc)
                # Try finding through the 3 different methods outlined under table_finder
                table = table_finder.find_hyperlink_text_to_table(list_of_hyperlink_texts,form='cash flow statements')
                if table == 'Null':
                    table = table_finder.find_text_before_table(list_of_texts_before_table,form='cash flow statements')
                if table == 'Null':
                    table = table_finder.find_text_in_table(list_of_texts_in_table,form='cash flow statements')
                # if table is empty
                if table == 'Null':
                    print(f'Couldnt find table for some reason -- {etree.tostring(fulldoc.xpath(".//table[1]")[0])}')
                    return []

                # Convert string to pandas dataframe -- it seems unicode encoding provides the best results (for the dataframe)
                # Remove $ from the data and () that indicate negative flows for easier cleaning
                table = str(etree.tostring(table, encoding='unicode', method='html')).replace("$", " ").replace('(','-').replace(')', ' ')
                panda_table = pd.DataFrame(pd.read_html(table)[0])
                df = panda_table.drop_duplicates().dropna(how='all').reset_index(drop=True).T.drop_duplicates().dropna(how='all').reset_index(drop=True).T

                # Get last row num that begins with a NaN value
                row_num = 0
                df = df.fillna('Null')
                for _ in df.itertuples(index=False):
                    row = df.iloc[row_num]
                    if row[0] == 'Null':
                        row_num += 1
                        continue
                    else:
                        row_num -= 1
                        break
                last_header_row = list(set([i for i in df.iloc[row_num, :] if i != 'Null']))
                df = df.replace('Null', np.nan)

                # Get columns where dates are the same
                # Get the one with most data and filter it out
                for column in last_header_row:
                    # Get all columns where dates are the same
                    all_columns = df[df.columns[df.iloc[row_num] == column]]
                    columns = [i for i in all_columns.columns]
                    first_column = df[columns[0]]
                    other_columns = columns[1:]
                    # Fill NaN values in first column with values from the other duplicate columns, then drop the duplicate columns
                    for i in other_columns:
                        first_column = first_column.fillna(all_columns[i])
                        df = df.drop(columns=[i])
                    df[columns[0]] = first_column

                df.columns = df.iloc[row_num - 1, :] + df.iloc[row_num, :]
                df = df.drop(row_num)
                df = df.drop(row_num - 1)
                df = df.dropna(axis=1, how='all')

                # Converting to multiindex
                # Get index of table
                first_column = df[df.columns[0]]
                second_part_index = list(first_column)

                # Get multiindex header of table
                first_column_name = str(first_column.name).lower().replace('-', '').strip(' ')

                # Up to you -- np.nan for other columns or the same column name
                new_column = [[first_column_name] + [np.nan for _ in range(len(list(df.columns)[1:]) - 1)],
                              list(df.columns)[1:]]
                new_column = [[np.nan, np.nan] + i for i in new_column]

                # Drop index of table
                df = df.drop(df.columns[0], axis=1)

                # Separate rows into first index rows and second index rows -- first index row are rows with empty values
                null_rows = df[df.isnull().all(1)]
                null_rows = list(null_rows.index)
                first_index = []
                for position, i in enumerate(second_part_index):
                    if position in null_rows:
                        first_index.append(i)
                    else:
                        first_index.append('Null')

                # Getting multiindex for index of table -- i.e. second and first index of the multiindex
                second_index = [b if a == 'Null' else np.nan for a, b in zip(first_index, second_part_index)]
                first_index = [np.nan if i == 'Null' else i for i in first_index]
                new_index = [[a, b] for a, b in zip(first_index, second_index)]

                table_values = df.values.tolist()
                table_values = [a + b for a, b in zip(new_index, table_values)]
                table_values = new_column + table_values
                return table_values
        # For older forms -- txt files
        else:
            doc_str = re.split("\n", str(resp.decode('utf-8')))

            find_next_table = False
            table_printer = []
            in_table = False
            date = '1990'
            for num, i in enumerate(doc_str):
                # Find date of filing
                if 'conformed period of report' in i.lower():
                    date = re.split('\s', i)[-1][:4]

                # Find table for balance sheet
                names = ['statement of cash flow', 'statements of cash flow', 'cash flow']
                checker = [name for name in names if name in i.lower()]
                if checker:
                    find_next_table = True
                    continue

                if find_next_table:
                    if '<table>' in i.lower():
                        in_table = True
                        find_next_table = False
                    continue

                if in_table:
                    table_printer.append(i)
                    if '</table>' in i.lower():
                        full_str = '\n'.join(table_printer[:-1])

                        if [i for i in ['operating activities', 'financing activities', 'investing activities'] if i in full_str.lower()]==['operating activities', 'financing activities', 'investing activities']:
                            starter = 0
                            for num, el in enumerate(table_printer[:-1]):
                                if date in el.lower():
                                    row = [i for i in re.split('\s', el) if i != '']
                                    print(row)
                                    col = [' '.join(row[:-3]), row[-3], row[-2], row[-1]]
                                    break
                            for num, el in enumerate(table_printer[:-1]):
                                if ('<s>' in el.lower()) or ('<c>' in el.lower()):
                                    starter = num
                            table_str = '\n'.join(table_printer[starter:-1])
                            table_str = table_str.replace('-', ' ')
                            table_str = table_str.replace(')', ' ')
                            table_str = table_str.replace('(', '-')
                            table_str = table_str.replace('$', ' ')
                            na_values = ['-' * i for i in range(20)] + ['=' * i for i in range(20)]
                            table = pd.DataFrame(pd.read_fwf(StringIO(table_str), na_values=na_values))
                            table.columns = col
                            table = table.dropna(axis=0, how='all')

                            df = table
                            # Converting to multiindex
                            # Get index of table
                            first_column = df[df.columns[0]]
                            second_part_index = list(first_column)

                            # Get multiindex header of table
                            first_column_name = str(first_column.name).lower().replace('-', ' ').strip(' ')

                            # Up to you -- np.nan for other columns or the same column name
                            new_column = [[first_column_name] + [np.nan for _ in range(len(list(df.columns)[1:]) - 1)],
                                          list(df.columns)[1:]]
                            new_column = [[np.nan, np.nan] + i for i in new_column]

                            # Drop index of table
                            df = df.drop(df.columns[0], axis=1)

                            # Separate rows into first index rows and second index rows -- first index row are rows with empty values
                            null_rows = df[df.isnull().all(1)]
                            null_rows = list(null_rows.index)
                            first_index = []
                            for position, i in enumerate(second_part_index):
                                if position in null_rows:
                                    first_index.append(i)
                                else:
                                    first_index.append('Null')

                            # Getting multiindex for index of table -- i.e. second and first index of the multiindex
                            second_index = [b if a == 'Null' else np.nan for a, b in
                                            zip(first_index, second_part_index)]
                            first_index = [np.nan if i == 'Null' else i for i in first_index]
                            new_index = [[a, b] for a, b in zip(first_index, second_index)]

                            table_values = df.values.tolist()
                            table_values = [a + b for a, b in zip(new_index, table_values)]
                            table_values = new_column + table_values

                            return table_values
                        else:
                            table_printer = []
                            in_table = False
            print(f"Couldn't find table for {date}")
            return []

if __name__ == '__main__':
    header = {
        'authority': 'scrapeme.live',
        'dnt': '1',
        'upgrade-insecure-requests': '1',
        'user-agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_4) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/83.0.4103.61 Safari/537.36',
        'accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.9',
        'sec-fetch-site': 'none',
        'sec-fetch-mode': 'navigate',
        'sec-fetch-user': '?1',
        'sec-fetch-dest': 'document',
        'accept-language': 'en-GB,en-US;q=0.9,en;q=0.8',
    }
    resp1 = requests.get('https://www.sec.gov/Archives/edgar/data/320193/000032019321000065/aapl-20210626.htm',
                         headers=header)
    tryio = scraper()
    pprint(tryio.get_balance_sheet_tables(resp1.content, html_bool=True))

