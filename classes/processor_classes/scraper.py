import pathlib
script_path = str(pathlib.Path(__file__).parent.resolve())
import sys
sys.path.append(script_path)
from table_finder import finder

import bs4 as bs
import pandas as pd
from pprint import pprint
import numpy as np
import requests
from lxml import etree, html


class scraper():
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
                    new_column = [[first_column_name] + [np.nan * (len(list(df.columns)[1:])-1)], list(df.columns)[1:]]
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
                list_of_texts_before_table = ['consolidated balance sheets',
                                              'condensed consolidated balance sheets',
                                              'consolidated condensed balance sheets',
                                              'consolidated balance sheet',
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
                ho = 'Null'
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
                new_column = [[first_column_name] + [np.nan * (len(list(df.columns)[1:]) - 1)], list(df.columns)[1:]]
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
            # Scrap everything under .txt files for the moment
            return []
            # tables = bs.BeautifulSoup(resp, 'lxml').findAll('table')
            # for table in tables:
            #     try:
            #         found = table.s.c.c
            #     except:
            #         found = ''
            #     if type(found) == bs.element.Tag:
            #         found_text = found.text
            #         if 'Current assets:' in found_text:
            #
            #             # Extract the Date Headers as a list
            #             try:
            #                 caption_text = table.caption.text.replace(found_text, '')
            #                 caption_text = caption_text.replace('-', ' ')
            #                 caption_text = caption_text.replace('=', ' ')
            #                 caption_text = re.split('\n', caption_text)
            #                 caption_text = [i for i in caption_text if (
            #                             i.replace(' ', '').replace('\t', '') != '' and i.replace(' ', '').replace('\t',
            #                                                                                                       '') != '(Unaudited)')]
            #                 new_caption_text = []
            #                 for row in caption_text:
            #                     new_row = re.split('\s\s|\t', row)
            #                     new_row = [i.strip(' ') for i in new_row if (i != '' and i != '(Unaudited)')]
            #                     new_caption_text.append(new_row)
            #                 caption_text = new_caption_text
            #                 if len(caption_text) == 1:
            #                     # Only 1 row
            #                     caption_text = caption_text[0]
            #                     if len(caption_text) == 2:
            #                         caption_text.insert(0, None)
            #                     elif len(caption_text) == 3:
            #                         pass
            #                     else:
            #                         print('caption is not of length 2 or 3: \n', caption_text)
            #                 elif len(caption_text) == 2:
            #                     # 2 rows
            #                     for row in caption_text:
            #                         if len(row) == 1:
            #                             if row[0] == 'ASSETS':
            #                                 row.append(None)
            #                                 row.append(None)
            #                             else:
            #                                 row.insert(0, None)
            #                                 row.insert(0, None)
            #                         elif len(row) == 2:
            #                             row.insert(0, None)
            #                         elif len(row) == 3:
            #                             pass
            #                         else:
            #                             print('why is this row in caption text not of length 2 or 3? \n', row)
            #             except AttributeError:
            #                 caption_text = [None,None,None]
            #
            #             # Extract the table without the headers
            #             found_text = str(found_text).replace('$', ' ')
            #             found_text = found_text.replace('-', ' ')
            #             found_text = found_text.replace('=', ' ')
            #             found_text = found_text.replace('.', ' ')
            #             found_text = found_text.replace('(', '-')
            #             found_text = found_text.replace(')', ' ')
            #             found_text = found_text.split("\n")
            #             new_li = []
            #             for i in found_text:
            #                 new_item = re.split('\t|\s\s', i)
            #                 new_item = [i for i in new_item if (i != '' and i != ' ')]
            #                 new_li.append(new_item)
            #
            #             new_li = [i for i in new_li if i != []]
            #             fin_res = []
            #             for i in new_li:
            #                 if len(i) == 1:
            #                     fin_res.append([i[0].strip(' '), None, None])
            #                 elif len(i) == 2:
            #                     try:
            #                         cleaned_0 = i[0].replace(' ', '').replace(',', '')
            #                         cleaned_1 = i[1].replace(' ', '').replace(',', '')
            #                         fin_res.append([None, str(int(cleaned_0)), str(int(cleaned_1))])
            #                     except:
            #                         #                         print(i)
            #                         fin_res.append(
            #                             [' '.join(i[0].split(' ')[:-1]), i[0].split(' ')[-1], i[1].strip(' ')])
            #                 elif len(i) == 3:
            #                     try:
            #                         cleaned_1 = i[1].replace(' ', '').replace(',', '')
            #                         cleaned_2 = i[2].replace(' ', '').replace(',', '')
            #                         fin_res.append([i[0].replace('.', ''), str(int(cleaned_1)), str(int(cleaned_2))])
            #                     except:
            #                         #                         print(i)
            #                         try:
            #                             cleaned_2 = i[2].replace(' ', '').replace(',', '').strip(' ')
            #                             fin_res.append([i[0] + ' ' + " ".join(i[1].split(' ')[:-1]), str(int(
            #                                 i[1].split(' ')[-1].replace(',', '').replace(' ', ''))),
            #                                             str(int(cleaned_2))])
            #                         except:
            #                             fin_res.append([(i[0] + ' ' + i[1] + ' ' + i[2]), None, None])
            #                 else:
            #                     try:
            #                         first_input = " ".join(i[:-2])
            #                         cleaned_1 = i[-2].replace(' ', '').replace(',', '')
            #                         cleaned_2 = i[-1].replace(' ', '').replace(',', '')
            #                         fin_res.append([i[0], str(int(cleaned_1)), str(int(cleaned_2))])
            #                     except:
            #                         print('why is this length greater than 3', '\n', i)
            #                         fin_res.append([" ".join(i)])
            #
            #             # Combine Caption with table
            #             if type(caption_text[0]) == list:
            #                 for row in caption_text[::-1]:
            #                     fin_res.insert(0, row)
            #             else:
            #                 fin_res.insert(0, caption_text)
            #
            #             return fin_res
            #
            # # The oldest tables -- txt files which don't return because the table is within a <page> tag and not
            # # a <table> tag
            #
            # # If the above block doesn't return a table, then it doesn't extract anything, so try this extraction instead, except raise errror
            # assets_pages = bs.BeautifulSoup(resp, 'lxml').find(string=re.compile('Current assets:'))
            # liabilities_pages = bs.BeautifulSoup(resp, 'lxml').find(string=re.compile('Current liabilities:'))
            #
            # def extract_table(assets_pages):
            #     assets = assets_pages.text
            #     assets = assets.replace('$', ' ')
            #     assets = assets.replace('(', '-')
            #     assets = assets.replace(')', ' ')
            #
            #
            #     assets = assets.split('\n')
            #     # Removes 'see accompanying notes' text and page number
            #     assets = [i.strip(' ') for i in assets if
            #               (i.strip(' ') != '' and i.strip(' ') != 'See accompanying notes.')][:-1]
            #
            #
            #     new_assets = [re.split('\s\s', i) for i in assets]
            #     #        removing the empty '' from the lists
            #     new_assets = [[i.strip(' ') for i in asset if i.strip(' ') != ''] for asset in new_assets]
            #     #         new_assets = [i.split('  ') for i in assets]
            #     for asset in new_assets:
            #         if len(asset) == 1:
            #             asset.append(None)
            #             asset.append(None)
            #         elif len(asset) == 2:
            #             asset.insert(0, None)
            #         elif len(asset) == 3:
            #             pass
            #         else:
            #             throw = " ".join(asset)
            #             asset.clear()
            #             asset.append([throw, None, None])
            #     return new_assets
            # try:
            #     assets = extract_table(assets_pages)
            #     liabilities = extract_table(liabilities_pages)
            #
            #     for i in liabilities:
            #         if i in assets:
            #             pass
            #         else:
            #             assets.append(i)
            #
            #     return assets
            # except TypeError:
            #     print(f'\n{resp}\n -- Type Error: no table found for this request')
            # except AttributeError:
            #     print(f'No table found for this request -- \n{resp}\n{AttributeError}')

    def get_income_statement_tables(self, resp, html_bool=True):
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
            def try_find(resp, list_of_texts):
                def try_find_once(resp, text_to_find):
                    res = resp.findAll(text=lambda x: x.lower().strip(' ') == text_to_find)
                    res = [i for i in res if i.find_parent('table') == None]
                    if len(res) == 0:
                        return None
                    elif len(res) == 1:
                        return res[0]
                    else:
                        print(res)
                        return None

                text_before_table = None
                for text in list_of_texts:
                    text_before_table = try_find_once(resp, text)
                    if text_before_table != None:
                        break
                    else:
                        continue
                return text_before_table

            page = bs.BeautifulSoup(resp, 'lxml')
            list_of_texts = ['consolidated statements of operations',
                             'consolidated statements of income',
                             'consolidated statement of income',
                             'consolidated statement of operations']
            tryout = try_find(page, list_of_texts)
            if tryout != None:
                print('Success')
                table = tryout.find_next('table')
                table = str(table).replace('$', ' ')
                df = pd.DataFrame(pd.read_html(table)[0])

                # Replace empty values with None
                df = df.replace('\xa0', np.nan)
                df = df.applymap(lambda x: np.nan if isinstance(x, str) and (not x or x.isspace()) else x)

                # Remove duplicates and NaN rows and columns in the dataframe
                df = df.drop_duplicates().dropna(how='all').reset_index(drop=True).T.drop_duplicates().dropna(
                    how='all').reset_index(drop=True).T

                row_num = 0
                df = df.fillna('Null')
                for i in df.itertuples(index=False):
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
                    all_columns = df[df.columns[df.iloc[row_num] == column]]
                    columns = [i for i in all_columns.columns]
                    first_column = df[columns[0]]
                    other_columns = columns[1:]
                    for i in other_columns:
                        first_column = first_column.fillna(all_columns[i])
                        df = df.drop(columns=[i])
                    df[columns[0]] = first_column

                # Get the first few rows that have first value NaN, break loop as soon as first value is no longer NaN
                df = df.fillna('Null')
                header_list = []
                header_row_no = []
                row_num = 0
                for i in df.itertuples(index=False):
                    row = df.iloc[row_num]
                    if row[0] == 'Null':
                        header_list.append(row.values.tolist())
                        header_row_no.append(row_num)
                    else:
                        break
                    row_num += 1

                # Create MultiIndex object from header list to append to dataframe
                indexed_header_list = pd.MultiIndex.from_arrays(header_list)

                # Add headers back to dataframe and remove the rows that are added as headers
                df.columns = indexed_header_list
                df = df.drop(header_row_no).reset_index(drop=True)

                # Change 'Null' values in table back to None/np.nan for easier cleaning
                # Drop NaN columns
                df = df.replace('Null', np.nan)
                df = df.dropna(axis=1, how='all')

                # Set first column as index -- NOTE: first column name taken from header_list
                df = df.set_index([(header_list[0][0], header_list[1][0])])
                # Before having edited the code to clean the dataframe, this line was needed so that
                # parantheses would not be around the index column,
                # Yet, surprisingly enough, it seems after the edit that this would instead cause an error
                # as the index is no longer a tuple?
                # df.index = df.index.map(lambda x: x[0])

                # Find all rows with full NaN values
                nan_rows = df.index[df.isnull().all(1)]
                nan_rows = [i for i in nan_rows]
                full_index = [i for i in df.index]

                # Get 2 lists to combine to be multiindex for new index
                first_index = [i if (i in nan_rows) else np.nan for i in full_index]
                second_index = [i if (i not in nan_rows) else np.nan for i in full_index]
                new_index = [first_index, second_index]
                new_index = pd.MultiIndex.from_arrays(new_index, names=[np.nan, np.nan])
                df = df.set_index(new_index)
        # For older forms -- txt files
        # else:


    def get_cash_flow_statement_tables(self, resp, html_bool=True):
        pass

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

