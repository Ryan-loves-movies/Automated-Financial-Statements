import bs4 as bs
# import sys
# if sys.version_info[0] < 3:
#     from StringIO import StringIO
# else:
#     from io import StringIO
import pandas as pd
from pprint import pprint
import re

import requests

class scraper():
    def __init__(self):
        pass
    def get_tables(self, resp1, html = True):
        """

        Parameters
        ----------
        resp1: Just the raw response from requests.get('...')
        resp1: Just the raw response text from aiohttp.ClientSession().get(url).text()
        html: If True, will scrape based on latest formats of filings to get table
              Else, will scrape using old format for older filings (typically before 2000)
              (True --> html files
              False --> txt files)

        Returns a list of lists which can be directly converted into a pandas dataframe
        --> Containing the raw table data with raw row headers and columns
        -------

        """
        # Newer forms, based on html
        if html==True:
            # resp = bs.BeautifulSoup(resp1.content, 'lxml')
            resp = bs.BeautifulSoup(resp1, 'lxml')
            # Try all possibilities of text as different companies may or may not include the ':' in the
            # header for the table and some forms may have current assets have a block of whitespace in the
            # text string after the phrase and such
            try:
                tables = resp.find(text='Current liabilities:').find_parent('table')
            except:
                try:
                    tables = resp.find(text='Current assets:').find_parent('table')
                except:
                    try:
                        tables = resp.findAll(text='Consolidated Balance Sheets')
                        print(f'Number of strings "Consolidated Balance Sheets" is {len(tables)}\n')
                        tables = [i for i in tables if i.find_parent('table')==None]
                        print(f'Number of strings "Consolidated Balance Sheets" with no tables containing them is {len(tables)}')
                        tables = tables[0].find_next('table')
                        if tables == None:
                            raise Exception("Couldn't find the table even when finding text 'Consolidated Balance Sheets'")
                    except:
                        try:
                            tables = resp.find(text='Current assets').find_parent('table')
                        except:
                            try:
                                tables = resp.find(text='Current liabilities').find_parent('table')
                            except Exception as e:
                                resp = resp.findAll('table')[:5]
                                return f'Couldn\'t find table for some reason --\n{e}\n{resp}'
            # Remove $ from the data and () that indicate negative flows for easier cleaning
            panda_table = pd.DataFrame(pd.read_html(str(tables).replace("$", " ").replace('(', '-').replace(')', ' '))[0])

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
                    print(new_row)
                    ho = resp.findAll('div')
                    print(ho[:40])
                else:
                    new_df.append(new_row)

            panda_table = pd.DataFrame(new_df)
            panda_table = panda_table.dropna(axis=0, how='all')

            return (panda_table.values.tolist())

        # Older forms -- Non-html forms
        else:
            tables = bs.BeautifulSoup(resp1, 'lxml').findAll('table')
            for table in tables:
                try:
                    found = table.s.c.c
                except:
                    found = ''
                if type(found) == bs.element.Tag:
                    found_text = found.text
                    if 'Current assets:' in found_text:

                        # Extract the Date Headers as a list
                        caption_text = table.caption.text.replace(found.text, '')
                        caption_text = caption_text.replace('-', ' ')
                        caption_text = caption_text.replace('=', ' ')
                        caption_text = re.split('\n', caption_text)
                        caption_text = [i for i in caption_text if (
                                    i.replace(' ', '').replace('\t', '') != '' and i.replace(' ', '').replace('\t',
                                                                                                              '') != '(Unaudited)')]
                        new_caption_text = []
                        for row in caption_text:
                            new_row = re.split('\s\s|\t', row)
                            new_row = [i.strip(' ') for i in new_row if (i != '' and i != '(Unaudited)')]
                            new_caption_text.append(new_row)
                        caption_text = new_caption_text
                        if len(caption_text) == 1:
                            # Only 1 row
                            caption_text = caption_text[0]
                            if len(caption_text) == 2:
                                caption_text.insert(0, None)
                            elif len(caption_text) == 3:
                                pass
                            else:
                                print('caption is not of length 2 or 3: \n', caption_text)
                        elif len(caption_text) == 2:
                            # 2 rows
                            for row in caption_text:
                                if len(row) == 1:
                                    if row[0] == 'ASSETS':
                                        row.append(None)
                                        row.append(None)
                                    else:
                                        row.insert(0, None)
                                        row.insert(0, None)
                                elif len(row) == 2:
                                    row.insert(0, None)
                                elif len(row) == 3:
                                    pass
                                else:
                                    print('why is this row in caption text not of length 2 or 3? \n', row)
                        #             print(caption_text)

                        # Extract the table without the headers
                        found_text = str(found_text).replace('$', ' ')
                        found_text = found_text.replace('-', ' ')
                        found_text = found_text.replace('=', ' ')
                        found_text = found_text.replace('.', ' ')
                        found_text = found_text.replace('(', '-')
                        found_text = found_text.replace(')', ' ')
                        found_text = found_text.split("\n")
                        new_li = []
                        for i in found_text:
                            new_item = re.split('\t|\s\s', i)
                            new_item = [i for i in new_item if (i != '' and i != ' ')]
                            new_li.append(new_item)

                        new_li = [i for i in new_li if i != []]
                        fin_res = []
                        for i in new_li:
                            if len(i) == 1:
                                fin_res.append([i[0].strip(' '), None, None])
                            elif len(i) == 2:
                                try:
                                    cleaned_0 = i[0].replace(' ', '').replace(',', '')
                                    cleaned_1 = i[1].replace(' ', '').replace(',', '')
                                    fin_res.append([None, str(int(cleaned_0)), str(int(cleaned_1))])
                                except:
                                    #                         print(i)
                                    fin_res.append(
                                        [' '.join(i[0].split(' ')[:-1]), i[0].split(' ')[-1], i[1].strip(' ')])
                            elif len(i) == 3:
                                try:
                                    cleaned_1 = i[1].replace(' ', '').replace(',', '')
                                    cleaned_2 = i[2].replace(' ', '').replace(',', '')
                                    fin_res.append([i[0].replace('.', ''), str(int(cleaned_1)), str(int(cleaned_2))])
                                except:
                                    #                         print(i)
                                    try:
                                        cleaned_2 = i[2].replace(' ', '').replace(',', '').strip(' ')
                                        fin_res.append([i[0] + ' ' + " ".join(i[1].split(' ')[:-1]), str(int(
                                            i[1].split(' ')[-1].replace(',', '').replace(' ', ''))),
                                                        str(int(cleaned_2))])
                                    except:
                                        fin_res.append([(i[0] + ' ' + i[1] + ' ' + i[2]), None, None])
                            else:
                                try:
                                    first_input = " ".join(i[:-2])
                                    cleaned_1 = i[-2].replace(' ', '').replace(',', '')
                                    cleaned_2 = i[-1].replace(' ', '').replace(',', '')
                                    fin_res.append([i[0], str(int(cleaned_1)), str(int(cleaned_2))])
                                except:
                                    print('why is this length greater than 3', '\n', i)
                                    fin_res.append([" ".join(i)])

                        # Combine Caption with table
                        if type(caption_text[0]) == list:
                            for row in caption_text[::-1]:
                                fin_res.insert(0, row)
                        else:
                            fin_res.insert(0, caption_text)

                        return fin_res

            # The oldest tables -- txt files which don't return because the table is within a <page> tag and not
            # a <table> tag

            # If the above block doesn't return a table, then it doesn't extract anything, so try this extraction instead, except raise errror
            assets_pages = bs.BeautifulSoup(resp1, 'lxml').find(string=re.compile('Current assets:'))
            liabilities_pages = bs.BeautifulSoup(resp1, 'lxml').find(string=re.compile('Current liabilities:'))

            def extract_table(assets_pages):
                assets = assets_pages.text
                assets = assets.replace('$', ' ')
                assets = assets.replace('(', '-')
                assets = assets.replace(')', ' ')


                assets = assets.split('\n')
                # Removes 'see accompanying notes' text and page number
                assets = [i.strip(' ') for i in assets if
                          (i.strip(' ') != '' and i.strip(' ') != 'See accompanying notes.')][:-1]


                new_assets = [re.split('\s\s', i) for i in assets]
                #        removing the empty '' from the lists
                new_assets = [[i.strip(' ') for i in asset if i.strip(' ') != ''] for asset in new_assets]
                #         new_assets = [i.split('  ') for i in assets]
                for asset in new_assets:
                    if len(asset) == 1:
                        asset.append(None)
                        asset.append(None)
                    elif len(asset) == 2:
                        asset.insert(0, None)
                    elif len(asset) == 3:
                        pass
                    else:
                        throw = " ".join(asset)
                        asset.clear()
                        asset.append([throw, None, None])
                return new_assets
            try:
                assets = extract_table(assets_pages)
                liabilities = extract_table(liabilities_pages)

                for i in liabilities:
                    if i in assets:
                        pass
                    else:
                        assets.append(i)

                return assets
            except TypeError:
                print(f'Type Error: no table found for this request -- \n{resp1}')
            except AttributeError:
                print(f'No table found for this request -- \n{resp1}{AttributeError}')

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
    pprint(tryio.get_tables(resp1, html=True))

