import requests
from classes.processor_classes.user_agents import headers
from json.decoder import JSONDecodeError
import pandas as pd
# import threading
from classes.processor_classes.balance_sheet_scraper import scraper
from pprint import pprint
import asyncio
import aiohttp


class processor():
    def __init__(self, ticker_form_list):
        """

        Parameters
        ----------
        ticker_form_list: Raw output from retriver(...).retrieve_data(...)
                          i.e. [['TSLA', '10-K'],
                                ['RBLX', '10-Q'],
                                ...]

        """
        self.ticker_form_list = ticker_form_list
        self.ticker_list = []
        for i in ticker_form_list:
            self.ticker_list.append(i[0])
        self.data = []

    def get_json_cik(self):
        """

        Returns
        -------
        Dictionary of ticker symbols of list containing cik, json cik, and form type

        E.g. {'AAPL': ['320193', 'CIK0000320193.json', '10-Q'], 'MSFT': ['789019', 'CIK0000789019.json', '10-K']}

        """
        header = headers().header()
        data = requests.get('https://www.sec.gov/files/company_tickers_exchange.json', headers = header).json()
        ticker_to_cik = {}
        for i in data['data']:
            ticker_to_cik[i[2]] = i[0]

        self.full_urls = {}
        for i in self.ticker_form_list:
            ticker = i[0]
            form = i[1]
            cik = str(ticker_to_cik.get(ticker))
            json_cik = 'CIK' + cik.zfill(10) + '.json'
            self.full_urls[ticker] = [cik, json_cik, form]
        # print(self.full_urls)
        return self.full_urls

    def get_form_links(self, dictionary_cik):
        """

        Parameters
        ----------
        dictionary_cik: Dictionary returned from scraper(ticker_form_list).get_json_cik()

        Returns a dictionary of ticker symbols that give list of reportDate, form type, form decription, link and
        a boolean as the last variable to signify if the url should be parsed as html or txt

        E.g.
        {'AAPL': {'10-K': [['2021-09-25',
                    '10-K',
                    '320193/000032019321000105/aapl-20210925.htm'
                    True], ...
                    }
        'MSFT': {'10-K': [['2021-06-30',
                    '10-K',
                    '789019/000156459021039151/msft-10k_20210630.htm',
                    True], ...
                    }
        -------

        """
        fin_dict = {}
        header = headers().header()
        base_filings_url = 'https://data.sec.gov/submissions/'
        for parameter_list in dictionary_cik:
            ticker_symbol = parameter_list
            fin_dict[ticker_symbol] = {}
            parameter_list = dictionary_cik[parameter_list]
            cik = parameter_list[0]
            json_cik = parameter_list[1]
            form_to_fetch = parameter_list[2]

            all_filings = requests.get(base_filings_url + json_cik, headers=header)
            try:
                all_filings = all_filings.json()
            except JSONDecodeError:
                print('empty response from filings urls!')

            # Recent Filings
            recent_filings = pd.DataFrame(all_filings['filings']['recent'])

            # Fetch older filings
            older_filings = all_filings['filings']['files']
            # Attempt at async to make things faster here --> I don't think I'll async the whole code
            # as the requests made above only makes 1 request per ticker, which isn't too excessive

            if older_filings != []:
                if len(older_filings) == 1:
                    filing = older_filings[0]
                    each_older_filings = filing['name']
                    each_older_filings = requests.get(base_filings_url+each_older_filings, headers=header).json()
                    each_older_filings = pd.DataFrame(each_older_filings)
                    recent_filings = pd.DataFrame(pd.concat([recent_filings, each_older_filings], ignore_index=True))
                else:
                    urls = []
                    for filing in older_filings:
                        each_older_filings = filing['name']
                        urls.append(base_filings_url+each_older_filings)
                    async def get(url, session, old_df):
                        try:
                            async with session.get(url=url) as response:
                                resp = await response.json()
                                # print("Successfully got url {} with resp of length {}.".format(url, len(resp)))
                                return resp
                        except Exception as e:
                            print("Unable to get url {} due to {}.".format(url, e.__class__))
                    async def main_get(urls, old_df):
                        async with aiohttp.ClientSession(headers=header) as session:
                            ret = await asyncio.gather(*[get(url, session, old_df) for url in urls])
                            for i in ret:
                                old_df = pd.concat([old_df, pd.DataFrame(i)], ignore_index=True)
                            return old_df
                        # print(f"Finalized all. Return is a list of len {len(ret)} outputs.")
                    recent_filings = asyncio.run(main_get(urls, recent_filings))


            if form_to_fetch == '10-Q':
                fin_dict[ticker_symbol]['10-K'] = []
                fin_dict[ticker_symbol]['10-Q'] = []
                filings10q = recent_filings[recent_filings['form'] == '10-Q']
                filings10k = recent_filings[recent_filings['form'] == '10-K']
                full_filings = pd.DataFrame(pd.concat([filings10k, filings10q], ignore_index=True)).reset_index().drop(columns=['filingDate', 'acceptanceDateTime', 'act', 'fileNumber', 'filmNumber', 'items', 'size', 'isXBRL', 'isInlineXBRL'])
                for row in full_filings.itertuples():
                    if str(row.primaryDocument) != '':
                        if str(row.primaryDocument[-4:]) == '.txt':
                            form_url = (cik + '/' + str(row.accessionNumber) + '.txt')
                            html_bool = False
                        else:
                            form_url = (cik + '/' + ''.join(str(row.accessionNumber).split('-')) + '/' + str(
                                row.primaryDocument))
                            html_bool = True
                    else:
                        form_url = (cik + '/' + str(row.accessionNumber) + '.txt')
                        html_bool = False
                    if str(row.form) == '10-K':
                        fin_dict[ticker_symbol]['10-K'].append([str(row.reportDate), str(row.primaryDocDescription), form_url, html_bool])
                    elif str(row.form) == '10-Q':
                        fin_dict[ticker_symbol]['10-Q'].append([str(row.reportDate), str(row.primaryDocDescription), form_url, html_bool])

            elif form_to_fetch == '10-K':
                fin_dict[ticker_symbol]['10-K'] = []
                filings10k = recent_filings[recent_filings['form'] == '10-K']
                full_filings = pd.DataFrame(filings10k).reset_index().drop(
                    columns=['filingDate', 'acceptanceDateTime', 'act', 'fileNumber', 'filmNumber', 'items', 'size',
                             'isXBRL', 'isInlineXBRL'])
                for row in full_filings.itertuples():
                    if str(row.primaryDocument) != '':
                        if str(row.primaryDocument[-4:]) == '.txt':
                            form_url = (cik + '/' + str(row.accessionNumber) + '.txt')
                            html_bool = False
                        else:
                            form_url = (cik + '/' + ''.join(str(row.accessionNumber).split('-')) + '/' + str(
                                row.primaryDocument))
                            html_bool = True
                    else:
                        form_url = (cik + '/' + str(row.accessionNumber) + '.txt')
                        html_bool = False
                    if str(row.form) == '10-K':
                        fin_dict[ticker_symbol]['10-K'].append(
                            [str(row.reportDate), str(row.primaryDocDescription), form_url, html_bool])
                # print(fin_dict[ticker_symbol].get('10-Q')) returns None
        # pprint(fin_dict)
        return fin_dict

    def download(self, forms_dict):
        """

        Parameters
        ----------
        forms_dict: The direct output of self.get_form_links()
                    (i.e. {'AAPL': {'10-K': [['2021-09-25',
                                              '10-K',
                                              '320193/000032019321000105/aapl-20210925.htm',
                                              True],
                                              ...
                                    }
                           'MSFT': {'10-K': [['2021-06-30',
                                              '10-K',
                                              '789019/000156459021039151/msft-10k_20210630.htm',
                                              True],
                                              ...
                                    }
                          }
                    )

        Returns a similar output but with the tables in place of the url and boolean
                    (i.e. {'AAPL': {'10-K': [['2021-09-25',
                                              '10-K',
                                              [['Current assets:',None,None],
                                              ...]
                                              ],
                                              ...]
                                    }
                           'MSFT': {'10-K': [['2021-06-30',
                                              '10-K',
                                              [['Current assets:',None,None],
                                              ...]
                                              ],
                                              ...]
                                    }
                          }
                    )
        -------

        """
        header = headers().header()
        base_data_url = 'https://www.sec.gov/Archives/edgar/data/'

        def thread_func(new_list, url_li, header):
            date = url_li[0]
            form = url_li[1]
            actual_url = base_data_url + url_li[2]
            html_bool = url_li[3]
            resp = requests.get(actual_url, headers = header)
            res = scraper()
            table = res.get_tables(resp1 = resp, html = html_bool)
            new_list.append([date, form, table])
            # print(new_list)

        # Substituted out threading for async --  I think performance should be better (Edit: It is about 3 times faster)
        for ticker_symbol in forms_dict:
            ticker = forms_dict.get(ticker_symbol)
            list10k = ticker.get('10-K')
            # new_10k = []
            # threads = []
            # for each_filing in list10k:
            #     thread_func(new_10k, each_filing, header)
            #     t = threading.Thread(target=thread_func, args=[new_10k, each_filing, header])
            #     t.daemon = True
            #     t.start()
            #     threads.append(t)
            # for thread in threads:
            #     thread.join(timeout=20)
            # ticker['10-K'] = new_10k

            async def get(url, session):
                base_data_url = 'https://www.sec.gov/Archives/edgar/data/'
                date = url[0]
                form = url[1]
                actual_url = base_data_url + url[2]
                html_bool = url[3]
                # try:
                #     async with session.get(url=url) as response:
                #         resp = await response.read()
                #         res = scraper()
                #         table = res.get_tables(resp1=resp, html=html_bool)
                #         return [date, form, table]
                # except Exception as e:
                #     print("Unable to get url {} due to {}.".format(url, e.__class__))
                async with session.get(url=actual_url) as response:
                    resp = await response.read()
                    res = scraper()
                    table = res.get_tables(resp1=resp, html=html_bool)
                    return [date, form, table]

            async def main_get(urls):
                async with aiohttp.ClientSession(headers = header) as session:
                    ret = await asyncio.gather(*[get(url, session) for url in urls])
                    return ret

            ticker['10-K'] = asyncio.run(main_get(list10k))

            if ticker.get('10-Q') == None:
                pass
            else:
                list10q = ticker.get('10-Q')
                # new_10q = []
                # threads = []
                # for each_filing in list10q:
                #     thread_func(new_10q, each_filing, header)
                #     t = threading.Thread(target=thread_func, args=[new_10q, each_filing, header])
                #     t.daemon = True
                #     t.start()
                #     threads.append(t)
                # for thread in threads:
                #     thread.join(timeout=20)
                # ticker['10-Q'] = new_10q
                ticker['10-Q'] = asyncio.run(main_get(list10q))

        return forms_dict

if __name__=='__main__':
    tryout = processor([['AAPL', '10-Q'], ['MSFT', '10-K']])
    list = tryout.get_json_cik()
    links = tryout.get_form_links(list)
    pprint(links)
    pprint(tryout.download(links))