import threading
import time
from lxml import etree, html
import requests
from classes.processor_classes.user_agents import headers
from json.decoder import JSONDecodeError
import pandas as pd
from concurrent.futures import ThreadPoolExecutor
from classes.processor_classes.scraper import scraper
from pprint import pprint
import asyncio
import aiohttp
import pathlib


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

    def get_form_links(self, dictionary_cik, financial_statement):
        """

        Parameters
        ----------
        dictionary_cik: Dictionary returned from processor(ticker_form_list).get_json_cik()
        financial_statement: Text to search in the reports in FilingSummary.xml link

        Returns a dictionary of ticker symbols that give list of
        reportDate,
        form type,
        link,
        boolean to signify if the url should be parsed as html or txt and
        either a link to the table directly or a boolean False indicating it doesn't exist

        E.g.
        {'AAPL': {'10-K': [['2021-09-25',
                    '10-K',
                    '320193/000032019321000105/aapl-20210925.htm'
                    True,
                    'http://sec.gov/Archives/data/edgar/320193/000032019321000105/R2.htm'], ...
                    }
        'MSFT': {'10-K': [['2021-06-30',
                    '10-K',
                    '789019/000156459021039151/msft-10k_20210630.htm',
                    True,
                    False], ...
                    }
        -------

        """

        # Function to determine if FilingSummary.xml index exits and if it does,
        # append the link to form_url directly instead (filingsummary=True)
        # Else, filingsummary=False
        def get_filing_sum_link(url, header, table):
            """

            Parameters
            ----------
            url: '(CIK)/(acs-number)/FilingSummary.xml'
            header: Header for requests
            table: Report wanted --> 'consolidated balance sheets', ...

            Returns the exact full link to retrieve the table wanted --> Balance Sheet if the link is valid
            Else, return False
            -------

            """
            base_data_url = 'https://www.sec.gov/Archives/edgar/data/'
            full_url = base_data_url + url
            filing = requests.get(full_url, headers=header).content
            utf8_parser = html.HTMLParser(encoding='utf-8')
            fulldoc = html.document_fromstring(filing, parser=utf8_parser)
            reports = fulldoc.findall('.//report')

            # If reports is not an empty list
            if not reports:
                return False

            if table == 'consolidated balance sheets':
                for report in reports:
                    name = report.find('./shortname').text.strip(' ').lower()
                    if name == 'consolidated balance sheets statement' or name == 'consolidated balance sheets' or name == 'condensed consolidated balance sheets' or name == 'consolidated balance sheets (unaudited)':
                        try:
                            html_url = report.find('./htmlfilename').text
                            return full_url.replace('FilingSummary.xml', html_url)
                        except AttributeError:
                            xml_url = report.find('./xmlfilename').text
                            return full_url.replace('FilingSummary.xml', xml_url)
                    else:
                        if 'consolidated balance sheets' in name:
                            try:
                                html_url = report.find('./htmlfilename').text
                                table = requests.get(full_url.replace('FilingSummary.xml', html_url), headers = header).content
                                table_str = table.decode('utf-8')
                                if 'asset' in table_str and 'liabilities' in table_str and 'equity' in table_str:
                                    return full_url.replace('FilingSummary.xml', html_url)
                            except AttributeError:
                                xml_url = report.find('./xmlfilename').text
                                resp = requests.get(full_url.replace('FilingSummary.xml', xml_url),headers=header).content
                                xsl_filename = f'{pathlib.Path().resolve()}/InstanceReport.xslt'

                                xmlparser = etree.XMLParser()
                                # Parse response from website
                                dom = etree.fromstring(resp, parser=xmlparser)
                                # Use the xslt file provided by edgar to transform the contents to a html table
                                xslt = etree.parse(xsl_filename, parser=xmlparser)
                                transform = etree.XSLT(xslt)
                                newdoc = transform(dom)

                                table_str = etree.tostring(newdoc.xpath('//table[1]')[0])
                                if 'asset' in table_str and 'liabilities' in table_str and 'equity' in table_str:
                                    return full_url.replace('FilingSummary.xml', xml_url)

                for report in reports:
                    print(etree.tostring(report))
                return False
            elif table == 'consolidated statement of operations':
                pass
            elif table == 'consolidated statement of cash flows':
                pass

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
                    async def get(url, session):
                        try:
                            async with session.get(url=url) as response:
                                resp = await response.json()
                                # print("Successfully got url {} with resp of length {}.".format(url, len(resp)))
                                return resp
                        except Exception as e:
                            print("Unable to get url {} due to {}.".format(url, e.__class__))

                    async def main_get(urls, old_df):
                        async with aiohttp.ClientSession(headers=header) as session:
                            ret = []
                            for url in urls:
                                ret.append(asyncio.create_task(get(url,session)))
                                await asyncio.sleep(0.1)
                            ret = await asyncio.gather(*ret)
                            for i in ret:
                                old_df = pd.concat([old_df, pd.DataFrame(i)], ignore_index=True)
                            return old_df
                        # print(f"Finalized all. Return is a list of len {len(ret)} outputs.")
                    recent_filings = asyncio.run(main_get(urls, recent_filings))

            # Get all links for 10-Q and 10-K
            if form_to_fetch == '10-Q':
                fin_dict[ticker_symbol]['10-K'] = []
                fin_dict[ticker_symbol]['10-Q'] = []
                # Filter from the full filings the 10-K and 10-Q
                filings10q = recent_filings[recent_filings['form'] == '10-Q']
                filings10k = recent_filings[recent_filings['form'] == '10-K']
                full_filings = pd.DataFrame(pd.concat([filings10k, filings10q], ignore_index=True)).reset_index().drop(columns=['filingDate', 'acceptanceDateTime', 'act', 'fileNumber', 'filmNumber', 'items', 'size', 'isXBRL', 'isInlineXBRL'])
                for row in full_filings.itertuples():
                    # Update data in fin_dict
                    if str(row.form) == '10-K':
                        # If not part of old filings -- .htm filings
                        if (str(row.primaryDocument) != '' and str(row.primaryDocument[-4:]) != '.txt'):
                            form_url = (cik + '/' + ''.join(str(row.accessionNumber).split('-')) + '/' + str(
                                row.primaryDocument))
                            html_bool = True
                            filingsummary_url = (cik + '/' + ''.join(
                                str(row.accessionNumber).split('-')) + '/' + 'FilingSummary.xml')
                            filingsummary = get_filing_sum_link(url=filingsummary_url, header=header, table = financial_statement)
                        # Old filings -- .txt filings
                        else:
                            form_url = (cik + '/' + str(row.accessionNumber) + '.txt')
                            html_bool = False
                            filingsummary = False
                        # update fin_dict
                        fin_dict[ticker_symbol]['10-K'].append([str(row.reportDate), str(row.primaryDocDescription), form_url, html_bool, filingsummary])

                    elif str(row.form) == '10-Q':
                        # If not part of old filings -- .htm filings
                        if (str(row.primaryDocument) != '' and str(row.primaryDocument[-4:]) != '.txt'):
                            form_url = (cik + '/' + ''.join(str(row.accessionNumber).split('-')) + '/' + str(
                                row.primaryDocument))
                            html_bool = True
                            filingsummary_url = (cik + '/' + ''.join(
                                str(row.accessionNumber).split('-')) + '/' + 'FilingSummary.xml')
                            filingsummary = get_filing_sum_link(url=filingsummary_url, header=header, table = financial_statement)
                        # Old filings -- .txt filings
                        else:
                            form_url = (cik + '/' + str(row.accessionNumber) + '.txt')
                            html_bool = False
                            filingsummary = False
                        # update fin_dict
                        fin_dict[ticker_symbol]['10-Q'].append([str(row.reportDate), str(row.primaryDocDescription), form_url, html_bool, filingsummary])

            # Get links for 10-K forms only
            elif form_to_fetch == '10-K':
                fin_dict[ticker_symbol]['10-K'] = []
                filings10k = recent_filings[recent_filings['form'] == '10-K']
                full_filings = pd.DataFrame(filings10k).reset_index().drop(
                    columns=['filingDate', 'acceptanceDateTime', 'act', 'fileNumber', 'filmNumber', 'items', 'size',
                             'isXBRL', 'isInlineXBRL'])
                for row in full_filings.itertuples():
                    if str(row.form) == '10-K':
                        # If not part of old filings -- .htm filings
                        if (str(row.primaryDocument) != '' and str(row.primaryDocument[-4:]) != '.txt'):
                            form_url = (cik + '/' + ''.join(str(row.accessionNumber).split('-')) + '/' + str(
                                row.primaryDocument))
                            html_bool = True
                            filingsummary_url = (cik + '/' + ''.join(
                                str(row.accessionNumber).split('-')) + '/' + 'FilingSummary.xml')
                            filingsummary = get_filing_sum_link(url=filingsummary_url, header=header, table=financial_statement)
                        # Old filings -- .txt filings
                        else:
                            form_url = (cik + '/' + str(row.accessionNumber) + '.txt')
                            html_bool = False
                            filingsummary = False
                        # update fin_dict
                        fin_dict[ticker_symbol]['10-K'].append(
                            [str(row.reportDate), str(row.primaryDocDescription), form_url, html_bool, filingsummary])
                # print(fin_dict[ticker_symbol].get('10-Q')) returns None
        # pprint(fin_dict)
        return fin_dict

    def download(self, forms_dict, table_type = 'balance_sheet_tables'):
        """

        Parameters
        ----------
        forms_dict: The direct output of self.get_form_links()
                    (i.e. {'AAPL': {'10-K': [['2021-09-25',
                                              '10-K',
                                              '320193/000032019321000105/aapl-20210925.htm',
                                              True,
                                              'http://sec.gov/Archives/data/edgar/320193/000032019321000105/R2.htm'],
                                              ...
                                    }
                           'MSFT': {'10-K': [['2021-06-30',
                                              '10-K',
                                              '789019/000156459021039151/msft-10k_20210630.htm',
                                              True,
                                              False],
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
        # Asyncio to get the network requests as fast as possible
        # Changed code to return content of GET response and store it in
        for ticker_symbol in forms_dict:
            ticker = forms_dict.get(ticker_symbol)
            list10k = ticker.get('10-K')

            # Defining the async functions
            async def get(url, session):
                base_data_url = 'https://www.sec.gov/Archives/edgar/data/'
                date = url[0]
                form = url[1]
                actual_url = base_data_url + url[2]
                html_bool = url[3]
                filingsummary = url[4]
                # If filingsummary contains something
                if filingsummary:
                    # If .xml file, it should be noted and saved in filingsummary
                    if filingsummary[-3:] == 'xml':
                        async with session.get(url=filingsummary) as response:
                            resp = await response.read()
                            return [date, form, resp, html_bool, 'xml']
                    # Should be a .htm file then, that is fine
                    else:
                        async with session.get(url=filingsummary) as response:
                            resp = await response.read()
                            return [date, form, resp, html_bool, 'html']
                else:
                    async with session.get(url=actual_url) as response:
                        resp = await response.read()
                        return [date, form, resp, html_bool, filingsummary]

            async def main_get(urls):
                async with aiohttp.ClientSession(headers = header) as session:
                    ret = []
                    for url in urls:
                        ret.append(asyncio.create_task(get(url, session)))
                        await asyncio.sleep(0.1)
                    ret = await asyncio.gather(*ret)
                    return ret
            # End of defining of async functions

            # Add 10-K files
            ticker['10-K'] = asyncio.run(main_get(list10k))
            # Add 10-Q files if needed
            if ticker.get('10-Q') != None:
                list10q = ticker.get('10-Q')
                ticker['10-Q'] = asyncio.run(main_get(list10q))

        def scrape(resp_list):
            date = resp_list[0]
            form = resp_list[1]
            resp = resp_list[2]
            html_bool = resp_list[3]
            filingsummary = resp_list[4]
            res = scraper()
            table = eval(f'res.get_{table_type}(resp={resp}, html_bool={html_bool}, filingsummary="{filingsummary}")')
            return [date, form, table]


        start = time.perf_counter()
        with ThreadPoolExecutor() as e:
            for ticker_symbol in forms_dict:
                ticker = forms_dict.get(ticker_symbol)
                res = [e.submit(scrape, i) for i in ticker.get('10-K')]
                ticker['10-K'] = [i.result() for i in res]
                if ticker.get('10-Q') != None:
                    res = [e.submit(scrape, i) for i in ticker.get('10-Q')]
                    ticker['10-Q'] = [i.result() for i in res]
        print(f'concurrent.futures module took {time.perf_counter() - start}s to scrape everything\n')

            # start = time.perf_counter()
            # # Add 10-K files
            # ticker['10-K'] = [scrape(i) for i in ticker.get('10-K')]
            # # Add 10-Q files if needed
            # if ticker.get('10-Q') != None:
            #     ticker['10-Q'] = [scrape(i) for i in ticker.get('10-Q')]
            # print(f'Normal for loop took {time.perf_counter() - start}s to scrape everything\n')

            # start = time.perf_counter()
            # threads=[]
            # for i in ticker.get('10-K'):
            #     t = threading.Thread(target=scrape, args = [i])
            #     t.daemon = True
            #     t.start()
            #     threads.append(t)
            # for thread in threads:
            #     thread.join(timeout=20)
            # if ticker.get('10-Q')!=None:
            #     for i in ticker.get('10-Q'):
            #         t = threading.Thread(target=scrape, args=[i])
            #         t.daemon = True
            #         t.start()
            #         threads.append(t)
            #     for thread in threads:
            #         thread.join(timeout=20)
            # print(f'threading module took {time.perf_counter()-start}s')


        return forms_dict

if __name__=='__main__':
    tryout = processor([['AAPL', '10-Q'], ['MSFT', '10-K']])
    list = tryout.get_json_cik()
    links = tryout.get_form_links(list)
    pprint(links)
    pprint(tryout.download(links))