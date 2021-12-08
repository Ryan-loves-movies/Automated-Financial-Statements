import time
from lxml import etree
from concurrent.futures import ThreadPoolExecutor


class finder():
    def __init__(self, doc):
        self.doc = doc
    def find_hyperlink_text_to_table(self, list_of_hyperlink_texts, form = 'balance_sheet'):
        """

        Parameters
        ----------
        list_of_hyperlink_texts: List of texts to search the hyperlink for
        form: What type of form -- 'balance sheet', 'statement of operations' and 'cash flow statements' are the only accepted inputs

        Returns the lxml table element if found
        else, Returns 'Null'
        -------

        """
        def find_text(text_and_doc):
            text = text_and_doc[0]
            fulldoc = text_and_doc[1]
            form = text_and_doc[2]

            # Find the phrase 'consolidated balance sheets' that has a href -- other texts could be implemented in the future
            tables = fulldoc.xpath(f'//*[@href and contains(translate(., "ABCDEFGHIJKLMNOPQRSTUVWXYZ", "abcdefghijklmnopqrstuvwxyz"),"{text}")]')
            tables = list(set(tables))

            # If/else clause to find table element
            # If tables is not an empty list
            if len(tables):
                print(f'Found {len(tables)} reference(s) containing the phrase "{text}" with a hyperlink')
                # There should only be one text with a href in the document that points to the table
                # Get the href from the text
                href = tables[0].get('href').lstrip('#')
                # link after '#' could link to both id or name -- We should try both
                reference = fulldoc.xpath(f'//*[@id="{href}"]')
                if reference == []:
                    reference = fulldoc.xpath(f'//*[@name="{href}"]')

                if reference == []:
                    print(f"Couldn't find the hyperlinked reference!")
                    return 'Null'
                # Get the first table after the referenced element
                for page in reference:
                    # Try/Except clause to see if text exists -- Checks if it exists within the tags or even just after the tags
                    try:
                        text = page.text.strip(' ').lower()
                    except:
                        try:
                            text = page.tail.strip(' ').lower()
                        except:
                            text = 'Null'
                            print(page)

                    # If text exists, we are alr at the header, so we just need to extract the following table
                    # Else, find the header then the table directly after the header
                    # -- so that we are certain we extract the right table
                    if form == 'balance sheet':
                        if 'balance sheet' in text:
                            table = page.xpath('./following::table[1]')
                            if table == []:
                                return 'Null'
                            else:
                                return table[0]
                        else:
                            table = page.xpath('./following::*[contains(translate(., "ABCDEFGHIJKLMNOPQRSTUVWXYZ", "abcdefghijklmnopqrstuvwxyz"),"balance sheet")]/following::table[1]')
                            if table == []:
                                return 'Null'
                            else:
                                return table[0]
                    elif form == 'statement of operations':
                        pass
                    elif form == 'cash flow statements':
                        pass
            else:
                return 'Null'
        with ThreadPoolExecutor() as e:
            ret = [e.submit(find_text, [text, self.doc, form]) for text in list_of_hyperlink_texts]
            ret = [i.result() for i in ret]
            ret = list(set(ret))
            ret = [i for i in ret if i!='Null']
            if len(ret) == 0:
                table = 'Null'
            elif len(ret) == 1:
                table = ret[0]
        return table
    def find_text_before_table(self, list_of_text_before_table, form = 'balance_sheet'):
        """

        Parameters
        ----------
        list_of_text_before_table: List of texts to search as header/before the table to find
        form: What type of form -- 'balance sheet', 'statement of operations' and 'cash flow statements' are the only accepted inputs

        Returns the lxml table element if found
        else, Returns 'Null'
        -------

        """
        def find_text(text_and_doc):
            text = text_and_doc[0]
            doc = text_and_doc[1]
            form = text_and_doc[2]
            res = []
            for i in doc.xpath(f'//*'):
                if str(i.text).lower().replace('(unaudited)', '').strip(' ') == text:
                    res.append(i)
            if len(res) == 0:
                return 'Null'
            elif len(res) == 1:
                table = res[0].xpath('./following::table[1]')[0]
                return table
            else:
                # Filter to just the few same tables
                res = list(set([i.xpath('./following::table[1]')[0] for i in res]))
                res = [i for i in res if i is not None]

                if form == 'balance sheet':
                    # Return the table if the table contains all the words 'assets', 'liabilities' and 'equity'
                    for i in res:
                        assets = list(set(['Positive' if 'assets' in str(a.text).lower().strip(' ') else 'Null' for a in
                                           i.xpath('./tr/td')])).remove('Null')
                        liabilities = list(
                            set(['Positive' if 'liabilities' in str(a.text).lower().strip(' ') else 'Null' for a in
                                 i.xpath('./tr/td')])).remove('Null')
                        equity = list(set(['Positive' if 'equity' in str(a.text).lower().strip(' ') else 'Null' for a in
                                           i.xpath('./tr/td')])).remove('Null')
                        if assets and liabilities and equity:
                            table = i
                            return table
                        else:
                            continue
                    return 'Null'
                elif form == 'statement of operations':
                    pass
                elif form == 'cash flow statements':
                    pass
        with ThreadPoolExecutor() as e:
            ret = [e.submit(find_text, [text, self.doc, form]) for text in list_of_text_before_table]
            ret = [i.result() for i in ret]
            ret = list(set(ret))
            ret = [i for i in ret if i!='Null']
            if len(ret) == 0:
                table = 'Null'
            elif len(ret) == 1:
                table = ret[0]
        return table
    def find_text_in_table(self, list_of_text_in_table, form = 'balance sheet'):
        """

        Parameters
        ----------
        list_of_text_in_table: List of texts to search for that exists in the table to find
        form: What type of form -- 'balance sheet', 'statement of operations' and 'cash flow statements' are the only accepted inputs

        Returns the lxml table element if found
        else, Returns 'Null'
        -------

        """
        def find_text(text_and_doc):
            text = text_and_doc[0]
            doc = text_and_doc[1]
            form = text_and_doc[2]
            res = []
            for i in doc.xpath(f'//*'):
                if str(i.text).lower().strip(' ') == text:
                    res.append(i)
            if len(res) == 0:
                return 'Null'
            elif len(res) == 1:
                table = res[0].xpath('./ancestor::table[1]')[0]
                return table
            else:
                # Filter to just the few same tables
                res = list(set([i.xpath('./ancestor::table[1]')[0] for i in res]))
                res = [i for i in res if i is not None]
                print(res)

                if form == 'balance sheet':
                    # Return the table if the table contains all the words 'assets', 'liabilities' and 'equity'
                    for i in res:
                        assets = list(set(['Positive' if 'assets' in str(a.text).lower().strip(' ') else 'Null' for a in
                                           i.xpath('./tr/td')])).remove('Null')
                        liabilities = list(
                            set(['Positive' if 'liabilities' in str(a.text).lower().strip(' ') else 'Null' for a in
                                 i.xpath('./tr/td')])).remove('Null')
                        equity = list(set(['Positive' if 'equity' in str(a.text).lower().strip(' ') else 'Null' for a in
                                           i.xpath('./tr/td')])).remove('Null')
                        if assets and liabilities and equity:
                            table = i
                            return table
                        else:
                            continue
                    return 'Null'
                elif form == 'statement of operations':
                    pass
                elif form == 'cash flow statements':
                    pass
        with ThreadPoolExecutor() as e:
            ret = [e.submit(find_text, [text,self.doc, form]) for text in list_of_text_in_table]
            ret = [i.result() for i in ret]
            ret = list(set(ret))
            ret = [i for i in ret if i!='Null']
            if len(ret) == 0:
                table = 'Null'
            elif len(ret) == 1:
                table = ret[0]
        return table

if __name__ == '__main__':
    import requests
    from user_agents import headers
    header = headers().header()
    try1 = 'https://www.sec.gov/Archives/edgar/data/0000050863/000005086321000018/intc-20210327.htm'
    try2 = 'https://www.sec.gov/Archives/edgar/data/0000050863/000091205702009698/a2072777z10-k.htm'
    page = requests.get(try1, headers=header).content
    doc = etree.fromstring(page, etree.HTMLParser())

    start = time.perf_counter()
    list_of_text_before_table = ['consolidated balance sheets', 'condensed consolidated balance sheets',
                                 'consolidated condensed balance sheets', 'consolidated balance sheet',
                                 'condensed consolidated balance sheet', 'consolidated condensed balance sheet']
    list_of_text_in_table = ['current assets:', 'current assets', 'total current assets']
    list_of_hyperlink_texts = ['balance sheet']
    finder = finder(doc)
    table = finder.find_hyperlink_text_to_table(list_of_hyperlink_texts)
    if table == 'Null':
        table = finder.find_text_in_table(list_of_text_in_table)
    if table == 'Null':
        table = finder.find_text_before_table(list_of_text_before_table)
    print(f'Finding the table took {time.perf_counter() - start}s')
    print(table)