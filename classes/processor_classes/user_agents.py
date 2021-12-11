import random

# get name of all filings
class headers():
    """
    Just to choose a header for the GET request being made to the website to circumvent the bot protection
    Use headers().header() to get a random usable dictionary header
    """
    def __init__(self):
        pass

    def header(self):
        def random_word():
            ua_str = 'abcdefghijklmnopqrstuvwxyz'
            ua_str_choice = [i for i in ua_str]
            return ''.join([random.choice(ua_str_choice) for _ in range(random.randint(1, 9))])

        return {'user-agent': f'{random_word()} {random_word()}@{random_word()}.com',
                'accept-language': 'en-GB,en-US;q=0.9,en;q=0.8'}