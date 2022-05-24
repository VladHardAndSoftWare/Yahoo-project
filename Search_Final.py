import pandas as pd
from whoosh.index import create_in
from whoosh.fields import Schema, TEXT
from whoosh.qparser import QueryParser

df = pd.read_csv('Tickers.csv', encoding = "utf-8", na_values="\\N") 

df['Name'] = df['Name'].str.upper()

df_kek = pd.read_csv('kek.csv', encoding = "utf-8", na_values="\\N")

abbreviations = {
    'AG': 'AKTIENGESELLSCHAFT',
    'AVRG': 'AVERAGE',
    'BK': 'BANK',
    'BKG': 'BANKING',
    'CAP': 'CAPITAL',
    'CHEMS': 'CHEMICALS',
    'CO': 'COMPANY',
    'CONSTR': 'CONSTRUCTION',
    'CORP': 'CORPORATION',
    'CTLS': 'CONTROLS',
    'FINL': 'FINANCIAL',
    'FLA': 'FLORIDA',
    'FLTNG': 'FLOATING',
    'HLDG': 'HOLDING',
    'HLDGS': 'HOLDINGS',
    'INDL': 'INDUSTRIAL',
    'INTL': 'INTERNATIONAL',
    'LABS': 'LABORATORIES',
    'MACHS': 'MACHINES',
    'MATLS': 'MATERIALS',
    'MGMT': 'MANAGEMENT',
    'NATL': 'NATIONAL',
    'PAC': 'PACIFIC',
    'PETE': 'PETROLEUM',
    'PPTYS': 'PROPERTIES',
    'PRODS': 'PRODUCTS',
    'RLTY': 'REALTY',
    'RTE': 'RATE',
    'RY': 'RAILWAY',
    'SR': 'SENIOR',
    'SVCS': 'SERVICES',
    'TR': 'TRUST'
}

def replace_abbreviation(name):
    name = [abbreviations.get(word) if abbreviations.get(word) is not None else word for word in name.split()]
    name = ' '.join(name)
    return name

for index in range(len(df_kek)):
    df_kek['Name of Issuer'].iloc[index] = replace_abbreviation(df_kek['Name of Issuer'].iloc[index])

for index in range(len(df_kek)):
    df['Name'].iloc[index] = replace_abbreviation(df['Name'].iloc[index])

schema = Schema(Ticker = TEXT(stored=True), Name=TEXT(stored=True))

ix = create_in(".", schema)

writer = ix.writer()

for index, row in df.iterrows():
    writer.add_document(Ticker = str(df.Ticker.iloc[index]),
                        Name   = str(df.Name.iloc[index])
                        )
writer.commit()

def Search_Ticker(query):
   
    searcher = ix.searcher()
    proposed_time_strings = []
    with ix.searcher() as searcher:
        parser = QueryParser("Name", ix.schema)
        querystring = str(query)
        # querystring = u"Apple"
        myquery = parser.parse(querystring)
        print(myquery)
        results = searcher.search(myquery)
        results.fragmenter.surround = 500
        results.fragmenter.maxchars = 1500
        # print(len(results))
        if (len(results) >= 1):
            print("Ticker: " + results[0]['Ticker'])
            print("__________________________________________________")
            return results[0]['Ticker']
        else:
            print("Not match found")
            print("__________________________________________________")
            pass

for index in range(len(df_kek)):
    df_kek['Ticker of Issuer'].iloc[index] = Search_Ticker(df_kek['Name of Issuer'].iloc[index])

df_kek.to_csv(r'D:\GitHub\Yahoo-project\export.csv', index = False, header=True)


