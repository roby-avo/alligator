from dateutil.parser import parse
import re

def clean_str(s):
    s = s.lower()
    s = re.sub("[\(\[].*?[\)\]]", "", s).strip()
    return " ".join(s.split())


def word2ngrams(text, n=None):
    """ Convert word into character ngrams. """
    if n is None:
        n = len(text)
    return [text[i:i+n] for i in range(len(text)-n+1)]


def get_ngrams(text, n=3):
    ngrams = set()
    for token in text.split(" "):
        temp = word2ngrams(token, n)
        for ngram in temp:
            ngrams.add(ngram)
    return set(ngrams)


def parse_date(str_date):
    date_parsed = None
    
    try:
        int(str_date)
        str_date = f"{str_date}-01-01"
    except:
        pass   
    
    try:
        date_parsed = parse(str_date)
    except:
        pass   
    
    if date_parsed is not None:
        return date_parsed
    
    try:
        str_date = str_date[1:]
        date_parsed = parse(str_date)
    except:
        pass

    if date_parsed is not None:
        return date_parsed
    
    try:
        year = str_date.split("-")[0]
        str_date = f"{year}-01-01"
        date_parsed = parse(str_date)
    except:
        pass

    return date_parsed


THRESHOLD = 0.03
def get_cea_pre_linking_data(metadata, rows):
    dataset_name = metadata["datasetName"]
    table_name = metadata["tableName"]
    kg_reference = metadata["kgReference"]
    page = metadata["page"]
    cea_prelinking_data = []
    
    for row in rows:
        winning_candidates =  []
        cea = {}
        for cell in row.get_cells():
            candidates = cell.candidates()
            wc = []
            for candidate in cell.candidates():
                if (candidates[0]["features"]["cea"] - candidate["features"]["cea"]) < THRESHOLD:
                    wc.append(candidate.copy())
            
            if len(wc) == 1:
                cea[str(cell._id_col)] = wc[0]["id"]

            winning_candidates.append(wc)

        cea_prelinking_data.append({
            "datasetName": dataset_name,
            "tableName": table_name,
            "row": row._id_row,
            "data": row.get_row_text(),
            "winningCandidates": winning_candidates,
            "cea": cea,
            "kgReference": kg_reference,
            "page": page
        })

    return cea_prelinking_data
        
