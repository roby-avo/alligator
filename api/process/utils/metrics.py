import nltk
import utils.utils as utils


def edit_distance(s1, s2):
    """
        Normalized Levhenstein distance function between two strings
    """
    return nltk.edit_distance(s1, s2) / max(len(s1), len(s2), 1)

def _my_abs(value1, value2):
    diff = 1 - (abs(value1 - value2) / max(abs(value1), abs(value2), 1))
    diff = round(diff, 4)
    return diff

def compute_similarty_between_numbers(value1, value2):
    try:
        value1 = float(value1)
        value2 = float(value2)
        score = _my_abs(value1, value2)    
    except:
        score = 0   
   
    return score
    
def compute_similarity_between_dates(date1: str, date2: str):
    try:
        date_parsed1 = utils.parse_date(date1)
        date_parsed2 = utils.parse_date(date2)
        score =  (_my_abs(date_parsed1.year, date_parsed2.year) + _my_abs(date_parsed1.month, date_parsed2.month) + _my_abs(date_parsed1.day, date_parsed2.day)) / 3    
    except:
        score = 0
            
    return score

def compute_similarity_between_string(str1, str2, ngram=None):
    ngrams_str1 = utils.get_ngrams(str1, ngram)
    ngrams_str2 = utils.get_ngrams(str2, ngram) 
    score = len(ngrams_str1.intersection(ngrams_str2)) / max(len(ngrams_str1), len(ngrams_str2), 1)
    return score


def compute_similarity_between_string_token_based(str1, str2):
    token_set_str1 = set(str1.split(" "))
    token_set_str2 = set(str2.split(" "))
    score = len(token_set_str1.intersection(token_set_str2)) / max(len(token_set_str1), len(token_set_str2), 1)
    return score    