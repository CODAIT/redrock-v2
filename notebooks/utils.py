import re
from nltk import WordNetLemmatizer
from nltk.tokenize import TweetTokenizer


def stop_words():
    '''
    INPUT None
    OUTPUT list
    '''
    with open('stop_words.txt', 'r') as f:
        stop = [line.strip() for line in f]
    return stop


def tokenize(articles):
    '''
    INPUT string
    OUTPUT list

    This is a tokenizer to replace the default tokenizer in TfidfVectorizer
    '''
    article = " ".join(articles)
    article = article.encode('ascii', errors='ignore')
    article = re.sub('[^\w\s\#\@\']+', ' ', str(article)) # dont remove # and @, tomenizer needs these
            
    stop = stop_words()

    tknzr = TweetTokenizer(preserve_case=True, strip_handles=True)

    tokens = tknzr.tokenize(article)
    tokens = [word.lower() if not (word[0] == '#' or word[0] == '@')
              else word for word in tokens]

    tokens = [word.replace("'s",'').replace("'d",'').replace("'ll",'').replace(
        "'ve",'').replace("'re",'') for word in tokens]
    
    # lemmatize
    lmtzr = WordNetLemmatizer()
    tokens = [lmtzr.lemmatize(word) for word in tokens]

    # now remove stop words and singel characters
    tokens = [word for word in tokens if word not in stop and len(word) > 1]

    return tokens
