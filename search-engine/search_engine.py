import json
import gzip
import re
import collections
import math
import sys
from array import array
import numpy as np
from progress.bar import Bar
# i need to do that in my machine in order for the stopwords to download
import nltk
import ssl
try:
    _create_unverified_https_context = ssl._create_unverified_context
except AttributeError:
    pass
else:
    ssl._create_default_https_context = _create_unverified_https_context

nltk.download('stopwords');
from nltk.stem import PorterStemmer
from nltk.corpus import stopwords

def id_to_tweetinfo(arrayofdict):
    """
        This function takes as an argument an array of dicts and returns a dict containing for each dict, its id 
        as a key and the same object as a body
    """
    info_to_id_dict = {}
    for entry in arrayofdict:
        # we do not want retweets, so get the original tweet in case it is not in the dict
        entry_id = entry["id"] if not "retweeted_status" in entry else entry["retweeted_status"]["id"] 
        info = entry if not "retweeted_status" in entry else entry["retweeted_status"]
        info_to_id_dict[entry_id] = info
    return info_to_id_dict

def text_to_id(tweets_dict):
    """
        This function takes as an argument dict of tweets and returns a dict containing for each id
        its text as a value. 
    """
    text_to_id_dict = {}
    for key in tweets_dict:
        # we assume that there are no retweets as this has been preprocessed before
        text_to_id_dict[key] = tweets_dict[key]["text"]
    return text_to_id_dict

def getTerms(text):
    """
    Preprocess the tweet text removing stop words, stemming,
    transforming in lowercase and return the tokens of the text.
    
    Argument:
    line -- string (text) to be preprocessed
    
    Returns:
    line - a list of tokens corresponding to the input text after the preprocessing
    """
        
    stemming = PorterStemmer()
    stops = set(stopwords.words("english"))
    # Transform in lowercase
    text = text.lower() 
    #Tokenize the text to get a list of terms
    text =  text.split() 
    # eliminate the stopwords 
    text = [token for token in text if token not in stopwords.words('english')]  
    # stem words
    text = [stemming.stem(token) for token in text]
    return text

def generate_tweet_scores(data):
    """
     Function to generate scores based on data from tweet.
     Args: 
        - Id to Tweet dict
     Outputs:
        - Max rt count
        - Max fav count
        - Index for rt by id
        - Index for fav by id
    """
    max_rt = 0
    max_likes = 0
    rt = {}
    likes = {}
    for i in data:
        max_rt = max(data[i]["retweet_count"], max_rt)
        max_likes = max(data[i]["favorite_count"], max_likes)
        rt[i] = data[i]["retweet_count"]
        likes[i] = data[i]["favorite_count"]
    for i in data:
        if max_rt > 0:
            rt[i] = rt[i]/max_rt
        if max_likes > 0:
            likes[i] = likes[i]/max_likes
    return rt, likes

def create_index_tfidf(lines, numDocuments):
    """
    Implement the inverted index and compute tf, df and idf
    
    Argument:
    lines -- collection of Wikipedia articles
    numDocuments -- total number of documents
    
    Returns:
    index - the inverted index (implemented through a python dictionary) containing terms as keys and the corresponding 
    list of document these keys appears in (and the positions) as values.
    tf - normalized term frequency for each term in each document
    df - number of documents each term appear in
    idf - inverse document frequency of each term
    """
        
    index=collections.defaultdict(list)
    tf=collections.defaultdict(list) #term frequencies of terms in documents (documents in the same order as in the main index)
    df=collections.defaultdict(int)         #document frequencies of terms in the corpus
    idf=collections.defaultdict(float)
    with Bar('Creating tf-idf index', max=len(lines)) as bar:
        for key in lines:
            page_id = key      
            terms = getTerms(lines[key])              

            ## create the index for the **current page** and store it in termdictPage
            ## termdictPage in form ==> { ‘term1’: [currentdoc, [list of positions]], ...,‘termn’: [currentdoc, [list of positions]]}

            termdictPage={}

            for position, term in enumerate(terms): 
                try:
                    # if the term is already in the dict append the position to the corrisponding list
                    termdictPage[term][1].append(position) 
                except:
                    # Add the new term as dict key and initialize the array of positions and add the position
                    termdictPage[term]=[page_id, array('I',[position])] 

            #normalize term frequencies
            norm=0
            for term, posting in termdictPage.items(): 
                # posting ==> [currentdoc, [list of positions]] 
                norm+=len(posting[1])**2
            norm=math.sqrt(norm)


            #calculate the tf(dividing the term frequency by the above computed norm) and df weights
            for term, posting in termdictPage.items():     
                # append the tf for current term (tf = term frequency in current doc/norm)
                tf[term].append(np.round(len(posting[1])/norm,4))  ## SEE formula (1) above
                #increment the document frequency of current term (number of documents containing the current term)
                df[term] += 1  

            #merge the current page index with the main index
            for termpage, postingpage in termdictPage.items():
                index[termpage].append(postingpage)

            # Compute idf following the formula (3) above. HINT: use np.log
            bar.next()
    for term in df:
        idf[term] = np.round(np.log(float(numDocuments/df[term])),4)
            
    return (index, tf, df, idf)


def rankDocuments(terms, docs, index, idf, tf, rt, likes, score):
    """
    Perform the ranking of the results of a search based on the tf-idf weights
    
    Argument:
    terms -- list of query terms
    docs -- list of documents, to rank, matching the query
    index -- inverted index data structure
    idf -- inverted document frequencies
    tf -- term frequencies
    rt -- dict pointing index to rt score
    likes -- dict pointing index to fav score
    score -- The type of score "1" for tf-idf| "2" for custom
    
    Returns:
        List of ranked documents
    """
        
    # init docvectors and queryvector to dict and array of 0, to be filled later
    docVectors=collections.defaultdict(lambda: [0]*len(terms)) 
    queryVector=[0]*len(terms)    

    if score == "1":
        # compute the norm for the query tf
        query_terms_count = collections.Counter(terms) # get the frequency of each term in the query. 
    
        query_norm = np.linalg.norm(list(query_terms_count.values()))
   
        for termIndex, term in enumerate(terms): #termIndex is the index of the term in the query
            if term not in index:
                continue
        
            ## Compute tf*idf(normalize tf as done with documents)
            queryVector[termIndex] = query_terms_count[term] / query_norm * idf[term]

            # Generate docVectors for matching docs
            for docIndex, (doc, postings) in enumerate(index[term]):
                # in form of [docIndex, (doc, postings)]      
                if doc in docs:
                    docVectors[doc][termIndex]=tf[term][docIndex] * idf[term]
        # calculate the score of each doc
        # compute the cosine similarity between queyVector and each docVector:
        docScores=[ [np.dot(curDocVec, queryVector), doc] for doc, curDocVec in docVectors.items() ]
    else:
        # as we just want cosine similarity but not use tf-idf, we're using the term frequency as a weight
        # in our custom ranking
        # compute the norm for the query tf
        query_terms_count = collections.Counter(terms) # get the frequency of each term in the query. 
    
        query_norm = np.linalg.norm(list(query_terms_count.values()))
   
        for termIndex, term in enumerate(terms): #termIndex is the index of the term in the query
            if term not in index:
                continue
        
            ## Compute tf (normalize tf as done with documents)
            queryVector[termIndex] = query_terms_count[term] / query_norm 

            # Generate docVectors for matching docs
            for docIndex, (doc, postings) in enumerate(index[term]):
                # in form of [docIndex, (doc, postings)]      
                if doc in docs:
                    docVectors[doc][termIndex]=tf[term][docIndex]
        # calculate the score of each doc
        # compute the cosine similarity and add rt and fav score
        # rt brings to more visibility than a like, hence a higher score
        docScores=[ [np.dot(curDocVec, queryVector) + 1.5*rt[doc] + likes[doc], doc] for doc, curDocVec in docVectors.items() ]
    docScores.sort(reverse=True)
    resultDocs=[x[1] for x in docScores]
    if len(resultDocs) == 0:
        print("No results found, try again")
        return None    
    return resultDocs

def search(query, index, idf, tf, rt, likes, score):
    '''
    Arguments: 
        query -- the query to be performed
        index -- the tf-idf index 
        idf -- the idf values per term
        tf -- the tf values per term
        rt -- dict pointing index to rt score
        likes -- dict pointing index to fav score
        score -- The type of score "1" for tf-idf| "2" for custom
    

    output is the list of documents that contain any of the query terms. 
    It gets the list of documents for each query term, and take the union of them.

    Returns:
        - ranked docs in a list
    '''
    query=getTerms(query)
    docs=set()
    for term in query:
        try:
            # store in termDocs the ids of the docs that contain "term"                        
            termDocs=[posting[0] for posting in index[term]]
            # docs = docs Union termDocs
            docs |= set(termDocs)
        except:
            #term is not in index
            pass
    docs=list(docs)
    ranked_docs = rankDocuments(query, docs, index, idf, tf, rt, likes, score)   
    return ranked_docs

def perform_query(tweets_dict, index, tf, idf, rt, likes, score):
    """
    This functions performs a query getting diven an input
        tweets_dict -- dictionary of information of tweets by tweet id
        query -- the query to be performed
        index -- the tf-idf index 
        idf -- the idf values per term
        tf -- the tf values per term
        rt -- dict pointing index to rt score
        likes -- dict pointing index to fav score
        score -- The type of score "1" for tf-idf| "2" for custom
    
    Returns:
        - The query
        - List of ranked documents
    """
    print("Insert your query:\n")
    query = input()
    ranked_docs = search(query, index, idf, tf, rt, likes, score) 
    return query, ranked_docs

def print_query_results(top, ranked_docs, tweets_dict):
    """
        This function prints the query results.
            - top: number of tweets to retrieve
            - ranked_docs: list of all docs containing the terms, ranked
            - tweets_dict: dictionary of information of tweets by tweet id
    """
    print("\n======================\nTop {} results out of {} for the seached query:\n".format(top, len(ranked_docs)))
    for tweet_id in ranked_docs[:top]:
        tweet_object = tweets_dict[tweet_id]
        txt = tweet_object["text"]
        usr = tweet_object["user"]["name"]
        date = tweet_object["created_at"]
        hashtags = tweet_object["entities"]["hashtags"]
        favs = tweet_object["favorite_count"]
        rt = tweet_object["retweet_count"]
        urls = tweet_object["entities"]["urls"]
        print("\n==================================================================\n")
        print("Username %s | Tweet: %s\n Date %s\n Likes %s| Retweets %s"%(usr, txt, date, favs, rt))
        if hashtags:
            print("Hashtags: ")
            for hashtag in hashtags:
                print(hashtag)
        if urls:
            print("URLs: ")
            for url in urls:
                print(url["url"])


def load_data(fname):
    """
        Simple function to load data from a json encoded in gzip file
        Args:
            - fname: the filename to open
        Returns:
            - data in form of an array of dictionarys
    """
    # load the json in gzip format
    with gzip.open(fname, 'r') as fin:
        data = json.loads(fin.read().decode('utf-8'))
    return data

def main():
    """
    Args in order:
        * File where the data is stored, must be of type gz (if json, please perform gzip file.json before)
        * Number of documents to rank (default 10)
    """
    # bind arguments
    fname = sys.argv[1]
    top = sys.argv[2]
    print("Welcome to the Tweet Search Engine, please wait until all the data is processed")
    data = load_data(fname)
    # generate tweets info dictionary
    tweets_dict = id_to_tweetinfo(data)
    # genetate id to text dictionary
    textsdict = text_to_id(tweets_dict)
    index, tf, df, idf = create_index_tfidf(textsdict, len(textsdict))
    rt, favs = generate_tweet_scores(tweets_dict)
    while(True):
        print("Please, select a ranking, type 1 for tf-idf or 2 for score based on popularity of tweets. (ctrl-c exits the program)")
        score = input()
        query, ranked_docs = perform_query(tweets_dict, index, tf, idf, rt, favs, score)
        if ranked_docs:
            print_query_results(int(top), ranked_docs, tweets_dict)
        

if __name__ == "__main__":
    main()