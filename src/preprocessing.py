import sys
from re import sub
import re
import gensim
import nltk
from nltk.corpus import stopwords
from nltk.stem import PorterStemmer, WordNetLemmatizer

import logging

nltk.download('wordnet')
nltk.download('stopwords')

stop = stopwords.words('english')
wnl = WordNetLemmatizer()
stemmer = PorterStemmer()

class Preprocessor:
    """Preprocess a data frame."""
    def __init__(self, df) -> None:
        """Initilize df."""
        try:
            self.df = df
            self.logger = logging.getLogger(__name__)
            self.logger.info(
                'Successfully Instantiated Preprocessing Class Object')
        except Exception:
            self.logger.exception(
                'Failed to Instantiate Preprocessing Class Object')
            sys.exit(1)
    def removePunc(self, myWord):
        """Remove punctuation from string inputs."""
        if myWord is None:
            return myWord
        else:
            return sub('[.:;()/!&-*@$,?^\d+]', '', myWord)
    def removeAscii(self, myWord):
        """Remove ascii from string input."""
        if myWord is None:
            return myWord
        else:
            return str(sub(r'[^\x00-\x7F]+', '', myWord.strip()))
    def lemmatize(self, myWord):
        """Lemmatize words."""
        if myWord is None:
            return myWord
        else:
            return str(wnl.lemmatize(myWord))
    def removeStopWords(self, myWord):
        """Remove stop words."""
        if myWord is None:
            return myWord
        if myWord not in str(stopwords.words('english')):
            return myWord
    def removeNames(self, myWord):
        """Remove names."""
        if myWord is None:
            return myWord
        else:
            return str(sub(r'"U\d{2}[A-Z0-9]{8}"', '', myWord))
    def remove_http_links(self, input_string):
    # Define a pattern to match URLs
        if input_string is None:
            return input_string
        else:
            url_pattern = re.compile(r'http[s]?://(?:[a-zA-Z]|[0-9]|[$-_@.&+]|[!*\\(\\),]|(?:%[0-9a-fA-F][0-9a-fA-F]))+')

            # Use the sub() method to replace URLs with an empty string
            result_string = re.sub(url_pattern, '', input_string)

            return result_string
    @staticmethod
    def sent_to_words(sentences):
        """
        Tokenize sentences into words.

        Args:
        sentences (iterable): Input sentences.

        Returns:
        list: List of tokenized sentences.
        """
        if sentences is None:
                return sentences
        else:
            return [gensim.utils.simple_preprocess(str(sentence), deacc=True) for sentence in sentences]

    

    def prep_text(self, myWord):
        """Preprocess text."""
        try:
            if myWord is None:
                return myWord
            else:
                myWord = self.removePunc(myWord)   
                myWord = self.removeAscii(myWord)
                myWord = self.lemmatize(myWord)
                myWord = self.removeStopWords(myWord)
                myWord = self.removeNames(myWord)
                myWord = self.remove_http_links(myWord)
                return myWord
        except Exception:
            self.logger.exception('Failed to preprocess text')
            sys.exit(1)
    def filterMessageList(self, message):
        """Remove stop words, lemmatize, and clean all tweets."""
        try:
            self.logger.info(
                'Remove stop words, lemmatize, and clean all tweets')
            return [[self.prep_text(word) for word
                     in tweet.split()
                     if self.prep_text(word) is not None]
                    for tweet in message]
        except Exception:
            self.logger.exception(
                'Fails to filter stop words, lemmatize, and clean all tweets')
            sys.exit(1)
    def clean_text(self,org_col,new_col):
        """Clean text.

        Args:
            org_col: Original column to be cleaned
            new_col: New column to hold cleaned text

        Returns:
            df: Dataframe with new column
        """
        try:
            self.df[org_col] = self.df[org_col].astype(str)
            self.df[new_col] = self.df[org_col].str.replace('[^\w\s]', '')
            self.df[new_col] = self.df[new_col].str.replace('\w*\d\w*', '')
            self.df[new_col] = self.df[new_col].apply(
                lambda x: " ".join(x.lower() for x in x.split()))
            self.df[new_col] = self.df[new_col].apply(
                lambda x: " ".join(x for x in x.split() if x not in stop))
            self.logger.info('cleans text')
            return self.df
        except Exception:
            self.logger.exception('Fails to clean text')
            sys.exit(1)
            
    def stem(self, col):
        """Stemm a word.

        Args:
            col: Column to be stemmed

        Returns:
            df: Dataframe with new column
        """
        try:
            # tokenize each tweet to its root word
            self.df[col] = self.df[col].apply(
                lambda x: " ".join([stemmer.stem(word) for word in x.split()]))
            self.logger.info('tokenizes tweet')
            return self.df
        except Exception:
            self.logger.exception('Fails to tokenize tweet')
            sys.exit(1)