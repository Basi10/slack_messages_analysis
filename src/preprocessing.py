import sys
from re import sub

import nltk
from nltk.corpus import stopwords
from nltk.stem import PorterStemmer, WordNetLemmatizer

import logging

nltk.download('wordnet')
nltk.download('stopwords')

stop = stopwords.words('english')
wnl = WordNetLemmatizer()
stemmer = PorterStemmer()

class Preprocessing:
    """Preprocess a data frame."""
    def __init__(self) -> None:
        """Initilize df."""
        try:
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
    def stem(self, myWord):
        """Stem words."""
        if myWord is None:
            return myWord
        else:
            return str(stemmer.stem(myWord))
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
                myWord = self.stem(myWord)
                return myWord
        except Exception:
            self.logger.exception('Failed to preprocess text')
            sys.exit(1)
    def preprocess(self, df):
        """Preprocess a data frame."""
        try:
            self.logger.info('Preprocessing data frame')
            df = df.applymap(self.prep_text)
            return df
        except Exception:
            self.logger.exception('Failed to preprocess data frame')
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
