# Import necessary libraries
from mrjob.job import MRJob
import nltk
from nltk.corpus import stopwords, words
from nltk.stem.snowball import SnowballStemmer
from collections import defaultdict
from nltk.tokenize import RegexpTokenizer
import sys
import os

# Set of English words
words = set(words.words())

# Set of stop words
stop_words = set(stopwords.words('english'))

# Initialize the Snowball Stemmer
stemmer = SnowballStemmer("english")

# Initialize the RegexpTokenizer to tokenize the text
tokenizer = nltk.tokenize.RegexpTokenizer(r'\w+')

# Check for command-line arguments
if len(sys.argv) <= 1:
    # Print usage and exit if no command-line arguments are provided
    print("Usage: python3 build_index.py <folderpath> -o <folderpathforoutput>")
    exit()

# Define MRJob class to build inverted index
class BuildIndex(MRJob):

    # Map each word to its (name, count) pair
    def mapper(self, _, line):
        text = line.strip() #remove trailing characters
        url = os.environ['map_input_file']
        name = os.path.basename(url)

        # Tokenize the text using RegexpTokenizer
        tokens = tokenizer.tokenize(text)

        # Stem each word and remove stop words and non-English words
        for word in tokens:
            stemmed_word = stemmer.stem(word.lower()) #doing isalpha to remove persian/words with unicode, etc
            if len(stemmed_word) > 1 and stemmed_word not in stop_words and stemmed_word in words and stemmed_word.isalpha():
                # Emit each word with its (name, count) pair
                yield stemmed_word.lower(), (name, 1)

    # Count the frequency of each word
    def reducer(self, text, values):
        # storing count in a dictionary
        count_store = defaultdict(int)

        # counting how many times each word appears in each file
        for name, count in values:
            count_store[name] += count

        # Yielding the word and the dictionary of file names and counts
        yield text, dict(count_store) 

# Run the MRJob class
if __name__ == '__main__':
    BuildIndex.run()
