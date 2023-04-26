import os,re,sys
import json
from collections import defaultdict
import nltk
from nltk.tokenize import RegexpTokenizer
import math
from nltk.stem.snowball import SnowballStemmer
import time

stemmer = SnowballStemmer("english")
tokenizer = nltk.tokenize.RegexpTokenizer(r'\w+')
tfidf_dict = defaultdict(dict) #dict to store tfidf of the files that has the search word


class LoadIndex:

    # initialize the class
    def __init__(self, i_directory, o_directory):
        self.input_directory = i_directory
        self.output_directory = o_directory
        self.total_num_of_files = set()
        self.inv_index = defaultdict(dict)

    # load the inverted index
    def create_index(self):
        # iterate over each file in the folder
        for file_name in os.listdir(self.output_directory):
            # check if the file is a partition file
            if 'part-' in file_name:
                # open the file
                with open(os.path.join(self.output_directory, file_name), 'r') as f:
                    # read each line
                    for line in f:
                        # split the line by tab character
                        line = line.strip().split('\t')
                        # extract the word and remove quotes
                        word = line[0].strip('"')
                        # load the inverted index for the word
                        word_dictionary = json.loads(line[1])
                        # add the word and its index to the inverted index
                        self.inv_index[word] = word_dictionary
                        # update the count of total files containing the word
                        self.total_num_of_files.update(word_dictionary.keys())


    # calculate the tf-idf score for each word in each file in the given word dictionary
    # tf = number of times the word appears in the file 
    # idf = log(total number of files / number of files the word appears in)
    def calculate_tf_idf(self, word_dictionary):
        global tfidf_dict
        # loop through each file and its corresponding word count in the word dictionary
        for file_name, word_count in word_dictionary.items():
            # calculate tf
            tf = word_count
            # calculate idf
            idf = math.log(len(self.total_num_of_files) / len(word_dictionary))
            # calculate tf-idf score
            tfidf_dict[file_name] = tf * idf

    # search for the top 10 most relevant documents when given a query
    def search(self, query):
        # start time
        start_time = time.time()

        # apply stemming to the query
        word = stemmer.stem(query.lower())

        # check if the word is in the index
        if word not in self.inv_index:
            print('Word not found')
            return

        # get the word dictionary
        word_dictionary = self.inv_index[word]

        # calculate the tf-idf score for each document
        self.calculate_tf_idf(word_dictionary)

        # sort the documents by their tf-idf score
        sorted_documents = dict(sorted(tfidf_dict.items(), key=lambda item: item[1], reverse=True))

        # loop through the sorted document dictionary
        for i, (file_name, tf_idf_score) in enumerate(sorted_documents.items()):
            if i == 10:
                break

            # open the file and get the title and content
            with open(os.path.join(self.input_directory, file_name), 'r') as f:
                # read the first line to get the title
                first_line = f.readline().strip().split(':')
                title = first_line[1]

                # read the remaining lines to get the content
                content = f.read()

                # tokenize the content
                tokens = tokenizer.tokenize(content)

                # convert tokens to lowercase
                tokens = [token.lower() for token in tokens]

                # find the index of the query word, returns first instance
                if word in tokens:
                    word_index = tokens.index(word)
                else:
                    # handle the case where the word is not in the list - error handling
                    for i in tokens:
                        if query.lower() in i:
                            word_index = tokens.index(i)
                # get a snippet of the context
                if word_index < 3 and word_index + 3 < len(tokens):
                    snippet = ' '.join(tokens[:word_index+3])
                elif word_index >= 3 and word_index + 3 < len(tokens):
                    snippet = ' '.join(tokens[word_index-3:word_index+3])
                elif word_index >= 3 and word_index + 3 >= len(tokens):
                    snippet = ' '.join(tokens[word_index-3:])
                else:
                    snippet = ' '.join(tokens[:word_index+3])

                # print the result in the format of filename (Article: title) #.. context ..
                print(file_name, '(Article:', title, '#.. ', snippet, ' ..')

        # clear the tfidf_dict
        tfidf_dict.clear()

        # end time
        end_time = time.time()

        # calculate the time taken to complete the search
        time_taken = end_time - start_time

        # print the number of documents and the time taken to complete the search
        print('About', len(sorted_documents), 'documents (in', time_taken, 'seconds)')

directories = LoadIndex('/Users/arham/Downloads/documents10k','/Users/arham/Downloads/output')

print('Loading inverted index...')
directories.create_index()
print('Done')

while True:
    action = input("Enter 's' to search or 'b' to exit: ")
    if action == 's':
        search_term = input("> Enter Search Term: ")
        directories.search(search_term)
    elif action == 'b':
        break
    