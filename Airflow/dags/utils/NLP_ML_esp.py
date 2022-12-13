# Modelo de Machine Learning
#Embeddings
from collections import defaultdict
import re
import os
import numpy as np
import pandas as pd   
from gensim import corpora, models, similarities, matutils
from gensim.models.word2vec import Word2Vec
from gensim.models import KeyedVectors
from gensim.matutils import cossim
from gensim.models.phrases import Phrases, Phraser
import nltk
from nltk import word_tokenize, sent_tokenize
from nltk.corpus import stopwords
nltk.download('punkt')
nltk.download('stopwords')
import pickle
import random
import sys
from tqdm import tqdm
from sklearn import manifold

# Z Score
from sklearn.feature_extraction.text import CountVectorizer
from sklearn.naive_bayes import MultinomialNB
from utils.NLP_utils import desarmar_lista


def stoplist():
        stoplist = stopwords.words('spanish')
        specifict_stoplist = ['num', 'ser', 'tener', 'poder', 'haber', 'hacer', 'ver', 'lugar', 'ir', 'parecer', 'si', 'no']
        adjetive_stoplist = ['perfecto', 'indescriptible', 'impecable','genial', 'excelente', 'exelente', 'impresionante', 'inolvidable', 'increible', 'excelente', 'increibl', 'lindo', 'linda', 'bueno', 'buena', 'super', 'mejor', 'majestuoso','inolvidable', 'impresionante', 'interesante', 'impactante','maravilla', 'maravilloso', 'espectacular', 'realmente', 'hermoso', 'imponente', 'belleza', 'unico', 'imperdible', 'malo', 'mal', 'altamente', 'bonito', 'magico']
        atractions_stoplist = ['perito', 'moreno', 'fitz', 'roy', 'cabo', 'virgenes']
        random_stoplist = ['entrar', 'minuto', 'ave', 'gracias', 'dejar', 'cada', 'mas', 'siempre', 'nunca', 'gracia']
        stoplist = stoplist + specifict_stoplist + adjetive_stoplist + atractions_stoplist + random_stoplist
        return stoplist


def word_embeddings_model():

# "window" es el tamaño de la ventana. windows = 10, usa 10 palabras a la izquierda y 10 palabras a la derecha
# "n_dim" es la dimension (i.e. el largo) de los vectores de word2vec
# "workers" es el numero de cores que usa en paralelo. Para aprovechar eso es necesario tener instalado Cython)
# "sample": word2vec filtra palabras que aparecen una fraccion mayor que "sample"
# "min_count": Word2vec filtra palabras con menos apariciones que  "min_count"
# "sg": para correr el Skipgram model (sg = 1), para correr el CBOW (sg = 0)
# para mas detalle ver: https://radimrehurek.com/gensim/models/word2vec.html

        w2v_model = Word2Vec(trainset_ngrams, workers=4, size= 20, min_count = 10, window = 10, sample = 1e-3,negative=5,sg=1)
        w2v_model.save(path + f"modelos/model_{lang}.model")


def bigrams():
        trainset = (df.text_norm.str.split(' ')).to_list()
        collocations = Phrases(sentences=trainset, min_count=10,threshold=0.5,scoring='npmi') # threshold: minimo score aceptado
        to_collocations = Phraser(collocations)
        df_collocations =pd.DataFrame([x for x in collocations.export_phrases(trainset)],columns=["bigram","score"])
        df_collocations.drop_duplicates().sort_values(by="score",ascending=False).to_csv(path + f'data/bigramas/bigramas_{lang}.csv', index = False)


def z_score_monroe_es(DataFrame, variable_clase, variable_contenido, smoth_alpha, preprocessor, min_df, stop_words):  
    
    X = DataFrame[variable_contenido]
    y = DataFrame[variable_clase]

    #cuenta las ocurrencia de token
    count_vect = CountVectorizer(preprocessor=preprocessor, min_df = min_df, stop_words=stop_words)
  #representacion optimizada de la matriz termino-documento (que en este caso sería termino-clase)
    X_train = count_vect.fit_transform(X)
  #guardo el vocabulario
    vocabulario = pd.DataFrame(count_vect.vocabulary_.keys(),  index=count_vect.vocabulary_.values(), columns=['termino'])
  #utilizo el clasificador de naive bayes si bien no se predice nada porque tiene unos atributos que  realiza unos calculos que utilizo
    clf = MultinomialNB(alpha=smoth_alpha)
    clf.fit(X_train, y)
  # feature_log_prob_ Empirical log probability of features given a class, P(x_i|y).
    #log_pro = clf.feature_log_prob_
    log_pro=np.log(np.divide(clf.feature_count_+ smoth_alpha, (clf.feature_count_.sum(axis=1) + clf.feature_count_.shape[1]*smoth_alpha).reshape(2,1)- (clf.feature_count_+ smoth_alpha)))
  #calculo lo que seria algo asi como el log ods ratio Ecuacion 16 de Monroe et al 2009
    log_odds_ratio = (-1)*log_pro[0]-log_pro[1]*(-1)
  #calculo la varianza Ecuacion 20
    varianza = 1 / (clf.feature_count_[0] + smoth_alpha)  + 1 / (clf.feature_count_[1] + smoth_alpha)
  #calculo el z-score 22
    z_score_monroe= np.divide(log_odds_ratio, np.sqrt(varianza))
  #lo paso a data frame
    z_score = pd.DataFrame( z_score_monroe, columns= ['z_score_monroe'])
  #joineo con el vocabulario
    palabras_z_score = pd.concat([vocabulario, z_score], axis=1, join='inner')
    
    palabras_z_score['frecuencia'] = clf.feature_count_[0] + clf.feature_count_[1]
      
  #devuelve data frame con termino - z_score_monroe
    return(palabras_z_score)






