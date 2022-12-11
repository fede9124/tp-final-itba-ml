import re 
import spacy
import nltk
from nltk.tokenize import word_tokenize
from nltk.stem.snowball import SnowballStemmer
from textblob import TextBlob
import emoji
import unicodedata
from nltk.stem import WordNetLemmatizer 
nltk.download('punkt')
# Se importa la lista de stopwords
nltk.download('stopwords')

nltk.download('averaged_perceptron_tagger')


def remove_accented_chars(text):
    '''
    Funcion para remover caracteres acentuados
    '''
    new_text = unicodedata.normalize('NFKD', text).encode('ascii', 'ignore').decode('utf-8', 'ignore')
    return new_text

def remove_special_characters(text, remove_digits=False):
    '''Se eliminan puntuaciones, parentesis, etc.'''
    pattern = r'[^a-zA-z0-9\s]' if not remove_digits else r'[^a-zA-z\s]'
    text = re.sub(pattern, '', text)
    return text


def lemmatize_text(text):
    lemmatizer = WordNetLemmatizer() 
    text = ' '.join([lemmatizer.lemmatize(word) for word in text.split()])
    return text

def simple_stemmer(text, language = 'spanish'):
    ps = SnowballStemmer(language)
    text = ' '.join([ps.stem(word) for word in text.split()])
    return text


def extract_emoji(text):
    emoji.replace_emoji(text, replace=' ')
    return text

def tokenize_and_remove_stopwords(text, language = 'spanish', specific_stopwords = [], only_words = True, lemma = True):
    stopword_list = nltk.corpus.stopwords.words(language)
    stopword_list = stopword_list + specific_stopwords
    if only_words:
        tokens = [word for word in word_tokenize(text, language = language) if (word.isalpha() and word not in stopword_list)]
    else:
        tokens = [word for word in word_tokenize(text, language = language) if  (word not in stopword_list)]
    if lemma:
        if language == 'spanish':
            nlp = spacy.load('es_core_news_sm')
        elif language == 'english':
            nlp = spacy.load('en_core_web_sm')
        elif language == 'portuguese':
            nlp = spacy.load('pt_core_news_sm')
        doc = nlp(text)
        tokens = [tok.lemma_.lower() for tok in doc ]
        if only_words:
            tokens = [ word for word in tokens if (word.isalpha() and word not in stopword_list)]
    return tokens

def postag(tokens, language):
    lang_iso = {'spanish': 'spa',
                'english': 'eng',
                'portuguese': 'por'}
    postag_tokens = nltk.pos_tag(tokens, lang= lang_iso.get(language, 'eng'))
    return postag_tokens


def desarmar_lista(text):
    return ' '.join(text)

def preprocesamiento(review, language = 'spanish', remove_typos=False, accented_char_removal=True,
                     text_lower_case=True,
                     text_stem = False, text_lemmatization=False,
                     special_char_removal=True, 
                     emoji_extraction = True, pos_tag = False, 
                     specific_stopwords = [], only_words = True, 
                     lemmatization = True,
                     return_string = True):
    
    # Se recibe la tupla de la review
    

    ###### LIMPIEZA ######
    #remuevo salto de linea

    review = review.replace('\t', ' ')

   # mask e-mails
    review = re.sub(r'\S*@\S*\s?', ' <email> ', review)

    # remove numbers and urls
    review = re.sub(
        r'''(?i)\b((?:https?://|www\d{0,3}[.]|[a-z0-9.\-]+[.][a-z]{2,4}/)'''
        + r'''(?:[^\s()<>]+|\(([^\s()<>]+|(\([^\s()<>]+\)))*\))+(?:\(([^\s()<>]'''
        + r'''+|(\([^\s()<>]+\)))*\)|[^\s`!()\[\]{};:'".,<>?«»“”‘’]))''',
        ' <url> ', review)

    # mask urls
    review = re.sub(r'''((https?|ftp|smtp):\/\/)?(www.)?[a-z0-9]+\.[a-z]+.[a-z]+'''
                        + r'''(\/[a-zA-Z0-9#]+\/?)*''', ' <url> ', review)

    # mask codes
    if any([w in review for w in ['codigo', 'token']]):
        review = re.sub(r'([A-Za-z]+[\d@]+[\w@]*|[\d@]+[A-Za-z]+[\w@]*)',
                            ' <codigo> ', review)

    # mask dates
    review = re.sub(r'(\d+/\d+/\d+)', ' <date> ', review)
    review = re.sub(r'(\d+/\d+)', ' <date> ', review)

    # mask time
    review = re.sub(r'^(?:(?:(\d+):)?(\d+):)?(\d+)$', ' <time> ', review)

    # Remove jump of lines
    review = re.sub(r'(?<!\\)\\n|\n', ' ', review)

    # mask Remaining numbers
    review = re.sub(r'\d+', ' <num> ', review)

    # Corrije errores de ortografia
    if remove_typos:
        review = str(TextBlob(review).correct())
    # Convierte emojis a text
    if emoji_extraction:
        review = extract_emoji(review)
    # Remueve saltos de linea
    review = re.sub(r'[\r|\n|\r\n]+', ' ',review)
    # Remueve acentos de los caracteres
    if accented_char_removal:
        review = remove_accented_chars(review)
    # Convierte todo a minuscula
    if text_lower_case:    
        review = review.lower()
    # Remueve caracteres especiales
    if special_char_removal:
        # insert spaces between special characters to isolate them    
        special_char_pattern = re.compile(r'([{.(-)!}])')
        review = special_char_pattern.sub(" \\1 ", review) 
    # Remueve espacios en blanco
    review = re.sub(' +', ' ', review)
    # Lematizacion
    if text_lemmatization:
        review = lemmatize_text(review)
    # Stemming
    if text_stem:
        review = simple_stemmer(review, language)
        
    ###### TOKENIZACION ######

    tokens = tokenize_and_remove_stopwords(review, language, specific_stopwords, only_words, lemmatization)

    if pos_tag:
        tokens = postag(tokens, language)

    if return_string:
        tokens = desarmar_lista(tokens)

    return tokens
