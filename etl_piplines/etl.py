import praw
import configparser
import os
import pandas as pd
import praw.models
import datetime

# Configuration du fichier de configuration
conf_reader = configparser.ConfigParser()
file_path = os.path.dirname(__file__)
parent_file = os.path.dirname(file_path)
config_file = os.path.join(parent_file, "config", "config.ini")

conf_reader.read(config_file)

# Connexion à l'API Reddit
def api_connection():
    try:
        reddit_instance = praw.Reddit(
            client_id=conf_reader.get('Reddit_info', 'client_id'),
            client_secret=conf_reader.get('Reddit_info', 'client_secret'),
            user_agent=conf_reader.get('Reddit_info', 'user_agent'),
            username=conf_reader.get('Reddit_info', 'username'),
            password=conf_reader.get('Reddit_info', 'password'),
        )
        print("API connection established successfully!")
        return reddit_instance
    except Exception as e:
        print(f"Error in API operation: {e}")

# Récupération du subreddit spécifié dans le fichier de configuration
def subreddit(reddit_instance):
    try:
        subreddit_instance = reddit_instance.subreddit(conf_reader.get('Extract_informations', 'subreddit'))
        return subreddit_instance
    except Exception as e:
        print(f"Error retrieving subreddit: {e}")

# Extraction des données
def extract_data(subreddit_instance):
    try:
        subreddit_dic = {
            'id': [],
            'author': [],
            'name': [],
            'title': [],
            'score': [],
            'url': [],
            'created_utc': [],
            'comment_limit': [],
            'num_comments': [],
            'ups': [],
            'over_18': [],
            'saved': [],
            'spoiler': [],
            'stickied': []
        }

        list_subreddit_dic_key = list(subreddit_dic.keys())

        # Parcourir les publications du subreddit
        for post in subreddit_instance.top(limit=10):
            dic_post = vars(post)
            for key in list_subreddit_dic_key:
                if key in dic_post:
                    value = dic_post[key]
                    if isinstance(value, praw.models.Redditor):
                        subreddit_dic[key].append(value.name if value else None)
                    else:
                        subreddit_dic[key].append(value)
                else:
                    subreddit_dic[key].append(None)

        df = pd.DataFrame(subreddit_dic)
        print(df)
        return df
    except Exception as e:
        print(f"Error in data extraction: {e}")

# Transformation des données
def transform_data(df_extracted_data):
    try:
        df_extracted_data['created_utc'] = pd.to_datetime(df_extracted_data['created_utc'], unit='s')
        return df_extracted_data
    except Exception as e:
        print(f"Error in data transformation: {e}")

# Chargement des données dans un fichier Excel
def load_data(df_final_data):
    try:
        output_file = os.path.join(parent_file, "output", "reddit_data_output.xlsx")

        # Créer le répertoire s'il n'existe pas
        os.makedirs(os.path.dirname(output_file), exist_ok=True)

        df_final_data.to_excel(output_file, index=False)
        print(f"Data loaded successfully into {output_file}!")
        return output_file
    except Exception as e:
        print(f"Error in data loading: {e}")
