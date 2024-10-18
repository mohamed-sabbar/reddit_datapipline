import configparser
import os
import pandas as pd
import psycopg2


def insert_the_data(df):
    try:
        list_ids = []
        config_reader = configparser.ConfigParser()
        file_path = os.path.dirname(__file__)
        parent_path = os.path.dirname(file_path)
        config_path = os.path.join(parent_path, "config", "config.ini")
        config_reader.read(config_path)

        # Lecture des informations de la base de données à partir du fichier de configuration
        host = config_reader.get('DATABASE', 'host')
        database = config_reader.get('DATABASE', 'database')
        user = config_reader.get('DATABASE', 'user')
        password = config_reader.get('DATABASE', 'password')
        port = config_reader.get('DATABASE', 'port')
        table = config_reader.get('DATABASE', 'table_name')

        # Connexion à la base de données avec psycopg2
        connection = psycopg2.connect(
            host=host,
            database=database,
            user=user,
            password=password,
            port=port
        )
        cursor = connection.cursor()

        # Récupérer les IDs existants
        cursor.execute(f"SELECT id FROM {table}")
        rows = cursor.fetchall()
        for row in rows:
            list_ids.append(str(row[0]))

        # Filtrer les nouvelles lignes à insérer
        df_new = df[~df['id'].astype(str).isin(list_ids)]

        # Insérer les nouvelles données
        if not df_new.empty:
            for _, row in df_new.iterrows():
                # Construire la requête d'insertion
                cursor.execute(
    f"INSERT INTO {table} (id, author, name, title, score, url, created_utc, comment_limit, num_comments, ups, over_18, saved, spoiler, stickied) "
    "VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)",
    (
        row['id'],
        row['author'][:100],  # Tronquer la valeur si elle dépasse 100 caractères
        row['name'][:100],    # Faites de même pour les autres colonnes concernées
        row['title'][:255],   # Exemple : si 'title' est limité à 255 caractères
        row['score'],
        row['url'][:200],     # Tronquez les URL si nécessaire
        row['created_utc'],
        row['comment_limit'],
        row['num_comments'],
        row['ups'],
        row['over_18'],
        row['saved'],
        row['spoiler'],
        row['stickied']
    )
)


            # Valider les changements dans la base de données
            connection.commit()

        print("Insertion des données réussie")

    except Exception as e:
        print(f"Erreur lors de l'opération de connexion, erreur : {e}")

    finally:
        # Fermeture de la connexion
        if cursor:
            cursor.close()
        if connection:
            connection.close()



