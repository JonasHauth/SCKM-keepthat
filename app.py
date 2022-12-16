from flask import Flask, jsonify, request
from repositories.knowledgeRepository import knowledgeRepository
import json

import spacy
from spacy.matcher import Matcher
from spacy.tokens import Token
Token.set_extension("ignore", default=False, force=True)


import os
import pandas as pd
from dotenv import load_dotenv
import requests
import datetime
import dateutil.parser
from datetime import datetime
from datetime import date
from pathlib import Path

nlp = spacy.load('notebooks/model/model-best')
nlp_pretrained = spacy.load("en_core_web_trf")


app = Flask(__name__)
knowledgeRepo = knowledgeRepository()

keywords_dict = {
    "Allgemeine Definitionen": ["bedeutet", "gleich", "gemeint", "steht für"],
    "HR": ["urlaub", "urlaubstage", "abrechnung", "abrechnungszeitraum", "stunden", "kostenstelle", "einkauf", "nutzer", "pe-as"],
    "SQL": ["tabelle", "tabellen", "sql", "query", "queries", "daten", "attribut", "spalte", "merkmal", "schema"],
    "DWH": ["tabelle", "tabellen", "sql", "query", "queries", "daten", "attribut", "spalte", "merkmal", "schema", "ebene", "dwh", "data warehouse", "postgresql", "daten", "feld", "felder"],
    "Python": ["feature", "engineering", "modell", "python", "klassifizieren", "klassifiziert"],
    "Machine Learning": ["feature", "fngineering", "modell", "python", "klassifizieren", "klassifiziert"],
    "CRM": ["crm", "sales", "deal", "kunde", "kunden", "vertrag"],
    "SAP": ["sap", "idoc", "berechtigung", "berechtigungen"],
    "ERP": ["erp", "mara", "marc", "material", "materialien"],
    "MLFlow": ["mlflow"],
    "Zertifikate": ["zertifikat"],
    "Kubernetes": ["kubernetes", "namespace", "d-bru", "p-bru", "ranger", "harbor"],
    "Grafana": ["grafana"],
    "DeepL": ["deepL", "deepL-api", "übersetzung"],
    "API": ["api"]    
}

# Secrets
load_dotenv()
database_id = os.environ.get("DATABASE_ID")
notion_secret = os.environ.get("NOTION_SECRET")
notion_version = os.environ.get("NOTION_VERSION")

"""
Clean Text and Predict Tags 
"""
@app.route('/prepare', methods=['POST'])
def prepare_text():

    data = request.get_json()

    # Create cleaned text from raw.
    text = data['text']
    print(text)
    cleaned_text = clean_text(text)

    # Deep Learning Predict Tags
    doc = nlp(cleaned_text)  
    cats = doc.cats
    dl_predicted_tags = list(dict(filter(lambda x: x[1] > 0.5, cats.items())).keys())
    
    # Keywords Predict Tags
    keywords, keyword_predicted_tags = get_keywords(text, keywords_dict)

    # Get all predictions
    tags = list(set(dl_predicted_tags + keyword_predicted_tags))


    return_dict = {
        "tags": tags,
        "cleaned_text": cleaned_text,
    }

    response = jsonify(return_dict)
    response.status_code = 200
    return response

"""
Save Knowledge with Tag in Know
"""
@app.route('/knowledge', methods=['POST'])
def save_knowledge_with_tag():

    data = request.get_json()

    knowledge_dict = {
        "cleaned_text": data['cleaned_text'],
        "tags": data['tags'],
        "source_uid": data['source_uid'],
        "raw_text": data['raw_text'],
        "editedTime": datetime.utcnow()
    }

    inserted_ID = knowledgeRepo.save(knowledge_dict)
    response = jsonify(inserted_ID)
    response.status_code = 200
    return response
    

"""
Get all Knowlegde from Know
"""
@app.route('/knowledge', methods=['GET'])
def get_all_knowledge():
    knowledgeEntries = knowledgeRepo.get_all()
    response = jsonify(knowledgeEntries)
    response.status_code = 200
    return response


"""
Get Knowledge by Tag from Know
"""
@app.route('/knowledge/tag/<string:tag_id>', methods=['GET'])
def get_all_knowledge_by_tag(tag_id):
    knowledgeEntries = knowledgeRepo.get_by_tag(tag_id)
    response = jsonify(knowledgeEntries)
    response.status_code = 200
    return response

"""
Get Knowledge by ID from Know
"""
@app.route('/knowledge/byid/<string:id>', methods=['GET'])
def get_knowledge_by_id(id):
    knowledgeEntry = knowledgeRepo.get_by_id(id)
    response = jsonify(knowledgeEntry)
    response.status_code = 200
    return response

"""
Update single Knowledge by ID from Know
"""
@app.route('/knowlegde/update/<string:id>', methods=['PUT'])
def update_knowledge_by_id(id):

    print(id)

    data = request.get_json()

    knowledgeEntry = knowledgeRepo.get_by_id(id)

    knowledgeEntry["tags"] = data['tags']
    knowledgeEntry["cleaned_text"] = data['cleaned_text']
    knowledgeEntry["source_uid"] = data['source_uid']
    knowledgeEntry["raw_text"] = data['raw_text']
    
    updated_ID = knowledgeRepo.update(knowledgeEntry)

    response = jsonify(updated_ID)
    response.status_code = 200
    return response


"""
Delete single KnowlegdeEntry by ID from Know
"""
@app.route('/knowlegde/delete/<string:id>', methods=['DELETE'])
def delete_knowledge_by_id(id):

    print(id)

    knowledgeEntry = knowledgeRepo.get_by_id(id)

    deleted_Count = knowledgeRepo.delete(knowledgeEntry['_id'])

    response = jsonify(deleted_Count)
    response.status_code = 200
    return response

"""
Sync with Notion
"""
@app.route('/sync/notion', methods=['POST'])
def sync_notion():

    # Alle Einträge von Notion holen.
    # Query Notion DB
    url = "https://api.notion.com/v1/databases/" + database_id + "/query"
    headers = {
        "Authorization": notion_secret,
        "accept": "application/json",
        "Notion-Version": notion_version,
        "content-type": "application/json"
    }
    response = requests.post(url, headers=headers)
    notion_KnowledgeEntries = json.loads(response.text)['results']

    # Einträge identifizieren, die noch nicht in Notion angelegt wurden
    local_mongo_ids = []
    local_KnowledgeEntries = knowledgeRepo.get_all()
    for knowledgeEntry in local_KnowledgeEntries:
        local_mongo_ids.append(knowledgeEntry['_id'])
    local_mongo_ids_not_present_in_notion = local_mongo_ids
    for notion_entry in notion_KnowledgeEntries:
        mongo_id = notion_entry['properties']['Local ID']['rich_text'][0]['text']['content']
        if mongo_id in local_mongo_ids:
            local_mongo_ids_not_present_in_notion.remove(mongo_id)
    print(f"The following local knowledgeEntries will be newly created in Notion: {local_mongo_ids_not_present_in_notion}")

    # KnowledgeEntreis, die noch nicht auf Notion bestehen, sollen neu nach Notion geschrieben werden
    for mongo_id in local_mongo_ids_not_present_in_notion:
        
        # lokalen KnowledgeEntry laden
        knowledgeEntry = knowledgeRepo.get_by_id(mongo_id)
        # KnowledgeEntry zu Request konkatenieren
        write_new_entry_to_notion = add_to_notion(knowledgeEntry, database_id)      
        url = "https://api.notion.com/v1/pages"
        headers = {
            "Authorization": notion_secret,
            "accept": "application/json",
            "Notion-Version": notion_version,
            "content-type": "application/json"
        }
        response = requests.post(url, headers=headers, json=write_new_entry_to_notion)
        print(f"Added mongoID: {mongo_id}")


    # Für jeden Notion entry überprüfen, ob eine Aktualisierung vorliegt
    for notion_entry in notion_KnowledgeEntries:

        # Notion last edited parsen
        print(f"Parsing KnowledgeEntry: {mongo_id}")
        mongo_id = notion_entry['properties']['Local ID']['rich_text'][0]['text']['content']
        page_id = notion_entry['id']
        notion_last_edited = notion_entry['properties']['Last edited time']['last_edited_time']
        notion_last_edited = dateutil.parser.parse(notion_last_edited)
        # notion_last_synced = notion_entry['properties']['editedTime']['date']['start']
        # notion_last_synced = dateutil.parser.parse(notion_last_synced)

        # local last edited parsen, wenn dies nicht gefunden wurde, KnowledgeEntry von Notion löschen
        try:
            knowledgeEntry = knowledgeRepo.get_by_id(mongo_id)
            mongo_time = knowledgeEntry['editedTime']
            mongo_time = dateutil.parser.parse(mongo_time)
            print(f'Mongo time: {mongo_time}, Notion time: {notion_last_edited} Mongo time > Notion time {mongo_time > notion_last_edited}')

        except Exception as e:
            print(e)

            print("Entry doesn't exist locally anymore > Delete from Notion")
            archive_knowledge_notion = archive_notion()
            url = "https://api.notion.com/v1/pages/" + page_id
            headers = {
                "Authorization": notion_secret,
                "accept": "application/json",
                "Notion-Version": notion_version,
                "content-type": "application/json"
            }
            response = requests.patch(url, headers=headers, json=archive_knowledge_notion)
            continue

        if mongo_time > notion_last_edited:

            # Update notion wenn local last edted aktueller als notion last edited        
            print(f"Mongo ist aktueller, update Notion for ID: {mongo_id}")
            write_update_to_notion = update_notion(knowledgeEntry, database_id)
            url = "https://api.notion.com/v1/pages/" + page_id
            headers = {
                "Authorization": notion_secret,
                "accept": "application/json",
                "Notion-Version": notion_version,
                "content-type": "application/json"
            }
            response = requests.patch(url, headers=headers, json=write_update_to_notion)
        
        else:
            
            # Update local wenn notion_last_edited größer als mongo_time
            # Vorausgesetzt es liegt eine Änderung vor.
            updateEntry = {}
            updateEntry['_id'] = mongo_id
            updateEntry['cleaned_text'] = notion_entry['properties']['Clean Text']['title'][0]['text']['content']
            tags=[]
            for select in notion_entry['properties']['Tags/Kategorie']['multi_select']:
                tags.append(select['name'])
            updateEntry['tags'] = tags
            updateEntry['raw_text'] = notion_entry['properties']['Original Text']['rich_text'][0]['text']['content']
            updateEntry['source_uid'] = notion_entry['properties']['Eingereicht von']['rich_text'][0]['text']['content']
            updateEntry['editedTime'] = notion_entry['properties']['editedTime']['date']['start']

            # Testen, ob eine Änderung vorliegt:
            if (knowledgeEntry['_id'] != updateEntry['_id'] or knowledgeEntry['cleaned_text'] != updateEntry['cleaned_text'] or knowledgeEntry['tags'] != updateEntry['tags'] or knowledgeEntry['raw_text'] != updateEntry['raw_text'] or knowledgeEntry['source_uid'] != updateEntry['source_uid'] or knowledgeEntry['editedTime'] != updateEntry['editedTime']):

                print(f"Notion ist aktueller und es liegt eine Änderung vor, update Mongo für ID: {mongo_id}.")

                # Get information from Notion and call update
                update_local_knowledge(mongo_id, updateEntry)

                # Also update editedTime in Notion
                write_update_to_notion = update_notion(updateEntry, database_id)


                url = "https://api.notion.com/v1/pages/" + page_id

                headers = {
                    "Authorization": notion_secret,
                    "accept": "application/json",
                    "Notion-Version": notion_version,
                    "content-type": "application/json"
                }

                response = requests.patch(url, headers=headers, json=write_update_to_notion)
            
            else:
                print(f"Notion ist aktueller aber es liegt keine Änderung vor für ID: {mongo_id}.")


    response = jsonify("Sucessfully synced.")
    response.status_code = 200
    return response


"""
Delete all Events from DB
"""
@app.route('/reset_test', methods=['DELETE'])
def delete_knowledge():
    myquery = {}
    deleted_Count = knowledgeRepo.delete_many(myquery)


    response = jsonify(deleted_Count)
    response.status_code = 200
    return response


"""
Create examples in DB
"""
@app.route('/create_test', methods=['GET'])
def create_knowledge():
        
    df = pd.read_excel("data/demo_data_real.xlsx")
    df['labels'] = df['Tags/Kategorie'].apply(lambda x : x.split(","))

    for index, row in df.iterrows():


        knowledge_dict = {
            "cleaned_text": row['Cleaned Text EN'],
            "tags": row['labels'],
            "source_uid": "Jonas Hauth, Marcel Orth",
            "raw_text": row['Original Text EN'],
            "editedTime": datetime.utcnow()
        }
        knowledgeRepo.save(knowledge_dict)

     
    return jsonify("Added samples."), 200

"""
Help method for Clean Text
"""
def clean_text(text):
    
    doc = nlp_pretrained(text)
    match_texts = Matcher(nlp.vocab)
    
    pattern = [
        [{"POS": "ADJ"}, {"POS": "NOUN"}, {"POS": "PROPN"}],
        [{"POS": "NOUN"}, {"POS": "PROPN"}],
        ]
    match_texts.add("Grußformeln", pattern, on_match=set_ignore)

    hello_synoym = ["hello", "hi", "greetings", "welcome", "hey", "olla", "hi-ya", "howdy"] #"good morning", "good evening", "good afternoon", 

    pattern = [
        [{"LOWER": {"IN": hello_synoym}}, {"POS": "PROPN"}, {"IS_PUNCT": True}],  
        [{"LOWER": {"IN": hello_synoym}}, {"POS": "ADV"}, {"IS_PUNCT": True}],
        [{"LOWER": {"IN": hello_synoym}}, {"POS": "PRON"}, {"IS_PUNCT": True}],
    ]

    match_texts.add("Begrusungformeln", pattern, on_match=set_ignore) 

    toks = [tok.text + tok.whitespace_ for tok in doc if not tok._.ignore]
    cleaned_text = "".join(toks)
    cleaned_text = cleaned_text[0].upper() + cleaned_text[1:]
    
    return(cleaned_text)


"""
Help method for Removing ignored speech parts
"""
def set_ignore(matcher, doc, id, matches):
    for _, start, end in matches:
        for tok in doc[start:end]:
            tok._.ignore = True



"""
Help method for Keywords
"""
def get_keywords(text, keywords_dict):  # input: sentences, keywords_dict
    all_keywords = []
    all_categories = []

    for categorie, keywords in keywords_dict.items():  
            
        for keyword in keywords:
            
            if keyword.lower() in text.lower():
                
                all_categories.append(categorie)
                all_keywords.append(keyword)

    all_keywords = list(set(all_keywords))
    all_categories = list(set(all_categories))

    return all_keywords, all_categories

"""
Help method add to Notion
"""
def add_to_notion(knowledgeEntry, database_id):

    mongo_id = knowledgeEntry['_id']
    clean_text = knowledgeEntry['cleaned_text']
    tags = knowledgeEntry['tags']
    tags_for_notion = []
    for tag in tags:
        tags_for_notion.append({"name": tag})
    source_uid = knowledgeEntry['source_uid']
    raw_text = knowledgeEntry['raw_text']
    editedTime = knowledgeEntry['editedTime']

    write_new_entry_to_notion = {}

    write_new_entry_to_notion['parent'] = {
        "type": "database_id",
        "database_id": database_id,
    }

    write_new_entry_to_notion['properties'] = {
        "Local ID": {
            "rich_text": [
            {
                "type": "text",
                "text": {
                "content": mongo_id,
                }
            },
            ]
        },

        "Clean Text": {
            "title": [
            {
                "type": "text",
                "text": {
                "content": clean_text,
                }
            },
            ]
        },

        "Tags/Kategorie": {
            "multi_select": tags_for_notion,
        },

        "Eingereicht von": {
            "rich_text": [
            {
                "type": "text",
                "text": {
                "content": source_uid,
                }
            },
            ]
        },

        "Original Text": {
            "rich_text": [
            {
                "type": "text",
                "text": {
                "content": raw_text,
                }
            },
            ]
        },

        "editedTime": {
            "date": {
            "start": editedTime
            }
        },   
    }

    return write_new_entry_to_notion

"""
Help method update to Notion
"""
def update_notion(knowledgeEntry, database_id):

    mongo_id = knowledgeEntry['_id']
    clean_text = knowledgeEntry['cleaned_text']
    tags = knowledgeEntry['tags']
    tags_for_notion = []
    for tag in tags:
        tags_for_notion.append({"name": tag})
    editedTime = knowledgeEntry['editedTime']

    source_uid = knowledgeEntry['source_uid']
    raw_text = knowledgeEntry['raw_text']

    
    write_update_to_notion = {}

    write_update_to_notion['properties'] = {
        "Local ID": {
            "rich_text": [
            {
                "type": "text",
                "text": {
                "content": mongo_id,
                }
            },
            ]
        },

        "Clean Text": {
            "title": [
            {
                "type": "text",
                "text": {
                "content": clean_text,
                }
            },
            ]
        },

        "Tags/Kategorie": {
            "multi_select": tags_for_notion,
        },

        "Eingereicht von": {
            "rich_text": [
            {
                "type": "text",
                "text": {
                "content": source_uid,
                }
            },
            ]
        },

        "Original Text": {
            "rich_text": [
            {
                "type": "text",
                "text": {
                "content": raw_text,
                }
            },
            ]
        },

        "editedTime": {
            "date": {
            "start": editedTime
            }
        },  
    }

    return write_update_to_notion

"""
Help method archive to Notion
"""
def archive_notion():
       
    write_archive_to_notion = {}

    write_archive_to_notion['archived'] = True

    return write_archive_to_notion

"""
Update local Knowledge by ID from Know
"""
def update_local_knowledge(id, updateEntry):

    knowledgeEntry = knowledgeRepo.get_by_id(id)

    knowledgeEntry["tags"] = updateEntry['tags']
    knowledgeEntry["cleaned_text"] = updateEntry['cleaned_text']
    knowledgeEntry["source_uid"] = updateEntry['source_uid']
    knowledgeEntry["raw_text"] = updateEntry['raw_text']
    knowledgeEntry["editedTime"] = updateEntry['editedTime']
    
    updated_ID = knowledgeRepo.update(knowledgeEntry)

    return updated_ID

if __name__ == '__main__':
    app.run(port=8080)


