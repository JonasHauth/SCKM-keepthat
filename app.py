from flask import Flask, jsonify, request
from repositories.knowledgeRepository import knowledgeRepository
import json

import spacy
from spacy.matcher import Matcher
from spacy.tokens import Token
Token.set_extension("ignore", default=False, force=True)


import os
import requests
import datetime
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

"""
Sync with Notion
"""
@app.route('/sync/notion', methods=['POST'])
def sync_notion():

    
    try:
        
        print("To-Do")


        # Sync Notion > Mongo (by createdTime und editedTime)

        # Sync Mongo > Notion (by createdTime und editedTime)

    except:
        print('An error occurred')

    response = jsonify("test")
    response.status_code = 200
    return response


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

    knowledge_test_dict = {
        "cleaned_text": data['cleaned_text'],
        "tags": data['tags'],
        "source_uid": data['source_uid'],
        "raw_text": data['raw_text'],
    }

    inserted_ID = knowledgeRepo.save(knowledge_test_dict)
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
@app.route('/knowledge/id/<string:id>', methods=['GET'])
def get_knowledge_by_id(id):
    knowledgeEntry = knowledgeRepo.get_by_id(id)
    response = jsonify(knowledgeEntry)
    response.status_code = 200
    return response

"""
Update single Knowledge by ID from Know
"""
@app.route('/knowlegde/id/<string:id>', methods=['PUT'])
def update_knowledge_by_id(id):
    
    data = request.get_json()

    knowledgeEntry = knowledgeRepo.get_by_id(id)

    knowledgeEntry["tag"] = data['tag']
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
@app.route('/knowlegde/<string:id>', methods=['DELETE'])
def delete_knowledge_by_id():
    
    data = request.get_json()

    print(data)

    knowledgeEntry = knowledgeRepo.get_by_id(data['id'])

    deleted_Count = knowledgeRepo.delete(knowledgeEntry)

    response = jsonify("Success")
    response.status_code = 201
    return response


"""
Delete all Events from DB
"""
@app.route('/reset_test', methods=['DELETE'])
def delete_knowledge():
    myquery = {}
    count = knowledgeRepo.delete_many(myquery)
    return jsonify(count), 200


"""
Create examplex in DB
"""
@app.route('/create_test', methods=['GET'])
def create_knowledge():
    
    knowledge_test_dict = {
        "cleaned_text": "Die Tabelle “Subscriptions” im Schema “CRM” enthält alle Daten zu den Abonnements.",
        "tag": ["DWH", "SQL"],
        "source_uid": "Marcel Orth",
        "raw_text": "Hey, du kannst die Tabelle “Subscriptions” im Schema “CRM” benutzen. Dort sind alle Daten zu den Abonnements enthalten.",
    }
    
    count = knowledgeRepo.insert(knowledge_test_dict)
    return jsonify(count), 200

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


if __name__ == '__main__':
    app.run(port=8080)


