from flask import Flask, jsonify, request
from repositories.knowledgeRepository import knowledgeRepository
import json

import os
import requests
import datetime
from datetime import date
from pathlib import Path

app = Flask(__name__)
knowledgeRepo = knowledgeRepository()


"""
Save Knowledge with Tag in Know
"""
@app.route('/knowledge', methods=['POST'])
def save_knowledge_with_tag():

    data = request.get_json()

    knowledge_test_dict = {
        "tag": data['tag'],
        "cleaned_text": data['cleaned_text'],
        "raw_text": data['raw_text'],
        "upvotes": data['upvotes'],
        "downvotes": data['downvotes'],
        "source_uid": data['source_uid'],
        "source_mail": data['source_mail']
    }

    knowledgeRepo.save(knowledge_test_dict)

    response = jsonify(knowledge_test_dict)
    response.status_code = 201
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
@app.route('/knowledge/<string:tag_id>', methods=['GET'])
def get_all_knowledge_by_tag(tag_id):
    knowledgeEntries = knowledgeRepo.get_by_tag(tag_id)
    response = jsonify(knowledgeEntries)
    response.status_code = 200
    return response

"""
Get Knowledge by ID from Know
"""
@app.route('/knowledge/<string:id>', methods=['GET'])
def get_knowledge_by_id(id):
    knowledgeEntry = knowledgeRepo.get_by_id(id)
    response = jsonify(knowledgeEntry)
    response.status_code = 200
    return response

"""
Update single Knowledge by ID from Know
"""
@app.route('/knowlegde/<string:id>', methods=['PUT'])
def update_knowledge_by_id():
    
    data = request.get_json()

    print(data)

    knowledgeEntry = knowledgeRepo.get_by_id(data['id'])

    knowledgeEntry["tag"] = data['tag']
    knowledgeEntry["cleaned_text"] = data['cleaned_text']
    knowledgeEntry["raw_text"] = data['raw_text']
    knowledgeEntry["upvotes"] = data['upvotes']
    knowledgeEntry["downvotes"] = data['downvotes']
    knowledgeEntry["source_uid"] = data['source_uid']
    knowledgeEntry["source_mail"] = data['source_mail']
    
    updated_Count = knowledgeRepo.update(knowledgeEntry)

    response = jsonify({"status_code": 200})
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
def delete_events():
    myquery = {}
    count = knowledgeRepo.delete_many(myquery)
    return jsonify(count), 200

if __name__ == '__main__':
    app.run(port=8080)
