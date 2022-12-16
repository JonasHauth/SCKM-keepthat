from pymongo import MongoClient
from bson import json_util, ObjectId
import json


class knowledgeRepository:

    def __init__(self):
        self.client = MongoClient('127.0.0.1', 27017) # mongodb
        self.db = self.client.knowledge_db
        self.knowledgeCollection = self.db.knowledge_collection

    """
    get knowledge by id
    """
    def get_by_id(self, knowledge_id):
        knowledge = self.knowledgeCollection.find_one({"_id": ObjectId(knowledge_id)})
        knowledge = json.loads(json_util.dumps(knowledge))
        knowledge["_id"] = knowledge["_id"]["$oid"]
        return knowledge

    """
    get knowledge by tag
    """
    def get_by_tag(self, tag_id):
        knowledge = self.knowledgeCollection.find({"tags": tag_id})
        knowledge = json.loads(json_util.dumps(knowledge))
        for item in knowledge:
            item["_id"] = item["_id"]["$oid"]
        return knowledge

    """
    get all knowledgeEntries per user
    """
    def get_all(self):
        cursor = self.knowledgeCollection.find({})
        knowledge = list(cursor)
        knowledge = json.loads(json_util.dumps(knowledge))
        for item in knowledge:
            item["_id"] = item["_id"]["$oid"]
        return knowledge

    """
    save knowledge
    """
    def save(self, knowlegde):
        persisted_knowledge = self.knowledgeCollection.insert_one(knowlegde)
        new_id = json.loads(json_util.dumps(persisted_knowledge.inserted_id))
        return new_id


    """
    update knowledge
    """
    def update(self, knowledge):
        event_id = knowledge["_id"]
        del knowledge["_id"]
        updated_knowledge = self.knowledgeCollection.update_one(filter={"_id": ObjectId(event_id)}, update={"$set": knowledge})
        modified_count = updated_knowledge.modified_count
        return modified_count

    """
    delete knowledge
    """
    def delete(self, knowledge):
        result = self.knowledgeCollection.delete_one({"_id": ObjectId(knowledge)})
        return result.deleted_count

    """"
    delete many knowledgeEntries
    """
    def delete_many(self, query):
        result = self.knowledgeCollection.delete_many(query)
        return result.deleted_count