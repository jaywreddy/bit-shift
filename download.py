import json, requests, redis, pymongo, sys, argparse
from pymongo import MongoClient


apikey = "150577811f95ef44520ce3e0ac900f42"

def enigmaGet(repokey,pdict):
	url = 'https://api.enigma.io/v2/data/%s/%s/'%(apikey,repokey)
	print(url)
	return requests.get(url, params= pdict).json()

def export(repokey):
	respDict= enigmaGet(repokey,{"page": 1})
	tpages = respDict['info']['total_pages']
	print("pages: " + str(tpages))
	yield respDict['result']
	for i in range(2,tpages+1):
		params = {"page":i}
		resultpart = enigmaGet(repokey, params)
		yield resultpart['result']

def mongoLoad(collection, dataIterator):
	for partial in dataIterator:
		collection.insert_many(partial)


def redisLoad(redis, keyspace, keyfun, dataIterator):
	for partial in dataIterator:
		for record in partial:
			redis.set(keyspace+keyfun(record), record)

def mongoImport(repokey):
	client = MongoClient('localhost', 27017)
	db = client['bitshift']
	collection = db[repokey]
	dataIterator = export(repokey)
	mongoLoad(collection, dataIterator)


def redisImport(repokey):
	client = redis.StrictRedis(host = 'localhost', port= 6379)
	dataIterator = export(repokey)
	keyfun = lambda x: x['uin']
	redisLoad(client, 'bitshift.', keyfun, dataIterator)

def main(argv):
	print(argv)
	platform = argv[0]
	repokey = argv[1]
	if platform == "redis":
		redisImport(repokey)
	if platform == "mongodb":
		mongoImport(repokey)

if __name__ == "__main__":
	main(sys.argv[1:])