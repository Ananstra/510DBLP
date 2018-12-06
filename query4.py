import couchdb
import config

couchserver = couchdb.Server("http://%s:%s@%s/" % (config.user, config.password, config.server))
pubdb = couchserver["publications"]
persondb = couchserver["persons"]

mango = {
    "selector": {
        "object-type": "proceedings",
        "year" : "2004"
    },
    "fields": ["_id", "proceedings-contents"],
    "limit": 1000000,
}

res = pubdb.find(mango)

max_proc = None
max_count = 0

for proc in res:
    papers = proc.get("proceedings-contents")
    if papers is None:
        break
    authors = set()
    for paper in papers:
        a = pubdb[paper].get("authored-by")
        if a is None:
            break
        authors.update(a)
    print(len(authors))
    if len(authors) > max_count:
        max_proc = proc
        max_count = len(authors)

proceedings = pubdb[max_proc["_id"]]

print("Proceeding with most authors is {} with {} distinct authors".format(proceedings["title"], max_count))
