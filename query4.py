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

    requests = list(map(lambda i: {'id': i}, papers))

    _,_,paper_response = pubdb.resource.post_json('_bulk_get', {'docs': requests})

    for paper in paper_response['results']:
        d = paper.get('docs')[0].get('ok')
        if d is None:
            break
        a = d.get("authored-by")
        if a is None:
            break
        authors.update(a)
    if len(authors) > max_count:
        max_proc = proc
        max_count = len(authors)

proceedings = pubdb[max_proc["_id"]]

print("Proceeding with most authors is {} with {} distinct authors".format(proceedings["title"], max_count))
