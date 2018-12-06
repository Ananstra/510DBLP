import couchdb
import config

# Server Setup Block
couchserver = couchdb.Server("http://%s:%s@%s/" % (config.user, config.password, config.server))
pubdb = couchserver["publications"]
persondb = couchserver["persons"]

# Get all proceedings, and their contents.
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

# For each proceeding, count the number of distinct authors.

for proc in res:
    # Get the list of paper IDs
    papers = proc.get("proceedings-contents")
    if papers is None:
        break
    authors = set()

    # Build and execute bulk request for paper info
    requests = list(map(lambda i: {'id': i}, papers))

    _,_,paper_response = pubdb.resource.post_json('_bulk_get', {'docs': requests})

    # For each paper, add its authors to the set of authors for this proceeding.
    for paper in paper_response['results']:
        d = paper.get('docs')[0].get('ok')
        if d is None:
            break
        a = d.get("authored-by")
        if a is None:
            break
        authors.update(a)
    # If this proceeding has more authors than the previous best, save it.
    if len(authors) > max_count:
        max_proc = proc
        max_count = len(authors)

# Grab the proceeding info and display it.
proceedings = pubdb[max_proc["_id"]]

print("Proceeding with most authors is {} with {} distinct authors".format(proceedings["title"], max_count))
