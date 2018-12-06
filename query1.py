import couchdb
import config

couchserver = couchdb.Server("http://%s:%s@%s/" % (config.user, config.password, config.server))
pubdb = couchserver["publications"]
persondb = couchserver["persons"]

author_targets = {}
mango = {'selector': {'object-type': 'paper'},
        'fields': ['authored-by', '_id'],
        'limit': 1000000,
}

res = pubdb.find(mango)

author_counts = {}
for r in res:
    authors = r.get("authored-by")
    if authors is None:
        break
    author_counts[r["_id"]] = len(authors)

max_paper = max(author_counts, key=lambda id: author_counts[id])

paper = pubdb[max_paper]

title = paper["title"]
author_ids = paper["authored-by"]
authors = []

for a in author_ids:
    author = persondb.get(a)
    if author is not None:
        authors.append(author.get("name"))

print(title,authors)
