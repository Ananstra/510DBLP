import couchdb
import config

# Server Setup Block
couchserver = couchdb.Server("http://%s:%s@%s/" % (config.user, config.password, config.server))
pubdb = couchserver["publications"]
persondb = couchserver["persons"]

# Get all papers
mango = {'selector': {'object-type': 'paper'},
        'fields': ['authored-by', '_id'],
        'limit': 1000000,
}

res = pubdb.find(mango)

# Search for max author count paper
max_paper = None
max_count = 0
for r in res:
    # Get List of authors
    authors = r.get("authored-by")
    if authors is None:
        break
    if len(authors) > max_count:
        max_paper = r.get("_id")
        max_count = len(authors)

# Fetch paper info for the paper with most authors.
paper = pubdb[max_paper]

title = paper["title"]
author_ids = paper["authored-by"]
authors = []

for a in author_ids:
    author = persondb.get(a)
    if author is not None:
        authors.append(author.get("name"))

print(title,authors)
