import couchdb
import config

# Server Setup Block
couchserver = couchdb.Server("http://%s:%s@%s/" % (config.user, config.password, config.server))
pubdb = couchserver["publications"]
persondb = couchserver["persons"]

# See if author already has cached co-authors
def cache_search(name):
    mango = {
        'selector': {
            'name': name
        },
        'fields': ['_id', 'co-authors'],
        'limit': 1
    }

    res = list(persondb.find(mango))[0]
    return res.get("co-authors")

# Cache co_authors for future queries
def cache_results(name, co_authors):
    mango = {
        'selector': {
            'name': name
        },
        'limit': 1
    }

    res = list(persondb.find(mango))[0]
    res["co-authors"] = co_authors
    persondb[res["_id"]] = res

# Find Level One co-authors
# Check_cache specifies whether to return cached information.
def level_one(name, check_cache=True):

    # Get Cached results, if applicable
    if check_cache:
        cached_authors = cache_search(name)
        if cached_authors is not None:
            return cached_authors

    # Select all publications authored by the provided person.
    mango = {
        'selector': {
            'name': name
        },
        'fields': ['author-of'],
        'limit': 1
    }

    papers = list(persondb.find(mango))[0].get("author-of")

    # Build and execute bulk request for paper info
    id_list = []

    requests = list(map(lambda i: {"id": i}, papers))

    _,_,paper_response = pubdb.resource.post_json('_bulk_get', {'docs': requests})

    # Grab the list of authors for each paper
    for paper in paper_response['results']:
        d = paper.get('docs')[0].get('ok')
        if d is None:
            break
        ids = d.get('authored-by')
        id_list.extend(ids)

    # Build and execute bulk request for co-author info
    co_authors = set()

    requests = list(map(lambda i: {"id": i}, id_list))

    _,_,author_response = persondb.resource.post_json('_bulk_get', {'docs': requests})

    # Get co-author names, add them to set
    for person in author_response['results']:
        d = person.get('docs')[0].get('ok')
        if d is None:
            break
        n = d.get('name')
        if n is None:
            break
        co_authors.add(n)

    # Remove the starting author, they are not their own co-author
    co_authors.remove(name)

    # Save results for future searches
    cache_results(name, list(co_authors))

    return list(co_authors)

# Recursive level n co-author function
def level_n_recurse(name, n):
    # Base Case, use level_one
    if n == 1:
        l = level_one(name)
        return {1: l}
    # Recursive Case, use previous levels to build this level.
    else:
        # Get previous levels
        levels = level_n_recurse(name,n-1)
        # Get all co-authors of level n-1 authors.
        candidates = set()
        for p in levels[n-1]:
            cas = level_one(p)
            candidates.update(cas)
        # Remove all authors already in lower levels.
        for i in range(1,n):
            candidates.difference_update(levels[i])
        # Remove ourself, if necessary
        candidates.discard(name)
        # The remainder are the level n co-authors.
        levels[n] = list(candidates)
        return levels

# Wrapper function for entry
def level_n(name, n):
    levels = level_n_recurse(name, n)
    return levels[n]

print(len(level_n("Barton C. Massey", 3)))
