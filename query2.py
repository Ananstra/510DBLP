import couchdb
import config

couchserver = couchdb.Server("http://%s:%s@%s/" % (config.user, config.password, config.server))
pubdb = couchserver["publications"]
persondb = couchserver["persons"]

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


def level_one(name, check_cache=True):

    if check_cache:
        cached_authors = cache_search(name)
        if cached_authors is not None:
            return cached_authors

    mango = {
        'selector': {
            'name': name
        },
        'fields': ['author-of'],
        'limit': 1
    }

    papers = list(persondb.find(mango))[0].get("author-of")
    co_authors = set()

    id_list = []

    requests = list(map(lambda i: {"id": i}, papers))

    _,_,paper_response = pubdb.resource.post_json('_bulk_get', {'docs': requests})

    for paper in paper_response['results']:
        d = paper.get('docs')[0].get('ok')
        if d is None:
            break
        ids = d.get('authored-by')
        id_list.extend(ids)

    co_authors = set()

    requests = list(map(lambda i: {"id": i}, id_list))

    _,_,author_response = persondb.resource.post_json('_bulk_get', {'docs': requests})

    for person in author_response['results']:
        d = person.get('docs')[0].get('ok')
        if d is None:
            break
        n = d.get('name')
        if n is None:
            break
        co_authors.add(n)

    co_authors.remove(name)

    cache_results(name, list(co_authors))

    return list(co_authors)

def level_n_recurse(name, n):
    if n == 1:
        l = level_one(name)
        return {1: l}
    else:
        levels = level_n_recurse(name,n-1)
        candidates = set()
        for p in levels[n-1]:
            cas = level_one(p)
            candidates.update(cas)
        for i in range(1,n):
            candidates.difference_update(levels[i])
        levels[n] = list(candidates)
        return levels

def level_n(name, n):
    levels = level_n_recurse(name, n)
    return levels[n]

print(len(level_n("Barton C. Massey", 3)))
