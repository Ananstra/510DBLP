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


def level_one(name):

    cached_authors = cache_search(name)
    if cached_authors is not None:
        return cached_authors

    mango = {
        'selector': {
            'object-type': 'person',
            'name': name
        },
        'fields': ['author-of'],
        'limit': 1
    }

    papers = list(persondb.find(mango))[0].get("author-of")
    co_authors = set()
    mango = {
        'selector': {
            '_id': {
                "$in": papers
            }
        },
        'fields': ["authored-by"],
        'limit': 100
    }
    co_id_res = list(pubdb.find(mango))
    id_list = []

    for co_ids in co_id_res:
        ids = co_ids.get("authored-by")
        id_list.extend(ids)

    mango = {
        'selector': {
            '_id': {
                "$in": id_list
            }
        },
        'fields': ["name"],
        'limit': 1000,
    }
    name_res = persondb.find(mango)
    co_authors = set()

    for n in name_res:
        co_authors.add(n["name"])

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

print(level_n("Michael Stonebraker", 2))
