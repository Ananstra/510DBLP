import couchdb
import csv
import config

couchserver = couchdb.Server("http://%s:%s@%s/" % (config.user, config.password, config.server))
objects = {}
links = {}

with open('Objects.csv') as csv_file:
    csv_reader = csv.reader(csv_file, delimiter=',')
    line_count = 0
    for row in csv_reader:
        if line_count == 0:
            cols = row
            line_count += 1
        else:
            objects[row[0]] = {"_id":row[0]}
            line_count += 1

with open('Links.csv') as csv_file:
    csv_reader = csv.reader(csv_file, delimiter=',')
    line_count = 0
    for row in csv_reader:
        if line_count == 0:
            cols = row
            line_count += 1
        else:
            links[row[0]] = {"_id": row[0], "o1":row[1], "o2":row[2]}
            line_count += 1


with open('Attributes.csv') as csv_file:
    csv_reader = csv.reader(csv_file, delimiter=',')
    line_count = 0
    for row in csv_reader:
        if line_count == 0:
            cols = row
            line_count += 1
        else:
            name = row[0],
            t = row[1]
            val = row[2]
            i = row[3]
            if isinstance(name, tuple):
                name = name[0]
            if t == "O":
                obj = objects.get(i)
                if obj is None:
                    break
                obj[name] = val
                objects[i] = obj
            elif t == "L":
                link = links.get(i)
                if link is None:
                    break
                link[name] = val
                links[i] = link

            line_count += 1

for link_id, link in links.items():
    link_type = link["link-type"]
    o1 = link["o1"]
    o2 = link["o2"]
    if link_type == "editor-of":
        editor = objects[o1]
        if editor.get("editor-of") is None:
            editor["editor-of"] = [o2]
        else:
            editor["editor-of"].append(o2)
        objects[o1] = editor

        edited = objects[o2]
        if edited.get("edited-by") is None:
            edited["edited-by"] = [o1]
        else:
            edited["edited-by"].append(o1)
        objects[o2] = edited
    elif link_type == "author-of":
        author = objects[o1]
        if author.get("author-of") is None:
            author["author-of"] = [o2]
        else:
            author["author-of"].append(o2)
        objects[o1] = author

        authored = objects[o2]
        if authored.get("authored-by") is None:
            authored["authored-by"] = [o1]
        else:
            authored["authored-by"].append(o1)
        objects[o2] = authored
    elif link_type == "in-journal":
        pub = objects[o1]
        if pub.get("in-journal") is None:
            pub["in-journal"] = [o2]
        else:
            pub["in-journal"].append(o2)
        objects[o1] = pub

        journal = objects[o2]
        if journal.get("journal-contents") is None:
            journal["journal-contents"] = [o1]
        else:
            journal["journal-contents"].append(o1)
        objects[o2] = journal
    elif link_type == "cites":
        cites = objects[o1]
        if cites.get("cites") is None:
            cites["cites"] = [o2]
        else:
            cites["cites"].append(o2)
        objects[o1] = cites

        cited = objects[o2]
        if cited.get("cited-by") is None:
            cited["cited-by"] = [o1]
        else:
            cited["cited-by"].append(o1)
        objects[o2] = cited
    elif link_type == "in-proceedings":
        pub = objects[o1]
        if pub.get("in-proceedings") is None:
            pub["in-proceedings"] = [o2]
        else:
            pub["in-proceedings"].append(o2)
        objects[o1] = pub

        proceedings = objects[o2]
        if proceedings.get("proceedings-contents") is None:
            proceedings["proceedings-contents"] = [o1]
        else:
            proceedings["proceedings-contents"].append(o1)
        objects[o2] = proceedings
    elif link_type == "in-collection":
        pub = objects[o1]
        if pub.get("in-collection") is None:
            pub["in-collection"] = [o2]
        else:
            pub["in-collection"].append(o2)
        objects[o1] = pub

        collection = objects[o2]
        if collection.get("collection-contents") is None:
            collection["collection-contents"] = [o1]
        else:
            collection["collection-contents"].append(o1)
        objects[o2] = collection
    else:
        print("Unknown link type, skipping")
        break

persons = [v for k,v in objects.items() if v["object-type"] == "person"]
publications = [v for k,v in objects.items() if v["object-type"] != "person"]

dbname = "persons"
if dbname in couchserver:
    persondb = couchserver[dbname]
else:
    persondb = couchserver.create(dbname)

dbname = "publications"
if dbname in couchserver:
    pubdb = couchserver[dbname]
else:
    pubdb = couchserver.create(dbname)

for pp in [persons[i:i + 1000] for i in range(0, len(persons), 1000)]:
    persondb.update(pp)

for pb in [publications[i:i+1000] for i in range(0, len(publications), 1000)]:
    pubdb.update(pb)
