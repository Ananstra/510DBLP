import couchdb
import csv

couchserver = couchdb.Server("http://user:password@server:5984/")
dbname = "links"
if dbname in couchserver:
    linkdb = couchserver[dbname]
else:
    linkdb = couchserver.create(dbname)

dbname = "objects"
if dbname in couchserver:
    objdb = couchserver[dbname]
else:
    objdb = couchserver.create(dbname)

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
                A
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

print(objects)
print(links)

objects = list(objects.values())
links = list(links.values())

for oo in [objects[i:i + 1000] for i in range(0, len(objects), 1000)]:
    objdb.update(oo)

for ll in [links[i:i + 1000] for i in range(0, len(links), 1000)]:
    linkdb.update(ll)
