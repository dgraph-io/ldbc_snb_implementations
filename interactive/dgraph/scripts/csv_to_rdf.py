import csv
import sys
import gzip
import os.path

input_dir = "/Users/chenxu/social_network"
output_file = sys.stdout
#output_file = gzip.open("/tmp/a.rdf.gz", "wb", 9)

MESSAGE     =0x0000000000000000
FORUM       =0x1000000000000000
ORGANIZATION=0x2000000000000000
PERSON      =0x3000000000000000
PLACE       =0x4000000000000000
TAG         =0x5000000000000000
TAGCLASS    =0x6000000000000000

count =0
def dot():
	global count
	count += 1
	if count %1000==0:
		if count %1000000==0:
			sys.stderr.write(str(count / 1000000))
		elif count %100000==0:
			sys.stderr.write("O")
		elif count %10000==0:
			sys.stderr.write("o")
		else:
			sys.stderr.write(".")
		sys.stderr.flush()

def get_csvreader(fn):
	f=open(fn, "r")
	csvreader = csv.reader(f, delimiter="|")
	# Skip header
	next(csvreader)
	return csvreader

def load_nodes(fn, prefix, fields, node_type=""):
	csvreader=get_csvreader(fn)
	if len(node_type)==0:
		node_type=prefix
	for row in csvreader:
		dot()
		# Add a node type edge
		sub = prefix | int(row[0])
		output_file.write('<0x%x> <nodeType> "%s" .\n' % (sub, node_type))
		for index, f in enumerate(fields):
			predicate=f
			# Dgraph cannot accept datetime with ":NNNN", remove it
			if f in ["creationDate", "joinDate"]:
				obj = '"%s"' % row[index+1][:-5]
			else:
				obj='"%s"' % row[index+1]
			output_file.write('<0x%x> <%s> %s .\n' % (sub, predicate, obj))

def load_edges(fn, sub_prefix, predicate, obj_prefix, fields=[]):
	csvreader=get_csvreader(fn)
	for row in csvreader:
		dot()
		sub = sub_prefix | int(row[0])
		obj = obj_prefix | int(row[1])
		if len(fields)==0:
			output_file.write('<0x%x> <%s> <0x%x> .\n' % (sub, predicate, obj))
		else:
			facets=[]
			for index, f in enumerate(fields):
				facets.append('%s="%s"' % (f, row[index+2]))
			output_file.write('<0x%x> <%s> <0x%x> (%s) .\n' % (sub, predicate, obj, ",".join(facets)))

load_nodes(os.path.join(input_dir, "tag_0_0.csv"), TAG, ["name", "url"])
load_nodes(os.path.join(input_dir, "tagclass_0_0.csv"), TAGCLASS, ["name", "url"])
load_nodes(os.path.join(input_dir, "place_0_0.csv"), PLACE, ["name", "url", "type"])
load_nodes(os.path.join(input_dir, "person_0_0.csv"), PERSON, ["firstName", "lastName", "gender", "birthday", "creationDate", "locationIP", "browserUsed"])
load_nodes(os.path.join(input_dir, "person_email_emailaddress_0_0.csv"), PERSON, ["email"])
load_nodes(os.path.join(input_dir, "person_speaks_language_0_0.csv"), PERSON, ["language"])
load_nodes(os.path.join(input_dir, "forum_0_0.csv"), FORUM, ["title", "creationDate"])
load_nodes(os.path.join(input_dir, "organisation_0_0.csv"), ORGANIZATION, ["type", "name", "url"])
load_nodes(os.path.join(input_dir, "post_0_0.csv"), MESSAGE, ["imageFile", "creationDate", "locationIP", "browserUsed", "language", "content", "length"], node_type="post")
load_nodes(os.path.join(input_dir, "comment_0_0.csv"), MESSAGE, ["creationDate", "locationIP", "browserUsed", "content", "length"], node_type="comment")

load_edges(os.path.join(input_dir, "comment_hasCreator_person_0_0.csv"), MESSAGE, "hasCreator", PERSON)
load_edges(os.path.join(input_dir, "comment_hasTag_tag_0_0.csv"), MESSAGE, "hasTag", TAG)
load_edges(os.path.join(input_dir, "comment_isLocatedIn_place_0_0.csv"), MESSAGE, "isLocatedIn", PLACE)
load_edges(os.path.join(input_dir, "comment_replyOf_comment_0_0.csv"), MESSAGE, "replyOf", MESSAGE)
load_edges(os.path.join(input_dir, "comment_replyOf_post_0_0.csv"), MESSAGE, "replyOf", MESSAGE)
load_edges(os.path.join(input_dir, "forum_containerOf_post_0_0.csv"), FORUM, "containerOf", MESSAGE)
load_edges(os.path.join(input_dir, "forum_hasMember_person_0_0.csv"), FORUM, "hasMember", PERSON, ["joinDate"])
load_edges(os.path.join(input_dir, "forum_hasModerator_person_0_0.csv"), FORUM, "hasModerator", PERSON)
load_edges(os.path.join(input_dir, "forum_hasTag_tag_0_0.csv"), FORUM, "hasTag", TAG)
load_edges(os.path.join(input_dir, "organisation_isLocatedIn_place_0_0.csv"), ORGANIZATION, "isLocatedIn", PLACE)
load_edges(os.path.join(input_dir, "person_hasInterest_tag_0_0.csv"), PERSON, "hasInterest", TAG)
load_edges(os.path.join(input_dir, "person_isLocatedIn_place_0_0.csv"), PERSON, "isLocatedIn", PLACE)
load_edges(os.path.join(input_dir, "person_knows_person_0_0.csv"), PERSON, "knows", PERSON, ["creationDate"])
load_edges(os.path.join(input_dir, "person_likes_comment_0_0.csv"), PERSON, "likes", MESSAGE, ["creationDate"])
load_edges(os.path.join(input_dir, "person_likes_post_0_0.csv"), PERSON, "likes", MESSAGE, ["creationDate"])
load_edges(os.path.join(input_dir, "person_studyAt_organisation_0_0.csv"), PERSON, "studyAt", ORGANIZATION, ["classYear"])
load_edges(os.path.join(input_dir, "person_workAt_organisation_0_0.csv"), PERSON, "workAt", ORGANIZATION, ["workFrom"])
load_edges(os.path.join(input_dir, "place_isPartOf_place_0_0.csv"), PLACE, "isPartOf", PLACE)
load_edges(os.path.join(input_dir, "post_hasCreator_person_0_0.csv"), MESSAGE, "hasCreator", PERSON)
load_edges(os.path.join(input_dir, "post_hasTag_tag_0_0.csv"), MESSAGE, "hasTag", TAG)
load_edges(os.path.join(input_dir, "post_isLocatedIn_place_0_0.csv"), MESSAGE, "isLocatedIn", PLACE)
load_edges(os.path.join(input_dir, "tag_hasType_tagclass_0_0.csv"), TAG, "hasType", TAGCLASS)
load_edges(os.path.join(input_dir, "tagclass_isSubclassOf_tagclass_0_0.csv"), TAGCLASS, "isSubclassOf", TAGCLASS)












