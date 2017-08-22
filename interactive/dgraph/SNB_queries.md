#Query 1:
Given a start Person, find up to 20 Persons with a given first name that the start Person is connected to (excluding start Person) by at most 3 steps via Knows relationships. Return Persons, including summaries of the Persons workplaces and places of study. Sort results ascending by their distance from the start Person, for Persons within the same distance sort ascending by their last name, and for Persons with same last name ascending by their identifier.

#Query 2:
Given a start Person, find (most recent) Posts and Comments from all of that Person’s friends, that were created before (and including) a given date. Return the top 20 Posts/Comments, and the Person that created each of them. Sort results descending by creation date, and then ascending by Post identifier.

#Query 3:
Given a start Person, find Persons that are their friends and friends of friends (excluding start Person) that have made Posts/Comments in the given Countries X and Y within a given period. Only Persons that are foreign to Countries X and Y are considered, that is Persons whose Location is not Country X or Country Y. Return top 20 Persons, and their Post/Comment counts, in the given countries and period. Sort results descending by total number of Posts/Comments, and then ascending by Person identifier.

#Query 4:
Given a start Person, find Tags that are attached to Posts that were created by that Person’s friends. Only include Tags that were attached to Posts created within a given time interval, and that were never attached to Posts created before this interval. Return top 10 Tags, and the count of Posts, which were created within the given time interval, that this Tag was attached to. Sort results descending by Post count, and then ascending by Tag name.

#Query 5:
Given a start Person, find the Forums which that Person's friends and friends of friends (excluding start Person) became Members of after a given date. Return top 20 Forums, and the number of Posts in each Forum that was Created by any of these Persons. For each Forum consider only those Persons which joined that particular Forum after the given date. Sort results descending by the count of Posts, and then ascending by Forum identifier

#Query 6:
Given a start Person and some Tag, find the other Tags that occur together with this Tag on  Posts that were created by start Person’s friends and friends of friends (excluding start Person). Return top 10 Tags, and the count of Posts that were created by these Persons, which contain both this Tag and the given Tag. Sort results descending by count, and then ascending by Tag name.

#Query 7:
Given a start Person and some Tag, find the other Tags that occur together with this Tag on Posts that were created by start Person’s friends and friends of friends (excluding start Person). Return top 10 Tags, and the count of Posts that were created by these Persons, which contain both this Tag and the given Tag. Sort results descending by count, and then ascending by Tag name.

#Query 8:
Given a start Person, find the (most recent) Posts/Comments created by that Person’s friends or friends of friends (excluding start Person). Only consider the Posts/Comments created before a given date (excluding that date). Return the top 20 Posts/Comments, and the Person that created each of those Posts/Comments. Sort results descending by creation date of Post/Comment, and then ascending by Post/Comment identifier.

#Query 9:
Given a start Person, find (most recent) Comments that are replies to Posts/Comments of the start Person. Only consider immediate (1-hop) replies, not the transitive (multi-hop) case. Return the top 20 reply Comments, and the Person that created each reply Comment. Sort results descending by creation date of reply Comment, and then ascending by identifier of reply Comment.

#Query 10:
Given a start Person, find that Person’s friends of friends (excluding start Person, and immediate friends), who were born on or after the 21st of a given month (in any year) and before the 22nd of the following month. Calculate the similarity between each of these Persons and start Person, where similarity for any Person is defined as follows:
– common = number of Posts created by that Person, such that the Post has a Tag that start Person is Interested in
– uncommon = number of Posts created by that Person, such that the Post has no Tag that start Person is Interested in
– similarity = common - uncommon Return top 10 Persons, their Place, and their similarity score. Sort results descending by similarity score, and then ascending by Person identifier

#Query 11:
Given a start Person, find that Person’s friends and friends of friends (excluding start Person) who started Working in some Company in a given Country, before a given date (year). Return top 10 Persons, the Company they worked at, and the year they started working at that Company. Sort results ascending by the start date, then ascending by Person identifier, and lastly by Organization name descending.

#Query 12:
Given a start Person, find the Comments that this Person’s friends made in reply to Posts, considering only those Comments that are immediate (1-hop) replies to Posts, not the transitive (multi-hop) case. Only consider Posts with a Tag in a given TagClass or in a descendent of that TagClass. Count the number of these reply Comments, and collect the Tags that were attached to the Posts they replied to. Return top 20 Persons, the reply count, and the collection of Tags. Sort results descending by Comment count, and then ascending by Person identifier

#Query 13:
Given two Persons, find the shortest path between these two Persons in the subgraph induced by the Knows relationships. Return the length of this path.
– -1 : no path found
– 0: start person = end person
– > 0: regular case

#Query 14:
Given two Persons, find all (unweighted) shortest paths between these two Persons, in the subgraph induced by the Knows relationship. Then, for each path calculate a weight. The nodes in the path are Persons, and the weight of a path is the sum of weights between every pair of consecutive Person nodes in the path. The weight for a pair of Persons is calculated such that every reply (by one of the Persons) to a Post (by the other Person) contributes 1.0, and every reply (by ones of the Persons) to a Comment (by the other Person) contributes 0.5. Return all the paths with shortest length, and their weights. Sort results descending by path weight. The order of paths with the same weight is unspecified.

#ShortQuery 1:
A person's properties

#ShortQuery 2:
Given a start Person, retrieve the last 10 Messages (Posts or Comments) created by that person. For each message, return that message, the original post in its conversation, and the author of that post. Order results descending by message creation date, then descending by message identifier

#ShortQuery 3:
Given a start Person, retrieve all of their friends, and the date at which they became friends. Order results descending by friendship creation date, then ascending by friend identifier

#ShortQuery 4:
Given a Message (Post or Comment), retrieve its content and creation date.

#ShortQuery 5:
Given a Message (Post or Comment), retrieve its author.

#ShortQuery 6:
Given a Message (Post or Comment), retrieve the Forum that contains it and the Person that moderates that forum.

#ShortQuery 7:
Given a Message (Post or Comment), retrieve the (1-hop) Comments that reply to it. In addition, return a boolean flag indicating if the author of the reply knows the author of the original message. If author is same as original author, return false for "knows" flag. Order results descending by comment identifier, then descending by author identifier.

#Update 1:
Add a person. Currently assuming all edge end point vertices exist

#Update 2:
Adds like, assumes person and post exist.

#Update 3:
Adds like from person to comment, assumes person and comment exist

#Update 4:
Adds forum and it's moderator and tags. Assume moderator person and tags exist

#Update 5:
Adds membership edge between forum and person. Assumes both exist.

#Update 6:
Add post vertex, creator edge, container edge, isLocatedIn edge and tag edges. All other vertices are assumed to exist.

#Update 7:
Add comment

#Update 8:
Add knows edge between two given people assumed to exist

