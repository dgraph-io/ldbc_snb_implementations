package com.ldbc.driver.workloads.ldbc.snb.interactive.db


import com.google.gson.JsonArray
import com.google.gson.JsonElement
import com.google.gson.JsonObject
import com.ldbc.driver.*
import com.ldbc.driver.control.LoggingService
import com.ldbc.driver.workloads.ldbc.snb.interactive.*
import io.dgraph.client.DgraphClient
import io.dgraph.client.GrpcDgraphClient
import java.io.BufferedReader
import java.io.File
import java.io.FileReader
import java.io.IOException
import java.sql.CallableStatement
import java.sql.SQLException
import java.sql.Statement
import java.text.SimpleDateFormat
import java.util.*
import kotlin.collections.HashMap

// Extensions
operator fun JsonElement.get(property: String): JsonElement = this.asJsonObject[property]

operator fun JsonElement.get(index: Int): JsonElement = this.asJsonArray[index]

val JsonElement.asDateLong: Long
    get() = DgraphDb.dateToLong(this)

val JsonElement.uid: Long
    get() = this.asJsonObject["_uid_"].asLong

val JsonElement.xid: Long
    get() = DgraphDb.getXid(this.uid)

class DgraphDb : Db() {

    private var connectionState: DgraphDbConnectionState? = null

    @Throws(DbException::class)
    override fun onInit(properties: Map<String, String>, loggingService: LoggingService) {
        try {
            connectionState = DgraphDbConnectionState(properties)
        } catch (e: ClassNotFoundException) {
            e.printStackTrace()
        } catch (e: SQLException) {
            e.printStackTrace()
        }

        registerOperationHandler(LdbcQuery1::class.java, LdbcQuery1ToDgraph::class.java)
        registerOperationHandler(LdbcQuery2::class.java, LdbcQuery2ToDgraph::class.java)
        registerOperationHandler(LdbcQuery3::class.java, LdbcQuery3ToDgraph::class.java)
        registerOperationHandler(LdbcQuery4::class.java, LdbcQuery4ToDgraph::class.java)
        registerOperationHandler(LdbcQuery5::class.java, LdbcQuery5ToDgraph::class.java)
        registerOperationHandler(LdbcQuery6::class.java, LdbcQuery6ToDgraph::class.java)
        registerOperationHandler(LdbcQuery7::class.java, LdbcQuery7ToDgraph::class.java)
        registerOperationHandler(LdbcQuery8::class.java, LdbcQuery8ToDgraph::class.java)
        registerOperationHandler(LdbcQuery9::class.java, LdbcQuery9ToDgraph::class.java)
        registerOperationHandler(LdbcQuery10::class.java, LdbcQuery10ToDgraph::class.java)
        registerOperationHandler(LdbcQuery11::class.java, LdbcQuery11ToDgraph::class.java)
        registerOperationHandler(LdbcQuery12::class.java, LdbcQuery12ToDgraph::class.java)
        registerOperationHandler(LdbcQuery13::class.java, LdbcQuery13ToDgraph::class.java)
        registerOperationHandler(LdbcQuery14::class.java, LdbcQuery14ToDgraph::class.java)

        registerOperationHandler(LdbcShortQuery1PersonProfile::class.java, LdbcShortQuery1ToDgraph::class.java)
        registerOperationHandler(LdbcShortQuery2PersonPosts::class.java, LdbcShortQuery2ToDgraph::class.java)
        registerOperationHandler(LdbcShortQuery3PersonFriends::class.java, LdbcShortQuery3ToDgraph::class.java)
        registerOperationHandler(LdbcShortQuery4MessageContent::class.java, LdbcShortQuery4ToDgraph::class.java)
        registerOperationHandler(LdbcShortQuery5MessageCreator::class.java, LdbcShortQuery5ToDgraph::class.java)
        registerOperationHandler(LdbcShortQuery6MessageForum::class.java, LdbcShortQuery6ToDgraph::class.java)
        registerOperationHandler(LdbcShortQuery7MessageReplies::class.java, LdbcShortQuery7ToDgraph::class.java)

        registerOperationHandler(LdbcUpdate1AddPerson::class.java, LdbcUpdate1AddPersonToDgraph::class.java)
        registerOperationHandler(LdbcUpdate2AddPostLike::class.java, LdbcUpdate2AddPostLikeToDgraph::class.java)
        registerOperationHandler(LdbcUpdate3AddCommentLike::class.java, LdbcUpdate3AddCommentLikeToDgraph::class.java)
        registerOperationHandler(LdbcUpdate4AddForum::class.java, LdbcUpdate4AddForumToDgraph::class.java)
        registerOperationHandler(LdbcUpdate5AddForumMembership::class.java, LdbcUpdate5AddForumMembershipToDgraph::class.java)
        registerOperationHandler(LdbcUpdate6AddPost::class.java, LdbcUpdate6AddPostToDgraph::class.java)
        registerOperationHandler(LdbcUpdate7AddComment::class.java, LdbcUpdate7AddCommentToDgraph::class.java)
        registerOperationHandler(LdbcUpdate8AddFriendship::class.java, LdbcUpdate8AddFriendshipToDgraph::class.java)
    }

    override fun onClose() {
        println("ON CLOSE()")
        try {
            connectionState!!.conn.close()
        } catch (e: Exception) {
            e.printStackTrace()
        }

    }

    @Throws(DbException::class)
    override fun getConnectionState(): DbConnectionState? {
        return connectionState
    }

    inner class DgraphDbConnectionState @Throws(ClassNotFoundException::class, SQLException::class)
    internal constructor(properties: Map<String, String>) : DbConnectionState() {
        val conn: DgraphClient = GrpcDgraphClient.newInstance(properties["host"], Integer.parseInt(properties["port"]))

        val queryDir: String = properties["queryDir"]!!

        val isPrintNames: Boolean = properties["printQueryNames"] == "true"
        val isPrintStrings: Boolean = properties["printQueryStrings"] == "true"
        val isPrintResults: Boolean = properties["printQueryResults"] == "true"
        private val placeMap: HashMap<Long, String> = HashMap()
        private val companyMap: HashMap<Long, String> = HashMap()
        private val universityMap: HashMap<Long, String> = HashMap()
        private val tagMap: HashMap<Long, String> = HashMap()

        // Node types
        private val typeMap: HashMap<String, Long> = HashMap()

        // xid -> uid map
        private val placeIdMap: HashMap<Long, Long> = HashMap()
        private val companyIdMap: HashMap<Long, Long> = HashMap()
        private val universityIdMap: HashMap<Long, Long> = HashMap()
        private val tagIdMap: HashMap<Long, Long> = HashMap()
        private val personIdMap: HashMap<Long, Long> = HashMap()

        init {
            // TODO: Fill up maps
            conn.query("""

                """)
        }

        @Throws(IOException::class)
        override fun close() {

        }

        fun placeUri(id: Long): String {
            return (placeMap[id])!!
        }

        fun companyUri(id: Long): String {
            return companyMap[id]!!
        }

        fun universityUri(id: Long): String {
            return universityMap[id]!!
        }

        fun tagUri(id: Long): String {
            return tagMap[id]!!
        }
    }

    /**
     * Given a start Person, find up to 20 Persons with a given first name that the start Person is connected to
     * (excluding start Person) by at most 3 steps via Knows relationships. Return Persons, including summaries of
     * the Persons workplaces and places of study. Sort results ascending by their distance from the start Person,
     * for Persons within the same distance sort ascending by their last name, and for Persons with same last name
     * ascending by their identifier.
     */
    class LdbcQuery1ToDgraph : OperationHandler<LdbcQuery1, DgraphDbConnectionState> {

        data class University(val name: String)
        data class Company(val name: String)

        @Throws(DbException::class)
        override fun executeOperation(operation: LdbcQuery1, state: DgraphDbConnectionState, resultReporter: ResultReporter) {
            val conn = state.conn
            val RESULT = ArrayList<LdbcQuery1Result>()
            var results_count = 0
            RESULT.clear()
            try {
                var queryString = file2string(File(state.queryDir, "q1.txt"))
                        .replace("@Person@", getPersonId(operation.personId()).toString())
                        .replace("@Name@", operation.firstName())

                if (state.isPrintNames)
                    println("########### LdbcQuery1")
                if (state.isPrintStrings)
                    println(queryString)

                val r0 = conn.query(queryString)
                val r1 = r0.toJsonObject()
                val result = r1["q"].asJsonArray
                result.map { it.asJsonObject }.forEach {
                    results_count++
                    RESULT.add(LdbcQuery1Result(
                            getXid(it["_uid_"].asLong),
                            it["lastName"].asString,
                            1, // TODO: How to get distance?
                            dateToLong(it["birthday"].asString),
                            if (it.has("~knows")) it["~knows"].asJsonObject["creationDate"].asLong else 0L,
                            it["gender"].asString,
                            it["browserUsed"].asString,
                            it["locationIP"].asString,
                            if (it.has("email")) arrayOf(it["email"].asString).toList() else arrayListOf(),
                            if (it.has("language")) arrayOf(it["language"].asString).toList() else arrayListOf(),
                            it["isLocatedIn"].asJsonArray[0].asJsonObject["name"].asString,
                            arrayListOf(),
                            arrayListOf()
                    ))
                }

            } catch (e: Exception) {
                e.printStackTrace()
            }

            resultReporter.report(results_count, RESULT, operation)
        }
    }

    /**
     * Given a start Person, find (most recent) Posts and Comments from all of that Person’s friends, that were created
     * before (and including) a given date. Return the top 20 Posts/Comments, and the Person that created each of them.
     * Sort results descending by creation date, and then ascending by Post identifier.
     */
    class LdbcQuery2ToDgraph : OperationHandler<LdbcQuery2, DgraphDbConnectionState> {

        @Throws(DbException::class)
        override fun executeOperation(operation: LdbcQuery2, state: DgraphDbConnectionState, resultReporter: ResultReporter) {
            val conn = state.conn
            val RESULT = ArrayList<LdbcQuery2Result>()
            var results_count = 0
            RESULT.clear()
            try {
                val sdf = SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSS'+00:00'")
                sdf.timeZone = TimeZone.getTimeZone("GMT")
                val queryString = file2string(File(state.queryDir, "q2.txt"))
                        .replace("@Person@", getPersonId(operation.personId()).toString())
                        .replace("@Date@", sdf.format(operation.maxDate()))

                if (state.isPrintNames)
                    println("########### LdbcQuery2")
                if (state.isPrintStrings)
                    println(queryString)

                val result = conn.query(queryString).toJsonObject()["q"].asJsonArray
                result.map { it.asJsonObject }.forEach {
                    results_count++
                    val person = it["hasCreator"][0].asJsonObject
                    RESULT.add(LdbcQuery2Result(
                            person.xid,
                            person["firstName"].asString,
                            person["lastName"].asString,
                            it.xid,
                            it["content"].asString,
                            it["creationDate"].asDateLong
                    ))
                }
                // conn.close()
            } catch (e: Exception) {
                e.printStackTrace()

            }

            resultReporter.report(results_count, RESULT, operation)
        }
    }

    /**
     * Given a start Person, find Persons that are their friends and friends of friends (excluding start Person) that
     * have made Posts/Comments in the given Countries X and Y within a given period. Only Persons that are foreign to
     * Countries X and Y are considered, that is Persons whose Location is not Country X or Country Y. Return top 20
     * Persons, and their Post/Comment counts, in the given countries and period. Sort results descending by total
     * number of Posts/Comments, and then ascending by Person identifier.
     */
    class LdbcQuery3ToDgraph : OperationHandler<LdbcQuery3, DgraphDbConnectionState> {

        @Throws(DbException::class)
        override fun executeOperation(operation: LdbcQuery3, state: DgraphDbConnectionState, resultReporter: ResultReporter) {
            val conn = state.conn
            val RESULT = ArrayList<LdbcQuery3Result>()
            val results_count = 0
            RESULT.clear()
            try {
                var queryString = file2string(File(state.queryDir, "query3.txt"))
                val sdf = SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSS'+00:00'")
                sdf.timeZone = TimeZone.getTimeZone("GMT")
                queryString = queryString.replace("@Person@".toRegex(), operation.personId().toString())
                queryString = queryString.replace("@Country1@".toRegex(), operation.countryXName())
                queryString = queryString.replace("@Country2@".toRegex(), operation.countryYName())
                queryString = queryString.replace("@Date1@".toRegex(), sdf.format(operation.startDate()))
                queryString = queryString.replace("@Duration@".toRegex(), operation.durationDays().toString())

                if (state.isPrintNames)
                    println("########### LdbcQuery3")
                if (state.isPrintStrings)
                    println(queryString)

                val result = conn.query(queryString).toJsonObject()
                // TODO:
                //                for(JsonElement v: result.getAsJsonArray()) {
                //                    String key=(String)keys.next();
                //                    results_count++;
                //                    v...
                //                    LdbcQuery3Result tmp = new LdbcQuery3Result(id, firstName, lastName, ct1, ct2, total);
                //                    if (state.isPrintResults())
                //                        System.out.println(tmp.toString());
                //                    RESULT.add(tmp);
                //                }
                // conn.close()
            } catch (e: Exception) {
                e.printStackTrace()

            }

            resultReporter.report(results_count, RESULT, operation)
        }
    }


    /**
     * Given a start Person, find Tags that are attached to Posts that were created by that Person’s friends.
     * Only include Tags that were attached to Posts created within a given time interval, and that were never attached
     * to Posts created before this interval. Return top 10 Tags, and the count of Posts, which were created within the
     * given time interval, that this Tag was attached to. Sort results descending by Post count, and then ascending by
     * Tag name.
     */
    class LdbcQuery4ToDgraph : OperationHandler<LdbcQuery4, DgraphDbConnectionState> {

        @Throws(DbException::class)
        override fun executeOperation(operation: LdbcQuery4, state: DgraphDbConnectionState, resultReporter: ResultReporter) {
            val conn = state.conn
            val RESULT = ArrayList<LdbcQuery4Result>()
            var results_count = 0
            RESULT.clear()
            try {
                val sdf = SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSS'+00:00'")
                sdf.timeZone = TimeZone.getTimeZone("GMT")
                val cal = Calendar.getInstance()
                cal.time = operation.startDate()
                cal.add(Calendar.DAY_OF_MONTH, operation.durationDays())
                val queryString = file2string(File(state.queryDir, "q4.txt"))
                        .replace("@Person@", getPersonId(operation.personId()).toString())
                        .replace("@Start@", sdf.format(operation.startDate()))
                        .replace("@End@", sdf.format(cal.time))

                if (state.isPrintNames)
                    println("########### LdbcQuery4")
                if (state.isPrintStrings)
                    println(queryString)

                val result = conn.query(queryString).toJsonObject()["data"].asJsonObject["q"].asJsonArray
                result.map { it.asJsonObject }.forEach {
                    results_count++
                    RESULT.add(LdbcQuery4Result(
                            it["name"].asString,
                            it["val(postCount)"].asInt
                    ))
                }
                // conn.close()
            } catch (e: Exception) {
                e.printStackTrace()

            }

            resultReporter.report(results_count, RESULT, operation)
        }
    }

    /**
     * Given a start Person, find the Forums which that Person's friends and friends of friends (excluding start Person)
     * became Members of after a given date. Return top 20 Forums, and the number of Posts in each Forum that was
     * Created by any of these Persons. For each Forum consider only those Persons which joined that particular Forum
     * after the given date. Sort results descending by the count of Posts, and then ascending by Forum identifier
     */
    class LdbcQuery5ToDgraph : OperationHandler<LdbcQuery5, DgraphDbConnectionState> {


        @Throws(DbException::class)
        override fun executeOperation(operation: LdbcQuery5, state: DgraphDbConnectionState, resultReporter: ResultReporter) {
            val conn = state.conn
            val RESULT = ArrayList<LdbcQuery5Result>()
            val results_count = 0
            RESULT.clear()
            try {
                var queryString = file2string(File(state.queryDir, "query5.txt"))
                val sdf = SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSS'+00:00'")
                sdf.timeZone = TimeZone.getTimeZone("GMT")
                queryString = queryString.replace("@Person@".toRegex(), operation.personId().toString())
                queryString = queryString.replace("@Date0@".toRegex(), sdf.format(operation.minDate()))

                if (state.isPrintNames)
                    println("########### LdbcQuery5")
                if (state.isPrintStrings)
                    println(queryString)

                val result = conn.query(queryString).toJsonObject()
                // TODO:
                //                for(JsonElement v: result.getAsJsonArray()) {
                //                    String key=(String)keys.next();
                //                    results_count++;
                //                    v...
                //                    LdbcQuery5Result tmp = new LdbcQuery5Result(forumTitle, postCount);
                //                    if (state.isPrintResults())
                //                        System.out.println(tmp.toString());
                //                    RESULT.add(tmp);
                //                }
                // conn.close()
            } catch (e: Exception) {
                e.printStackTrace()

            }

            resultReporter.report(results_count, RESULT, operation)
        }
    }


    /**
     * Given a start Person and some Tag, find the other Tags that occur together with this Tag on Posts that were
     * created by start Person’s friends and friends of friends (excluding start Person).
     * Return top 10 Tags, and the count of Posts that were created by these Persons, which contain both this Tag and
     * the given Tag. Sort results descending by count, and then ascending by Tag name.
     */
    class LdbcQuery6ToDgraph : OperationHandler<LdbcQuery6, DgraphDbConnectionState> {


        @Throws(DbException::class)
        override fun executeOperation(operation: LdbcQuery6, state: DgraphDbConnectionState, resultReporter: ResultReporter) {
            val conn = state.conn
            val RESULT = ArrayList<LdbcQuery6Result>()
            val results_count = 0
            RESULT.clear()
            try {
                var queryString = file2string(File(state.queryDir, "query6.txt"))
                queryString = queryString.replace("@Person@".toRegex(), operation.personId().toString())
                queryString = queryString.replace("@Tag@".toRegex(), operation.tagName())

                if (state.isPrintNames)
                    println("########### LdbcQuery6")
                if (state.isPrintStrings)
                    println(queryString)

                val result = conn.query(queryString).toJsonObject()
                // TODO:
                //                for(JsonElement v: result.getAsJsonArray()) {
                //                    String key=(String)keys.next();
                //                    results_count++;
                //                    v...
                //                    LdbcQuery6Result tmp = new LdbcQuery6Result(tagName, tagCount);
                //                    if (state.isPrintResults())
                //                        System.out.println(tmp.toString());
                //                    RESULT.add(tmp);
                //                }
                // conn.close()
            } catch (e: Exception) {
                e.printStackTrace()

            }

            resultReporter.report(results_count, RESULT, operation)
        }
    }


    /**
     * Given a start Person and some Tag, find the other Tags that occur together with this Tag on Posts that were
     * created by start Person’s friends and friends of friends (excluding start Person). Return top 10 Tags, and the
     * count of Posts that were created by these Persons, which contain both this Tag and the given Tag.
     * Sort results descending by count, and then ascending by Tag name.
     */
    class LdbcQuery7ToDgraph : OperationHandler<LdbcQuery7, DgraphDbConnectionState> {

        @Throws(DbException::class)
        override fun executeOperation(operation: LdbcQuery7, state: DgraphDbConnectionState, resultReporter: ResultReporter) {
            val conn = state.conn
            val RESULT = ArrayList<LdbcQuery7Result>()
            val results_count = 0
            RESULT.clear()
            try {
                var queryString = file2string(File(state.queryDir, "query7.txt"))
                queryString = queryString.replace("@Person@".toRegex(), operation.personId().toString())

                if (state.isPrintNames)
                    println("########### LdbcQuery7")
                if (state.isPrintStrings)
                    println(queryString)

                val result = conn.query(queryString).toJsonObject()
                // TODO:
                //                for(JsonElement v: result.getAsJsonArray()) {
                //                    String key=(String)keys.next();
                //                    results_count++;
                //                    v...
                //                    LdbcQuery7Result tmp = new LdbcQuery7Result(personId, personFirstName, personLastName, likeCreationDate, postId, postContent, milliSecondDelay, isNew);
                //                    if (state.isPrintResults())
                //                        System.out.println(tmp.toString());
                //                    RESULT.add(tmp);
                //                }
                // conn.close()
            } catch (e: Exception) {
                e.printStackTrace()

            }

            resultReporter.report(results_count, RESULT, operation)
        }
    }


    /**
     * Given a start Person, find (most recent) Comments that are replies to Posts/Comments of the start Person.
     * Only consider immediate (1-hop) replies, not the transitive (multi-hop) case. Return the top 20 reply Comments,
     * and the Person that created each reply Comment. Sort results descending by creation date of reply Comment, and
     * then ascending by identifier of reply Comment.
     */
    class LdbcQuery8ToDgraph : OperationHandler<LdbcQuery8, DgraphDbConnectionState> {


        @Throws(DbException::class)
        override fun executeOperation(operation: LdbcQuery8, state: DgraphDbConnectionState, resultReporter: ResultReporter) {
            val conn = state.conn
            val RESULT = ArrayList<LdbcQuery8Result>()
            var results_count = 0
            RESULT.clear()
            try {
                val queryString = file2string(File(state.queryDir, "q8.txt"))
                        .replace("@Person@", getPersonId(operation.personId()).toString())

                if (state.isPrintNames)
                    println("########### LdbcQuery8")
                if (state.isPrintStrings)
                    println(queryString)

                val result = conn.query(queryString).toJsonObject()["data"].asJsonObject["q"].asJsonArray
                result.map { it.asJsonObject }.forEach {
                    results_count++
                    val person = it["hasCreator"].asJsonArray[0].asJsonObject
                    RESULT.add(LdbcQuery8Result(
                            getXid(person["_uid_"].asLong),
                            person["firstName"].asString,
                            person["lastName"].asString,
                            it["creationDate"].asLong,
                            getXid(it["_uid_"].asLong),
                            it["content"].asString
                    ))

                }
                // conn.close()
            } catch (e: Exception) {
                e.printStackTrace()

            }

            resultReporter.report(results_count, RESULT, operation)
        }
    }

    /**
     * Given a start Person, find the (most recent) Posts/Comments created by that Person’s friends or friends of
     * friends (excluding start Person). Only consider the Posts/Comments created before a given date (excluding that date).
     * Return the top 20 Posts/Comments, and the Person that created each of those Posts/Comments.
     * Sort results descending by creation date of Post/Comment, and then ascending by Post/Comment identifier.
     */
    class LdbcQuery9ToDgraph : OperationHandler<LdbcQuery9, DgraphDbConnectionState> {


        @Throws(DbException::class)
        override fun executeOperation(operation: LdbcQuery9, state: DgraphDbConnectionState, resultReporter: ResultReporter) {
            val conn = state.conn
            val RESULT = ArrayList<LdbcQuery9Result>()
            var results_count = 0
            RESULT.clear()
            try {
                var queryString = file2string(File(state.queryDir, "q9.txt"))
                val sdf = SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSS'+00:00'")
                sdf.timeZone = TimeZone.getTimeZone("GMT")
                queryString = queryString.replace("@Person@", operation.personId().toString())
                queryString = queryString.replace("@Date0@", sdf.format(operation.maxDate()))

                if (state.isPrintNames)
                    println("########### LdbcQuery9")
                if (state.isPrintStrings)
                    println(queryString)

                val result = conn.query(queryString).toJsonObject()["data"].asJsonObject["q"].asJsonArray
                result.map { it.asJsonObject }.forEach {
                    results_count++
                    val person = it["hasCreator"].asJsonArray[0].asJsonObject
                    RESULT.add(LdbcQuery9Result(
                            getXid(person["_uid_"].asLong),
                            person["firstName"].asString,
                            person["lastName"].asString,
                            getXid(it["_uid_"].asLong),
                            it["content"].asString,
                            it["creationDate"].asLong
                    ))

                }
            } catch (e: Exception) {
                e.printStackTrace()

            }

            resultReporter.report(results_count, RESULT, operation)
        }
    }


    /**
     * Given a start Person, find that Person’s friends of friends (excluding start Person, and immediate friends),
     * who were born on or after the 21st of a given month (in any year) and before the 22nd of the following month.
     * Calculate the similarity between each of these Persons and start Person, where similarity for any Person is
     * defined as follows:
     * – common = number of Posts created by that Person, such that the Post has a Tag that start Person is Interested in
     * – uncommon = number of Posts created by that Person, such that the Post has no Tag that start Person is Interested in
     * – similarity = common - uncommon Return top 10 Persons, their Place, and their similarity score.
     * Sort results descending by similarity score, and then ascending by Person identifier
     */
    class LdbcQuery10ToDgraph : OperationHandler<LdbcQuery10, DgraphDbConnectionState> {

        @Throws(DbException::class)
        override fun executeOperation(operation: LdbcQuery10, state: DgraphDbConnectionState, resultReporter: ResultReporter) {
            val conn = state.conn
            val RESULT = ArrayList<LdbcQuery10Result>()
            val results_count = 0
            RESULT.clear()
            try {
                var queryString = file2string(File(state.queryDir, "q10.txt"))
                queryString = queryString.replace("@Person@".toRegex(), operation.personId().toString())
                queryString = queryString.replace("@HS0@".toRegex(), operation.month().toString())
                var nextMonth = operation.month() + 1
                if (nextMonth == 13)
                    nextMonth = 1
                queryString = queryString.replace("@HS1@".toRegex(), nextMonth.toString())

                if (state.isPrintNames)
                    println("########### LdbcQuery10")
                if (state.isPrintStrings)
                    println(queryString)

                val result = conn.query(queryString).toJsonObject()
                // TODO:
                //                for(JsonElement v: result.getAsJsonArray()) {
                //                    String key=(String)keys.next();
                //                    results_count++;
                //                    v...
                //                    LdbcQuery10Result tmp = new LdbcQuery10Result(personId, personFirstName, personLastName, commonInterestScore, gender, personCityName);
                //                    if (state.isPrintResults())
                //                        System.out.println(tmp.toString());
                //                    RESULT.add(tmp);
                //                }
                // conn.close()
            } catch (e: Exception) {
                e.printStackTrace()

            }

            resultReporter.report(results_count, RESULT, operation)
        }
    }


    /**
     * Given a start Person, find that Person’s friends and friends of friends (excluding start Person) who started
     * Working in some Company in a given Country, before a given date (year). Return top 10 Persons, the Company they
     * worked at, and the year they started working at that Company. Sort results ascending by the start date, then
     * ascending by Person identifier, and lastly by Organization name descending.
     */
    class LdbcQuery11ToDgraph : OperationHandler<LdbcQuery11, DgraphDbConnectionState> {

        @Throws(DbException::class)
        override fun executeOperation(operation: LdbcQuery11, state: DgraphDbConnectionState, resultReporter: ResultReporter) {
            val conn = state.conn
            val RESULT = ArrayList<LdbcQuery11Result>()
            val results_count = 0
            RESULT.clear()
            try {
                var queryString = file2string(File(state.queryDir, "q11.txt"))
                queryString = queryString.replace("@Person@".toRegex(), operation.personId().toString())
                queryString = queryString.replace("@Date0@".toRegex(), operation.workFromYear().toString())
                queryString = queryString.replace("@Country@".toRegex(), operation.countryName())

                if (state.isPrintNames)
                    println("########### LdbcQuery11")
                if (state.isPrintStrings)
                    println(queryString)

                val result = conn.query(queryString).toJsonObject()
                // TODO:
                //                for(JsonElement v: result.getAsJsonArray()) {
                //                    String key=(String)keys.next();
                //                    results_count++;
                //                    v...
                //                    LdbcQuery11Result tmp = new LdbcQuery11Result(personId, personFirstName, personLastName, organizationName, organizationWorkFromYear);
                //                    if (state.isPrintResults())
                //                        System.out.println(tmp.toString());
                //                    RESULT.add(tmp);
                //                }
                // conn.close()
            } catch (e: Exception) {
                e.printStackTrace()

            }

            resultReporter.report(results_count, RESULT, operation)
        }
    }

    /**
     * Given a start Person, find the Comments that this Person’s friends made in reply to Posts, considering only those
     * Comments that are immediate (1-hop) replies to Posts, not the transitive (multi-hop) case. Only consider Posts
     * with a Tag in a given TagClass or in a descendent of that TagClass. Count the number of these reply Comments, and
     * collect the Tags that were attached to the Posts they replied to. Return top 20 Persons, the reply count, and the
     * collection of Tags.
     * Sort results descending by Comment count, and then ascending by Person identifier
     */
    class LdbcQuery12ToDgraph : OperationHandler<LdbcQuery12, DgraphDbConnectionState> {

        @Throws(DbException::class)
        override fun executeOperation(operation: LdbcQuery12, state: DgraphDbConnectionState, resultReporter: ResultReporter) {
            val conn = state.conn
            val RESULT = ArrayList<LdbcQuery12Result>()
            val results_count = 0
            RESULT.clear()
            try {
                var queryString = file2string(File(state.queryDir, "q12.txt"))
                queryString = queryString.replace("@Person@".toRegex(), operation.personId().toString())
                queryString = queryString.replace("@TagType@".toRegex(), operation.tagClassName())

                if (state.isPrintNames)
                    println("########### LdbcQuery12")
                if (state.isPrintStrings)
                    println(queryString)

                val result = conn.query(queryString).toJsonObject()
                // TODO:
                //                for(JsonElement v: result.getAsJsonArray()) {
                //                    String key=(String)keys.next();
                //                    results_count++;
                //                    v...
                //                    LdbcQuery12Result tmp = new LdbcQuery12Result(personId, personFirstName, personLastName, tagNames, replyCount);
                //                    if (state.isPrintResults())
                //                        System.out.println(tmp.toString());
                //                    RESULT.add(tmp);
                //                }
                // conn.close()
            } catch (e: Exception) {
                e.printStackTrace()

            }

            resultReporter.report(results_count, RESULT, operation)
        }
    }


    /**
     * Given two Persons, find the shortest path between these two Persons in the subgraph induced by the Knows
     * relationships. Return the length of this path.
     * – -1 : no path found
     * – 0: start person = end person
     * – > 0: regular case
     */
    class LdbcQuery13ToDgraph : OperationHandler<LdbcQuery13, DgraphDbConnectionState> {
        fun depth(o: JsonArray): Int {
            var obj = o[0].asJsonObject
            var ret = 0
            while (obj.has("knows")) {
                ret++
                obj = obj["knows"].asJsonObject
            }
            return ret
        }

        @Throws(DbException::class)
        override fun executeOperation(operation: LdbcQuery13, state: DgraphDbConnectionState, resultReporter: ResultReporter) {
            val conn = state.conn
            val RESULT = ArrayList<LdbcQuery13Result>()
            var results_count = 0
            RESULT.clear()
            try {
                var queryString = file2string(File(state.queryDir, "q13.txt"))
                queryString = queryString.replace("@Person1@".toRegex(), operation.person1Id().toString())
                queryString = queryString.replace("@Person2@".toRegex(), operation.person2Id().toString())

                if (state.isPrintNames)
                    println("########### LdbcQuery13")
                if (state.isPrintStrings)
                    println(queryString)

                val result = conn.query(queryString).toJsonObject()["data"].asJsonObject
                if (!result.has("_path_")) {
                    // Path not found
                    RESULT.add(LdbcQuery13Result(-1))
                } else {
                    RESULT.add(LdbcQuery13Result(depth(result["_path_"].asJsonArray)))
                }
                results_count++
            } catch (e: Exception) {
                e.printStackTrace()

            }

            resultReporter.report(results_count, RESULT[0], operation)
        }
    }

    /**
     * Given two Persons, find all (unweighted) shortest paths between these two Persons, in the subgraph induced by the
     * Knows relationship. Then, for each path calculate a weight. The nodes in the path are Persons, and the weight of
     * a path is the sum of weights between every pair of consecutive Person nodes in the path.
     * The weight for a pair of Persons is calculated such that every reply (by one of the Persons) to a Post (by the
     * other Person) contributes 1.0, and every reply (by ones of the Persons) to a Comment (by the other Person)
     * contributes 0.5.
     * Return all the paths with shortest length, and their weights.
     * Sort results descending by path weight.
     * The order of paths with the same weight is unspecified.
     */
    class LdbcQuery14ToDgraph : OperationHandler<LdbcQuery14, DgraphDbConnectionState> {

        @Throws(DbException::class)
        override fun executeOperation(operation: LdbcQuery14, state: DgraphDbConnectionState, resultReporter: ResultReporter) {
            val conn = state.conn
            val RESULT = ArrayList<LdbcQuery14Result>()
            val results_count = 0
            RESULT.clear()
            try {
                var queryString = file2string(File(state.queryDir, "query14.txt"))
                queryString = queryString.replace("@Person1@".toRegex(), operation.person1Id().toString())
                queryString = queryString.replace("@Person2@".toRegex(), operation.person2Id().toString())

                if (state.isPrintNames)
                    println("########### LdbcQuery14")
                if (state.isPrintStrings)
                    println(queryString)

                val result = conn.query(queryString).toJsonObject()
                // TODO:
                //                for(JsonElement v: result.getAsJsonArray()) {
                //                    String key=(String)keys.next();
                //                    results_count++;
                //                    v...
                //                    LdbcQuery14Result tmp = new LdbcQuery14Result(new ArrayList<Long>(Arrays.asList(ttt)), weight);
                //                    if (state.isPrintResults())
                //                        System.out.println(tmp.toString());
                //                    RESULT.add(tmp);
                //                }
                // conn.close()
            } catch (e: Exception) {
                e.printStackTrace()

            }

            resultReporter.report(results_count, RESULT, operation)
        }
    }

    /**
     * A person's properties
     */
    class LdbcShortQuery1ToDgraph : OperationHandler<LdbcShortQuery1PersonProfile, DgraphDbConnectionState> {

        @Throws(DbException::class)
        override fun executeOperation(operation: LdbcShortQuery1PersonProfile, state: DgraphDbConnectionState, resultReporter: ResultReporter) {
            var RESULT: LdbcShortQuery1PersonProfileResult? = null
            var results_count = 0
            val conn = state.conn

            try {
                val queryString = file2string(File(state.queryDir, "s1.txt")).replace("@Person@", getPersonId(operation.personId()).toString())
                if (state.isPrintNames)
                    println("########### LdbcShortQuery1")
                if (state.isPrintStrings)
                    println(queryString)

                val result = conn.query(queryString).toJsonObject()
                result["person"].asJsonArray.map { it.asJsonObject }.forEach {
                    results_count++
                    RESULT = LdbcShortQuery1PersonProfileResult(
                            it["firstName"].asString,
                            it["lastName"].asString,
                            dateToLong(it["birthday"]),
                            it["locationIP"].asString,
                            it["browserUsed"].asString,
                            getXid(it["isLocatedIn"][0]["_uid_"].asLong),
                            it["gender"].asString,
                            dateToLong(it["creationDate"])
                    )
                    if (state.isPrintResults)
                        println(RESULT.toString())
                }
                // conn.close()
            } catch (e: Exception) {
                e.printStackTrace()
            }

            resultReporter.report(results_count, RESULT, operation)
        }
    }

    /**
     * Given a start Person, retrieve the last 10 Messages (Posts or Comments) created by that person.
     * For each message, return that message, the original post in its conversation, and the author of that post.
     * Order results descending by message creation date, then descending by message identifier
     */
    class LdbcShortQuery2ToDgraph : OperationHandler<LdbcShortQuery2PersonPosts, DgraphDbConnectionState> {

        @Throws(DbException::class)
        override fun executeOperation(operation: LdbcShortQuery2PersonPosts, state: DgraphDbConnectionState, resultReporter: ResultReporter) {
            val RESULT = ArrayList<LdbcShortQuery2PersonPostsResult>()
            var results_count = 0
            val conn = state.conn

            try {
                val queryString = file2string(File(state.queryDir, "s2.txt")).replace("@Person@", getPersonId(operation.personId()).toString())

                if (state.isPrintNames)
                    println("########### LdbcShortQuery2")
                if (state.isPrintStrings)
                    println(queryString)

                val result = conn.query(queryString).toJsonObject()["recurse"].asJsonArray

                result.map { it.asJsonObject }.forEach {
                    val postObj = findLeaf("replyOf", it)
                    if (postObj.has("hasCreator")) {
                        val postAuthor = postObj["hasCreator"][0].asJsonObject
                        RESULT.add(LdbcShortQuery2PersonPostsResult(
                                it.uid,
                                it["content"].asString,
                                it["creationDate"].asDateLong,
                                getXid(postObj["_uid_"].asLong),
                                getXid(postAuthor["_uid_"].asLong),
                                postAuthor["firstName"].asString,
                                postAuthor["lastName"].asString
                        ))
                    } else {
                        // NOTE: Data issue, reply doesn't have "hasCreator"?
                        RESULT.add(LdbcShortQuery2PersonPostsResult(
                                it.uid,
                                it["content"].asString,
                                it["creationDate"].asDateLong,
                                postObj.uid,
                                0,
                                "",
                                ""
                        ))
                    }
                    results_count++
                }

                // conn.close()
            } catch (e: Exception) {
                println("Err: LdbcShortQuery2 (" + operation.personId() + ")")
                e.printStackTrace()
            }

            resultReporter.report(results_count, RESULT, operation)
        }
    }

    /**
     * Given a start Person, retrieve all of their friends, and the date at which they became friends.
     * Order results descending by friendship creation date, then ascending by friend identifier
     */
    class LdbcShortQuery3ToDgraph : OperationHandler<LdbcShortQuery3PersonFriends, DgraphDbConnectionState> {

        @Throws(DbException::class)
        override fun executeOperation(operation: LdbcShortQuery3PersonFriends, state: DgraphDbConnectionState, resultReporter: ResultReporter) {
            val RESULT = ArrayList<LdbcShortQuery3PersonFriendsResult>()
            var results_count = 0
            val conn = state.conn

            try {
                val queryString = file2string(File(state.queryDir, "s3.txt")).replace("@Person@", getPersonId(operation.personId()).toString())
                if (state.isPrintNames)
                    println("########### LdbcShortQuery3")
                if (state.isPrintStrings)
                    println(queryString)

                val result = conn.query(queryString).toJsonObject()["q"][0].asJsonObject
                if (result.has("knows"))
                    result["knows"].asJsonArray.map { it.asJsonObject }.forEach {
                        results_count++
                        RESULT.add(LdbcShortQuery3PersonFriendsResult(
                                it.xid,
                                it["firstName"].asString,
                                it["lastName"].asString,
                                it["creationDate"].asDateLong
                        ))
                    }
                if (state.isPrintResults)
                    println(RESULT.toString())

                // conn.close()
            } catch (e: Exception) {
                e.printStackTrace()
            }

            resultReporter.report(results_count, RESULT, operation)
        }
    }

    /**
     * Given a Message (Post or Comment), retrieve its content and creation date.
     */
    class LdbcShortQuery4ToDgraph : OperationHandler<LdbcShortQuery4MessageContent, DgraphDbConnectionState> {

        @Throws(DbException::class)
        override fun executeOperation(operation: LdbcShortQuery4MessageContent, state: DgraphDbConnectionState, resultReporter: ResultReporter) {
            var RESULT: LdbcShortQuery4MessageContentResult? = null
            var results_count = 0
            val conn = state.conn

            try {
                val queryString = file2string(File(state.queryDir, "s4.txt")).replace("@Message@", getMessageId(operation.messageId()).toString())
                if (state.isPrintNames)
                    println("########### LdbcShortQuery4")
                if (state.isPrintStrings)
                    println(queryString)
                val result = conn.query(queryString).toJsonObject()
                val obj = result["q"][0].asJsonObject
                results_count++
                RESULT = LdbcShortQuery4MessageContentResult(obj["content"].asString, obj["creationDate"].asDateLong)

                // conn.close()
            } catch (e: Exception) {
                println("Err: LdbcShortQuery4 (" + operation.messageId() + ")")
                e.printStackTrace()
            }

            resultReporter.report(results_count, RESULT, operation)
        }
    }

    /**
     * Given a Message (Post or Comment), retrieve its author.
     */
    class LdbcShortQuery5ToDgraph : OperationHandler<LdbcShortQuery5MessageCreator, DgraphDbConnectionState> {

        @Throws(DbException::class)
        override fun executeOperation(operation: LdbcShortQuery5MessageCreator, state: DgraphDbConnectionState, resultReporter: ResultReporter) {
            var RESULT: LdbcShortQuery5MessageCreatorResult? = null
            var results_count = 0
            val conn = state.conn
            try {
                val queryString = file2string(File(state.queryDir, "s5.txt")).replace("@Message@", getMessageId(operation.messageId()).toString())
                if (state.isPrintNames)
                    println("########### LdbcShortQuery5")
                if (state.isPrintStrings)
                    println(queryString)
                val result = conn.query(queryString).toJsonObject()
                val obj = result["q"][0]["hasCreator"].asJsonObject
                results_count++
                RESULT = LdbcShortQuery5MessageCreatorResult(
                        obj.xid,
                        obj["firstName"].asString,
                        obj["lastName"].asString)

                // conn.close()
            } catch (e: Exception) {
                e.printStackTrace()
            }

            resultReporter.report(results_count, RESULT, operation)
        }
    }

    /**
     * Given a Message (Post or Comment), retrieve the Forum that contains it and the Person that moderates that forum.
     */
    class LdbcShortQuery6ToDgraph : OperationHandler<LdbcShortQuery6MessageForum, DgraphDbConnectionState> {

        @Throws(DbException::class)
        override fun executeOperation(operation: LdbcShortQuery6MessageForum, state: DgraphDbConnectionState, resultReporter: ResultReporter) {
            var RESULT: LdbcShortQuery6MessageForumResult? = null
            var results_count = 0
            val conn = state.conn
            try {
                val queryString = file2string(File(state.queryDir, "s6.txt")).replace("@Message@", getMessageId(operation.messageId()).toString())

                if (state.isPrintNames)
                    println("########### LdbcShortQuery6")
                if (state.isPrintStrings)
                    println(queryString)
                val r0 = conn.query(queryString).toJsonObject()
                val result = r0["recurse"][0].asJsonObject
                val post = findLeaf("replyOf", result)
                val forum = post["~containerOf"][0].asJsonObject
                val person = forum["hasModerator"][0].asJsonObject

                results_count++
                RESULT = LdbcShortQuery6MessageForumResult(
                        forum.xid,
                        forum["title"].asString,
                        person.xid,
                        person["firstName"].asString,
                        person["lastName"].asString
                )

                // conn.close()
            } catch (e: Exception) {
                e.printStackTrace()
            }

            resultReporter.report(results_count, RESULT, operation)
        }
    }

    /**
     * Given a Message (Post or Comment), retrieve the (1-hop) Comments that reply to it.
     * In addition, return a boolean flag indicating if the author of the reply knows the author of the original message.
     * If author is same as original author, return false for "knows" flag.
     * Order results descending by comment identifier, then descending by author identifier.
     */
    class LdbcShortQuery7ToDgraph : OperationHandler<LdbcShortQuery7MessageReplies, DgraphDbConnectionState> {

        @Throws(DbException::class)
        override fun executeOperation(operation: LdbcShortQuery7MessageReplies, state: DgraphDbConnectionState, resultReporter: ResultReporter) {
            val RESULT = ArrayList<LdbcShortQuery7MessageRepliesResult>()
            var results_count = 0
            val conn = state.conn
            var stmt1: CallableStatement? = null
            var stmt2: Statement? = null

            try {
                val queryString = file2string(File(state.queryDir, "s7.txt"))
                        .replace("@Message@", operation.messageId().toString())
                if (state.isPrintNames)
                    println("########### LdbcShortQuery7")
                if (state.isPrintStrings) {
                    println(queryString)
                }
                val result = conn.query(queryString).toJsonObject()["data"].asJsonObject["q"].asJsonArray
                result.map { it.asJsonObject }.forEach {
                    results_count++
                    val replyAuthor = it["hasCreator"].asJsonArray[0].asJsonObject
                    RESULT.add(LdbcShortQuery7MessageRepliesResult(
                            getXid(it["_uid_"].asLong),
                            it["content"].asString,
                            it["creationDate"].asLong,
                            getXid(replyAuthor["_uid_"].asLong),
                            replyAuthor["firstName"].asString,
                            replyAuthor["lastName"].asString,
                            replyAuthor["count(knows)"].asLong > 0
                    ))
                }

                // conn.close()
            } catch (e: Exception) {
                e.printStackTrace()
            }

            resultReporter.report(results_count, RESULT, operation)
        }
    }

    class LdbcUpdate1AddPersonToDgraph : OperationHandler<LdbcUpdate1AddPerson, DgraphDbConnectionState> {

        @Throws(DbException::class)
        override fun executeOperation(operation: LdbcUpdate1AddPerson, state: DgraphDbConnectionState, resultReporter: ResultReporter) {
            val conn = state.conn
            try {
                if (state.isPrintNames)
                    println("########### LdbcUpdate1")
                if (state.isPrintStrings) {
                    println(operation.personId().toString() + " " + operation.personFirstName() + " " + operation.personLastName())
                    println(operation.gender())
                    println(operation.birthday())
                    println(operation.creationDate())
                    println(operation.locationIp())
                    println(operation.browserUsed())
                    println(operation.cityId())
                    println("[")
                    for (lan in operation.languages()) {
                        println(lan)
                    }
                    println("]")
                    println("[")
                    for (email in operation.emails()) {
                        println(email)
                    }
                    println("]")
                    println("[")
                    for (tag in operation.tagIds()) {
                        println(tag)
                    }
                    println("]")
                    println("[")
                    for (tag in operation.studyAt()) {
                        println(tag.organizationId().toString() + " - " + tag.year())
                    }
                    println("]")
                    println("[")
                    for (tag in operation.workAt()) {
                        println(tag.organizationId().toString() + " - " + tag.year())
                    }
                    println("]")
                }
                // Make it unique by adding prefix
                val uid = getPersonId(operation.personId())
                val languages = operation.languages().joinToString("\n") {
                    "<$uid> <language> \"$it\" ."
                }
                val emails = operation.emails().joinToString("\n") {
                    "<$uid> <email> \"$it\" ."
                }
                val tags = operation.tagIds().joinToString("\n") {
                    "<$uid> <hasTag> <0x${getTagId(it).toString(16)}> ."
                }
                val universities = operation.studyAt().joinToString("\n") {
                    "<$uid> <studyAt> <0x${getOrganizationId(it.organizationId()).toString(16)}> (year=${it.year()}) ."
                }
                val companies = operation.workAt().joinToString("\n") {
                    "<$uid> <workAt> <0x${getOrganizationId(it.organizationId()).toString(16)}> (year=${it.year()}) ."
                }
                val queryString = """
mutation {
  set {
    <$uid> <firstName> "${operation.personFirstName()}" .
    <$uid> <lastName> "${operation.personLastName()}" .
    <$uid> <gender> "${operation.gender()}" .
    <$uid> <birthday> "${operation.birthday()}" .
    <$uid> <creationDate> "${df.format(operation.creationDate())}" .
    <$uid> <locationIP> "${operation.locationIp()}" .
    <$uid> <browserUsed> "${operation.browserUsed()}" .
    $languages
    $emails
    $tags
    $universities
    $companies
  }
}
"""
                println("Query to be sent to Dgraph:")
                println(queryString)
                conn.query(queryString)
                // conn.close()
            } catch (e: Throwable) {
                e.printStackTrace()
            }

            resultReporter.report(0, LdbcNoResult.INSTANCE, operation)
        }
    }

    class LdbcUpdate2AddPostLikeToDgraph : OperationHandler<LdbcUpdate2AddPostLike, DgraphDbConnectionState> {


        @Throws(DbException::class)
        override fun executeOperation(operation: LdbcUpdate2AddPostLike, state: DgraphDbConnectionState, resultReporter: ResultReporter) {
            val conn = state.conn
            try {
                if (state.isPrintNames)
                    println("########### LdbcUpdate2")
                if (state.isPrintStrings)
                    println(operation.personId().toString() + " " + operation.postId() + " " + operation.creationDate())

                // Find uid from xid
                val person = getPersonId(operation.personId())
                val post = getMessageId(operation.postId())

                val queryString = """
mutation {
    <0x${person.toString(16)}> <likes> <0x${post.toString(16)}> (creationDate=${df.format(operation.creationDate())}) .
}"""
                conn.query(queryString)
                // conn.close()
            } catch (e: Throwable) {
                e.printStackTrace()
            }

            resultReporter.report(0, LdbcNoResult.INSTANCE, operation)
        }
    }

    class LdbcUpdate3AddCommentLikeToDgraph : OperationHandler<LdbcUpdate3AddCommentLike, DgraphDbConnectionState> {

        @Throws(DbException::class)
        override fun executeOperation(operation: LdbcUpdate3AddCommentLike, state: DgraphDbConnectionState, resultReporter: ResultReporter) {
            val conn = state.conn
            try {
                if (state.isPrintNames)
                    println("########### LdbcUpdate3")
                if (state.isPrintStrings)
                    println(operation.personId().toString() + " " + operation.commentId() + " " + operation.creationDate())

                // Find uid from xid
                val person = getPersonId(operation.personId())
                val comment = getMessageId(operation.commentId())

                val queryString = """
mutation {
    <0x${person.toString(16)}> <likes> <0x${comment.toString(16)}> (creationDate=${df.format(operation.creationDate())}) .
}"""
                conn.query(queryString)
                // conn.close()
            } catch (e: Throwable) {
                e.printStackTrace()
            }

            resultReporter.report(0, LdbcNoResult.INSTANCE, operation)
        }
    }

    class LdbcUpdate4AddForumToDgraph : OperationHandler<LdbcUpdate4AddForum, DgraphDbConnectionState> {


        @Throws(DbException::class)
        override fun executeOperation(operation: LdbcUpdate4AddForum, state: DgraphDbConnectionState, resultReporter: ResultReporter) {
            val conn = state.conn
            try {
                if (state.isPrintNames)
                    println("########### LdbcUpdate4")
                if (state.isPrintStrings) {
                    println(operation.forumId().toString() + " " + operation.forumTitle())
                    println(operation.creationDate().toString() + " " + operation.moderatorPersonId())
                    println("[")
                    for (tag in operation.tagIds()) {
                        println(tag)
                    }
                    println("]")
                }
                // Find uid from xid
                val uid = getForumId(operation.forumId())
                val moderatorPerson = getPersonId(operation.moderatorPersonId())

                val tags = operation.tagIds().joinToString("\n") {
                    "<$uid> <hasTag> <0x${getTagId(it).toString(16)}> ."
                }
                val queryString = """
mutation {
    <$uid> <title> "${operation.forumTitle()}" .
    <$uid> <creationDate> "${df.format(operation.creationDate())}" .
    <$uid> <hasModerator> <0x$moderatorPerson> .
    $tags
}"""

                conn.query(queryString)
                // conn.close()
            } catch (e: Throwable) {
                e.printStackTrace()
            }

            resultReporter.report(0, LdbcNoResult.INSTANCE, operation)
        }
    }

    class LdbcUpdate5AddForumMembershipToDgraph : OperationHandler<LdbcUpdate5AddForumMembership, DgraphDbConnectionState> {

        @Throws(DbException::class)
        override fun executeOperation(operation: LdbcUpdate5AddForumMembership, state: DgraphDbConnectionState, resultReporter: ResultReporter) {
            val conn = state.conn
            try {
                if (state.isPrintNames)
                    println("########### LdbcUpdate5")
                if (state.isPrintStrings)
                    println(operation.forumId().toString() + " " + operation.personId() + " " + operation.joinDate())

                // Find uid from xid
                val person = getPersonId(operation.personId())
                val forum = getForumId(operation.forumId())

                val queryString = """
mutation {
    <0x${forum.toString(16)}> <hasMember> <0x${person.toString(16)}> (joinDate=${df.format(operation.joinDate())}) .
}"""
                conn.query(queryString)
                // conn.close()
            } catch (e: Throwable) {
                e.printStackTrace()
            }

            resultReporter.report(0, LdbcNoResult.INSTANCE, operation)
        }
    }

    class LdbcUpdate6AddPostToDgraph : OperationHandler<LdbcUpdate6AddPost, DgraphDbConnectionState> {

        @Throws(DbException::class)
        override fun executeOperation(operation: LdbcUpdate6AddPost, state: DgraphDbConnectionState, resultReporter: ResultReporter) {
            val conn = state.conn
            try {
                if (state.isPrintNames)
                    println("########### LdbcUpdate6")
                if (state.isPrintStrings) {
                    println(operation.postId().toString() + " " + operation.imageFile())
                    println(operation.creationDate().toString() + " " + operation.locationIp())
                    println(operation.browserUsed() + " " + operation.language())
                    println(operation.content())
                    println(operation.length().toString() + " " + operation.authorPersonId())
                    println(operation.forumId().toString() + " " + operation.countryId())
                    println("[")
                    for (tag in operation.tagIds()) {
                        println(tag)
                    }
                    println("]")
                }

                // Find uid from xid
                val uid = getMessageId(operation.postId())
                val author = getPersonId(operation.authorPersonId())
                val forum = getForumId(operation.forumId())
                val country = getPlaceId(operation.countryId())

                val tags = operation.tagIds().joinToString("\n") {
                    "<$uid> <hasTag> <0x${getTagId(it).toString(16)}> ."
                }
                val queryString = """
mutation {
  set {
    <$uid> <imageFile> "${operation.imageFile()}" .
    <$uid> <creationDate> "${df.format(operation.creationDate())}" .
    <$uid> <locationIP> "${operation.locationIp()}" .
    <$uid> <browserUsed> "${operation.browserUsed()}" .
    <$uid> <language> "${operation.language()}" .
    <$uid> <content> "${operation.content()}" .
    <$uid> <length> "${operation.length()}" .
    <$uid> <hasCreator> <0x$author> .
    <$uid> <isLocatedIn> <0x$country> .
    $tags

    <0x$forum> <containerOf> <$uid> .
  }
}
"""

                conn.query(queryString)
                // conn.close()
            } catch (e: Throwable) {
                e.printStackTrace()
                try {
                    // conn.close()
                } catch (e1: SQLException) {
                }

            }

            resultReporter.report(0, LdbcNoResult.INSTANCE, operation)
        }
    }

    class LdbcUpdate7AddCommentToDgraph : OperationHandler<LdbcUpdate7AddComment, DgraphDbConnectionState> {


        @Throws(DbException::class)
        override fun executeOperation(operation: LdbcUpdate7AddComment, state: DgraphDbConnectionState, resultReporter: ResultReporter) {
            val conn = state.conn
            try {
                if (state.isPrintNames)
                    println("########### LdbcUpdate7")
                if (state.isPrintStrings) {
                    println("################################################ LdbcUpdate7AddComment")
                    println(operation.commentId())
                    println(operation.creationDate().toString() + " " + operation.locationIp())
                    println(operation.browserUsed())
                    println(operation.content())
                    println(operation.length().toString() + " " + operation.authorPersonId())
                    println(operation.countryId())
                    println(operation.replyToPostId().toString() + " " + operation.replyToCommentId())
                    println("[")
                    for (tag in operation.tagIds()) {
                        println(tag)
                    }
                    println("]")
                }

                // Find uid from xid
                val uid = getMessageId(operation.commentId())
                val author = getPersonId(operation.authorPersonId())
                val country = getPlaceId(operation.countryId())
                val postLine = if (operation.replyToPostId() == 0L)
                    ""
                else
                    "<$uid> <replyOf> <0x${getMessageId(operation.replyToPostId())}> ."
                val commentLine = if (operation.replyToCommentId() == 0L)
                    ""
                else
                    "<$uid> <replyOf> <0x${getMessageId(operation.replyToCommentId())}> ."
                val tags = operation.tagIds().joinToString("\n") {
                    "<$uid> <hasTag> <0x${getTagId(it).toString(16)}> ."
                }
                val queryString = """
mutation {
  set {
    <$uid> <xid> "message_${operation.commentId()}" .
    <$uid> <creationDate> "${df.format(operation.creationDate())}" .
    <$uid> <locationIP> "${operation.locationIp()}" .
    <$uid> <browserUsed> "${operation.browserUsed()}" .
    <$uid> <content> "${operation.content()}" .
    <$uid> <length> "${operation.length()}" .
    <$uid> <hasCreator> <0x$author> .
    <$uid> <isLocatedIn> <0x$country> .
    $postLine
    $commentLine
    $tags
  }
}
"""

                conn.query(queryString)
                // conn.close()
            } catch (e: Throwable) {
                e.printStackTrace()
            }

            resultReporter.report(0, LdbcNoResult.INSTANCE, operation)
        }
    }

    class LdbcUpdate8AddFriendshipToDgraph : OperationHandler<LdbcUpdate8AddFriendship, DgraphDbConnectionState> {


        @Throws(DbException::class)
        override fun executeOperation(operation: LdbcUpdate8AddFriendship, state: DgraphDbConnectionState, resultReporter: ResultReporter) {
            val conn = state.conn
            var cs: CallableStatement? = null
            try {
                if (state.isPrintNames)
                    println("########### LdbcUpdate8")
                if (state.isPrintStrings)
                    println(operation.person1Id().toString() + " " + operation.person2Id() + " " + operation.creationDate())

                // Find uid from xid
                val person1 = getPersonId(operation.person1Id())
                val person2 = getPersonId(operation.person2Id())

                val queryString = """
mutation {
    <$person1> <knows> <$person2> (creationDate=${df.format(operation.creationDate())}) .
}"""
                conn.query(queryString)
                // conn.close()
            } catch (e: Throwable) {
                e.printStackTrace()
            }

            resultReporter.report(0, LdbcNoResult.INSTANCE, operation)
        }
    }

    companion object {
        // Same as csv_to_rdf.py
        enum class NodeType(val value: Long) {
            TYPE_MASK(0x0FFF_FFFF_FFFF_FFFFL),
            MASK(0x0FFF_FFFF_FFFF_FFFFL),
            MESSAGE(0x0000_0000_0000_0000L),
            FORUM(0x1000_0000_0000_0000L),
            ORGANIZATION(0x2000_0000_0000_0000L),
            PERSON(0x3000_0000_0000_0000L),
            PLACE(0x4000_0000_0000_0000L),
            TAG(0x5000_0000_0000_0000L),
            TAGCLASS(0x6000_0000_0000_0000L)
        }

        fun getXid(uid: Long): Long = NodeType.MASK.value and uid
        fun getUid(type: NodeType, xid: Long): Long = type.value or xid

        fun getMessageId(xid: Long): Long = getUid(NodeType.MESSAGE, xid)
        fun getForumId(xid: Long): Long = getUid(NodeType.FORUM, xid)
        fun getOrganizationId(xid: Long): Long = getUid(NodeType.ORGANIZATION, xid)
        fun getPersonId(xid: Long): Long = getUid(NodeType.PERSON, xid)
        fun getPlaceId(xid: Long): Long = getUid(NodeType.PLACE, xid)
        fun getTagId(xid: Long): Long = getUid(NodeType.TAG, xid)
        fun getTagclassId(xid: Long): Long = getUid(NodeType.TAGCLASS, xid)

        fun typeOfUid(uid: Long): NodeType = when (uid and NodeType.TYPE_MASK.value) {
            NodeType.MESSAGE.value -> NodeType.MESSAGE
            NodeType.FORUM.value -> NodeType.FORUM
            NodeType.ORGANIZATION.value -> NodeType.ORGANIZATION
            NodeType.PERSON.value -> NodeType.PERSON
            NodeType.PLACE.value -> NodeType.PLACE
            NodeType.TAG.value -> NodeType.TAG
            NodeType.TAGCLASS.value -> NodeType.TAGCLASS
            else -> {
                throw IllegalArgumentException()
            }
        }

        fun findLeaf(key: String, obj: JsonObject): JsonObject {
            var ret: JsonObject = obj
            while (ret.has(key)) {
                ret = ret[key][0].asJsonObject
            }
            return ret
        }

        var df = SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ssZ")

        init {
            df.timeZone = TimeZone.getTimeZone("GMT")
        }

        @Throws(Exception::class)
        fun file2string(file: File): String {
            var reader: BufferedReader? = null
            try {
                reader = BufferedReader(FileReader(file))
                val sb = StringBuffer()

                while (true) {
                    val line = reader.readLine()
                    if (line == null)
                        break
                    else {
                        sb.append(line)
                        sb.append("\n")
                    }
                }
                val ret = sb.toString()
                // Anything below a line will be ignored
                return ret.substring(0, ret.indexOf("----------"))
            } catch (e: IOException) {
                throw Exception("Error opening or reading file: " + file.absolutePath, e)
            } finally {
                try {
                    if (reader != null)
                        reader.close()
                } catch (e: IOException) {
                    e.printStackTrace()
                }

            }
        }

        val PARSER = SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss'Z'")
        fun dateToLong(value: String): Long = PARSER.parse(value).time
        fun dateToLong(value: JsonElement): Long = PARSER.parse(value.asString).time

        @JvmStatic
        fun main(args: Array<String>) {
        }
    }

}

