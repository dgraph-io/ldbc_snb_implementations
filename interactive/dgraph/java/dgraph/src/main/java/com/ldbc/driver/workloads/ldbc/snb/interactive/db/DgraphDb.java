package com.ldbc.driver.workloads.ldbc.snb.interactive.db;


import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.ldbc.driver.*;
import com.ldbc.driver.control.LoggingService;
import com.ldbc.driver.workloads.ldbc.snb.interactive.*;
import com.ldbc.driver.workloads.ldbc.snb.interactive.LdbcUpdate1AddPerson.Organization;
import io.dgraph.client.DgraphClient;
import io.dgraph.client.DgraphResult;
import io.dgraph.client.GrpcDgraphClient;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.sql.CallableStatement;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.*;

public class DgraphDb extends Db {
    private DgraphDbConnectionState DgraphDbConnectionState;

    public static String file2string(File file) throws Exception {
        BufferedReader reader = null;
        try {
            reader = new BufferedReader(new FileReader(file));
            StringBuffer sb = new StringBuffer();

            while (true) {
                String line = reader.readLine();
                if (line == null)
                    break;
                else {
                    sb.append(line);
                    sb.append("\n");
                }
            }
            return sb.toString();
        } catch (IOException e) {
            throw new Exception("Error openening or reading file: " + file.getAbsolutePath(), e);
        } finally {
            try {
                if (reader != null)
                    reader.close();
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
    }

    @Override
    protected void onInit(Map<String, String> properties, LoggingService loggingService) throws DbException {
        try {
            DgraphDbConnectionState = new DgraphDbConnectionState(properties);
        } catch (ClassNotFoundException e) {
            e.printStackTrace();
        } catch (SQLException e) {
            e.printStackTrace();
        }

        registerOperationHandler(LdbcQuery1.class, LdbcQuery1ToDgraph.class);
        registerOperationHandler(LdbcQuery2.class, LdbcQuery2ToDgraph.class);
        registerOperationHandler(LdbcQuery3.class, LdbcQuery3ToDgraph.class);
        registerOperationHandler(LdbcQuery4.class, LdbcQuery4ToDgraph.class);
        registerOperationHandler(LdbcQuery5.class, LdbcQuery5ToDgraph.class);
        registerOperationHandler(LdbcQuery6.class, LdbcQuery6ToDgraph.class);
        registerOperationHandler(LdbcQuery7.class, LdbcQuery7ToDgraph.class);
        registerOperationHandler(LdbcQuery8.class, LdbcQuery8ToDgraph.class);
        registerOperationHandler(LdbcQuery9.class, LdbcQuery9ToDgraph.class);
        registerOperationHandler(LdbcQuery10.class, LdbcQuery10ToDgraph.class);
        registerOperationHandler(LdbcQuery11.class, LdbcQuery11ToDgraph.class);
        registerOperationHandler(LdbcQuery12.class, LdbcQuery12ToDgraph.class);
        registerOperationHandler(LdbcQuery13.class, LdbcQuery13ToDgraph.class);
        registerOperationHandler(LdbcQuery14.class, LdbcQuery14ToDgraph.class);

        registerOperationHandler(LdbcShortQuery1PersonProfile.class, LdbcShortQuery1ToDgraph.class);
        registerOperationHandler(LdbcShortQuery2PersonPosts.class, LdbcShortQuery2ToDgraph.class);
        registerOperationHandler(LdbcShortQuery3PersonFriends.class, LdbcShortQuery3ToDgraph.class);
        registerOperationHandler(LdbcShortQuery4MessageContent.class, LdbcShortQuery4ToDgraph.class);
        registerOperationHandler(LdbcShortQuery5MessageCreator.class, LdbcShortQuery5ToDgraph.class);
        registerOperationHandler(LdbcShortQuery6MessageForum.class, LdbcShortQuery6ToDgraph.class);
        registerOperationHandler(LdbcShortQuery7MessageReplies.class, LdbcShortQuery7ToDgraph.class);

        registerOperationHandler(LdbcUpdate1AddPerson.class, LdbcUpdate1AddPersonToDgraph.class);
        registerOperationHandler(LdbcUpdate2AddPostLike.class, LdbcUpdate2AddPostLikeToDgraph.class);
        registerOperationHandler(LdbcUpdate3AddCommentLike.class, LdbcUpdate3AddCommentLikeToDgraph.class);
        registerOperationHandler(LdbcUpdate4AddForum.class, LdbcUpdate4AddForumToDgraph.class);
        registerOperationHandler(LdbcUpdate5AddForumMembership.class, LdbcUpdate5AddForumMembershipToDgraph.class);
        registerOperationHandler(LdbcUpdate6AddPost.class, LdbcUpdate6AddPostToDgraph.class);
        registerOperationHandler(LdbcUpdate7AddComment.class, LdbcUpdate7AddCommentToDgraph.class);
        registerOperationHandler(LdbcUpdate8AddFriendship.class, LdbcUpdate8AddFriendshipToDgraph.class);
    }

    @Override
    protected void onClose() {
        System.out.println("ON CLOSE()");
        try {
            DgraphDbConnectionState.getConn().close();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    @Override
    protected DbConnectionState getConnectionState() throws DbException {
        return DgraphDbConnectionState;
    }

    public class DgraphDbConnectionState extends DbConnectionState {
        private DgraphClient ds;

        private String queryDir;

        private boolean printNames;
        private boolean printStrings;
        private boolean printResults;
        private HashMap<Long, String> placeMap;
        private HashMap<Long, String> companyMap;
        private HashMap<Long, String> universityMap;
        private HashMap<Long, String> tagMap;

        DgraphDbConnectionState(Map<String, String> properties) throws ClassNotFoundException, SQLException {
            super();
            ds = GrpcDgraphClient.newInstance(properties.get("host"), Integer.parseInt(properties.get("port")));
            queryDir = properties.get("queryDir");
            printNames = properties.get("printQueryNames").equals("true") ? true : false;
            printStrings = properties.get("printQueryStrings").equals("true") ? true : false;
            printResults = properties.get("printQueryResults").equals("true") ? true : false;
        }

        public DgraphClient getConn() {
            return ds;
        }

        public String getQueryDir() {
            return queryDir;
        }

        public boolean isPrintNames() {
            return printNames;
        }

        public boolean isPrintStrings() {
            return printStrings;
        }

        public boolean isPrintResults() {
            return printResults;
        }

        public void close() throws IOException {

        }

        public String placeUri(long id) {
            return placeMap.get(id);
        }

        public String companyUri(long id) {
            return companyMap.get(id);
        }

        public String universityUri(long id) {
            return universityMap.get(id);
        }

        public String tagUri(long id) {
            return tagMap.get(id);
        }

    }

    /**
     * Given a start Person, find up to 20 Persons with a given first name that the start Person is connected to
     * (excluding start Person) by at most 3 steps via Knows relationships. Return Persons, including summaries of
     * the Persons workplaces and places of study. Sort results ascending by their distance from the start Person,
     * for Persons within the same distance sort ascending by their last name, and for Persons with same last name
     * ascending by their identifier.
     */
    public static class LdbcQuery1ToDgraph implements OperationHandler<LdbcQuery1, DgraphDbConnectionState> {
        public void executeOperation(LdbcQuery1 operation, DgraphDbConnectionState state, ResultReporter resultReporter) throws DbException {
            DgraphClient conn = state.getConn();
            List<LdbcQuery1Result> RESULT = new ArrayList<LdbcQuery1Result>();
            int results_count = 0;
            RESULT.clear();
            try {
                String queryString = file2string(new File(state.getQueryDir(), "query1.txt"));
                queryString = queryString.replaceAll("@Person@", String.valueOf(operation.personId()));
                queryString = queryString.replaceAll("@Name@", operation.firstName());

                if (state.isPrintNames())
                    System.out.println("########### LdbcQuery1");
                if (state.isPrintStrings())
                    System.out.println(queryString);

                JsonObject result = conn.query(queryString).toJsonObject();
                // TODO:
//                for(JsonElement v: result.getAsJsonArray()) {
//                    String key=(String)keys.next();
//                    results_count++;
//                    v...
//                    LdbcQuery1Result tmp = new LdbcQuery1Result(id, lastName, dist, birthday, creationDate,
//                            gender, browserUsed, ip, emails, languages, place, universities, companies);
//                    if (state.isPrintResults())
//                        System.out.println(tmp.toString());
//                    RESULT.add(tmp);
//                }
                conn.close();
            } catch (Exception e) {
                e.printStackTrace();
            }
            resultReporter.report(results_count, RESULT, operation);
        }
    }

    /**
     * Given a start Person, find (most recent) Posts and Comments from all of that Person’s friends, that were created
     * before (and including) a given date. Return the top 20 Posts/Comments, and the Person that created each of them.
     * Sort results descending by creation date, and then ascending by Post identifier.
     */
    public static class LdbcQuery2ToDgraph implements OperationHandler<LdbcQuery2, DgraphDbConnectionState> {

        public void executeOperation(LdbcQuery2 operation, DgraphDbConnectionState state, ResultReporter resultReporter) throws DbException {
            DgraphClient conn = state.getConn();
            List<LdbcQuery2Result> RESULT = new ArrayList<LdbcQuery2Result>();
            int results_count = 0;
            RESULT.clear();
            try {
                String queryString = file2string(new File(state.getQueryDir(), "query2.txt"));
                SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSS'+00:00'");
                sdf.setTimeZone(TimeZone.getTimeZone("GMT"));
                queryString = queryString.replaceAll("@Person@", String.valueOf(operation.personId()));
                queryString = queryString.replaceAll("@Date0@", sdf.format(operation.maxDate()));

                if (state.isPrintNames())
                    System.out.println("########### LdbcQuery2");
                if (state.isPrintStrings())
                    System.out.println(queryString);

                JsonObject result = conn.query(queryString).toJsonObject();
                // TODO:
//                for(JsonElement v: result.getAsJsonArray()) {
//                    String key=(String)keys.next();
//                    results_count++;
//                    v...
//                    LdbcQuery2Result tmp = new LdbcQuery2Result(id, firstName, lastName, postid, content, postdate);
//                    if (state.isPrintResults())
//                        System.out.println(tmp.toString());
//                    RESULT.add(tmp);
//                }
                conn.close();
            } catch (Exception e) {
                e.printStackTrace();

            }
            resultReporter.report(results_count, RESULT, operation);
        }
    }

    /**
     * Given a start Person, find Persons that are their friends and friends of friends (excluding start Person) that
     * have made Posts/Comments in the given Countries X and Y within a given period. Only Persons that are foreign to
     * Countries X and Y are considered, that is Persons whose Location is not Country X or Country Y. Return top 20
     * Persons, and their Post/Comment counts, in the given countries and period. Sort results descending by total
     * number of Posts/Comments, and then ascending by Person identifier.
     */
    public static class LdbcQuery3ToDgraph implements OperationHandler<LdbcQuery3, DgraphDbConnectionState> {

        public void executeOperation(LdbcQuery3 operation, DgraphDbConnectionState state, ResultReporter resultReporter) throws DbException {
            DgraphClient conn = state.getConn();
            List<LdbcQuery3Result> RESULT = new ArrayList<LdbcQuery3Result>();
            int results_count = 0;
            RESULT.clear();
            try {
                String queryString = file2string(new File(state.getQueryDir(), "query3.txt"));
                SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSS'+00:00'");
                sdf.setTimeZone(TimeZone.getTimeZone("GMT"));
                queryString = queryString.replaceAll("@Person@", String.valueOf(operation.personId()));
                queryString = queryString.replaceAll("@Country1@", operation.countryXName());
                queryString = queryString.replaceAll("@Country2@", operation.countryYName());
                queryString = queryString.replaceAll("@Date0@", sdf.format(operation.startDate()));
                queryString = queryString.replaceAll("@Duration@", String.valueOf(operation.durationDays()));

                if (state.isPrintNames())
                    System.out.println("########### LdbcQuery3");
                if (state.isPrintStrings())
                    System.out.println(queryString);

                JsonObject result = conn.query(queryString).toJsonObject();
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
                conn.close();
            } catch (Exception e) {
                e.printStackTrace();

            }
            resultReporter.report(results_count, RESULT, operation);
        }
    }


    /**
     * Given a start Person, find Tags that are attached to Posts that were created by that Person’s friends.
     * Only include Tags that were attached to Posts created within a given time interval, and that were never attached
     * to Posts created before this interval. Return top 10 Tags, and the count of Posts, which were created within the
     * given time interval, that this Tag was attached to. Sort results descending by Post count, and then ascending by
     * Tag name.
     */
    public static class LdbcQuery4ToDgraph implements OperationHandler<LdbcQuery4, DgraphDbConnectionState> {

        public void executeOperation(LdbcQuery4 operation, DgraphDbConnectionState state, ResultReporter resultReporter) throws DbException {
            DgraphClient conn = state.getConn();
            List<LdbcQuery4Result> RESULT = new ArrayList<LdbcQuery4Result>();
            int results_count = 0;
            RESULT.clear();
            try {
                String queryString = file2string(new File(state.getQueryDir(), "query4.txt"));
                SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSS'+00:00'");
                sdf.setTimeZone(TimeZone.getTimeZone("GMT"));
                queryString = queryString.replaceAll("@Person@", String.valueOf(operation.personId()));
                queryString = queryString.replaceAll("@Date0@", sdf.format(operation.startDate()));
                queryString = queryString.replaceAll("@Duration@", String.valueOf(operation.durationDays()));

                if (state.isPrintNames())
                    System.out.println("########### LdbcQuery4");
                if (state.isPrintStrings())
                    System.out.println(queryString);

                JsonObject result = conn.query(queryString).toJsonObject();
                // TODO:
//                for(JsonElement v: result.getAsJsonArray()) {
//                    String key=(String)keys.next();
//                    results_count++;
//                    v...
//                    LdbcQuery4Result tmp = new LdbcQuery4Result(tagName, tagCount);
//                    if (state.isPrintResults())
//                        System.out.println(tmp.toString());
//                    RESULT.add(tmp);
//                }
                conn.close();
            } catch (Exception e) {
                e.printStackTrace();

            }
            resultReporter.report(results_count, RESULT, operation);
        }
    }

    /**
     * Given a start Person, find the Forums which that Person's friends and friends of friends (excluding start Person)
     * became Members of after a given date. Return top 20 Forums, and the number of Posts in each Forum that was
     * Created by any of these Persons. For each Forum consider only those Persons which joined that particular Forum
     * after the given date. Sort results descending by the count of Posts, and then ascending by Forum identifier
     */
    public static class LdbcQuery5ToDgraph implements OperationHandler<LdbcQuery5, DgraphDbConnectionState> {


        public void executeOperation(LdbcQuery5 operation, DgraphDbConnectionState state, ResultReporter resultReporter) throws DbException {
            DgraphClient conn = state.getConn();
            List<LdbcQuery5Result> RESULT = new ArrayList<LdbcQuery5Result>();
            int results_count = 0;
            RESULT.clear();
            try {
                String queryString = file2string(new File(state.getQueryDir(), "query5.txt"));
                SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSS'+00:00'");
                sdf.setTimeZone(TimeZone.getTimeZone("GMT"));
                queryString = queryString.replaceAll("@Person@", String.valueOf(operation.personId()));
                queryString = queryString.replaceAll("@Date0@", sdf.format(operation.minDate()));

                if (state.isPrintNames())
                    System.out.println("########### LdbcQuery5");
                if (state.isPrintStrings())
                    System.out.println(queryString);

                JsonObject result = conn.query(queryString).toJsonObject();
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
                conn.close();
            } catch (Exception e) {
                e.printStackTrace();

            }
            resultReporter.report(results_count, RESULT, operation);
        }
    }


    /**
     * Given a start Person and some Tag, find the other Tags that occur together with this Tag on Posts that were
     * created by start Person’s friends and friends of friends (excluding start Person).
     * Return top 10 Tags, and the count of Posts that were created by these Persons, which contain both this Tag and
     * the given Tag. Sort results descending by count, and then ascending by Tag name.
     */
    public static class LdbcQuery6ToDgraph implements OperationHandler<LdbcQuery6, DgraphDbConnectionState> {


        public void executeOperation(LdbcQuery6 operation, DgraphDbConnectionState state, ResultReporter resultReporter) throws DbException {
            DgraphClient conn = state.getConn();
            List<LdbcQuery6Result> RESULT = new ArrayList<LdbcQuery6Result>();
            int results_count = 0;
            RESULT.clear();
            try {
                String queryString = file2string(new File(state.getQueryDir(), "query6.txt"));
                    queryString = queryString.replaceAll("@Person@", String.valueOf(operation.personId()));
                    queryString = queryString.replaceAll("@Tag@", operation.tagName());

                if (state.isPrintNames())
                    System.out.println("########### LdbcQuery6");
                if (state.isPrintStrings())
                    System.out.println(queryString);

                JsonObject result = conn.query(queryString).toJsonObject();
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
                conn.close();
            } catch (Exception e) {
                e.printStackTrace();

            }
            resultReporter.report(results_count, RESULT, operation);
        }
    }


    /**
     * Given a start Person and some Tag, find the other Tags that occur together with this Tag on Posts that were
     * created by start Person’s friends and friends of friends (excluding start Person). Return top 10 Tags, and the
     * count of Posts that were created by these Persons, which contain both this Tag and the given Tag.
     * Sort results descending by count, and then ascending by Tag name.
     */
    public static class LdbcQuery7ToDgraph implements OperationHandler<LdbcQuery7, DgraphDbConnectionState> {

        public void executeOperation(LdbcQuery7 operation, DgraphDbConnectionState state, ResultReporter resultReporter) throws DbException {
            DgraphClient conn = state.getConn();
            List<LdbcQuery7Result> RESULT = new ArrayList<LdbcQuery7Result>();
            int results_count = 0;
            RESULT.clear();
            try {
                String queryString = file2string(new File(state.getQueryDir(), "query7.txt"));
                queryString = queryString.replaceAll("@Person@", String.valueOf(operation.personId()));

                if (state.isPrintNames())
                    System.out.println("########### LdbcQuery7");
                if (state.isPrintStrings())
                    System.out.println(queryString);

                JsonObject result = conn.query(queryString).toJsonObject();
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
                conn.close();
            } catch (Exception e) {
                e.printStackTrace();

            }
            resultReporter.report(results_count, RESULT, operation);
        }
    }


    /**
     * Given a start Person, find (most recent) Comments that are replies to Posts/Comments of the start Person.
     * Only consider immediate (1-hop) replies, not the transitive (multi-hop) case. Return the top 20 reply Comments,
     * and the Person that created each reply Comment. Sort results descending by creation date of reply Comment, and
     * then ascending by identifier of reply Comment.
     */
    public static class LdbcQuery8ToDgraph implements OperationHandler<LdbcQuery8, DgraphDbConnectionState> {


        public void executeOperation(LdbcQuery8 operation, DgraphDbConnectionState state, ResultReporter resultReporter) throws DbException {
            DgraphClient conn = state.getConn();
            List<LdbcQuery8Result> RESULT = new ArrayList<LdbcQuery8Result>();
            int results_count = 0;
            RESULT.clear();
            try {
                String queryString = file2string(new File(state.getQueryDir(), "query8.txt"));
                    queryString = queryString.replaceAll("@Person@", String.valueOf(operation.personId()));

                if (state.isPrintNames())
                    System.out.println("########### LdbcQuery8");
                if (state.isPrintStrings())
                    System.out.println(queryString);

                JsonObject result = conn.query(queryString).toJsonObject();
                // TODO:
//                for(JsonElement v: result.getAsJsonArray()) {
//                    String key=(String)keys.next();
//                    results_count++;
//                    v...
//                    LdbcQuery8Result tmp = new LdbcQuery8Result(personId, personFirstName, personLastName, replyCreationDate, replyId, replyContent);
//                    if (state.isPrintResults())
//                        System.out.println(tmp.toString());
//                    RESULT.add(tmp);
//                }
                conn.close();
            } catch (Exception e) {
                e.printStackTrace();

            }
            resultReporter.report(results_count, RESULT, operation);
        }
    }

    /**
     * Given a start Person, find (most recent) Comments that are replies to Posts/Comments of the start Person.
     * Only consider immediate (1-hop) replies, not the transitive (multi-hop) case. Return the top 20 reply Comments,
     * and the Person that created each reply Comment. Sort results descending by creation date of reply Comment, and
     * then ascending by identifier of reply Comment.
     */
    public static class LdbcQuery9ToDgraph implements OperationHandler<LdbcQuery9, DgraphDbConnectionState> {


        public void executeOperation(LdbcQuery9 operation, DgraphDbConnectionState state, ResultReporter resultReporter) throws DbException {
            DgraphClient conn = state.getConn();
            List<LdbcQuery9Result> RESULT = new ArrayList<LdbcQuery9Result>();
            int results_count = 0;
            RESULT.clear();
            try {
                String queryString = file2string(new File(state.getQueryDir(), "query9.txt"));
                SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSS'+00:00'");
                sdf.setTimeZone(TimeZone.getTimeZone("GMT"));
                    queryString = queryString.replaceAll("@Person@", String.valueOf(operation.personId()));
                    queryString = queryString.replaceAll("@Date0@", sdf.format(operation.maxDate()));

                if (state.isPrintNames())
                    System.out.println("########### LdbcQuery9");
                if (state.isPrintStrings())
                    System.out.println(queryString);

                JsonObject result = conn.query(queryString).toJsonObject();
                // TODO:
//                for(JsonElement v: result.getAsJsonArray()) {
//                    String key=(String)keys.next();
//                    results_count++;
//                    v...
//                    LdbcQuery9Result tmp = new LdbcQuery9Result(personId, personFirstName, personLastName, postOrCommentId, postOrCommentContent, postOrCommentCreationDate);
//                    if (state.isPrintResults())
//                        System.out.println(tmp.toString());
//                    RESULT.add(tmp);
//                }
                conn.close();
            } catch (Exception e) {
                e.printStackTrace();

            }
            resultReporter.report(results_count, RESULT, operation);
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
    public static class LdbcQuery10ToDgraph implements OperationHandler<LdbcQuery10, DgraphDbConnectionState> {

        public void executeOperation(LdbcQuery10 operation, DgraphDbConnectionState state, ResultReporter resultReporter) throws DbException {
            DgraphClient conn = state.getConn();
            List<LdbcQuery10Result> RESULT = new ArrayList<LdbcQuery10Result>();
            int results_count = 0;
            RESULT.clear();
            try {
                String queryString = file2string(new File(state.getQueryDir(), "query10.txt"));
                    queryString = queryString.replaceAll("@Person@", String.valueOf(operation.personId()));
                    queryString = queryString.replaceAll("@HS0@", String.valueOf(operation.month()));
                    int nextMonth = operation.month() + 1;
                    if (nextMonth == 13)
                        nextMonth = 1;
                    queryString = queryString.replaceAll("@HS1@", String.valueOf(nextMonth));

                if (state.isPrintNames())
                    System.out.println("########### LdbcQuery10");
                if (state.isPrintStrings())
                    System.out.println(queryString);

                JsonObject result = conn.query(queryString).toJsonObject();
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
                conn.close();
            } catch (Exception e) {
                e.printStackTrace();

            }
            resultReporter.report(results_count, RESULT, operation);
        }
    }


    /**
     * Given a start Person, find that Person’s friends and friends of friends (excluding start Person) who started
     * Working in some Company in a given Country, before a given date (year). Return top 10 Persons, the Company they
     * worked at, and the year they started working at that Company. Sort results ascending by the start date, then
     * ascending by Person identifier, and lastly by Organization name descending.
     */
    public static class LdbcQuery11ToDgraph implements OperationHandler<LdbcQuery11, DgraphDbConnectionState> {

        public void executeOperation(LdbcQuery11 operation, DgraphDbConnectionState state, ResultReporter resultReporter) throws DbException {
            DgraphClient conn = state.getConn();
            List<LdbcQuery11Result> RESULT = new ArrayList<LdbcQuery11Result>();
            int results_count = 0;
            RESULT.clear();
            try {
                String queryString = file2string(new File(state.getQueryDir(), "query11.txt"));
                    queryString = queryString.replaceAll("@Person@", String.valueOf(operation.personId()));
                    queryString = queryString.replaceAll("@Date0@", String.valueOf(operation.workFromYear()));
                    queryString = queryString.replaceAll("@Country@", operation.countryName());

                if (state.isPrintNames())
                    System.out.println("########### LdbcQuery11");
                if (state.isPrintStrings())
                    System.out.println(queryString);

                JsonObject result = conn.query(queryString).toJsonObject();
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
                conn.close();
            } catch (Exception e) {
                e.printStackTrace();

            }
            resultReporter.report(results_count, RESULT, operation);
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
    public static class LdbcQuery12ToDgraph implements OperationHandler<LdbcQuery12, DgraphDbConnectionState> {

        public void executeOperation(LdbcQuery12 operation, DgraphDbConnectionState state, ResultReporter resultReporter) throws DbException {
            DgraphClient conn = state.getConn();
            List<LdbcQuery12Result> RESULT = new ArrayList<LdbcQuery12Result>();
            int results_count = 0;
            RESULT.clear();
            try {
                String queryString = file2string(new File(state.getQueryDir(), "query12.txt"));
                    queryString = queryString.replaceAll("@Person@", String.valueOf(operation.personId()));
                    queryString = queryString.replaceAll("@TagType@", operation.tagClassName());

                if (state.isPrintNames())
                    System.out.println("########### LdbcQuery12");
                if (state.isPrintStrings())
                    System.out.println(queryString);

                JsonObject result = conn.query(queryString).toJsonObject();
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
                conn.close();
            } catch (Exception e) {
                e.printStackTrace();

            }
            resultReporter.report(results_count, RESULT, operation);
        }
    }


    /**
     * Given two Persons, find the shortest path between these two Persons in the subgraph induced by the Knows
     * relationships. Return the length of this path.
     * – -1 : no path found
     * – 0: start person = end person
     * – > 0: regular case
     */
    public static class LdbcQuery13ToDgraph implements OperationHandler<LdbcQuery13, DgraphDbConnectionState> {

        public void executeOperation(LdbcQuery13 operation, DgraphDbConnectionState state, ResultReporter resultReporter) throws DbException {
            DgraphClient conn = state.getConn();
            List<LdbcQuery13Result> RESULT = new ArrayList<LdbcQuery13Result>();
            int results_count = 0;
            RESULT.clear();
            try {
                String queryString = file2string(new File(state.getQueryDir(), "query13.txt"));
                    queryString = queryString.replaceAll("@Person1@", String.valueOf(operation.person1Id()));
                    queryString = queryString.replaceAll("@Person2@", String.valueOf(operation.person2Id()));

                if (state.isPrintNames())
                    System.out.println("########### LdbcQuery13");
                if (state.isPrintStrings())
                    System.out.println(queryString);

                JsonObject result = conn.query(queryString).toJsonObject();
                // TODO:
//                for(JsonElement v: result.getAsJsonArray()) {
//                    String key=(String)keys.next();
//                    results_count++;
//                    v...
//                    LdbcQuery13Result tmp = new LdbcQuery13Result(shortestPathlength);
//                    if (state.isPrintResults())
//                        System.out.println(tmp.toString());
//                    RESULT.add(tmp);
//                }
                conn.close();
            } catch (Exception e) {
                e.printStackTrace();

            }
            resultReporter.report(results_count, RESULT.get(0), operation);
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
    public static class LdbcQuery14ToDgraph implements OperationHandler<LdbcQuery14, DgraphDbConnectionState> {

        public void executeOperation(LdbcQuery14 operation, DgraphDbConnectionState state, ResultReporter resultReporter) throws DbException {
            DgraphClient conn = state.getConn();
            List<LdbcQuery14Result> RESULT = new ArrayList<LdbcQuery14Result>();
            int results_count = 0;
            RESULT.clear();
            try {
                String queryString = file2string(new File(state.getQueryDir(), "query14.txt"));
                    queryString = queryString.replaceAll("@Person1@", String.valueOf(operation.person1Id()));
                    queryString = queryString.replaceAll("@Person2@", String.valueOf(operation.person2Id()));

                if (state.isPrintNames())
                    System.out.println("########### LdbcQuery14");
                if (state.isPrintStrings())
                    System.out.println(queryString);

                JsonObject result = conn.query(queryString).toJsonObject();
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
                conn.close();
            } catch (Exception e) {
                e.printStackTrace();

            }
            resultReporter.report(results_count, RESULT, operation);
        }
    }

    public static class LdbcShortQuery1ToDgraph implements OperationHandler<LdbcShortQuery1PersonProfile, DgraphDbConnectionState> {

        public void executeOperation(LdbcShortQuery1PersonProfile operation, DgraphDbConnectionState state, ResultReporter resultReporter) throws DbException {
            LdbcShortQuery1PersonProfileResult RESULT = null;
            int results_count = 0;
            DgraphClient conn = state.getConn();
            CallableStatement stmt1 = null;
            Statement stmt2 = null;

            try {
                if (state.isRunSql())
                    stmt1 = conn.prepareCall("person_view_1(?)");
                else
                    stmt1 = conn.prepareCall("person_view_1_sparql(?)");
                stmt1.setLong(1, operation.personId());

                stmt2 = conn.createStatement();

                String queryString = null;
                if (!state.isStoredProceduresEnabled()) {
                    queryString = file2string(new File(state.getQueryDir(), "s1.txt"));
                    if (state.isRunSql()) {
                        //TODO:

                    } else {
                        queryString = queryString.replaceAll("%Id%", String.format("%d", operation.personId()));
                    }
                }

                if (state.isPrintNames())
                    System.out.println("########### LdbcShortQuery1");
                if (state.isPrintStrings())
                    if (state.isStoredProceduresEnabled())
                        System.out.println("LdbcShortQuery1 (" + operation.personId() + ")");
                    else
                        System.out.println(queryString);

                boolean results = false;
                if (state.isStoredProceduresEnabled())
                    results = stmt1.execute();
                if (results || !state.isStoredProceduresEnabled()) {
                    ResultSet rs = null;
                    if (state.isStoredProceduresEnabled())
                        rs = stmt1.getResultSet();
                    else
                        rs = stmt2.executeQuery(queryString);
                    while (rs.next()) {
                        results_count++;
                        String firstName = new String(rs.getString(1).getBytes("ISO-8859-1"));
                        String lastName = new String(rs.getString(2).getBytes("ISO-8859-1"));
                        String gender = rs.getString(3);
                        long birthday = rs.getLong(4);
                        long creationDate = rs.getLong(5);
                        String locationIp = rs.getString(6);
                        String browserUsed = rs.getString(7);
                        long cityId = rs.getLong(8);
                        RESULT = new LdbcShortQuery1PersonProfileResult(firstName, lastName, birthday, locationIp, browserUsed, cityId, gender, creationDate);
                        if (state.isPrintResults())
                            System.out.println(RESULT.toString());
                    }
                }
                stmt1.close();
                stmt2.close();
                conn.close();
            } catch (SQLException e) {
                e.printStackTrace();
                try {
                    stmt1.close();
                    stmt2.close();
                    conn.close();
                } catch (SQLException e1) {
                }
            } catch (Exception e) {
                e.printStackTrace();
            }
            resultReporter.report(results_count, RESULT, operation);
        }
    }

    public static class LdbcShortQuery2ToDgraph implements OperationHandler<LdbcShortQuery2PersonPosts, DgraphDbConnectionState> {

        public void executeOperation(LdbcShortQuery2PersonPosts operation, DgraphDbConnectionState state, ResultReporter resultReporter) throws DbException {
            List<LdbcShortQuery2PersonPostsResult> RESULT = new ArrayList<LdbcShortQuery2PersonPostsResult>();
            int results_count = 0;
            DgraphClient conn = state.getConn();
            CallableStatement stmt1 = null;
            Statement stmt2 = null;

            try {
                if (state.isRunSql())
                    stmt1 = conn.prepareCall("person_view_2(?)");
                else
                    stmt1 = conn.prepareCall("person_view_2_sparql(?)");
                stmt1.setLong(1, operation.personId());
                stmt2 = conn.createStatement();

                String queryString = null;
                if (!state.isStoredProceduresEnabled()) {
                    queryString = file2string(new File(state.getQueryDir(), "s2.txt"));
                    if (state.isRunSql()) {
                        //TODO:

                    } else {
                        queryString = queryString.replaceAll("%Id%", String.format("%d", operation.personId()));
                    }
                }

                if (state.isPrintNames())
                    System.out.println("########### LdbcShortQuery2");
                if (state.isPrintStrings())
                    if (state.isStoredProceduresEnabled())
                        System.out.println("LdbcShortQuery2 (" + operation.personId() + ")");
                    else
                        System.out.println(queryString);

                boolean results = false;
                if (state.isStoredProceduresEnabled())
                    results = stmt1.execute();
                if (results || !state.isStoredProceduresEnabled()) {
                    ResultSet rs = null;
                    if (state.isStoredProceduresEnabled())
                        rs = stmt1.getResultSet();
                    else
                        rs = stmt2.executeQuery(queryString);
                    while (rs.next()) {
                        results_count++;
                        long postId;
                        if (state.isRunSql())
                            postId = rs.getLong(1);
                        else
                            postId = Long.parseLong(rs.getString(1).substring(47));
                        String postContent = rs.getString(2);
                        if (postContent == null || postContent.length() == 0)
                            postContent = new String(rs.getString(3).getBytes("ISO-8859-1"));
                        else
                            postContent = new String(postContent.getBytes("ISO-8859-1"));
                        long postCreationTime = rs.getLong(4);
                        long origPostId = 0;
                        if (state.isRunSql())
                            origPostId = rs.getLong(5);
                        else {
                            if (rs.getString(5) != null)
                                origPostId = Long.parseLong(rs.getString(5).substring(47));
                        }
                        long origPersonId = 0;
                        if (state.isRunSql())
                            origPersonId = rs.getLong(6);
                        else {
                            if (rs.getString(6) != null)
                                origPersonId = Long.parseLong(rs.getString(6).substring(47));
                        }
                        String origFirstName = rs.getString(7);
                        if (origFirstName != null)
                            origFirstName = new String(origFirstName.getBytes("ISO-8859-1"));
                        String origLastName = rs.getString(8);
                        if (origLastName != null)
                            origLastName = new String(rs.getString(8).getBytes("ISO-8859-1"));
                        LdbcShortQuery2PersonPostsResult tmp = new LdbcShortQuery2PersonPostsResult(postId, postContent, postCreationTime, origPostId, origPersonId, origFirstName, origLastName);
                        if (state.isPrintResults())
                            System.out.println(tmp.toString());
                        RESULT.add(tmp);
                    }
                }
                stmt1.close();
                stmt2.close();
                conn.close();
            } catch (SQLException e) {
                System.out.println("Err: LdbcShortQuery2 (" + operation.personId() + ")");
                e.printStackTrace();
                try {
                    stmt1.close();
                    stmt2.close();
                    conn.close();
                } catch (SQLException e1) {
                }
            } catch (Exception e) {
                System.out.println("Err: LdbcShortQuery2 (" + operation.personId() + ")");
                e.printStackTrace();
            }
            resultReporter.report(results_count, RESULT, operation);
        }
    }

    public static class LdbcShortQuery3ToDgraph implements OperationHandler<LdbcShortQuery3PersonFriends, DgraphDbConnectionState> {

        public void executeOperation(LdbcShortQuery3PersonFriends operation, DgraphDbConnectionState state, ResultReporter resultReporter) throws DbException {
            List<LdbcShortQuery3PersonFriendsResult> RESULT = new ArrayList<LdbcShortQuery3PersonFriendsResult>();
            int results_count = 0;
            DgraphClient conn = state.getConn();
            CallableStatement stmt1 = null;
            Statement stmt2 = null;

            try {
                if (state.isRunSql())
                    stmt1 = conn.prepareCall("person_view_3(?)");
                else
                    stmt1 = conn.prepareCall("person_view_3_sparql(?)");
                stmt1.setLong(1, operation.personId());
                stmt2 = conn.createStatement();

                String queryString = null;
                if (!state.isStoredProceduresEnabled()) {
                    queryString = file2string(new File(state.getQueryDir(), "s3.txt"));
                    if (state.isRunSql()) {
                        //TODO:

                    } else {
                        queryString = queryString.replaceAll("%Id%", String.format("%d", operation.personId()));
                    }
                }

                if (state.isPrintNames())
                    System.out.println("########### LdbcShortQuery3");
                if (state.isPrintStrings())
                    if (state.isStoredProceduresEnabled())
                        System.out.println("LdbcShortQuery3 (" + operation.personId() + ")");
                    else
                        System.out.println(queryString);

                boolean results = false;
                if (state.isStoredProceduresEnabled())
                    results = stmt1.execute();
                if (results || !state.isStoredProceduresEnabled()) {
                    ResultSet rs = null;
                    if (state.isStoredProceduresEnabled())
                        rs = stmt1.getResultSet();
                    else
                        rs = stmt2.executeQuery(queryString);
                    while (rs.next()) {
                        results_count++;
                        long personId;
                        if (state.isRunSql())
                            personId = rs.getLong(1);
                        else
                            personId = Long.parseLong(rs.getString(1).substring(47));
                        String firstName = new String(rs.getString(2).getBytes("ISO-8859-1"));
                        ;
                        String lastName = new String(rs.getString(3).getBytes("ISO-8859-1"));
                        ;
                        long since = rs.getLong(4);
                        LdbcShortQuery3PersonFriendsResult tmp = new LdbcShortQuery3PersonFriendsResult(personId, firstName, lastName, since);
                        if (state.isPrintResults())
                            System.out.println(tmp.toString());
                        RESULT.add(tmp);
                    }
                }
                stmt1.close();
                stmt2.close();
                conn.close();
            } catch (SQLException e) {
                e.printStackTrace();
                try {
                    stmt1.close();
                    stmt2.close();
                    conn.close();
                } catch (SQLException e1) {
                }
            } catch (Exception e) {
                e.printStackTrace();
            }
            resultReporter.report(results_count, RESULT, operation);
        }
    }

    public static class LdbcShortQuery4ToDgraph implements OperationHandler<LdbcShortQuery4MessageContent, DgraphDbConnectionState> {

        public void executeOperation(LdbcShortQuery4MessageContent operation, DgraphDbConnectionState state, ResultReporter resultReporter) throws DbException {
            LdbcShortQuery4MessageContentResult RESULT = null;
            int results_count = 0;
            DgraphClient conn = state.getConn();
            CallableStatement stmt1 = null;
            Statement stmt2 = null;

            try {
                if (state.isRunSql())
                    stmt1 = conn.prepareCall("post_view_1(?)");
                else
                    stmt1 = conn.prepareCall("post_view_1_sparql(?)");
                stmt1.setLong(1, operation.messageId());
                stmt2 = conn.createStatement();

                String queryString = null;
                if (!state.isStoredProceduresEnabled()) {
                    queryString = file2string(new File(state.getQueryDir(), "s4.txt"));
                    if (state.isRunSql()) {
                        //TODO:

                    } else {
                        queryString = queryString.replaceAll("%Id%", String.format("%d", operation.messageId()));
                    }
                }

                if (state.isPrintNames())
                    System.out.println("########### LdbcShortQuery4");
                if (state.isPrintStrings())
                    if (state.isStoredProceduresEnabled())
                        System.out.println("LdbcShortQuery4 (" + operation.messageId() + ")");
                    else
                        System.out.println(queryString);

                boolean results = false;
                if (state.isStoredProceduresEnabled())
                    results = stmt1.execute();
                if (results || !state.isStoredProceduresEnabled()) {
                    ResultSet rs = null;
                    if (state.isStoredProceduresEnabled())
                        rs = stmt1.getResultSet();
                    else
                        rs = stmt2.executeQuery(queryString);
                    while (rs.next()) {
                        results_count++;
                        String messageContent = null;
                        if (rs.getString(1) == null || rs.getString(1).length() == 0)
                            messageContent = new String(rs.getString(2).getBytes("ISO-8859-1"));
                        else
                            messageContent = new String(rs.getString(1).getBytes("ISO-8859-1"));
                        long creationDate = rs.getLong(3);
                        RESULT = new LdbcShortQuery4MessageContentResult(messageContent, creationDate);
                        if (state.isPrintResults())
                            System.out.println(RESULT.toString());
                    }
                }
                stmt1.close();
                stmt2.close();
                conn.close();
            } catch (SQLException e) {
                System.out.println("Err: LdbcShortQuery4 (" + operation.messageId() + ")");
                e.printStackTrace();
                try {
                    stmt1.close();
                    stmt2.close();
                    conn.close();
                } catch (SQLException e1) {
                }
            } catch (Exception e) {
                System.out.println("Err: LdbcShortQuery4 (" + operation.messageId() + ")");
                e.printStackTrace();
            }
            resultReporter.report(results_count, RESULT, operation);
        }
    }

    public static class LdbcShortQuery5ToDgraph implements OperationHandler<LdbcShortQuery5MessageCreator, DgraphDbConnectionState> {

        public void executeOperation(LdbcShortQuery5MessageCreator operation, DgraphDbConnectionState state, ResultReporter resultReporter) throws DbException {
            LdbcShortQuery5MessageCreatorResult RESULT = null;
            int results_count = 0;
            DgraphClient conn = state.getConn();
            CallableStatement stmt1 = null;
            Statement stmt2 = null;

            try {
                if (state.isRunSql())
                    stmt1 = conn.prepareCall("post_view_2(?)");
                else
                    stmt1 = conn.prepareCall("post_view_2_sparql(?)");
                stmt1.setLong(1, operation.messageId());
                stmt2 = conn.createStatement();

                String queryString = null;
                if (!state.isStoredProceduresEnabled()) {
                    queryString = file2string(new File(state.getQueryDir(), "s5.txt"));
                    if (state.isRunSql()) {
                        //TODO:

                    } else {
                        queryString = queryString.replaceAll("%Id%", String.format("%d", operation.messageId()));
                    }
                }

                if (state.isPrintNames())
                    System.out.println("########### LdbcShortQuery5");
                if (state.isPrintStrings())
                    if (state.isStoredProceduresEnabled())
                        System.out.println("LdbcShortQuery5 (" + operation.messageId() + ")");
                    else
                        System.out.println(queryString);

                boolean results = false;
                if (state.isStoredProceduresEnabled())
                    results = stmt1.execute();
                if (results || !state.isStoredProceduresEnabled()) {
                    ResultSet rs = null;
                    if (state.isStoredProceduresEnabled())
                        rs = stmt1.getResultSet();
                    else
                        rs = stmt2.executeQuery(queryString);
                    while (rs.next()) {
                        results_count++;
                        long personId;
                        if (state.isRunSql())
                            personId = rs.getLong(1);
                        else
                            personId = Long.parseLong(rs.getString(1).substring(47));
                        String firstName = new String(rs.getString(2).getBytes("ISO-8859-1"));
                        ;
                        String lastName = new String(rs.getString(3).getBytes("ISO-8859-1"));
                        ;
                        RESULT = new LdbcShortQuery5MessageCreatorResult(personId, firstName, lastName);
                        if (state.isPrintResults())
                            System.out.println(RESULT.toString());
                    }
                }
                stmt1.close();
                stmt2.close();
                conn.close();
            } catch (SQLException e) {
                e.printStackTrace();
                try {
                    stmt1.close();
                    stmt2.close();
                    conn.close();
                } catch (SQLException e1) {
                }
            } catch (Exception e) {
                e.printStackTrace();
            }
            resultReporter.report(results_count, RESULT, operation);
        }
    }

    public static class LdbcShortQuery6ToDgraph implements OperationHandler<LdbcShortQuery6MessageForum, DgraphDbConnectionState> {

        public void executeOperation(LdbcShortQuery6MessageForum operation, DgraphDbConnectionState state, ResultReporter resultReporter) throws DbException {
            LdbcShortQuery6MessageForumResult RESULT = null;
            int results_count = 0;
            DgraphClient conn = state.getConn();
            CallableStatement stmt1 = null;
            Statement stmt2 = null;

            try {
                if (state.isRunSql())
                    stmt1 = conn.prepareCall("post_view_3(?)");
                else
                    stmt1 = conn.prepareCall("post_view_3_sparql(?)");
                stmt1.setLong(1, operation.messageId());
                stmt2 = conn.createStatement();

                String queryString = null;
                if (!state.isStoredProceduresEnabled()) {
                    queryString = file2string(new File(state.getQueryDir(), "s6.txt"));
                    if (state.isRunSql()) {
                        //TODO:

                    } else {
                        queryString = queryString.replaceAll("%Id%", String.format("%d", operation.messageId()));
                    }
                }

                if (state.isPrintNames())
                    System.out.println("########### LdbcShortQuery6");
                if (state.isPrintStrings())
                    if (state.isStoredProceduresEnabled())
                        System.out.println("LdbcShortQuery6 (" + operation.messageId() + ")");
                    else
                        System.out.println(queryString);

                boolean results = false;
                if (state.isStoredProceduresEnabled())
                    results = stmt1.execute();
                if (results || !state.isStoredProceduresEnabled()) {
                    ResultSet rs = null;
                    if (state.isStoredProceduresEnabled())
                        rs = stmt1.getResultSet();
                    else
                        rs = stmt2.executeQuery(queryString);
                    while (rs.next()) {
                        results_count++;
                        long forumId;
                        if (state.isRunSql())
                            forumId = rs.getLong(1);
                        else
                            forumId = Long.parseLong(rs.getString(1).substring(48));
                        String forumTitle = new String(rs.getString(2).getBytes("ISO-8859-1"));
                        ;
                        long moderatorId;
                        if (state.isRunSql())
                            moderatorId = rs.getLong(3);
                        else
                            moderatorId = Long.parseLong(rs.getString(3).substring(47));
                        String moderatorFirstName = new String(rs.getString(4).getBytes("ISO-8859-1"));
                        ;
                        String moderatorLastName = new String(rs.getString(5).getBytes("ISO-8859-1"));
                        ;
                        RESULT = new LdbcShortQuery6MessageForumResult(forumId, forumTitle, moderatorId, moderatorFirstName, moderatorLastName);
                        if (state.isPrintResults())
                            System.out.println(RESULT.toString());
                    }
                }
                stmt1.close();
                stmt2.close();
                conn.close();
            } catch (SQLException e) {
                e.printStackTrace();
                try {
                    stmt1.close();
                    stmt2.close();
                    conn.close();
                } catch (SQLException e1) {
                }
            } catch (Exception e) {
                e.printStackTrace();
            }
            resultReporter.report(results_count, RESULT, operation);
        }
    }

    public static class LdbcShortQuery7ToDgraph implements OperationHandler<LdbcShortQuery7MessageReplies, DgraphDbConnectionState> {

        public void executeOperation(LdbcShortQuery7MessageReplies operation, DgraphDbConnectionState state, ResultReporter resultReporter) throws DbException {
            List<LdbcShortQuery7MessageRepliesResult> RESULT = new ArrayList<LdbcShortQuery7MessageRepliesResult>();
            int results_count = 0;
            DgraphClient conn = state.getConn();
            CallableStatement stmt1 = null;
            Statement stmt2 = null;

            try {
                if (state.isRunSql())
                    stmt1 = conn.prepareCall("post_view_4(?)");
                else
                    stmt1 = conn.prepareCall("post_view_4_sparql(?)");
                stmt1.setLong(1, operation.messageId());
                stmt2 = conn.createStatement();

                String queryString = null;
                if (!state.isStoredProceduresEnabled()) {
                    queryString = file2string(new File(state.getQueryDir(), "s7.txt"));
                    if (state.isRunSql()) {
                        //TODO:

                    } else {
                        queryString = queryString.replaceAll("%Id%", String.format("%d", operation.messageId()));
                    }
                }

                if (state.isPrintNames())
                    System.out.println("########### LdbcShortQuery7");
                if (state.isPrintStrings())
                    if (state.isStoredProceduresEnabled())
                        System.out.println("LdbcShortQuery7 (" + operation.messageId() + ")");
                    else
                        System.out.println(queryString);

                boolean results = false;
                if (state.isStoredProceduresEnabled())
                    results = stmt1.execute();
                if (results || !state.isStoredProceduresEnabled()) {
                    ResultSet rs = null;
                    if (state.isStoredProceduresEnabled())
                        rs = stmt1.getResultSet();
                    else
                        rs = stmt2.executeQuery(queryString);
                    while (rs.next()) {
                        results_count++;
                        long commentId;
                        if (state.isRunSql())
                            commentId = rs.getLong(1);
                        else
                            commentId = Long.parseLong(rs.getString(1).substring(47));
                        String commentContent = new String(rs.getString(2).getBytes("ISO-8859-1"));
                        ;
                        long creationDate = rs.getLong(3);
                        long personId;
                        if (state.isRunSql())
                            personId = rs.getLong(4);
                        else
                            personId = Long.parseLong(rs.getString(4).substring(47));
                        String firstName = new String(rs.getString(5).getBytes("ISO-8859-1"));
                        ;
                        String lastName = new String(rs.getString(6).getBytes("ISO-8859-1"));
                        ;
                        int knows = rs.getInt(7);
                        boolean knows_b = (knows == 1) ? true : false;
                        LdbcShortQuery7MessageRepliesResult tmp = new LdbcShortQuery7MessageRepliesResult(commentId, commentContent, creationDate, personId, firstName, lastName, knows_b);
                        if (state.isPrintResults())
                            System.out.println(tmp.toString());
                        RESULT.add(tmp);
                    }
                }
                stmt1.close();
                stmt2.close();
                conn.close();
            } catch (SQLException e) {
                e.printStackTrace();
                try {
                    stmt1.close();
                    stmt2.close();
                    conn.close();
                } catch (SQLException e1) {
                }
            } catch (Exception e) {
                e.printStackTrace();
            }
            resultReporter.report(results_count, RESULT, operation);
        }
    }

    public static class LdbcUpdate1AddPersonToDgraph implements OperationHandler<LdbcUpdate1AddPerson, DgraphDbConnectionState> {


        public void executeOperation(LdbcUpdate1AddPerson operation, DgraphDbConnectionState state, ResultReporter resultReporter) throws DbException {
            DgraphClient conn = state.getConn();
            PreparedStatement cs = null;
            try {
                if (state.isPrintNames())
                    System.out.println("########### LdbcUpdate1");
                if (state.isPrintStrings()) {
                    System.out.println(operation.personId() + " " + operation.personFirstName() + " " + operation.personLastName());
                    System.out.println(operation.gender());
                    System.out.println(operation.birthday());
                    System.out.println(operation.creationDate());
                    System.out.println(operation.locationIp());
                    System.out.println(operation.browserUsed());
                    System.out.println(operation.cityId());
                    System.out.println("[");
                    for (String lan : operation.languages()) {
                        System.out.println(lan);
                    }
                    System.out.println("]");
                    System.out.println("[");
                    for (String email : operation.emails()) {
                        System.out.println(email);
                    }
                    System.out.println("]");
                    System.out.println("[");
                    for (long tag : operation.tagIds()) {
                        System.out.println(tag);
                    }
                    System.out.println("]");
                    System.out.println("[");
                    for (Organization tag : operation.studyAt()) {
                        System.out.println(tag.organizationId() + " - " + tag.year());
                    }
                    System.out.println("]");
                    System.out.println("[");
                    for (Organization tag : operation.workAt()) {
                        System.out.println(tag.organizationId() + " - " + tag.year());
                    }
                    System.out.println("]");
                }
                String queryString = "LdbcUpdate1AddPerson(?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)";
                cs = conn.prepareStatement(queryString);
                cs.setLong(1, operation.personId());
                cs.setString(2, new String(operation.personFirstName().getBytes("UTF-8"), "ISO-8859-1"));
                cs.setString(3, new String(operation.personLastName().getBytes("UTF-8"), "ISO-8859-1"));
                cs.setString(4, operation.gender());
                DateFormat df = new SimpleDateFormat("yyyy.MM.dd HH:mm:ss.SSS'+00:00'");
                df.setTimeZone(TimeZone.getTimeZone("GMT"));
                cs.setString(5, df.format(operation.birthday()));
                cs.setString(6, df.format(operation.creationDate()));
                cs.setString(7, operation.locationIp());
                cs.setString(8, operation.browserUsed());
                cs.setLong(9, operation.cityId());
                cs.setArray(10, conn.createArrayOf("varchar", operation.languages().toArray(new String[operation.languages().size()])));
                cs.setArray(11, conn.createArrayOf("varchar", operation.emails().toArray(new String[operation.emails().size()])));
                Long tagIds1[] = new Long[operation.tagIds().size()];
                int i = 0;
                for (long temp : operation.tagIds()) {
                    tagIds1[i++] = temp;
                }
                cs.setArray(12, conn.createArrayOf("int", tagIds1));
                Long universityIds[] = new Long[operation.studyAt().size()];
                Integer universityYears[] = new Integer[operation.studyAt().size()];
                i = 0;
                for (Organization temp : operation.studyAt()) {
                    universityIds[i] = temp.organizationId();
                    universityYears[i++] = temp.year();
                }
                cs.setArray(13, conn.createArrayOf("int", universityIds));
                cs.setArray(14, conn.createArrayOf("int", universityYears));
                Long companyIds[] = new Long[operation.workAt().size()];
                Integer companyYears[] = new Integer[operation.workAt().size()];
                i = 0;
                for (Organization temp : operation.workAt()) {
                    companyIds[i] = temp.organizationId();
                    companyYears[i++] = temp.year();
                }
                cs.setArray(15, conn.createArrayOf("int", companyIds));
                cs.setArray(16, conn.createArrayOf("int", companyYears));
                cs.execute();
                cs.close();
                conn.close();
            } catch (Throwable e) {
                e.printStackTrace();
                try {
                    cs.close();
                    conn.close();
                } catch (SQLException e1) {
                }
            }
            resultReporter.report(0, LdbcNoResult.INSTANCE, operation);
        }
    }

    public static class LdbcUpdate1AddPersonToDgraphSparql implements OperationHandler<LdbcUpdate1AddPerson, DgraphDbConnectionState> {

        public void executeOperation(LdbcUpdate1AddPerson operation, DgraphDbConnectionState state, ResultReporter resultReporter) throws DbException {
            try {
                DgraphClient conn = state.getConn();
                if (state.isPrintNames())
                    System.out.println("########### LdbcUpdate1");
                if (state.isPrintStrings()) {
                    System.out.println(operation.personId() + " " + operation.personFirstName() + " " + operation.personLastName());
                    System.out.println(operation.gender());
                    System.out.println(operation.birthday());
                    System.out.println(operation.creationDate());
                    System.out.println(operation.locationIp());
                    System.out.println(operation.browserUsed());
                    System.out.println(operation.cityId());
                    System.out.println("[");
                    for (String lan : operation.languages()) {
                        System.out.println(lan);
                    }
                    System.out.println("]");
                    System.out.println("[");
                    for (String email : operation.emails()) {
                        System.out.println(email);
                    }
                    System.out.println("]");
                    System.out.println("[");
                    for (long tag : operation.tagIds()) {
                        System.out.println(tag);
                    }
                    System.out.println("]");
                    System.out.println("[");
                    for (Organization tag : operation.studyAt()) {
                        System.out.println(tag.organizationId() + " - " + tag.year());
                    }
                    System.out.println("]");
                    System.out.println("[");
                    for (Organization tag : operation.workAt()) {
                        System.out.println(tag.organizationId() + " - " + tag.year());
                    }
                    System.out.println("]");
                }
                //TODO: This has to be done with and WITHOUT stored procedure
                String queryString = "LdbcUpdateSparql(?)";
                PreparedStatement cs = conn.prepareStatement(queryString);
                String personUri = "<http://www.ldbc.eu/ldbc_socialnet/1.0/data/pers" + String.format("%020d", operation.personId()) + ">";
                DateFormat df1 = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSS'+00:00'");
                DateFormat df2 = new SimpleDateFormat("yyyy-MM-dd");
                df1.setTimeZone(TimeZone.getTimeZone("GMT"));
                df2.setTimeZone(TimeZone.getTimeZone("GMT"));
                String triplets[] = new String[10 + operation.languages().size() + operation.emails().size() + operation.tagIds().size() + operation.studyAt().size() + operation.workAt().size()];
                triplets[0] = personUri + " a <http://www.ldbc.eu/ldbc_socialnet/1.0/vocabulary/Person> .";
                triplets[1] = personUri + " <http://www.ldbc.eu/ldbc_socialnet/1.0/vocabulary/firstName> \"" + operation.personFirstName() + "\" .";
                triplets[2] = personUri + " <http://www.ldbc.eu/ldbc_socialnet/1.0/vocabulary/lastName> \"" + operation.personLastName() + "\" .";
                triplets[3] = personUri + " <http://www.ldbc.eu/ldbc_socialnet/1.0/vocabulary/gender> \"" + operation.gender() + "\" .";
                triplets[4] = personUri + " <http://www.ldbc.eu/ldbc_socialnet/1.0/vocabulary/birthday> \"" + df2.format(operation.birthday()) + "\"^^xsd:date .";
                triplets[5] = personUri + " <http://www.ldbc.eu/ldbc_socialnet/1.0/vocabulary/creationDate> \"" + df1.format(operation.creationDate()) + "\"^^xsd:dateTime .";
                triplets[6] = personUri + " <http://www.ldbc.eu/ldbc_socialnet/1.0/vocabulary/locationIP> \"" + operation.locationIp() + "\" .";
                triplets[7] = personUri + " <http://www.ldbc.eu/ldbc_socialnet/1.0/vocabulary/browserUsed> \"" + operation.browserUsed() + "\" .";
                triplets[8] = personUri + " <http://www.ldbc.eu/ldbc_socialnet/1.0/vocabulary/isLocatedIn> <" + state.placeUri(operation.cityId()) + "> .";
                triplets[9] = personUri + " <http://www.ldbc.eu/ldbc_socialnet/1.0/vocabulary/id> \"" + operation.personId() + "\"^^xsd:long .";
                int j = 10;
                for (int k = 0; k < operation.languages().size(); k++, j++)
                    triplets[j] = personUri + " <http://www.ldbc.eu/ldbc_socialnet/1.0/vocabulary/speaks> \"" + operation.languages().get(k) + "\" .";
                for (int k = 0; k < operation.emails().size(); k++, j++)
                    triplets[j] = personUri + " <http://www.ldbc.eu/ldbc_socialnet/1.0/vocabulary/email> \"" + operation.emails().get(k) + "\" .";
                for (int k = 0; k < operation.tagIds().size(); k++, j++)
                    triplets[j] = personUri + " <http://www.ldbc.eu/ldbc_socialnet/1.0/vocabulary/hasInterest> <" + state.tagUri(operation.tagIds().get(k)) + "> .";
                for (int k = 0; k < operation.studyAt().size(); k++, j++)
                    triplets[j] = personUri + " <http://www.ldbc.eu/ldbc_socialnet/1.0/vocabulary/studyAt> [ <http://www.ldbc.eu/ldbc_socialnet/1.0/vocabulary/hasOrganisation> <" + state.universityUri(operation.studyAt().get(k).organizationId()) + ">; <http://www.ldbc.eu/ldbc_socialnet/1.0/vocabulary/classYear> \"" + operation.studyAt().get(k).year() + "\"] .";
                for (int k = 0; k < operation.workAt().size(); k++, j++)
                    triplets[j] = personUri + " <http://www.ldbc.eu/ldbc_socialnet/1.0/vocabulary/workAt> [ <http://www.ldbc.eu/ldbc_socialnet/1.0/vocabulary/hasOrganisation> <" + state.companyUri(operation.workAt().get(k).organizationId()) + ">; <http://www.ldbc.eu/ldbc_socialnet/1.0/vocabulary/workFrom> \"" + operation.workAt().get(k).year() + "\"] .";
                cs.setArray(1, conn.createArrayOf("varchar", triplets));
                cs.execute();
                cs.close();
                conn.close();
            } catch (Throwable e) {
                e.printStackTrace();
            }

            resultReporter.report(0, LdbcNoResult.INSTANCE, operation);
        }
    }

    public static class LdbcUpdate2AddPostLikeToDgraph implements OperationHandler<LdbcUpdate2AddPostLike, DgraphDbConnectionState> {


        public void executeOperation(LdbcUpdate2AddPostLike operation, DgraphDbConnectionState state, ResultReporter resultReporter) throws DbException {
            DgraphClient conn = state.getConn();
            CallableStatement cs = null;
            try {
                if (state.isPrintNames())
                    System.out.println("########### LdbcUpdate2");
                if (state.isPrintStrings())
                    System.out.println(operation.personId() + " " + operation.postId() + " " + operation.creationDate());
                String queryString = "{call LdbcUpdate2AddPostLike(?, ?, ?)}";
                cs = conn.prepareCall(queryString);
                cs.setLong(1, operation.personId());
                cs.setLong(2, operation.postId());
                DateFormat df = new SimpleDateFormat("yyyy.MM.dd HH:mm:ss.SSS'+00:00'");
                df.setTimeZone(TimeZone.getTimeZone("GMT"));
                cs.setString(3, df.format(operation.creationDate()));
                cs.execute();
                cs.close();
                conn.close();
            } catch (Throwable e) {
                e.printStackTrace();
                try {
                    cs.close();
                    conn.close();
                } catch (SQLException e1) {
                }
            }
            resultReporter.report(0, LdbcNoResult.INSTANCE, operation);
        }
    }

    public static class LdbcUpdate2AddPostLikeToDgraphSparql implements OperationHandler<LdbcUpdate2AddPostLike, DgraphDbConnectionState> {

        public void executeOperation(LdbcUpdate2AddPostLike operation, DgraphDbConnectionState state, ResultReporter resultReporter) throws DbException {
            try {
                DgraphClient conn = state.getConn();
                if (state.isPrintNames())
                    System.out.println("########### LdbcUpdate2");
                if (state.isPrintStrings())
                    System.out.println(operation.personId() + " " + operation.postId() + " " + operation.creationDate());
                //TODO: This has to be done with and WITHOUT stored procedure
                String queryString = "LdbcUpdateSparql(?)";
                PreparedStatement cs = conn.prepareStatement(queryString);
                String personUri = "<http://www.ldbc.eu/ldbc_socialnet/1.0/data/pers" + String.format("%020d", operation.personId()) + ">";
                String postUri = "<http://www.ldbc.eu/ldbc_socialnet/1.0/data/post" + String.format("%020d", operation.postId()) + ">";
                DateFormat df1 = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSS'+00:00'");
                df1.setTimeZone(TimeZone.getTimeZone("GMT"));
                String triplets[] = new String[1];
                triplets[0] = personUri + " <http://www.ldbc.eu/ldbc_socialnet/1.0/vocabulary/likes> [ <http://www.ldbc.eu/ldbc_socialnet/1.0/vocabulary/hasPost> " + postUri + "; <http://www.ldbc.eu/ldbc_socialnet/1.0/vocabulary/creationDate> \"" + df1.format(operation.creationDate()) + "\"^^xsd:dateTime ] .";
                cs.setArray(1, conn.createArrayOf("varchar", triplets));
                cs.execute();
                cs.close();
                conn.close();
            } catch (Throwable e) {
                e.printStackTrace();
            }
            resultReporter.report(0, LdbcNoResult.INSTANCE, operation);
        }
    }

    public static class LdbcUpdate3AddCommentLikeToDgraph implements OperationHandler<LdbcUpdate3AddCommentLike, DgraphDbConnectionState> {

        public void executeOperation(LdbcUpdate3AddCommentLike operation, DgraphDbConnectionState state, ResultReporter resultReporter) throws DbException {
            DgraphClient conn = state.getConn();
            CallableStatement cs = null;
            try {
                if (state.isPrintNames())
                    System.out.println("########### LdbcUpdate3");
                if (state.isPrintStrings())
                    System.out.println(operation.personId() + " " + operation.commentId() + " " + operation.creationDate());
                String queryString = "{call LdbcUpdate2AddPostLike(?, ?, ?)}";
                cs = conn.prepareCall(queryString);
                cs.setLong(1, operation.personId());
                cs.setLong(2, operation.commentId());
                DateFormat df = new SimpleDateFormat("yyyy.MM.dd HH:mm:ss.SSS'+00:00'");
                df.setTimeZone(TimeZone.getTimeZone("GMT"));
                cs.setString(3, df.format(operation.creationDate()));
                cs.execute();
                cs.close();
                conn.close();
            } catch (Throwable e) {
                e.printStackTrace();
                try {
                    cs.close();
                    conn.close();
                } catch (SQLException e1) {
                }
            }
            resultReporter.report(0, LdbcNoResult.INSTANCE, operation);
        }
    }

    public static class LdbcUpdate3AddCommentLikeToDgraphSparql implements OperationHandler<LdbcUpdate3AddCommentLike, DgraphDbConnectionState> {

        public void executeOperation(LdbcUpdate3AddCommentLike operation, DgraphDbConnectionState state, ResultReporter resultReporter) throws DbException {
            try {
                DgraphClient conn = state.getConn();
                if (state.isPrintNames())
                    System.out.println("########### LdbcUpdate3");
                if (state.isPrintStrings())
                    System.out.println(operation.personId() + " " + operation.commentId() + " " + operation.creationDate());
                //TODO: This has to be done with and WITHOUT stored procedure
                String queryString = "LdbcUpdateSparql(?)";
                PreparedStatement cs = conn.prepareStatement(queryString);
                String personUri = "<http://www.ldbc.eu/ldbc_socialnet/1.0/data/pers" + String.format("%020d", operation.personId()) + ">";
                String commentUri = "<http://www.ldbc.eu/ldbc_socialnet/1.0/data/comm" + String.format("%020d", operation.commentId()) + ">";
                DateFormat df1 = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSS'+00:00'");
                df1.setTimeZone(TimeZone.getTimeZone("GMT"));
                String triplets[] = new String[1];
                triplets[0] = personUri + " <http://www.ldbc.eu/ldbc_socialnet/1.0/vocabulary/likes> [ <http://www.ldbc.eu/ldbc_socialnet/1.0/vocabulary/hasComment> " + commentUri + "; <http://www.ldbc.eu/ldbc_socialnet/1.0/vocabulary/creationDate> \"" + df1.format(operation.creationDate()) + "\"^^xsd:dateTime ] .";
                cs.setArray(1, conn.createArrayOf("varchar", triplets));
                cs.execute();
                cs.close();
                conn.close();
            } catch (Throwable e) {
                e.printStackTrace();
            }
            resultReporter.report(0, LdbcNoResult.INSTANCE, operation);

        }
    }

    public static class LdbcUpdate4AddForumToDgraph implements OperationHandler<LdbcUpdate4AddForum, DgraphDbConnectionState> {


        public void executeOperation(LdbcUpdate4AddForum operation, DgraphDbConnectionState state, ResultReporter resultReporter) throws DbException {
            DgraphClient conn = state.getConn();
            CallableStatement cs = null;
            try {
                if (state.isPrintNames())
                    System.out.println("########### LdbcUpdate4");
                if (state.isPrintStrings()) {
                    System.out.println(operation.forumId() + " " + operation.forumTitle());
                    System.out.println(operation.creationDate() + " " + operation.moderatorPersonId());
                    System.out.println("[");
                    for (long tag : operation.tagIds()) {
                        System.out.println(tag);
                    }
                    System.out.println("]");
                }
                String queryString = "LdbcUpdate4AddForum(?, ?, ?, ?, ?)";
                cs = conn.prepareCall(queryString);
                cs.setLong(1, operation.forumId());
                cs.setString(2, new String(operation.forumTitle().getBytes("UTF-8"), "ISO-8859-1"));
                DateFormat df = new SimpleDateFormat("yyyy.MM.dd HH:mm:ss.SSS'+00:00'");
                df.setTimeZone(TimeZone.getTimeZone("GMT"));
                cs.setString(3, df.format(operation.creationDate()));
                cs.setLong(4, operation.moderatorPersonId());
                Long tagIds1[] = new Long[operation.tagIds().size()];
                int i = 0;
                for (long temp : operation.tagIds()) {
                    tagIds1[i++] = temp;
                }
                cs.setArray(5, conn.createArrayOf("int", tagIds1));
                cs.execute();
                cs.close();
                conn.close();
            } catch (Throwable e) {
                e.printStackTrace();
                try {
                    cs.close();
                    conn.close();
                } catch (SQLException e1) {
                }
            }
            resultReporter.report(0, LdbcNoResult.INSTANCE, operation);
        }
    }

    public static class LdbcUpdate4AddForumToDgraphSparql implements OperationHandler<LdbcUpdate4AddForum, DgraphDbConnectionState> {


        public void executeOperation(LdbcUpdate4AddForum operation, DgraphDbConnectionState state, ResultReporter resultReporter) throws DbException {
            try {
                DgraphClient conn = state.getConn();
                if (state.isPrintNames())
                    System.out.println("########### LdbcUpdate4");
                if (state.isPrintStrings()) {
                    System.out.println(operation.forumId() + " " + operation.forumTitle());
                    System.out.println(operation.creationDate() + " " + operation.moderatorPersonId());
                    System.out.println("[");
                    for (long tag : operation.tagIds()) {
                        System.out.println(tag);
                    }
                    System.out.println("]");
                }
                //TODO: This has to be done with and WITHOUT stored procedure
                String queryString = "LdbcUpdateSparql(?)";
                PreparedStatement cs = conn.prepareStatement(queryString);
                String forumUri = "<http://www.ldbc.eu/ldbc_socialnet/1.0/data/forum" + String.format("%020d", operation.forumId()) + ">";
                String moderatorUri = "<http://www.ldbc.eu/ldbc_socialnet/1.0/data/pers" + String.format("%020d", operation.moderatorPersonId()) + ">";
                DateFormat df1 = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSS'+00:00'");
                df1.setTimeZone(TimeZone.getTimeZone("GMT"));
                String triplets[] = new String[5 + operation.tagIds().size()];
                triplets[0] = forumUri + " a <http://www.ldbc.eu/ldbc_socialnet/1.0/vocabulary/Forum> .";
                triplets[1] = forumUri + " <http://www.ldbc.eu/ldbc_socialnet/1.0/vocabulary/title> \"" + operation.forumTitle() + "\" .";
                triplets[2] = forumUri + " <http://www.ldbc.eu/ldbc_socialnet/1.0/vocabulary/creationDate> \"" + df1.format(operation.creationDate()) + "\"^^xsd:dateTime .";
                triplets[3] = forumUri + " <http://www.ldbc.eu/ldbc_socialnet/1.0/vocabulary/hasModerator> " + moderatorUri + " .";
                triplets[4] = forumUri + " <http://www.ldbc.eu/ldbc_socialnet/1.0/vocabulary/id> \"" + operation.forumId() + "\"^^xsd:long . ";
                for (int k = 0; k < operation.tagIds().size(); k++)
                    triplets[5 + k] = forumUri + " <http://www.ldbc.eu/ldbc_socialnet/1.0/vocabulary/hasTag> <" + state.tagUri(operation.tagIds().get(k)) + "> .";
                cs.setArray(1, conn.createArrayOf("varchar", triplets));
                cs.execute();
                cs.close();
                conn.close();
            } catch (Throwable e) {
                e.printStackTrace();
            }
            resultReporter.report(0, LdbcNoResult.INSTANCE, operation);
        }
    }

    public static class LdbcUpdate5AddForumMembershipToDgraph implements OperationHandler<LdbcUpdate5AddForumMembership, DgraphDbConnectionState> {

        public void executeOperation(LdbcUpdate5AddForumMembership operation, DgraphDbConnectionState state, ResultReporter resultReporter) throws DbException {
            DgraphClient conn = state.getConn();
            CallableStatement cs = null;
            try {
                if (state.isPrintNames())
                    System.out.println("########### LdbcUpdate5");
                if (state.isPrintStrings())
                    System.out.println(operation.forumId() + " " + operation.personId() + " " + operation.joinDate());
                String queryString = "{call LdbcUpdate5AddForumMembership(?, ?, ?)}";
                cs = conn.prepareCall(queryString);
                cs.setLong(1, operation.forumId());
                cs.setLong(2, operation.personId());
                DateFormat df = new SimpleDateFormat("yyyy.MM.dd HH:mm:ss.SSS'+00:00'");
                df.setTimeZone(TimeZone.getTimeZone("GMT"));
                cs.setString(3, df.format(operation.joinDate()));
                cs.execute();
                cs.close();
                conn.close();
            } catch (Throwable e) {
                e.printStackTrace();
                try {
                    cs.close();
                    conn.close();
                } catch (SQLException e1) {
                }
            }
            resultReporter.report(0, LdbcNoResult.INSTANCE, operation);
        }
    }

    public static class LdbcUpdate5AddForumMembershipToDgraphSparql implements OperationHandler<LdbcUpdate5AddForumMembership, DgraphDbConnectionState> {


        public void executeOperation(LdbcUpdate5AddForumMembership operation, DgraphDbConnectionState state, ResultReporter resultReporter) throws DbException {
            try {
                DgraphClient conn = state.getConn();
                if (state.isPrintNames())
                    System.out.println("########### LdbcUpdate5");
                if (state.isPrintStrings())
                    System.out.println(operation.forumId() + " " + operation.personId() + " " + operation.joinDate());
                //TODO: This has to be done with and WITHOUT stored procedure
                String queryString = "LdbcUpdateSparql(?)";
                PreparedStatement cs = conn.prepareStatement(queryString);
                String forumUri = "<http://www.ldbc.eu/ldbc_socialnet/1.0/data/forum" + String.format("%020d", operation.forumId()) + ">";
                String memberUri = "<http://www.ldbc.eu/ldbc_socialnet/1.0/data/pers" + String.format("%020d", operation.personId()) + ">";
                DateFormat df1 = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSS'+00:00'");
                df1.setTimeZone(TimeZone.getTimeZone("GMT"));
                String triplets[] = new String[1];
                triplets[0] = forumUri + " <http://www.ldbc.eu/ldbc_socialnet/1.0/vocabulary/hasMember> [ <http://www.ldbc.eu/ldbc_socialnet/1.0/vocabulary/hasPerson> " + memberUri + "; <http://www.ldbc.eu/ldbc_socialnet/1.0/vocabulary/joinDate> \"" + df1.format(operation.joinDate()) + "\"] .";
                cs.setArray(1, conn.createArrayOf("varchar", triplets));
                cs.execute();
                cs.close();
                conn.close();
            } catch (Throwable e) {
                e.printStackTrace();
            }
            resultReporter.report(0, LdbcNoResult.INSTANCE, operation);
        }
    }

    public static class LdbcUpdate6AddPostToDgraph implements OperationHandler<LdbcUpdate6AddPost, DgraphDbConnectionState> {


        public void executeOperation(LdbcUpdate6AddPost operation, DgraphDbConnectionState state, ResultReporter resultReporter) throws DbException {
            DgraphClient conn = state.getConn();
            CallableStatement cs = null;
            try {
                if (state.isPrintNames())
                    System.out.println("########### LdbcUpdate6");
                if (state.isPrintStrings()) {
                    System.out.println(operation.postId() + " " + operation.imageFile());
                    System.out.println(operation.creationDate() + " " + operation.locationIp());
                    System.out.println(operation.browserUsed() + " " + operation.language());
                    System.out.println(operation.content());
                    System.out.println(operation.length() + " " + operation.authorPersonId());
                    System.out.println(operation.forumId() + " " + operation.countryId());
                    System.out.println("[");
                    for (long tag : operation.tagIds()) {
                        System.out.println(tag);
                    }
                    System.out.println("]");
                }
                String queryString = "LdbcUpdate6AddPost(?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)";
                cs = conn.prepareCall(queryString);
                cs.setLong(1, operation.postId());
                cs.setString(2, new String(operation.imageFile().getBytes("UTF-8"), "ISO-8859-1"));
                DateFormat df = new SimpleDateFormat("yyyy.MM.dd HH:mm:ss.SSS'+00:00'");
                df.setTimeZone(TimeZone.getTimeZone("GMT"));
                cs.setString(3, df.format(operation.creationDate()));
                cs.setString(4, operation.locationIp());
                cs.setString(5, operation.browserUsed());
                cs.setString(6, operation.language());
                cs.setString(7, new String(operation.content().getBytes("UTF-8"), "ISO-8859-1"));
                cs.setInt(8, operation.length());
                cs.setLong(9, operation.authorPersonId());
                cs.setLong(10, operation.forumId());
                cs.setLong(11, operation.countryId());
                Long tagIds1[] = new Long[operation.tagIds().size()];
                int i = 0;
                for (long temp : operation.tagIds()) {
                    tagIds1[i++] = temp;
                }
                cs.setArray(12, conn.createArrayOf("int", tagIds1));
                cs.execute();
                cs.close();
                conn.close();
            } catch (Throwable e) {
                e.printStackTrace();
                try {
                    cs.close();
                    conn.close();
                } catch (SQLException e1) {
                }
            }
            resultReporter.report(0, LdbcNoResult.INSTANCE, operation);
        }
    }

    public static class LdbcUpdate6AddPostToDgraphSparql implements OperationHandler<LdbcUpdate6AddPost, DgraphDbConnectionState> {


        public void executeOperation(LdbcUpdate6AddPost operation, DgraphDbConnectionState state, ResultReporter resultReporter) throws DbException {
            try {
                DgraphClient conn = state.getConn();
                if (state.isPrintNames())
                    System.out.println("########### LdbcUpdate6");
                if (state.isPrintStrings()) {
                    System.out.println(operation.postId() + " " + operation.imageFile());
                    System.out.println(operation.creationDate() + " " + operation.locationIp());
                    System.out.println(operation.browserUsed() + " " + operation.language());
                    System.out.println(operation.content());
                    System.out.println(operation.length() + " " + operation.authorPersonId());
                    System.out.println(operation.forumId() + " " + operation.countryId());
                    System.out.println("[");
                    for (long tag : operation.tagIds()) {
                        System.out.println(tag);
                    }
                    System.out.println("]");
                }
                //TODO: This has to be done with and WITHOUT stored procedure
                String queryString = "LdbcUpdateSparql(?)";
                PreparedStatement cs = conn.prepareStatement(queryString);
                String postUri = "<http://www.ldbc.eu/ldbc_socialnet/1.0/data/post" + String.format("%020d", operation.postId()) + ">";
                String forumUri = "<http://www.ldbc.eu/ldbc_socialnet/1.0/data/forum" + String.format("%020d", operation.forumId()) + ">";
                String authorUri = "<http://www.ldbc.eu/ldbc_socialnet/1.0/data/pers" + String.format("%020d", operation.authorPersonId()) + ">";
                DateFormat df1 = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSS'+00:00'");
                df1.setTimeZone(TimeZone.getTimeZone("GMT"));
                if (operation.imageFile().equals("")) {
                    String triplets[] = new String[11 + operation.tagIds().size()];
                    triplets[0] = postUri + " a <http://www.ldbc.eu/ldbc_socialnet/1.0/vocabulary/Post> .";
                    triplets[1] = postUri + " <http://www.ldbc.eu/ldbc_socialnet/1.0/vocabulary/locationIP> \"" + operation.locationIp() + "\" .";
                    triplets[2] = postUri + " <http://www.ldbc.eu/ldbc_socialnet/1.0/vocabulary/creationDate> \"" + df1.format(operation.creationDate()) + "\"^^xsd:dateTime .";
                    triplets[3] = postUri + " <http://www.ldbc.eu/ldbc_socialnet/1.0/vocabulary/browserUsed> \"" + operation.browserUsed() + "\" .";
                    triplets[4] = postUri + " <http://www.ldbc.eu/ldbc_socialnet/1.0/vocabulary/language> \"" + operation.language() + "\" .";
                    triplets[5] = postUri + " <http://www.ldbc.eu/ldbc_socialnet/1.0/vocabulary/content> \"" + new String(operation.content().getBytes("UTF-8"), "ISO-8859-1") + "\" .";
                    triplets[6] = postUri + " <http://www.ldbc.eu/ldbc_socialnet/1.0/vocabulary/length> \"" + operation.length() + "\" .";
                    triplets[7] = postUri + " <http://www.ldbc.eu/ldbc_socialnet/1.0/vocabulary/hasCreator> " + authorUri + " .";
                    triplets[8] = postUri + " <http://www.ldbc.eu/ldbc_socialnet/1.0/vocabulary/isLocatedIn> <" + state.placeUri(operation.countryId()) + "> .";
                    triplets[9] = postUri + " <http://www.ldbc.eu/ldbc_socialnet/1.0/vocabulary/id> \"" + operation.postId() + "\"^^xsd:long .";
                    triplets[10] = forumUri + " <http://www.ldbc.eu/ldbc_socialnet/1.0/vocabulary/containerOf> " + postUri + " .";
                    for (int k = 0; k < operation.tagIds().size(); k++)
                        triplets[11 + k] = postUri + " <http://www.ldbc.eu/ldbc_socialnet/1.0/vocabulary/hasTag> <" + state.tagUri(operation.tagIds().get(k)) + "> .";
                    cs.setArray(1, conn.createArrayOf("varchar", triplets));
                    cs.execute();
                } else {
                    String triplets[] = new String[9];
                    triplets[0] = postUri + " a <http://www.ldbc.eu/ldbc_socialnet/1.0/vocabulary/Post> .";
                    triplets[1] = postUri + " <http://www.ldbc.eu/ldbc_socialnet/1.0/vocabulary/locationIP> \"" + operation.locationIp() + "\" .";
                    triplets[2] = postUri + " <http://www.ldbc.eu/ldbc_socialnet/1.0/vocabulary/creationDate> \"" + df1.format(operation.creationDate()) + "\"^^xsd:dateTime .";
                    triplets[3] = postUri + " <http://www.ldbc.eu/ldbc_socialnet/1.0/vocabulary/browserUsed> \"" + operation.browserUsed() + "\" .";
                    triplets[4] = postUri + " <http://www.ldbc.eu/ldbc_socialnet/1.0/vocabulary/imageFile> \"" + operation.imageFile() + "\" .";
                    triplets[5] = postUri + " <http://www.ldbc.eu/ldbc_socialnet/1.0/vocabulary/hasCreator> " + authorUri + " .";
                    triplets[6] = postUri + " <http://www.ldbc.eu/ldbc_socialnet/1.0/vocabulary/isLocatedIn> <" + state.placeUri(operation.countryId()) + "> .";
                    triplets[7] = postUri + " <http://www.ldbc.eu/ldbc_socialnet/1.0/vocabulary/id> \"" + operation.postId() + "\"^^xsd:long .";
                    triplets[8] = forumUri + " <http://www.ldbc.eu/ldbc_socialnet/1.0/vocabulary/containerOf> " + postUri + " .";
                    cs.setArray(1, conn.createArrayOf("varchar", triplets));
                    cs.execute();
                }
                cs.close();
                conn.close();
            } catch (Throwable e) {
                e.printStackTrace();
            }
            resultReporter.report(0, LdbcNoResult.INSTANCE, operation);
        }
    }

    public static class LdbcUpdate7AddCommentToDgraph implements OperationHandler<LdbcUpdate7AddComment, DgraphDbConnectionState> {


        public void executeOperation(LdbcUpdate7AddComment operation, DgraphDbConnectionState state, ResultReporter resultReporter) throws DbException {
            DgraphClient conn = state.getConn();
            CallableStatement cs = null;
            try {
                if (state.isPrintNames())
                    System.out.println("########### LdbcUpdate7");
                if (state.isPrintStrings()) {
                    System.out.println("################################################ LdbcUpdate7AddComment");
                    System.out.println(operation.commentId());
                    System.out.println(operation.creationDate() + " " + operation.locationIp());
                    System.out.println(operation.browserUsed());
                    System.out.println(operation.content());
                    System.out.println(operation.length() + " " + operation.authorPersonId());
                    System.out.println(operation.countryId());
                    System.out.println(operation.replyToPostId() + " " + operation.replyToCommentId());
                    System.out.println("[");
                    for (long tag : operation.tagIds()) {
                        System.out.println(tag);
                    }
                    System.out.println("]");
                }
                String queryString = "LdbcUpdate7AddComment(?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)";
                cs = conn.prepareCall(queryString);
                cs.setLong(1, operation.commentId());
                DateFormat df = new SimpleDateFormat("yyyy.MM.dd HH:mm:ss.SSS'+00:00'");
                df.setTimeZone(TimeZone.getTimeZone("GMT"));
                cs.setString(2, df.format(operation.creationDate()));
                cs.setString(3, operation.locationIp());
                cs.setString(4, operation.browserUsed());
                cs.setString(5, new String(operation.content().getBytes("UTF-8"), "ISO-8859-1"));
                cs.setInt(6, operation.length());
                cs.setLong(7, operation.authorPersonId());
                cs.setLong(8, operation.countryId());
                cs.setLong(9, operation.replyToPostId());
                cs.setLong(10, operation.replyToCommentId());
                Long tagIds1[] = new Long[operation.tagIds().size()];
                int i = 0;
                for (long temp : operation.tagIds()) {
                    tagIds1[i++] = temp;
                }
                cs.setArray(11, conn.createArrayOf("int", tagIds1));
                cs.execute();
                cs.close();
                conn.close();
            } catch (Throwable e) {
                e.printStackTrace();
                try {
                    cs.close();
                    conn.close();
                } catch (SQLException e1) {
                }
            }
            resultReporter.report(0, LdbcNoResult.INSTANCE, operation);
        }
    }

    public static class LdbcUpdate7AddCommentToDgraphSparql implements OperationHandler<LdbcUpdate7AddComment, DgraphDbConnectionState> {


        public void executeOperation(LdbcUpdate7AddComment operation, DgraphDbConnectionState state, ResultReporter resultReporter) throws DbException {
            try {
                DgraphClient conn = state.getConn();
                if (state.isPrintNames())
                    System.out.println("########### LdbcUpdate7");
                if (state.isPrintStrings()) {
                    System.out.println("################################################ LdbcUpdate7AddComment");
                    System.out.println(operation.commentId());
                    System.out.println(operation.creationDate() + " " + operation.locationIp());
                    System.out.println(operation.browserUsed());
                    System.out.println(operation.content());
                    System.out.println(operation.length() + " " + operation.authorPersonId());
                    System.out.println(operation.countryId());
                    System.out.println(operation.replyToPostId() + " " + operation.replyToCommentId());
                    System.out.println("[");
                    for (long tag : operation.tagIds()) {
                        System.out.println(tag);
                    }
                    System.out.println("]");
                }
                //TODO: This has to be done with and WITHOUT stored procedure
                String queryString = "LdbcUpdateSparql(?)";
                PreparedStatement cs = conn.prepareStatement(queryString);
                String commentUri = "<http://www.ldbc.eu/ldbc_socialnet/1.0/data/comm" + String.format("%020d", operation.commentId()) + ">";
                String authorUri = "<http://www.ldbc.eu/ldbc_socialnet/1.0/data/pers" + String.format("%020d", operation.authorPersonId()) + ">";
                String postUri = null;
                DateFormat df1 = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSS'+00:00'");
                df1.setTimeZone(TimeZone.getTimeZone("GMT"));
                String triplets[] = new String[10 + operation.tagIds().size()];
                triplets[0] = commentUri + " a <http://www.ldbc.eu/ldbc_socialnet/1.0/vocabulary/Comment> .";
                triplets[1] = commentUri + " <http://www.ldbc.eu/ldbc_socialnet/1.0/vocabulary/locationIP> \"" + operation.locationIp() + "\" .";
                triplets[2] = commentUri + " <http://www.ldbc.eu/ldbc_socialnet/1.0/vocabulary/creationDate> \"" + df1.format(operation.creationDate()) + "\"^^xsd:dateTime .";
                triplets[3] = commentUri + " <http://www.ldbc.eu/ldbc_socialnet/1.0/vocabulary/browserUsed> \"" + operation.browserUsed() + "\" .";
                triplets[4] = commentUri + " <http://www.ldbc.eu/ldbc_socialnet/1.0/vocabulary/content> \"" + new String(operation.content().getBytes("UTF-8"), "ISO-8859-1") + "\" .";
                triplets[5] = commentUri + " <http://www.ldbc.eu/ldbc_socialnet/1.0/vocabulary/length> \"" + operation.length() + "\" .";
                triplets[6] = commentUri + " <http://www.ldbc.eu/ldbc_socialnet/1.0/vocabulary/hasCreator> " + authorUri + " .";
                triplets[7] = commentUri + " <http://www.ldbc.eu/ldbc_socialnet/1.0/vocabulary/isLocatedIn> <" + state.placeUri(operation.countryId()) + "> .";
                if (operation.replyToPostId() == -1)
                    postUri = "<http://www.ldbc.eu/ldbc_socialnet/1.0/data/comm" + String.format("%020d", operation.replyToCommentId()) + ">";
                else
                    postUri = "<http://www.ldbc.eu/ldbc_socialnet/1.0/data/post" + String.format("%020d", operation.replyToPostId()) + ">";
                triplets[8] = commentUri + " <http://www.ldbc.eu/ldbc_socialnet/1.0/vocabulary/replyOf> " + postUri + " .";
                triplets[9] = commentUri + " <http://www.ldbc.eu/ldbc_socialnet/1.0/vocabulary/id> \"" + operation.commentId() + "\"^^xsd:long .";
                for (int k = 0; k < operation.tagIds().size(); k++)
                    triplets[10 + k] = commentUri + " <http://www.ldbc.eu/ldbc_socialnet/1.0/vocabulary/hasTag> <" + state.tagUri(operation.tagIds().get(k)) + "> .";
                cs.setArray(1, conn.createArrayOf("varchar", triplets));
                cs.execute();
                cs.close();
                conn.close();
            } catch (Throwable e) {
                e.printStackTrace();
            }
            resultReporter.report(0, LdbcNoResult.INSTANCE, operation);
        }
    }

    public static class LdbcUpdate8AddFriendshipToDgraph implements OperationHandler<LdbcUpdate8AddFriendship, DgraphDbConnectionState> {


        public void executeOperation(LdbcUpdate8AddFriendship operation, DgraphDbConnectionState state, ResultReporter resultReporter) throws DbException {
            DgraphClient conn = state.getConn();
            CallableStatement cs = null;
            try {
                if (state.isPrintNames())
                    System.out.println("########### LdbcUpdate8");
                if (state.isPrintStrings())
                    System.out.println(operation.person1Id() + " " + operation.person2Id() + " " + operation.creationDate());
                String queryString = "{call LdbcUpdate8AddFriendship(?, ?, ?)}";
                cs = conn.prepareCall(queryString);
                cs.setLong(1, operation.person1Id());
                cs.setLong(2, operation.person2Id());
                DateFormat df = new SimpleDateFormat("yyyy.MM.dd HH:mm:ss.SSS'+00:00'");
                df.setTimeZone(TimeZone.getTimeZone("GMT"));
                cs.setString(3, df.format(operation.creationDate()));
                cs.execute();
                cs.close();
                conn.close();
            } catch (Throwable e) {
                e.printStackTrace();
                try {
                    cs.close();
                    conn.close();
                } catch (SQLException e1) {
                }
            }
            resultReporter.report(0, LdbcNoResult.INSTANCE, operation);
        }
    }

    public static class LdbcUpdate8AddFriendshipToDgraphSparql implements OperationHandler<LdbcUpdate8AddFriendship, DgraphDbConnectionState> {


        public void executeOperation(LdbcUpdate8AddFriendship operation, DgraphDbConnectionState state, ResultReporter resultReporter) throws DbException {
            try {
                DgraphClient conn = state.getConn();
                if (state.isPrintNames())
                    System.out.println("########### LdbcUpdate8");
                if (state.isPrintStrings())
                    System.out.println(operation.person1Id() + " " + operation.person2Id() + " " + operation.creationDate());
                //TODO: This has to be done with and WITHOUT stored procedure
                String queryString = "LdbcUpdateSparql(?)";
                PreparedStatement cs = conn.prepareStatement(queryString);
                String person1Uri = "<http://www.ldbc.eu/ldbc_socialnet/1.0/data/pers" + String.format("%020d", operation.person1Id()) + ">";
                String person2Uri = "<http://www.ldbc.eu/ldbc_socialnet/1.0/data/pers" + String.format("%020d", operation.person2Id()) + ">";
                DateFormat df1 = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSS'+00:00'");
                df1.setTimeZone(TimeZone.getTimeZone("GMT"));
                String triplets[] = new String[4];
                triplets[0] = person1Uri + " <http://www.ldbc.eu/ldbc_socialnet/1.0/vocabulary/knows> [ <http://www.ldbc.eu/ldbc_socialnet/1.0/vocabulary/hasPerson> " + person2Uri + "; <http://www.ldbc.eu/ldbc_socialnet/1.0/vocabulary/creationDate> \"" + df1.format(operation.creationDate()) + "\"^^xsd:dateTime ] .";
                triplets[1] = person2Uri + " <http://www.ldbc.eu/ldbc_socialnet/1.0/vocabulary/knows> [ <http://www.ldbc.eu/ldbc_socialnet/1.0/vocabulary/hasPerson> " + person1Uri + "; <http://www.ldbc.eu/ldbc_socialnet/1.0/vocabulary/creationDate> \"" + df1.format(operation.creationDate()) + "\"^^xsd:dateTime ] .";
                triplets[2] = person1Uri + " <http://www.ldbc.eu/ldbc_socialnet/1.0/vocabulary/knows> " + person2Uri + " .";
                triplets[3] = person2Uri + " <http://www.ldbc.eu/ldbc_socialnet/1.0/vocabulary/knows> " + person1Uri + " .";
                cs.setArray(1, conn.createArrayOf("varchar", triplets));
                cs.execute();
                cs.close();
                conn.close();
            } catch (Throwable e) {
                e.printStackTrace();
            }
            resultReporter.report(0, LdbcNoResult.INSTANCE, operation);
        }
    }
}

