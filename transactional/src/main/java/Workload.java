import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import model.Question;
import model.User;

import java.sql.*;
import java.util.*;

import static java.util.Map.entry;

public class Workload {
        private final Random rand;
        private final Connection conn;
        private final ObjectMapper mapper;
        private PreparedStatement addQuestion, addQuestionTag, addAnswer, updateQuestionActivity, addVote,
            getQuestionInfo, getPostPoints, getUserInfo, getUserBadges, search, latestQuestionsByTag;
        // considered ids for this client
        private final Map<String, List<Long>> ids;
        // to simplify, the list of search terms is static. however, the query must consider any other term
        private final List<String> searchTerms = List.of(
            "java", "html", "react", "malloc", "windows", "mac", "linux", "postgres", "font-size", "gcc",
                "socket", "pipe", "lock", "printf", "fork", "exec", "bash", "curl", "monad", "nullpointerexception",
                "overflow", "hashing", "bitmap", "lambda", "linq", "json", "streaming", "ssl", "pdf", "graphs"
        );
        // weights for each transaction type
        private final List<Map.Entry<TransactionType, Integer>> transactionWeights = List.of(
            entry(TransactionType.NewQuestion, 10),
            entry(TransactionType.NewAnswer, 10),
            entry(TransactionType.Vote, 25),
            entry(TransactionType.QuestionInfo, 15),
            entry(TransactionType.PostPoints, 5),
            entry(TransactionType.UserProfile, 20),
            entry(TransactionType.Search, 5),
            entry(TransactionType.LatestByTag, 10)
        );
        private final int totalWeight = transactionWeights.stream().mapToInt(Map.Entry::getValue).sum();


    public Workload(Connection c) throws Exception {
        rand = new Random();
        mapper = new ObjectMapper();
        conn = c;
        conn.setAutoCommit(false); // autocommit = off to execute operations inside a transaction
        ids = Map.of(
            "users", new ArrayList<>(),
            "questions", new ArrayList<>(),
            "tags", new ArrayList<>(),
            "answers", new ArrayList<>()
        );
        prepareStatements();
        populateIds();
    }


    /**
     *  Prepares the statements used in this workload
     */
    private void prepareStatements() throws SQLException {
        addQuestion = conn.prepareStatement("""
            insert into questions (id, owneruserid, title, creationdate, lastactivitydate, body, contentlicense)
            values (default, ?, ?, now(), now(), ?, 'CC BY-SA 4.0')
            returning id
        """);

        addQuestionTag = conn.prepareStatement("""
            insert into questionstags (questionid, tagid) values (?, ?)
        """);

        addAnswer = conn.prepareStatement("""
            insert into answers (id, parentid, owneruserid, creationdate, lastactivitydate, body, contentlicense)
            values (default, ?, ?, now(), now(), ?, 'CC BY-SA 4.0')
        """);

        updateQuestionActivity = conn.prepareStatement("""
            update questions set lastactivitydate = now() where id = ?
        """);

        addVote = conn.prepareStatement("""
            insert into votes (id, postid, votetypeid, creationdate)
            values (default, ?, ?, now());
        """);

        getQuestionInfo = conn.prepareStatement("""
            select title, body, creationdate, owneruserid, acceptedanswerid, tag_list, answers_list, links_list
            from questions q
            -- tags
            left join (
                select qt.questionid, array_agg(tagname) as tag_list
                from tags t
                join questionstags qt on qt.tagid = t.id
                group by qt.questionid
            ) t on t.questionid = q.id
            -- answers
            left join (
                select a.parentid, json_agg(json_build_object('user', a.owneruserid, 'body', a.body)) as answers_list
                from answers a
                group by a.parentid
            ) a on a.parentid = q.id
            -- links
            left join (
                select ql.questionid, json_agg(json_build_object('question', ql.relatedquestionid, 'type', ql.linktypeid)) as links_list
                from questionslinks ql
                group by ql.questionid
            ) ql on ql.questionid = q.id
            where q.id = ?;
        """);

        getPostPoints = conn.prepareStatement("""
            select sum(case when vt.name = 'UpMod' then 1 when vt.name = 'DownMod' then -1 else 0 end)
            from votes v
            join votestypes vt on vt.id = v.votetypeid
            where v.postid = ?
        """);

        getUserInfo = conn.prepareStatement("""
            select displayname, creationdate, aboutme, websiteurl, location, reputation
            from users
            where id = ?
        """);

        getUserBadges = conn.prepareStatement("""
            select array_agg(distinct name)
            from badges
            where userid = ?
        """);

        search = conn.prepareStatement("""
            select id, title
            from questions
            where to_tsvector('english', title) @@ to_tsquery('english', ?)
            limit 25
        """);

        latestQuestionsByTag = conn.prepareStatement("""
            select id, title
            from questions q
            join questionstags qt on qt.questionid = q.id
            where qt.tagid = ?
            order by q.creationdate desc
            limit 25
        """);
    }


    /**
     * Populates each entity with at most 100k random ids from the database
     */
    private void populateIds() throws SQLException {
        Statement s = this.conn.createStatement();

        for (String table: ids.keySet()) {
            ResultSet rs;

            if (table.equals("tags")) {
                rs = s.executeQuery("select tagid from questionstags group by 1 having count(*) > 1000 order by random() limit 100000");
            }
            else {
                rs = s.executeQuery("select id from " + table + " order by random() limit 100000");
            }

            while (rs.next()) {
                ids.get(table).add(rs.getLong(1));
            }
        }
    }


    /**
     * Creates a new question
     */
    private void newQuestion(long user, String title, String body, long tag) throws SQLException {
        addQuestion.setLong(1, user);
        addQuestion.setString(2, title);
        addQuestion.setString(3, body);
        var rs = addQuestion.executeQuery();
        rs.next();
        long questionId = rs.getLong(1);

        addQuestionTag.setLong(1, questionId);
        addQuestionTag.setLong(2, tag);
        addQuestionTag.execute();

        conn.commit();
    }


    /**
     * Creates a new answer
     */
    private void newAnswer(long question, long user, String body) throws SQLException {
        addAnswer.setLong(1, question);
        addAnswer.setLong(2, user);
        addAnswer.setString(3, body);
        addAnswer.execute();

        updateQuestionActivity.setLong(1, question);
        updateQuestionActivity.execute();

        conn.commit();
    }


    /**
     * Votes on a post
     */
    private void vote(long post, int type) throws SQLException {
        addVote.setLong(1, post);
        addVote.setInt(2, type);
        addVote.execute();
        conn.commit();
    }


    /**
     * Retrieves a question's general information
     */
    private Question questionInfo(long question) throws SQLException, JsonProcessingException {
        getQuestionInfo.setLong(1, question);
        var rs = getQuestionInfo.executeQuery();
        rs.next();

        var q = new Question();
        q.title = rs.getString(1);
        q.body = rs.getString(2);
        q.creationDate = rs.getTimestamp(3).toLocalDateTime();
        q.user = rs.getLong(4);
        q.acceptedAnswerId = rs.getLong(5);

        var tags = rs.getArray(6);
        if (tags != null) {
            q.tagList = Arrays.asList((String[]) tags.getArray());
        }

        var json = rs.getString(7);
        if (json != null) {
            var mapper = new ObjectMapper();
            q.answersList = mapper.readValue(json, List.class);
        }

        json = rs.getString(8);
        if (json != null) {
            var mapper = new ObjectMapper();
            q.linksList = mapper.readValue(json, List.class);
        }

        conn.commit();
        return q;
    }


    /**
     * Retrieves the points for some post. 'UpMod' counts as +1; 'DownMod' counts as -1; every other vote type is ignored
     */
    private int postPoints(long post) throws SQLException {
        getPostPoints.setLong(1, post);
        var rs = getPostPoints.executeQuery();
        rs.next();
        return rs.getInt(1);
    }


    /**
     * Retrieves a user's profile
     */
    private User userProfile(long user) throws SQLException {
        getUserInfo.setLong(1, user);
        var rs = getUserInfo.executeQuery();
        rs.next();

        var u = new User();
        u.displayName = rs.getString(1);
        u.creationDate = rs.getTimestamp(2).toLocalDateTime();
        u.aboutMe = rs.getString(3);
        u.websiteUrl = rs.getString(4);
        u.location = rs.getString(5);
        u.reputation = rs.getInt(6);

        getUserBadges.setLong(1, user);
        rs = getUserBadges.executeQuery();
        rs.next();
        var badges = rs.getArray(1);
        if (badges != null) {
            u.badges = Arrays.asList((String[]) badges.getArray());
        }

        conn.commit();
        return u;
    }


    /**
     * Searches questions by title
     */
    private Map<Long, String> searchQuestions(String query) throws SQLException {
        search.setString(1, query);
        var rs = search.executeQuery();

        var result = new HashMap<Long, String>();
        while (rs.next()) {
            result.put(rs.getLong(1), rs.getString(2));
        }

        conn.commit();
        return result;
    }


    /**
     * Retrieves the latest questions by tag
     */
    private Map<Long, String> latestByTag(long tag) throws SQLException {
        latestQuestionsByTag.setLong(1, tag);
        var rs = latestQuestionsByTag.executeQuery();

        var result = new HashMap<Long, String>();
        while (rs.next()) {
            result.put(rs.getLong(1), rs.getString(2));
        }

        conn.commit();
        return result;
    }


    /**
     * Randomly selects a TransactionType, based on the weights in this.transactionWeights
     */
    private TransactionType getRandomTransaction() throws NullPointerException {
        int r = rand.nextInt(0, totalWeight);
        int curr = 0;

        for (var entry: transactionWeights) {
            if (r < entry.getValue() + curr) {
                return entry.getKey();
            }
            curr += entry.getValue();
        }

        throw new NullPointerException();
    }


    /**
     * Executes a random transaction
     */
    public TransactionType transaction() throws Exception {
        var type = getRandomTransaction();

        switch (type) {
            case NewQuestion -> {
                var user = Utils.randomElement(ids.get("users"));
                var title = Utils.randomString(50);
                var body = Utils.randomString(1500);
                var tag = Utils.randomElement(ids.get("tags"));
                newQuestion(user, title, body, tag);
            }
            case NewAnswer -> {
                var question = Utils.randomElement(ids.get("questions"));
                var user = Utils.randomElement(ids.get("users"));
                var body = Utils.randomString(1500);
                newAnswer(question, user, body);
            }
            case Vote -> {
                var post = Utils.randomElement(ids.get("questions"), ids.get("answers"));
                // 2 - UpMod, 3 - DownMod
                var voteType = rand.nextInt(100) < 90 ? 2 : 3;
                vote(post, voteType);
            }
            case QuestionInfo -> {
                var question = Utils.randomElement(ids.get("questions"));
                var q = questionInfo(question);
            }
            case PostPoints -> {
                var post = Utils.randomElement(ids.get("questions"), ids.get("answers"));
                var p = postPoints(post);
            }
            case UserProfile -> {
                var user = Utils.randomElement(ids.get("users"));
                var u = userProfile(user);
            }
            case Search -> {
                var query = Utils.randomElement(searchTerms);
                var s = searchQuestions(query);
            }
            case LatestByTag -> {
                var tag = Utils.randomElement(ids.get("tags"));
                var l = latestByTag(tag);
            }
        }

        return type;
    }
}
