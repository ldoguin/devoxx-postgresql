package fr.ybonnel;


import com.couchbase.client.deps.com.fasterxml.jackson.databind.ObjectMapper;
import com.couchbase.client.java.Bucket;
import com.couchbase.client.java.CouchbaseCluster;
import com.couchbase.client.java.document.JsonDocument;
import com.couchbase.client.java.document.json.JsonObject;
import twitter4j.*;

import java.sql.SQLException;
import java.text.SimpleDateFormat;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

public class DevoxxPostgresMain {

    private static SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ssZ");

    private static AtomicInteger counter = new AtomicInteger(0);

    public static void main(String[] args) throws TwitterException, SQLException, ClassNotFoundException {
        CouchbaseCluster cc = CouchbaseCluster.create();
        Bucket bucket = cc.openBucket("tweets_devoxx");
        Twitter twitter = TwitterFactory.getSingleton();
        Query query = new Query("devoxxfr");
        query.setCount(100);

        QueryResult result;
        do {
            result = twitter.search(query);
            List<Status> tweets = result.getTweets();
            tweets.forEach((tweet) -> DevoxxPostgresMain.processTweet(tweet, bucket));
        } while ((query = result.nextQuery()) != null);

        bucket.close();
    }

    private static void processTweet(Status tweet, Bucket bucket) {
        ObjectMapper om = new ObjectMapper();
        String id = String.valueOf(tweet.getId());
        String handle = tweet.getUser().getScreenName();

        try {
            String json = om.writer().writeValueAsString(tweet);

            JsonObject obj = JsonObject.fromJson(json);
            obj.put("type", "tweet");
            obj.put("handle", handle);

//            deleteStatement.setLong(1, id);
//            int nb = deleteStatement.executeUpdate();
            Boolean exists = bucket.exists(id);

//            preparedStatement.setLong(1, id);
//            preparedStatement.setString(2, handle);
//            preparedStatement.setString(3, json);
//            preparedStatement.executeUpdate();
            JsonDocument jsonDocument = JsonDocument.create(id, obj);
            bucket.upsert(jsonDocument);

            if (!exists) {
                System.out.println(counter.incrementAndGet() + " - " + sdf.format(tweet.getCreatedAt()) + " : @" + tweet.getUser().getScreenName() + "(" + tweet.getUser().getName() + "):" + tweet.getText());
            } else {
                System.out.println(counter.incrementAndGet() + " - OLD TWEET - " + sdf.format(tweet.getCreatedAt()) + " : @" + tweet.getUser().getScreenName() + "(" + tweet.getUser().getName() + "):" + tweet.getText());
            }

        }
        catch (Exception e){
            e.printStackTrace();
            throw new RuntimeException(e);
        }
    }
}
