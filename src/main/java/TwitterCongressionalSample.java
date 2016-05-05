import twitter4j.Status;

import java.util.List;

/**
 * Created by nikola on 5.5.16.
 */
public class TwitterCongressionalSample extends CongressionalSample<Status, String> {

    public TwitterCongressionalSample(Integer sampleSize, List<String> groupList) {
        super(sampleSize, groupList);
    }

    @Override
    public String get(Status item, String group) {
        switch (group) {
            case "retweet":
                return item.isRetweet()? "true" : "false";
            case "language":
                return item.getLang();
            default:
                return null;
        }
    }
}
