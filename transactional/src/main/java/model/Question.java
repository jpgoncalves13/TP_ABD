package model;

import java.time.LocalDateTime;
import java.util.List;
import java.util.Map;

public class Question {
    public String title;
    public String body;
    public LocalDateTime creationDate;
    public long user;
    public long acceptedAnswerId;
    public List<String> tagList;
    public List<Map<String, Object>> answersList;
    public List<Map<String, Object>> linksList;

    @Override
    public String toString() {
        return "Question{" +
                "title='" + title + '\'' +
                ", body='" + body + '\'' +
                ", creationDate=" + creationDate +
                ", user=" + user +
                ", acceptedAnswerId=" + acceptedAnswerId +
                ", tagList=" + tagList +
                ", answersList=" + answersList +
                ", linksList=" + linksList +
                '}';
    }
}
