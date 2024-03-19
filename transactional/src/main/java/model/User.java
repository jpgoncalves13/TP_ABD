package model;

import java.time.LocalDateTime;
import java.util.List;

public class User {
    public String displayName;
    public LocalDateTime creationDate;
    public String aboutMe;
    public String websiteUrl;
    public String location;
    public int reputation;
    public List<String> badges;


    @Override
    public String toString() {
        return "User{" +
                "displayName='" + displayName + '\'' +
                ", creationDate=" + creationDate +
                ", aboutMe='" + aboutMe + '\'' +
                ", websiteUrl='" + websiteUrl + '\'' +
                ", location='" + location + '\'' +
                ", reputation=" + reputation +
                ", badges=" + badges +
                '}';
    }
}
