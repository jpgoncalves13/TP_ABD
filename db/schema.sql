DROP TABLE IF EXISTS Badges;
DROP TABLE IF EXISTS Comments;
DROP TABLE IF EXISTS QuestionsLinks;
DROP TABLE IF EXISTS Questions;
DROP TABLE IF EXISTS QuestionsTags;
DROP TABLE IF EXISTS Answers;
DROP TABLE IF EXISTS Tags;
DROP TABLE IF EXISTS Users;
DROP TABLE IF EXISTS Votes;
DROP TABLE IF EXISTS VotesTypes;

CREATE TABLE Badges (
    UserId bigint,
    Name varchar,
    Date timestamp NOT NULL,
    Class smallint NOT NULL,
    TagBased boolean NOT NULL,
    Count int NOT NULL,
    PRIMARY KEY (UserId, Name, Count)
);

CREATE TABLE Comments (
    Id serial PRIMARY KEY,
    PostId bigint NOT NULL,
    UserId bigint,
    CreationDate timestamp NOT NULL,
    Text text,
    ContentLicense varchar NOT NULL
);

CREATE TABLE Questions (
    Id serial PRIMARY KEY,
    OwnerUserId bigint,
    LastEditorUserId bigint,
    AcceptedAnswerId bigint,
    Title text NOT NULL,
    CreationDate timestamp NOT NULL,
    ClosedDate timestamp,
    LastActivityDate timestamp NOT NULL,
    LastEditDate timestamp,
    Body text NOT NULL,
    ContentLicense varchar NOT NULL
);

CREATE TABLE QuestionsTags (
    QuestionId bigint,
    TagId bigint,
    PRIMARY KEY (QuestionId, TagId)
);

CREATE TABLE QuestionsLinks (
    QuestionId bigint,
    RelatedQuestionId bigint,
    LinkTypeId smallint,
    CreationDate timestamp NOT NULL,
    PRIMARY KEY (QuestionId, RelatedQuestionId, LinkTypeId)
);

CREATE TABLE Answers (
    Id serial PRIMARY KEY,
    ParentId bigint NOT NULL,
    OwnerUserId bigint,
    LastEditorUserId bigint,
    CreationDate timestamp NOT NULL,
    LastActivityDate timestamp NOT NULL,
    LastEditDate timestamp,
    Body text NOT NULL,
    ContentLicense varchar NOT NULL
);

CREATE TABLE Tags (
    Id serial PRIMARY KEY,
    TagName varchar NOT NULL,
    ExcerptPostId bigint,
    WikiPostId bigint
);

CREATE TABLE Users (
    Id serial PRIMARY KEY,
    AccountId bigint NOT NULL,
    DisplayName text NOT NULL,
    CreationDate timestamp NOT NULL,
    AboutMe text,
    WebsiteUrl varchar,
    Location varchar,
    LastAccessDate timestamp NOT NULL,
    Reputation int NOT NULL
);

CREATE TABLE Votes (
    Id serial PRIMARY KEY,
    PostId bigint NOT NULL,
    VoteTypeId smallint NOT NULL,
    CreationDate timestamp NOT NULL
);

CREATE TABLE VotesTypes (
    Id smallint PRIMARY KEY,
    Name varchar NOT NULL
);
