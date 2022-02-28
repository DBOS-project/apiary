CREATE TABLE RetwisPosts (
    UserID INTEGER NOT NULL,
    PostID INTEGER NOT NULL,
    Timestamp INTEGER NOT NULL,
    Post VARCHAR(1000) NOT NULL,
    PRIMARY KEY(UserID, PostID)
);
PARTITION TABLE RetwisPosts ON COLUMN UserID;
CREATE INDEX RetwisPostsIndex ON RetwisPosts (UserID);

CREATE TABLE RetwisFollowees (
     UserID INTEGER NOT NULL,
     FolloweeID INTEGER NOT NULL,
     PRIMARY KEY(UserID, FolloweeID)
);
PARTITION TABLE RetwisFollowees ON COLUMN UserID;
CREATE INDEX RetwisFolloweesIndex ON RetwisFollowees (UserID);
