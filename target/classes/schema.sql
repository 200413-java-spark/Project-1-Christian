CREATE TABLE pokemon(
        ID serial primary key,
        P_Name varchar,
        P_Type varchar
);

CREATE TABLE strongest(
        ID serial primary key,
        P_Name varchar,
        TotalStat int
);