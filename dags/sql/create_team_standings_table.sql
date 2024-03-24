DROP TABLE IF EXISTS team_standings;
CREATE TABLE team_standings (
    team_id INTEGER PRIMARY KEY,
    team_name TEXT, 
    conference TEXT, 
    record TEXT, 
    wins INTEGER,
    losses INTEGER, 
    division TEXT, 
    divisionrecord TEXT,
    divisionrank INTEGER, 
    winpct FLOAT, 
    l10 TEXT, 
    ot TEXT,
    currentstreak INTEGER, 
    pointspg FLOAT, 
    opppointspg FLOAT,
    diffpointspg FLOAT
)