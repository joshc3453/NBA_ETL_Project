DROP TABLE IF EXISTS team_standings CASCADE;
CREATE TABLE team_standings (
    -- team_id INTEGER PRIMARY KEY, -- When I add the teams table, team_id will be the primary key
    team_id INTEGER,
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