(latID as text, longID as text, yearID as text, monthID as text, dateID as text) => 
let
    Kilde = Json.Document(Web.Contents("https://api01.nve.no/hydrology/forecast/avalanche/v5.0.1/api/AvalancheWarningByCoordinates/Simple/"&latID&"/"&longID&"/1/"&yearID&"-"&monthID&"-"&dateID&"/"&yearID&"-"&monthID&"-"&dateID&""))
in
    Kilde